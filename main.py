import luigi
import os
import requests
import tarfile
import gzip
import shutil
import pandas as pd
import io

# Задача 1: Скачивание архива 
class DownloadDataset(luigi.Task):
    task_namespace = "pipeline"  # Пространство имен для организации задач
    dataset_name = luigi.Parameter()  # Параметр для имени набора данных
    output_dir = luigi.Parameter()  # Параметр для выходного каталога

    def output(self):
        # Определяем выходной файл (архив .tar)
        return luigi.LocalTarget(os.path.join(self.output_dir, f"{self.dataset_name}.tar"))

    def run(self):
        # Формирование URL для скачивания архива
        url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{self.dataset_name[:-3]}nnn/{self.dataset_name}/suppl/{self.dataset_name}_RAW.tar"
        os.makedirs(self.output_dir, exist_ok=True)  # Создаем выходной каталог, если он не существует
        
        # Скачивание архива по частям
        response = requests.get(url, stream=True)
        with open(self.output().path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)  # Записываем скачанные данные в файл

# Задача 2: Распаковка архива
class ExtractTar(luigi.Task):
    task_namespace = "pipeline"
    input_tar = luigi.Parameter()  # Параметр для входного tar-файла
    output_dir = luigi.Parameter()  # Параметр для выходного каталога

    def requires(self):
        tar_name = os.path.basename(self.input_tar)      
        dataset_name = tar_name.replace(".tar", "")      

        return DownloadDataset(
            dataset_name=dataset_name,
            output_dir=os.path.dirname(self.input_tar)
        )

    def output(self):
        # Определяем выходной каталог для распакованных файлов
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)  # Создаем выходной каталог, если он не существует
        with tarfile.open(self.input().path, "r") as tar:
            tar.extractall(path=self.output_dir)  # Распаковываем архив в указанный каталог

# Задача 3: Распаковка GZIP-файлов 
class DecompressGzipFiles(luigi.Task):
    task_namespace = "pipeline"
    input_dir = luigi.Parameter()  # Параметр для входного каталога

    def requires(self):
        # Определяем зависимость от задачи ExtractTar
        return ExtractTar(
            input_tar=self.input_dir + ".tar",
            output_dir=self.input_dir
        )

    def output(self):
        # Определяем выходной каталог для распакованных GZIP-файлов
        return luigi.LocalTarget(os.path.join(self.input_dir, "decompressed"))

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)  # Создаем выходной каталог, если он не существует
        for root, _, files in os.walk(self.input_dir):  # Проходим по всем файлам в каталоге
            for file in files:
                if file.endswith(".gz"):  # Проверяем на наличие GZIP-файлов
                    file_path = os.path.join(root, file)
                    with gzip.open(file_path, "rb") as f_in:  # Открываем GZIP-файл для чтения
                        output_file = os.path.join(self.output().path, file.replace(".gz", ""))  # Формируем имя выходного файла
                        with open(output_file, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)  # Копируем содержимое из GZIP-файла

# Задача 4: Разделение текстового файла на таблицы 
class SplitTables(luigi.Task):
    task_namespace = "pipeline"
    input_dir = luigi.Parameter()  # Параметр для входного каталога

    def requires(self):
        return DecompressGzipFiles(input_dir=self.input_dir)  

    def output(self):
        # Определяем выходной каталог для таблиц
        return luigi.LocalTarget(os.path.join(self.input_dir, "tables"))

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)  # Создаем выходной каталог, если он не существует
        for root, _, files in os.walk(self.input_dir):  # Проходим по всем файлам в каталоге
            for file in files:
                if file.endswith(".txt"):  # Проверяем на наличие текстовых файлов
                    file_path = os.path.join(root, file)
                    dfs = {}  # Словарь для хранения DataFrame'ов по ключам
                    
                    with open(file_path, "r") as f:
                        write_key = None  # Ключ для записи текущего DataFrame'а
                        fio = io.StringIO()  # Используем StringIO для временного хранения данных

                        for l in f.readlines():
                            if l.startswith("["):  # Если строка начинается с "["
                                if write_key:  
                                    fio.seek(0)
                                    dfs[write_key] = pd.read_csv(fio, sep="\t")  # Читаем текущие данные в DataFrame и сохраняем его по ключу

                                fio = io.StringIO()  # Обнуляем StringIO для новой таблицы
                                write_key = l.strip("[]\n")  # Устанавливаем новый ключ таблицы
                                continue

                            if write_key:  
                                fio.write(l)  # Записываем строку в StringIO

                        fio.seek(0)
                        if write_key:  
                            dfs[write_key] = pd.read_csv(fio, sep="\t")  # Читаем оставшиеся данные в последний DataFrame

                    for key, df in dfs.items():
                        output_file = os.path.join(self.output().path, f"{key}.csv")  
                        df.to_csv(output_file, sep=",", index=False)  # Сохраняем каждый DataFrame как CSV файл

# Задача 5: Удаление ненужных колонок
class TrimColumns(luigi.Task):
    task_namespace = "pipeline"
    input_dir = luigi.Parameter()  # Параметр для входного каталога

    def requires(self):
        return SplitTables(input_dir=self.input_dir)  

    def output(self):
        return luigi.LocalTarget(os.path.join(self.input_dir, "trimmed"))  # Определяем выходной каталог для обрезанных файлов

    def run(self):
        os.makedirs(self.output().path, exist_ok=True)  # Создаем выходной каталог, если он не существует
        
        for root, _, files in os.walk(self.input_dir):  # Проходим по всем файлам в каталоге
            for file in files:
                if file.endswith("Probes.csv"):  # Проверяем на наличие файлов Probes.csv
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path, sep=",")  # Читаем CSV файл в DataFrame
                    
                    columns_to_remove = [
                        "Definition", 
                        "Ontology_Component", 
                        "Ontology_Process", 
                        "Ontology_Function", 
                        "Synonyms", 
                        "Obsolete_Probe_Id", 
                        "Probe_Sequence"
                    ]
                    
                    trimmed_df = df.drop(columns=columns_to_remove, errors="ignore")  # Удаляем ненужные колонки
                    
                    trimmed_df.to_csv(
                        os.path.join(self.output().path, file),
                        sep=",",
                        index=False   # Сохраняем обрезанный DataFrame как CSV файл без индексации
                    )

# Главный пайплайн задач 
class DatasetPipeline(luigi.WrapperTask):
    task_namespace = "pipeline"
    dataset_name = luigi.Parameter()   # Параметр для имени набора данных
    base_dir = luigi.Parameter()   # Параметр для базового каталога

    def requires(self):
        input_dir = os.path.join(self.base_dir, self.dataset_name)  
        return TrimColumns(input_dir=input_dir)  

if __name__ == "__main__":
    luigi.run()   # Запуск пайплайна при выполнении скрипта напрямую.
