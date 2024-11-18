from utils.mongo_db_utils import PyMongoUtils
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import pandas as pd
import time
from typing import List

def job(file_path:str, catalog_name:str) -> None:
    print(f'Start working on file: {file_path}')
    mongo = PyMongoUtils()
    counter = 1
    for chunk in pd.read_csv(filepath_or_buffer=file_path, chunksize=1000):
        print(f'Processing chunk no {counter} from file {file_path}')
        mongo.upload_df(chunk, catalog_name, "sensors_db")
        counter += 1
    
    print(f'Finished working on file: {file_path}')



def get_files_list() -> dict:
    files_to_process = dict()
    for data_file in Path('data').iterdir():    
        if Path(data_file).is_dir():
            file_list = []
            for csv_file in Path(data_file).iterdir():
                if csv_file.name.endswith('.csv'):
                    file_list.append(csv_file)
            files_to_process[data_file.name] = file_list
    return files_to_process


def execute() -> None:
    start = time.time()
    processing_tasks = []
    processing_files = get_files_list()
    pool = ThreadPoolExecutor(5)
    for catalog_name, data_file_list in processing_files.items():
        for data_file in data_file_list:
            task = pool.submit(job, data_file, catalog_name)
            processing_tasks.append(task)
    processing_tasks[-1].result()
    end = time.time()
    print(end - start)





if __name__ == '__main__':
    execute()