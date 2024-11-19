from utils.mongo_db_utils import PyMongoUtils
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import pandas as pd
import time
from typing import Union, List, Dict
import logging
from utils.exceptions import UploadException


logging.basicConfig(level=logging.INFO, format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M")

def job(file_path:Union[str, Path], catalog_name:str) -> None:
    """ Upload job definision. Csv file will be loaded into MongoDB in chunks

    Args:
        file_path (str, Path): File path or Path objcet to the file
        catalog_name (str): Name of the paret catalog of file
    """
    logging.info(f'Start working on file: {file_path}')
    mongo = PyMongoUtils()
    counter = 1
    try:
        for chunk in pd.read_csv(filepath_or_buffer=file_path, chunksize=1000):
            logging.info(f'Processing chunk no {counter} from file {file_path}')
            mongo.upload_df(chunk, catalog_name, "sensors_db")
            counter += 1
            del chunk
    except (IOError, FileNotFoundError) as ne:
        logging.error(f'Could not open file: {file_path}. Detals {ne}')
    except UploadException as ue:
        logging.error(f'Could not upload file {file_path} to MongoDB. Details {ue}')
    
    logging.info(f'Finished working on file: {file_path}')

def get_files_list() -> Dict[str, List[Path]]:
    """Generates dictionray with list of files.

    Returns:
        Dict[str, List[str]]: dictionary {"<catalog_name>": [Path,Path]}
    """
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
    """Main execution function of script with Trhead logic
    """
    start = time.time()
    logging.info(f'Starting process')
    processing_tasks = []
    processing_files = get_files_list()
    pool = ThreadPoolExecutor(4)
    for catalog_name, data_file_list in processing_files.items():
        for data_file in data_file_list:
            task = pool.submit(job, data_file, catalog_name)
            processing_tasks.append(task)
    processing_tasks[-1].result()
    end = time.time()
    logging.info(f'Finished procees. Execution time: {end - start} seconds')

if __name__ == '__main__':
    execute()