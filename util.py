import logging
import os
import pandas as pd
import glob

from datetime import datetime

def get_logger(job_name):
    job_run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    log_folder = f"./logs/{job_name}"
    os.makedirs(log_folder, exist_ok=True)

    logging.basicConfig(
        filename=f'{log_folder}/{job_name}_{job_run_timestamp}.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s')
    return logging


def pd_read_csv_files(root_folder):
    csv_files = glob.glob(f"{root_folder}/*.csv")
    return pd.concat((pd.read_csv(file) for file in csv_files), ignore_index=True)


def pd_read_parquet_files(root_folder):
    parquet_files = glob.glob(f"{root_folder}/*.parquet")
    return pd.concat((pd.read_csv(file) for file in parquet_files), ignore_index=True)