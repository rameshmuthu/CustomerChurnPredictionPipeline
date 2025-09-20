"""
2. Data Ingestion
    •	Perform transformations for feature engineering:
        o	Create aggregated features (e.g., total spend per customer)
        o	Derive new features (e.g., customer tenure, activity frequency)
        o	Scale and normalize features where necessary
    •	Store the transformed data in a relational database or a data warehouse.
    •	Deliverables:
        o	SQL schema design or database setup script
        o	Sample queries to retrieve transformed data
        o	A summary of the transformation logic applied
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime
import argparse

import util

# get logger
# file_arrival_date = datetime.now().strftime('%Y%m%d')
job_name = "2_data_ingestion"
logging = util.get_logger(job_name)

# Define paths
landing_path = "./LocalDataLake/Landing"
sqlite_db_path = "./db/customer_data.db"

logging.info(f"Data ingestion job started. It will ingest Loan Info & Customer Info to Raw layer.")


def data_ingestion(file_arrival_date):
    loan_info_raw_folder = f"./LocalDataLake/Raw/loan_info/file_arrival={file_arrival_date}"
    customer_info_raw_folder = f"./LocalDataLake/Raw/customer_info/file_arrival={file_arrival_date}"

    # Ensure raw folder exists
    os.makedirs(loan_info_raw_folder, exist_ok=True)
    os.makedirs(customer_info_raw_folder, exist_ok=True)

    # 1. Read loan_info CSV
    try:
        loan_df = util.pd_read_csv_files(landing_path)
        logging.info("Loan Info CSV file is loaded successfully.")
    except FileNotFoundError:
        logging.error(f"Loan Info CSV file not found at path: {landing_path}, Please upload.")

    # 2. Read customer_info table from SQLite
    conn = sqlite3.connect(sqlite_db_path)
    try:
        customer_df = pd.read_sql_query("SELECT * FROM customer_info", conn)
        logging.info("Customer Info is successfully loaded from database.")
    except Exception as e:
        logging.error(f"An error occurred while loading Customer Info from database: {e}")
    conn.close()

    # 3. Write both DataFrames to CSV
    loan_df.to_csv(os.path.join(loan_info_raw_folder, "loan_info.csv"), index=False)
    customer_df.to_csv(os.path.join(customer_info_raw_folder, "customer_info.csv"), index=False)

    logging.info("Data Ingestion job is successfully completed. Files written to raw folder in Parquet format.")


def main():
    logging.info(f"=== {job_name} started ===")

    parser = argparse.ArgumentParser(description="Model customer churn prediction")
    parser.add_argument("file_arrival_time", help="time of file arrival in YYYYMMDD_HHMMSS")
    args = parser.parse_args()

    file_arrival = args.file_arrival_time

    logging.info(f"file_arrival : {file_arrival}")

    data_ingestion(file_arrival)

    logging.info(f"=== {job_name} ended ===")


if __name__ == "__main__":
    main()
