"""
3. Raw Data Storage
    •	Store ingested data in a data lake or storage system (e.g., AWS S3, Google Cloud Storage, HDFS, or a local filesystem)
    •	Design an efficient folder/bucket structure:
        o	Partition data by source, type, and timestamp
    •	Deliverables:
        o	Folder/bucket structure documentation
        o	Python code demonstrating the upload of raw data to the storage system
"""

import os
import shutil
import argparse
import logging
from datetime import datetime

import util

job_run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
job_name = "3_raw_data_storage"

logging = util.get_logger(job_name)

LANDING_FOLDER = 'LocalDataLake/Landing'


def upload_file_to_landing(source_path):
    if not os.path.isfile(source_path):
        logging.error("Source file does not exist: %s", source_path)
        return

    # Ensure landing folder exists
    os.makedirs(LANDING_FOLDER, exist_ok=True)

    # Get the filename
    filename = os.path.basename(source_path)
    destination_path = os.path.join(LANDING_FOLDER, filename)

    try:
        shutil.copy2(source_path, destination_path)
        logging.info("File copied to landing folder: %s", destination_path)
    except Exception as e:
        logging.error("Failed to upload file: %s", e)


def main():
    parser = argparse.ArgumentParser(description="upload file to landing folder")
    parser.add_argument("filepath", help="Path to the source file")
    args = parser.parse_args()

    file_path = "./sample_data/loan_info/customer_loan_info_1.csv" if not args.filepath else args.filepath
    upload_file_to_landing(file_path)


if __name__ == "__main__":
    main()
