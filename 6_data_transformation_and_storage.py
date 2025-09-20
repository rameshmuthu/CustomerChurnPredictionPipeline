"""
6. Data Transformation and Storage
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

import os
import argparse
import pandas as pd
from datetime import datetime

import util

job_name = "6_data_transformation_and_storage"
logging = util.get_logger(job_name)

customer_info_silver_path = "LocalDataLake/silver/customer_info"
loan_info_silver_path = "LocalDataLake/silver/loan_info"

customer_loan_info_folder = "LocalDataLake/gold/customer_loan_info/file_arrival="


def apply_min_max_scaling(df, col):
    df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    return df


def data_transformation_and_storage(file_arrival):

    customer_info = util.pd_read_csv_files(f"{customer_info_silver_path}/file_arrival={file_arrival}")
    loan_info = util.pd_read_csv_files(f"{loan_info_silver_path}/file_arrival={file_arrival}")

    # 1. Combining below features into credit_commitment, since these are related information.
    loan_info['credit_commitment'] = loan_info['has_credit'] \
                                     + loan_info['has_housing_loan'] \
                                     + loan_info['has_personal_loan']

    # 2. drop has_credit, has_housing_loan, has_personal_loan
    loan_info = loan_info.drop(columns=['has_credit', 'has_housing_loan', 'has_personal_loan'])

    # 2. Splitting Age into different groups using Equal width partitioning
    customer_info['age_binned'] = pd.cut(customer_info['age'], bins=[10, 25, 35, 45, 55, 65, 75])
    customer_info['age_binned'] = pd.to_numeric(customer_info['age_binned'] \
                                            .apply(lambda x: x.mid if isinstance(x, pd.Interval) else x))

    # 3. splitting avg_yearly_balance into different groups
    loan_info['avg_yearly_balance_binned'] = pd.cut(loan_info['avg_yearly_balance'],
                                                    bins=[-7000, -200, 0, 200, 400, 600, 800, 1200, 2500, 5000, 10281])
    loan_info['avg_yearly_balance_binned'] = pd.to_numeric(loan_info['avg_yearly_balance_binned'] \
                                                           .apply(lambda x: x.mid if isinstance(x, pd.Interval) else x))

    # 4. applying feature scaling on numerical column
    loan_info = apply_min_max_scaling(loan_info, 'contacted_duration_sec')

    # 5. join loan_info and customer_info
    customer_loan_info = pd.merge(customer_info, loan_info, on='customer_id', how='inner')

    # 6. add event_timestamp to be used in feature store
    event_timestamp_for_feature_store = datetime.strptime(file_arrival, "%Y%m%d")
    customer_loan_info["event_timestamp"] = event_timestamp_for_feature_store

    # 7. write the customer_loan_campaign_info to gold layer
    customer_loan_info_folder_this_run = f"{customer_loan_info_folder}{file_arrival}"
    os.makedirs(customer_loan_info_folder_this_run, exist_ok=True)

    customer_loan_info_full_path_parquet = os.path.join(customer_loan_info_folder_this_run, "customer_loan_info.parquet")
    customer_loan_info.to_parquet(customer_loan_info_full_path_parquet, index=False)

    customer_loan_info.to_csv("LocalDataLake/gold/csv_of_last_run/customer_loan_info.csv", index=False)

    logging.info(f"Customer loan info gold file is written to: {customer_loan_info_full_path_parquet}")
    print(customer_loan_info.dtypes)

def main():
    logging.info(f"=== {job_name} started ===")

    parser = argparse.ArgumentParser(description="prepare loan and customer info for gold layer")
    parser.add_argument("file_arrival_time", help="time of file arrival in YYYYMMDD_HHMMSS")
    args = parser.parse_args()

    file_arrival = args.file_arrival_time

    data_transformation_and_storage(file_arrival)

    logging.info(f"=== {job_name} ended ===")


if __name__ == "__main__":
    main()

