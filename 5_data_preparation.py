"""
5. Data Preparation
    •	Clean and preprocess the raw data:
        o	Handle missing values (e.g., imputation or removal)
        o	Standardize or normalize numerical attributes
        o	Encode categorical variables using one-hot encoding or label encoding
    •	Perform EDA to identify trends, distributions, and outliers.
    •	Deliverables:
        o	Jupyter notebook/Python script showcasing the data preparation process
        o	Visualizations and summary statistics (e.g., histograms, box plots)
        o	A clean dataset ready for transformations
"""

import os
import argparse

import util

job_name = "5_data_preparation"
logging = util.get_logger(job_name)

customer_info_raw_path = "LocalDataLake/Raw/customer_info"
loan_info_raw_path = "LocalDataLake/Raw/loan_info"

loan_info_clean_folder = "LocalDataLake/silver/loan_info/file_arrival="
customer_info_clean_folder = "LocalDataLake/silver/customer_info/file_arrival="


def get_LF_UF_3STD(df, col):
    mean = df[col].mean()
    std = df[col].std()
    LF = mean - 3 * std
    UF = mean + 3 * std
    return LF, UF


def remove_outliers(df, col):
    """
        Filters outliers in a column using 3 Standard Deviation method
        :param df:  input dataframe
        :param col: column name on which outliers to be removed
        :return : input data frame with outliers removed
    """
    LF, UF = get_LF_UF_3STD(df, col)
    records_before_cleaning = len(df[col])
    df = df[(df[col] >= LF) & (df[col] <= UF)]

    logging.info("-" * 5 + " Removing outliers for {col.upper()} " + "-" * 5)
    logging.info(f"Min-Max {df[col].min()}-{df[col].max()}")
    logging.info(f"Lower_Fence-Upper_Fence {LF}, {UF}")
    logging.info(f"Records Cleaned: {records_before_cleaning - len(df[col])}")
    return df


def data_preparation(file_arrival):
    customer_info = util.pd_read_csv_files(f"{customer_info_raw_path}/file_arrival={file_arrival}")
    loan_info = util.pd_read_csv_files(f"{loan_info_raw_path}/file_arrival={file_arrival}")

    # 1.Renaming the columns to match column description
    customer_info.columns = ['customer_id', 'age', 'job_type', 'marital_status', 'educational_level']
    loan_info.columns = ['customer_id', 'has_credit', 'avg_yearly_balance',
                         'has_housing_loan', 'has_personal_loan', 'contact_communication_type',
                         'contacted_day', 'contacted_month', 'contacted_duration_sec',
                         'total_times_contacted', 'days_passed_from_last_campaign',
                         'total_times_contacted_before_this_campaign',
                         'outcome_of_previous_campaign', 'outcome']

    # 2. PDA Analysis: Find min, max, mean, median, standard deviation
    logging.info("=" * 5 + " Describe Customer Info " + "=" * 5)
    logging.info(customer_info.describe())
    logging.info("=" * 5 + " Describe Loan Info " + "=" * 5)
    logging.info(loan_info.describe())

    # 3. PDA Analysis: print data dimensionality
    logging.info("=" * 5 + " Dimensionality of input data " + "=" * 5)
    logging.info(f"Customer info -> {customer_info.shape}")
    logging.info(f"Loan info -> {loan_info.shape}")

    # 4. Handle missing values: remove rows with unknown values
    customer_info_clean = customer_info.copy()
    loan_info_clean = loan_info.copy()

    customer_info_clean = customer_info_clean[customer_info_clean['job_type'] != 'unknown']
    customer_info_clean = customer_info_clean[customer_info_clean['educational_level'] != 'unknown']

    # 5. Handle wrong values: replace .admin with admin in job column
    customer_info_clean['job_type'] = customer_info_clean['job_type'].replace('admin.', 'admin')

    # 6. Handle missing columns: drop column days_passed_from_last_campaign from loan info,
    # since it has only -1 as a value and not valid
    loan_info_clean = loan_info_clean.drop(columns=['days_passed_from_last_campaign',
                                                    'outcome_of_previous_campaign',
                                                    'contact_communication_type',
                                                    'total_times_contacted_before_this_campaign'])

    # 7. Standardize or normalize numerical : Remove outliers
    logging.info("=" * 5 + " Removing outliers for Customer Info " + "=" * 5)
    customer_info_clean = remove_outliers(customer_info_clean, 'age')

    logging.info("=" * 5 + " Removing outliers for Loan Info " + "=" * 5)
    loan_info_clean = remove_outliers(loan_info_clean, 'avg_yearly_balance')
    loan_info_clean = remove_outliers(loan_info_clean, 'contacted_duration_sec')
    loan_info_clean = remove_outliers(loan_info_clean, 'total_times_contacted')

    # 8. one-hot encoding of categorical features of binary class
    loan_info_clean['has_credit'] = loan_info_clean['has_credit'].map({'yes': 1, 'no': 0})
    loan_info_clean['has_housing_loan'] = loan_info_clean['has_housing_loan'].map({'yes': 1, 'no': 0})
    loan_info_clean['has_personal_loan'] = loan_info_clean['has_personal_loan'].map({'yes': 1, 'no': 0})
    loan_info_clean['outcome'] = loan_info_clean['outcome'].map({'yes': 1, 'no': 0})

    # 9. label encoding of categorical features of multiclass
    customer_info_clean['job_type_encoded'] = customer_info_clean['job_type'] \
        .map({'student': 1,
              'unemployed': 2,
              'housemaid': 3,
              'self-employed': 4,
              'blue-collar': 5,
              'services': 6,
              'admin': 7,
              'entrepreneur': 8,
              'technician': 9,
              'management': 10,
              'retired': 11})

    customer_info_clean['marital_status_encoded'] = customer_info_clean['marital_status'] \
        .map({'single': 1,
              'married': 2,
              'divorced': 3})

    customer_info_clean['educational_level_encoded'] = customer_info_clean['educational_level'] \
        .map({'primary': 1,
              'secondary': 2,
              'tertiary': 3})

    # 10. write data in the clean layer
    customer_info_clean_folder_this_run = f"{customer_info_clean_folder}{file_arrival}"
    loan_info_clean_folder_this_run = f"{loan_info_clean_folder}{file_arrival}"

    os.makedirs(customer_info_clean_folder_this_run, exist_ok=True)
    os.makedirs(loan_info_clean_folder_this_run, exist_ok=True)

    customer_info_clean_full_path = os.path.join(customer_info_clean_folder_this_run, "customer_info.csv")
    loan_info_clean_full_path = os.path.join(loan_info_clean_folder_this_run, "loan_info.csv")

    customer_info_clean.to_csv(customer_info_clean_full_path, index=False)
    loan_info_clean.to_csv(loan_info_clean_full_path, index=False)

    logging.info(f"Customer info cleaned file is written to: {customer_info_clean_full_path}")
    logging.info(f"Loan info cleaned file is written to: {loan_info_clean_full_path}")


def main():
    logging.info(f"=== {job_name} started ===")

    parser = argparse.ArgumentParser(description="prepare loan and customer info for silver layer")
    parser.add_argument("file_arrival_time", help="time of file arrival in YYYYMMDD_HHMMSS")
    args = parser.parse_args()

    file_arrival = args.file_arrival_time

    data_preparation(file_arrival)

    logging.info(f"=== {job_name} ended ===")


if __name__ == "__main__":
    main()
