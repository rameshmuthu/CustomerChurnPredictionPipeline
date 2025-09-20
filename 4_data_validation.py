"""
4. Data Validation
    •	Implement data validation checks to ensure data quality:
        o	Check for missing or inconsistent data
        o	Validate data types, formats, and ranges
        o	Identify duplicates or anomalies
    •	Generate a comprehensive data quality report
    •	Deliverables:
        o	A Python script for automated validation (e.g., using pandas, great_expectations, or pydeequ)
        o	Sample data quality report in PDF or CSV format, summarizing issues and resolutions
"""

import argparse
from datetime import datetime
import csv
import os

from great_expectations.validator.validator import Validator
from great_expectations.core.batch import Batch
from great_expectations.datasource.fluent import PandasDatasource
import great_expectations as ge

import util

job_name = "4_data_validation"
logging = util.get_logger(job_name)
validation_reports_path = "./validation_reports"
context = ge.get_context()


def prepare_validation_summary(validation_results_dict):
    failed_results = [
        {
            "expectation": r["expectation_config"]["type"],
            "column": r["expectation_config"].get("kwargs", {}).get("column"),
            "success": r["success"],
            "result": r["result"].get("observed_value", {}) \
                if r["result"].get("observed_value", False) \
                else r["result"].get("partial_unexpected_list", [])
        }
        for r in validation_results_dict["results"]
        if not r["success"]
    ]
    return failed_results


def validate_loan_info(loan_file_path):
    logging.info("Data validation for Loan Info file is started")
    loan_df = util.pd_read_csv_files(loan_file_path)

    datasource = PandasDatasource(name="loan_data_source")
    validator = Validator(
        execution_engine=datasource.get_execution_engine(),
        batches=[Batch(data=loan_df)],
        context=context
    )

    # Check expected columns
    validator.expect_table_columns_to_match_ordered_list(
        ["id", "default", "balance", "housing", "loan", "contact", "day", "month", "duration", "campaign", "pdays",
         "previous", "poutcome", "y"])

    # check column types
    validator.expect_column_values_to_be_of_type("id", "int64")
    validator.expect_column_values_to_be_of_type("balance", "int64")
    validator.expect_column_values_to_be_of_type("day", "int64")
    validator.expect_column_values_to_be_of_type("duration", "int64")
    validator.expect_column_values_to_be_of_type("campaign", "int64")
    validator.expect_column_values_to_be_of_type("pdays", "int64")
    validator.expect_column_values_to_be_of_type("previous", "int64")

    # check categorical column values
    validator.expect_column_values_to_be_in_set("default", ["yes", "no"])
    validator.expect_column_values_to_be_in_set("housing", ["yes", "no"])
    validator.expect_column_values_to_be_in_set("loan", ["yes", "no"])
    validator.expect_column_values_to_be_in_set("contact", ["cellular", "telephone"])
    validator.expect_column_values_to_be_in_set("month",
                                                ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
                                                 "nov", "dec"])
    validator.expect_column_values_to_be_in_set("poutcome", ["failure", "success"])
    validator.expect_column_values_to_be_in_set("y", ["yes", "no"])

    # check numerical columns values range
    validator.expect_column_values_to_be_between("balance", min_value=0)
    validator.expect_column_values_to_be_between("day", min_value=1, max_value=31)
    validator.expect_column_values_to_be_between("duration", min_value=0)
    validator.expect_column_values_to_be_between("campaign", min_value=0)
    validator.expect_column_values_to_be_between("pdays", min_value=0)
    validator.expect_column_values_to_be_between("previous", min_value=0)

    # check uniqueness check
    validator.expect_column_values_to_be_unique("id")

    # check nulls
    validator.expect_column_values_to_not_be_null("id")
    validator.expect_column_values_to_not_be_null("y")

    # generate summary
    results = validator.validate()
    loan_info_validation_report = f"{validation_reports_path}/loan_info_validation_report.csv"
    failed_validations = prepare_validation_summary(results.to_json_dict())

    with open(loan_info_validation_report, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=failed_validations[0].keys())
        writer.writeheader()
        writer.writerows(failed_validations)

    logging.info("Data validation for Loan Info file is completed")
    logging.info(f"Loan info validation summary is available at :{loan_info_validation_report}")


def validate_customer_info(customer_file_path):
    logging.info("Data validation for Customer file is started")
    customer_df = util.pd_read_csv_files(customer_file_path)

    datasource = PandasDatasource(name="customer_data_source")
    validator = Validator(
        execution_engine=datasource.get_execution_engine(),
        batches=[Batch(data=customer_df)],
        context=context
    )
    # check expected columns
    validator.expect_table_columns_to_match_ordered_list(
        ["id", "age", "job", "marital", "education"])

    # check column types
    validator.expect_column_values_to_be_of_type("id", "int")
    validator.expect_column_values_to_be_of_type("age", "int")

    # check categorical column values
    validator.expect_column_values_to_be_in_set("job", ["admin", "blue-collar", "entrepreneur", "housemaid",
                                                        "management", "retired", "self-employed", "services",
                                                        "student", "technician", "unemployed"])
    validator.expect_column_values_to_be_in_set("marital", ["single", "married", "divorced"])
    validator.expect_column_values_to_be_in_set("education", ["primary", "secondary", "tertiary"])

    # check numerical columns values range
    validator.expect_column_values_to_be_between("id", min_value=0)
    validator.expect_column_values_to_be_between("age", min_value=0, max_value=100)

    # check uniqueness check
    validator.expect_column_values_to_be_unique("id")

    # check nulls
    validator.expect_column_values_to_not_be_null("id")

    # generate summary
    results = validator.validate()
    customer_info_validation_report = f"{validation_reports_path}/customer_validation_report.csv"

    failed_validations = prepare_validation_summary(results.to_json_dict())

    with open(customer_info_validation_report, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=failed_validations[0].keys())
        writer.writeheader()
        writer.writerows(failed_validations)

    logging.info("Data validation for Customer Info file is completed")
    logging.info(f"Loan info validation summary is available at :{customer_info_validation_report}")


def main():
    logging.info(f"=== {job_name} started ===")
    parser = argparse.ArgumentParser(description="generate validation report for loan and customer info")
    parser.add_argument("file_arrival_time", help="time of file arrival in YYYYMMDD_HHMMSS")
    args = parser.parse_args()

    file_arrival = args.file_arrival_time
    logging.info(f"file_arrival for this job: {file_arrival}")

    loan_file_path = f"./LocalDataLake/Raw/loan_info/file_arrival={file_arrival}"
    customer_file_path = f"./LocalDataLake/Raw/customer_info/file_arrival={file_arrival}"

    logging.info(f"loan_file_path: {loan_file_path}")
    logging.info(f"customer_file_path: {customer_file_path}")

    # make validation directory for this file arrival
    global validation_reports_path
    validation_reports_path = f"{validation_reports_path}/{file_arrival}"
    os.makedirs(validation_reports_path, exist_ok=True)

    validate_loan_info(loan_file_path)
    validate_customer_info(customer_file_path)

    logging.info(f"=== {job_name} ended ===")


if __name__ == "__main__":
    main()
