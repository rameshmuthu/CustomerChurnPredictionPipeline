"""
9. Model Building
    •	Train a machine learning model to predict customer churn using the prepared features:
        o	Use a framework like scikit-learn or TensorFlow
        o	Experiment with multiple algorithms (e.g., logistic regression, random forest)
        o	Evaluate model performance using metrics such as accuracy, precision, recall, and F1 score
    •	Save the trained model using a versioning tool (e.g., MLflow)
    •	Deliverables:
        o	Python script for model training and evaluation
        o	Model performance report
        o	A versioned, saved model file (e.g., .pkl, .h5)
"""

from feast import FeatureStore
import pandas as pd
import argparse

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report

from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier

import joblib

import util

job_name = "9_model_building"
logging = util.get_logger(job_name)

gold_layer_path = "LocalDataLake/gold/customer_loan_info/file_arrival="
models_path = "models/"


def model_building(file_arrival):
    # 1. Read the data from gold layer to verify label distribution
    customer_loan_info = pd.read_parquet(f"{gold_layer_path}{file_arrival}")
    min_possible_count_of_label_class = customer_loan_info["outcome"].value_counts().min()

    customer_loan_info_1 = customer_loan_info[customer_loan_info['outcome'] == 1] \
        .sample(n=min_possible_count_of_label_class)
    customer_loan_info_2 = customer_loan_info[customer_loan_info['outcome'] == 0] \
        .sample(n=min_possible_count_of_label_class)

    data_available_for_model = pd.concat([customer_loan_info_1, customer_loan_info_2])
    logging.info("==== Outcome label distribution ====")
    logging.info(data_available_for_model["outcome"].value_counts())

    ids_to_query_feature_store = data_available_for_model[["customer_id", "event_timestamp"]]
    Y = data_available_for_model["outcome"]

    # 2. Query feature store for training data
    store = FeatureStore(repo_path="feature_repo")

    X = store.get_historical_features(
        entity_df=ids_to_query_feature_store,
        features=[
            "loan_features:age_binned",
            "loan_features:job_type_encoded",
            "loan_features:marital_status_encoded",
            "loan_features:educational_level_encoded",
            "loan_features:credit_commitment",
            "loan_features:avg_yearly_balance_binned",
        ]
    ).to_df()

    X.drop(["customer_id", "event_timestamp"], axis=1, inplace=True)

    # 3. Training-Test split: allocate 20% given data for testing
    test_size = 0.20
    training_x, test_x, training_y, test_y = (
        train_test_split(X, Y, test_size=test_size, stratify=Y, random_state=1234))

    # 4. Applying Logistic Regression
    LR_model = LogisticRegression(solver='liblinear')
    LR_model.fit(training_x, training_y)
    test_y_pred = LR_model.predict(test_x)

    accuracy_LR = accuracy_score(test_y, test_y_pred)
    conf_matrix_LR = confusion_matrix(test_y, test_y_pred)
    class_report_LR = classification_report(test_y, test_y_pred)
    logging.info("==== LOGISTIC REGRESSION ====")
    logging.info(f"Accuracy: {accuracy_LR}")
    logging.info("Confusion Matrix:")
    logging.info(conf_matrix_LR)
    logging.info("Classification Report:")
    logging.info(class_report_LR)

    # 5. Applying Decision Trees
    DR_Model = DecisionTreeClassifier(random_state=42)
    DR_Model.fit(training_x, training_y)
    test_y_pred_DR = DR_Model.predict(test_x)

    accuracy_DR = accuracy_score(test_y, test_y_pred_DR)
    conf_matrix_DR = confusion_matrix(test_y, test_y_pred_DR)
    class_report_DR = classification_report(test_y, test_y_pred_DR, target_names=["not-subsccribed", "subscribed"])
    logging.info("==== DECISION TREES ====")
    logging.info(f"Accuracy: {accuracy_DR}")
    logging.info("Confusion Matrix:")
    logging.info(conf_matrix_DR)
    logging.info("Classification Report:")
    logging.info(class_report_DR)

    # 6. Applying Random Forest
    RF_Model = RandomForestClassifier(n_estimators=50, random_state=12)
    RF_Model.fit(training_x, training_y)
    test_y_pred_RF = RF_Model.predict(test_x)

    accuracy_RF = accuracy_score(test_y, test_y_pred_RF)
    conf_matrix_RF = confusion_matrix(test_y, test_y_pred_RF)
    class_report_RF = classification_report(test_y, test_y_pred_RF, target_names=["not-subsccribed", "subscribed"])
    logging.info("==== RANDOM FOREST ====")
    logging.info(f"Accuracy: {accuracy_RF}")
    logging.info("Confusion Matrix:")
    logging.info(conf_matrix_RF)
    logging.info("Classification Report:")
    logging.info(class_report_RF)

    # 7. KNN Classifier
    KNN_Model = KNeighborsClassifier(n_neighbors=5)
    KNN_Model.fit(training_x, training_y)
    test_y_pred_KNN = KNN_Model.predict(test_x)

    accuracy_KNN = accuracy_score(test_y, test_y_pred_KNN)
    conf_matrix_KNN = confusion_matrix(test_y, test_y_pred_KNN)
    class_report_KNN = classification_report(test_y, test_y_pred_KNN, target_names=["not-subsccribed", "subscribed"])
    logging.info("==== KNN CLASSIFIER ====")
    logging.info(f"Accuracy: {accuracy_KNN}")
    logging.info("Confusion Matrix:")
    logging.info(conf_matrix_KNN)
    logging.info("Classification Report:")
    logging.info(class_report_KNN)

    # 8. Save Random Forest model, since it is performing well, using versioning tool
    joblib.dump(RF_Model, f"{models_path}RF_customer_churn_prediction_model_{file_arrival}.pkl")


def main():
    logging.info(f"=== {job_name} started ===")

    parser = argparse.ArgumentParser(description="Model customer churn prediction")
    parser.add_argument("file_arrival_time", help="time of file arrival in YYYYMMDD_HHMMSS")
    args = parser.parse_args()

    file_arrival = args.file_arrival_time

    logging.info(f"file_arrival : {file_arrival}")

    model_building(file_arrival)

    logging.info(f"=== {job_name} ended ===")


if __name__ == "__main__":
    main()
