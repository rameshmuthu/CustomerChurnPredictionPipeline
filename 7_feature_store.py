"""
7. Feature Store
    •	Implement a feature store to manage engineered features:
        o	Define metadata for each feature (e.g., description, source, version)
        o	Use a feature store tool (e.g., Feast) or a custom solution
    •	Automate feature retrieval for training and inference
    •	Deliverables:
        o	Feature store configuration/code
        o	Sample API or query demonstrating feature retrieval
        o	Documentation of feature metadata and versions
"""
from feast import FeatureStore
import pandas as pd
from datetime import datetime

import util

job_name = "7_feature_store"
logging = util.get_logger(job_name)

store = FeatureStore(repo_path="feature_repo")

# 1. get online features
features = store.get_online_features(
    features=[
        "loan_features:age",
        "loan_features:avg_yearly_balance",
        "loan_features:credit_commitment"
    ],
    entity_rows=[{"customer_id": 1}]
).to_dict()
logging.info("======= Online Features =======")
logging.info(features)

# 2. get historical features

entity_df = pd.DataFrame({
    "customer_id": [1000, 2000],
    "event_timestamp": [datetime(2025, 8, 23), datetime(2025, 8, 23)]
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "loan_features:age",
        "loan_features:avg_yearly_balance",
        "loan_features:credit_commitment"
    ]
).to_df()
logging.info("======= Historical Features =======")
logging.info(training_df.head(10).to_string(index=False))