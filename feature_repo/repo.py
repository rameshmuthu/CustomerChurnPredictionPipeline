from datetime import timedelta
from feast import Entity, FeatureView, FileSource, ValueType, Field
from feast.types import Int64, Float64, String

# Offline source
customer_loan_source = FileSource(
    path="data/customer_loan_info.parquet",
    timestamp_field="event_timestamp",
)

# Entity
customer = Entity(
    name="customer_id",
    value_type=ValueType.INT64,
    description="Unique customer identifier"
)

# Feature view
loan_features_view = FeatureView(
    name="loan_features",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="age", dtype=Int64, description="Customer age"),
        Field(name="job_type_encoded", dtype=Int64, description="Encoded job type"),
        Field(name="marital_status_encoded", dtype=Int64, description="Encoded marital status"),
        Field(name="educational_level_encoded", dtype=Int64, description="Encoded education level"),
        Field(name="age_binned", dtype=Float64, description="Binned age category"),
        Field(name="avg_yearly_balance", dtype=Int64, description="Average yearly account balance"),
        Field(name="contacted_day", dtype=Int64, description="Day of last contact"),
        Field(name="contacted_month", dtype=String, description="Month of last contact"),
        Field(name="contacted_duration_sec", dtype=Float64, description="Duration of last contact in "
                                                                                    "seconds"),
        Field(name="total_times_contacted", dtype=Int64, description="Total times contacted"),
        Field(name="credit_commitment", dtype=Int64, description="Credit commitment amount"),
        Field(name="avg_yearly_balance_binned", dtype=Float64, description="Binned average yearly balance"),
    ],
    online=True,
    source=customer_loan_source,
)