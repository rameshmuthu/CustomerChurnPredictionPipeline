# Feature Metadata - Customer Loan Info

**Entity:** `customer_id`  
**Source:** LocalDataLake/gold/customer_loan_info  
**Version:** 1.0  
**Event Timestamp:** `event_timestamp`

| Feature Name                | Type   | Description                                 |
|----------------------------|--------|---------------------------------------------|
| age                        | INT32  | Customer age                                |
| job_type_encoded           | INT32  | Encoded job type                            |
| marital_status_encoded     | INT32  | Encoded marital status                      |
| educational_level_encoded  | INT32  | Encoded education level                     |
| age_binned                 | STRING | Age category bin                            |
| avg_yearly_balance         | FLOAT  | Average yearly account balance              |
| contacted_day              | INT32  | Day of last contact                         |
| contacted_month            | STRING | Month of last contact                       |
| contacted_duration_sec     | INT32  | Duration of last contact in seconds         |
| total_times_contacted      | INT32  | Total times contacted                       |
| outcome                    | STRING | Outcome of last campaign                    |
| credit_commitment          | FLOAT  | Credit commitment amount                    |
| avg_yearly_balance_binned  | STRING | Binned average yearly balance               |