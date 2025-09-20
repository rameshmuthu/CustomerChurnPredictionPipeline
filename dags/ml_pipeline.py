from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='CustomerChurnPredictionPipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    file_arrival_date = datetime.now().strftime('%Y%m%d')

    upload_file_to_landing = BashOperator(
        task_id='upload_file_to_landing',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/3_raw_data_storage.py'
    )

    landing_to_raw = BashOperator(
        task_id='landing_to_raw',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/2_data_ingestion.py {file_arrival_date}'
    )

    data_validation = BashOperator(
        task_id='data_validation',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/4_data_validation.py {file_arrival_date}'
    )

    data_preparation = BashOperator(
        task_id='data_preparation',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/5_data_preparation.py {file_arrival_date}'
    )

    data_transformation_and_storage = BashOperator(
        task_id='data_transformation_and_storage',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/6_data_transformation_and_storage.py {file_arrival_date}'
    )

    query_feature_store = BashOperator(
        task_id='query_feature_store',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/7_feature_store.py'
    )

    model_building = BashOperator(
        task_id='model_building',
        bash_command=f'/usr/bin/python3 /home/ubuntu/projects/CustomerChurnPredictionPipeline/9_model_building.py {file_arrival_date}'
    )
    upload_file_to_landing >> landing_to_raw >> data_validation >> data_preparation >> data_transformation_and_storage >> query_feature_store >> model_building