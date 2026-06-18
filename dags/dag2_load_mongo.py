from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime

PROCESSED_DATASET = Dataset("data/processed_data.csv")

with DAG(
    dag_id="dag2_load_mongo",
    start_date=datetime(2024, 1, 1),
    schedule=[PROCESSED_DATASET],
    catchup=False,
) as dag:

    load_to_mongo = BashOperator(
        task_id="load_to_mongo",
        bash_command="python /opt/airflow/scripts/load_to_mongo.py",
    )