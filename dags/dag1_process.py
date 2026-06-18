from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

INPUT_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
PROCESSED_DATASET = Dataset("data/processed_data.csv")

with DAG(
    dag_id="dag1_process",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    process_file = BashOperator(
        task_id="process_file",
        bash_command="python /opt/airflow/scripts/process_reviews.py",
        outlets=[PROCESSED_DATASET],
    )

    wait_for_file >> process_file