from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
import pandas as pd

INPUT_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
OUTPUT_FILE = "/opt/airflow/data/processed_data.csv"

processed_dataset = Dataset("data/processed_data.csv")

def check_file():
    if not os.path.exists(INPUT_FILE) or os.path.getsize(INPUT_FILE) == 0:
        return "log_empty_file"

    try:
        df = pd.read_csv(INPUT_FILE, nrows=1)
        if df.empty:
            return "log_empty_file"
    except Exception:
        return "log_empty_file"

    return "transform_group.step_replace_nulls"


with DAG(
    dag_id="dag1_process",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INPUT_FILE,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    branch = BranchPythonOperator(
        task_id="branch_check_size",
        python_callable=check_file,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "Input file is empty. Skipping processing."',
    )

    with TaskGroup("transform_group") as transform_group:

        step_replace_nulls = BashOperator(
            task_id="step_replace_nulls",
            bash_command="python /opt/airflow/scripts/step_replace_nulls.py",
        )

        step_sort_by_date = BashOperator(
            task_id="step_sort_by_date",
            bash_command="python /opt/airflow/scripts/step_sort_by_date.py",
        )

        step_clean_content = BashOperator(
            task_id="step_clean_content",
            bash_command="python /opt/airflow/scripts/step_clean_content.py",
            outlets=[processed_dataset],
        )

        step_replace_nulls >> step_sort_by_date >> step_clean_content

    wait_for_file >> branch >> [log_empty_file, transform_group]