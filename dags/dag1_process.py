from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
import os
import re

INPUT_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"

STEP1_FILE = "/opt/airflow/data/tmp_step1.csv"
STEP2_FILE = "/opt/airflow/data/tmp_step2.csv"
OUTPUT_FILE = "/opt/airflow/data/processed_data.csv"

processed_dataset = Dataset("data/processed_data.csv")


def _check_empty_file():
    if os.path.getsize(INPUT_FILE) == 0:
        return "log_empty_file"

    try:
        df = pd.read_csv(INPUT_FILE, nrows=1)
        if df.empty:
            return "log_empty_file"
    except Exception:
        return "log_empty_file"

    return "transform_group.replace_nulls"


def _replace_nulls():
    df = pd.read_csv(INPUT_FILE)
    df = df.fillna('-')
    df = df.replace('null', '-', regex=False)
    df.to_csv(STEP1_FILE, index=False)
    print(f"[replace_nulls] Saved to {STEP1_FILE}")


def _sort_by_date():
    df = pd.read_csv(STEP1_FILE)
    df['at'] = pd.to_datetime(df['at'], errors='coerce')
    df = df.sort_values(by='at')
    df.to_csv(STEP2_FILE, index=False)
    print(f"[sort_by_date] Saved to {STEP2_FILE}")


EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "]",
    flags=re.UNICODE
)

special_char_pattern = re.compile(r"[^\w\s.,!?;:\-'\"()]", flags=re.UNICODE)


def _clean_content():
    def clean_text(value: str) -> str:
        if pd.isna(value) or value == '-':
            return value
        text = str(value)
        text = EMOJI_PATTERN.sub('', text)
        text = special_char_pattern.sub('', text)
        text = re.sub(r'\s{2,}', ' ', text).strip()
        return text if text else '-'

    df = pd.read_csv(STEP2_FILE)

    if 'content' not in df.columns:
        raise ValueError("Column 'content' not found")

    df['content'] = df['content'].apply(clean_text)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"[clean_content] Final output saved to {OUTPUT_FILE}")


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

    check_file = BranchPythonOperator(
        task_id="check_empty_file",
        python_callable=_check_empty_file,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "Input file is empty. Skipping processing."',
    )

    with TaskGroup("transform_group") as transform_group:

        replace_nulls_task = PythonOperator(
            task_id="replace_nulls",
            python_callable=_replace_nulls,
        )

        sort_by_date_task = PythonOperator(
            task_id="sort_by_date",
            python_callable=_sort_by_date,
        )

        clean_content_task = PythonOperator(
            task_id="clean_content",
            python_callable=_clean_content,
            outlets=[processed_dataset],
        )

        replace_nulls_task >> sort_by_date_task >> clean_content_task

    wait_for_file >> check_file >> [log_empty_file, transform_group]