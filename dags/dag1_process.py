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
OUTPUT_FILE = "/opt/airflow/data/processed_data.csv"

# Dataset used as outlet here and as schedule trigger in dag2_load_mongo
processed_dataset = Dataset("data/processed_data.csv")

def _check_empty_file():
    '''Checks if the input file is empty or contains only null values.
    - If the file size is zero or if the DataFrame is empty after reading the CSV, it returns the task ID for logging an empty file.
    - Otherwise, it returns the task ID for the next step in the transformation process.
    '''
    if os.path.getsize(INPUT_FILE) == 0:
        return "log_empty_file"

    df = pd.read_csv(INPUT_FILE)
    if df.empty:
        return "log_empty_file"

    return "transform_group.replace_nulls"

def _replace_nulls():
    '''
    Replaces null values in the dataset with '-'.
    - Reads the input CSV file into a DataFrame.
    - Fills NaN values with '-'.
    - Replaces any string 'null' with '-'.
    - Saves the processed DataFrame to the output CSV file.
    '''
    df = pd.read_csv(INPUT_FILE)
    df = df.fillna('-')
    df = df.replace('null', '-', regex=False)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"[replace_nulls] Processed data {len(df)} rows saved to {OUTPUT_FILE}, Null values replaced with '-'")

def _sort_by_date():
    '''
    Sorts the data by the 'created_date' column in ascending order.
    - Reads the processed data from the output file.
    - Converts the 'created_date' column to datetime format, coercing errors to NaT.
    - Sorts the DataFrame by the 'created_date' column.
    - Saves the sorted DataFrame back to the output file.
    '''
    df = pd.read_csv(OUTPUT_FILE)
    df['at'] = pd.to_datetime(df['at'], errors='coerce')
    df = df.sort_values(by='at')
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Data sorted by date and saved to {OUTPUT_FILE}")

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # Emoticons
    "\U0001F300-\U0001F5FF"  # Misc symbols & pictographs
    "\U0001F680-\U0001F6FF"  # Transport & map symbols
    "]",
    flags=re.UNICODE
)

special_char_pattern = re.compile(r"[^\w\s.,!?;:\-'\"()]", flags=re.UNICODE)

def _clean_content():
    '''
    Cleans the content column by removing emojis and special characters, and normalizing whitespace.
    - If the value is null or '-', it returns the value as is.
    - Otherwise, it removes emojis and special characters, collapses multiple spaces into one, and trims leading/trailing whitespace.
    '''
    def clean_text(value: str) -> str:
        if pd.isna(value) or value == '-':
            return value
        text = str(value)
        text = EMOJI_PATTERN.sub('', text)
        text = special_char_pattern.sub('', text)
        text = re.sub(r'\s{2,}', ' ', text).strip()
        return text if text else '-'

    df = pd.read_csv(OUTPUT_FILE)

    if 'content' not in df.columns:
        raise ValueError("Column 'content' not found in the DataFrame.")

    df['content'] = df['content'].apply(clean_text)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"[clean_content] Cleaned {len(df)} rows.")

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
    )

    check_file = BranchPythonOperator(
        task_id="check_empty_file",
        python_callable=_check_empty_file,
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "Input file is empty or contains only null values. Skipping processing."',
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
            outlets=[processed_dataset],  # triggers dag2 when this task succeeds
        )

        replace_nulls_task >> sort_by_date_task >> clean_content_task

    wait_for_file >> check_file >> [log_empty_file, transform_group]