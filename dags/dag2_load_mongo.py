from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import pandas as pd

MONGO_CONNECTION_ID = "mongo_default"
MONGO_DB_NAME = "tiktok_reviews"
MONGO_COLLECTION_NAME = "processed_reviews"

OUTPUT_FILE = "/opt/airflow/data/processed_data.csv"
PROCESSED_DATASET_URI = "data/processed_data.csv"

PROCESSED_DATASET = Dataset(PROCESSED_DATASET_URI)


def _load_to_mongo():
    print(f"[load_to_mongo] Reading {OUTPUT_FILE}")

    chunks = pd.read_csv(OUTPUT_FILE, chunksize=10000)

    records = []

    for chunk in chunks:
        # restore datetime
        chunk['at'] = pd.to_datetime(chunk['at'], errors='coerce')
        chunk['at'] = chunk['at'].where(chunk['at'].notna(), None)

        # restore nulls
        chunk = chunk.replace('-', None)
        chunk = chunk.where(pd.notna(chunk), None)

        records.extend(chunk.to_dict(orient='records'))

    if not records:
        raise ValueError("No data to insert")

    def sanitise_key(key: str) -> str:
        return key.replace(".", "_").replace("$", "_").strip()

    records = [
        {sanitise_key(k): v for k, v in record.items()}
        for record in records
    ]

    hook = MongoHook(mongo_conn_id=MONGO_CONNECTION_ID)
    client = hook.get_conn()

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    previous_count = collection.count_documents({})
    collection.drop()

    collection.insert_many(records)

    new_count = collection.count_documents({})
    print(f"Inserted {new_count} records (previous: {previous_count})")

    # indexes
    collection.create_index("at")
    collection.create_index("content")

    client.close()


with DAG(
    dag_id="dag2_load_mongo",
    start_date=datetime(2024, 1, 1),
    schedule=[PROCESSED_DATASET],
    catchup=False,
) as dag:

    load_to_mongo = PythonOperator(
        task_id="load_to_mongo",
        python_callable=_load_to_mongo,
    )