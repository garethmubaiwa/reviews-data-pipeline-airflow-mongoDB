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
    '''
    Loads the processed data from the output CSV file into a MongoDB collection.
    - Reads the processed data from the output CSV file into a DataFrame.
    - Converts the 'created_date' column to datetime format, coercing errors to NaT.
    - Replaces NaN values with None to ensure compatibility with MongoDB.
    - Converts the DataFrame to a list of dictionaries (records) for insertion into MongoDB.
    - Sanitizes the keys in the records to remove characters that are not allowed in MongoDB field names (e.g., '.' and '$').
    - Establishes a connection to MongoDB using the MongoHook and inserts the records into the specified collection.
    - Prints the number of records inserted and the previous count of documents in the collection before insertion.
    '''
    print(f'[load_to_mongo] Reading processed data from {OUTPUT_FILE}')
    df = pd.read_csv(OUTPUT_FILE)
    if df.empty:
        raise ValueError(f"No data to load into MongoDB. The file {OUTPUT_FILE} is empty.")

    print(f'[load_to_mongo] Processing {len(df)} records for MongoDB insertion')
    df = df.where(pd.notna(df), other=None)

    if "created_date" in df.columns:
        df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
        df['created_date'] = df['created_date'].where(df['created_date'].notna(), other=None)

    records = df.to_dict(orient='records')

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

    if records:
        collection.insert_many(records)

    new_count = collection.count_documents({})
    print(f'[load_to_mongo] Inserted {new_count} records into MongoDB collection "{MONGO_COLLECTION_NAME}" (previous count: {previous_count})')

    collection.create_index("created_date")
    collection.create_index("content")

    client.close()

with DAG(
    dag_id="dag2_load_mongo",
    start_date=datetime(2024, 1, 1),
    schedule=[PROCESSED_DATASET],  # triggers automatically when dag1 emits the dataset
    catchup=False,
) as dag:

    load_to_mongo = PythonOperator(
        task_id="load_to_mongo",
        python_callable=_load_to_mongo,
    )