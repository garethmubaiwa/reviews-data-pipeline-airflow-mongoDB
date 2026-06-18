from pymongo import MongoClient
import pandas as pd
import os

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "tiktok_reviews")
COLLECTION = os.getenv("MONGO_COLLECTION", "processed_reviews")
TEMP_COLLECTION = os.getenv("MONGO_TEMP_COLLECTION", "processed_reviews_tmp")

FILE = "/opt/airflow/data/processed_data.csv"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

temp_col = db[TEMP_COLLECTION]

temp_col.delete_many({})

chunks = pd.read_csv(FILE, chunksize=5000)
total = 0

for chunk in chunks:
    records = chunk.to_dict(orient="records")

    if records:
        temp_col.insert_many(records)
        total += len(records)

if total == 0:
    raise ValueError("No data to insert")

db[TEMP_COLLECTION].rename(COLLECTION, dropTarget=True)

db[COLLECTION].create_index("at")
db[COLLECTION].create_index("content")

client.close()
print(f"Inserted {total} records safely")