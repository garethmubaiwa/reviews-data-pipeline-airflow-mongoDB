import pandas as pd

INPUT = "/opt/airflow/data/tiktok_google_play_reviews.csv"
OUTPUT = "/opt/airflow/data/tmp_step1.csv"

df = pd.read_csv(INPUT)

df = df.fillna("-")

df.to_csv(OUTPUT, index=False)