import pandas as pd

INPUT = "/opt/airflow/data/tmp_step1.csv"
OUTPUT = "/opt/airflow/data/tmp_step2.csv"

df = pd.read_csv(INPUT)

df["at"] = pd.to_datetime(df["at"], errors="coerce")
df = df.sort_values(by="at")

df.to_csv(OUTPUT, index=False)