import pandas as pd
import re

INPUT = "/opt/airflow/data/tmp_step2.csv"
OUTPUT = "/opt/airflow/data/processed_data.csv"

EMOJI_PATTERN = re.compile(
    "[" 
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "]",
    flags=re.UNICODE
)

df = pd.read_csv(INPUT)

def clean_text(x):
    if x == "-" or pd.isna(x):
        return "-"
    x = str(x)
    x = EMOJI_PATTERN.sub("", x)
    x = re.sub(r"\s{2,}", " ", x).strip()
    return x if x else "-"

df["content"] = df["content"].apply(clean_text)

df.to_csv(OUTPUT, index=False)