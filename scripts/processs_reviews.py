import pandas as pd
import os
import re
import sys

INPUT_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
OUTPUT_FILE = "/opt/airflow/data/processed_data.csv"

EMOJI_PATTERN = re.compile(
    "[" 
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "]",
    flags=re.UNICODE
)

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value)
    text = EMOJI_PATTERN.sub('', text)
    text = re.sub(r'\s{2,}', ' ', text).strip()
    return text if text else None

try:
    if not os.path.exists(INPUT_FILE) or os.path.getsize(INPUT_FILE) == 0:
        print("Empty or missing file")
        sys.exit(1)

    df = pd.read_csv(INPUT_FILE)

    if df.empty:
        print("Empty dataframe")
        sys.exit(1)

    df = df.where(pd.notna(df), None)

    if 'at' in df.columns:
        df['at'] = pd.to_datetime(df['at'], errors='coerce')

    if 'content' in df.columns:
        df['content'] = df['content'].apply(clean_text)

    df.to_csv(OUTPUT_FILE, index=False)

    print("Processing successful")
    sys.exit(0)

except Exception as e:
    print(f"Processing failed: {e}")
    sys.exit(1)