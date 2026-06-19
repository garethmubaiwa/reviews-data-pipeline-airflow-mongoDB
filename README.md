# TikTok Reviews Data Pipeline

This project implements a local data pipeline using Apache Airflow and MongoDB to process and store TikTok Google Play reviews. It was developed during an internship at Innowise by Gareth Mubaiwa.

The pipeline demonstrates workflow orchestration, conditional branching, task grouping, and dataset-driven scheduling within a fully containerized environment.

---

## Overview

The system consists of two independent Airflow DAGs:

* **`dag1_process`**
  Handles ingestion, validation, and transformation of raw CSV data.

* **`dag2_load_mongo`**
  Triggered automatically via Airflow Datasets and loads processed data into MongoDB.

The DAGs are decoupled using dataset-based scheduling rather than direct dependencies.

---

## Architecture

```
                      /---> [log_empty_file]
[wait_for_file] ---> [branch_check_size]
                      \---> [transform_group]
                                  |
                                  v
                        [step_replace_nulls]
                                  |
                                  v
                        [step_sort_by_date]
                                  |
                                  v
                        [step_clean_content]
                                  |
                                  v
                             (Dataset)
                                  |
                                  v
                        [dag2_load_mongo]
                                  |
                                  v
                              [MongoDB]
```

---

## Tech Stack

* Apache Airflow 2.9.2 (LocalExecutor)
* MongoDB 7.0
* PostgreSQL 15
* Python 3.12 (pandas)
* Docker Compose

---

## Setup

1. Clone the repository

2. Add dataset:

```
data/tiktok_google_play_reviews.csv
```

3. Create `.env`:

```
AIRFLOW_UID=50000
```

---

## Configuration

Defined in `config.py`:

```python
MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "tiktok_reviews"
COLLECTION = "processed_reviews"
TEMP_COLLECTION = "processed_reviews_tmp"

INPUT_FILE = "/opt/airflow/data/tiktok_google_play_reviews.csv"
OUTPUT_FILE = "/opt/airflow/data/processed_reviews.csv"
```

---

## Run

```bash
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

---

## Access

* Airflow: http://localhost:8080 (admin / admin)
* Mongo Express: http://localhost:8081

---

## Airflow Setup

Create connection:

* Conn Id: `fs_default`
* Conn Type: `File (path)`

---

## Pipeline Flow

1. File is detected using `FileSensor`
2. Branching logic checks if file is empty
3. If empty → logs and stops
4. If valid → transformations run inside a TaskGroup:
   * Replace null values with `"-"`
   * Sort by date (`at` column)
   * Clean text content
5. Dataset is emitted
6. Second DAG loads data into MongoDB

---

## DAG Tasks

### dag1_process

* `wait_for_file` (FileSensor)
* `branch_check_size` (BranchPythonOperator)
* `log_empty_file` (BashOperator)
* `transform_group.step_replace_nulls`
* `transform_group.step_sort_by_date`
* `transform_group.step_clean_content`

### dag2_load_mongo

* `load_to_mongo`

---

## Data Notes

* Missing values are replaced with `"-"`
* Data is sorted chronologically
* Text is cleaned (special characters removed)
* Dates are stored in MongoDB as native datetime values

---

## Example Queries

**Top 5 comments**

```javascript
[
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
]
```

**Short content (<5 chars)**

```javascript
[
  { $project: { content: 1, length: { $strLenCP: "$content" } } },
  { $match: { length: { $lt: 5 } } }
]
```

**Average rating per day**

```javascript
[
  {
    $group: {
      _id: { $dateTrunc: { date: "$at", unit: "day" } },
      avg_rating: { $avg: "$score" }
    }
  },
  { $sort: { _id: 1 } }
]
```

---

## Project Structure

```
.
├── dags/
│   ├── dag1_process.py
│   └── dag2_load_mongo.py
├── data/
├── logs/
├── config.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Summary

This project demonstrates a practical Airflow pipeline with:

* Conditional branching
* Task grouping
* Dataset-based scheduling
* Clean data transformation
* MongoDB integration

It reflects production-oriented design principles in a local development setup.
