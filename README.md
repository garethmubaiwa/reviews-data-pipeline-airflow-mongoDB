# TikTok Reviews Data Pipeline

A local data pipeline built with Apache Airflow and MongoDB that processes TikTok Google Play reviews and loads them into a document store. The project was developed as a practical exercise in orchestration, data-aware scheduling, and containerised infrastructure.

## Overview

The pipeline is split across two DAGs. The first handles ingestion and transformation of the raw review data. The second is triggered automatically when the first completes, and is responsible for loading the processed output into MongoDB. Communication between the two DAGs is handled through Airflow's Dataset scheduling mechanism rather than explicit dependencies, which keeps them decoupled.

The entire environment runs locally via Docker Compose and requires no external services.

## Architecture

```
tiktok_google_play_reviews.csv
        |
   [FileSensor]
        |
 [check_empty_file]
      /         \
[log_empty]   [TaskGroup: transform_group]
                  |
            [replace_nulls]
                  |
            [sort_by_date]
                  |
            [clean_content] --> emits Dataset
                                      |
                              [dag2_load_mongo]
                                      |
                                  [MongoDB]
```

## Stack

- Apache Airflow 2.9.2 (LocalExecutor)
- MongoDB 7.0
- PostgreSQL 15 (Airflow metadata database)
- Python 3.12 with pandas
- Docker Compose

## Prerequisites

- Docker Desktop installed and running
- Git

## Setup

Clone the repository and place the dataset in the `data/` directory:

```
data/tiktok_google_play_reviews.csv
```

The dataset can be downloaded from [Google Drive](https://drive.google.com/file/d/1crEUrJMn3XI4ukzlTN8r0ZAzdOVYhpNq/view).

Rename `_env` to `.env` in the project root. It contains a single variable:

```
AIRFLOW_UID=50000
```

## Running the project

Build the custom Airflow image:

```bash
docker compose build --no-cache
```

Initialise the database and create the admin user:

```bash
docker compose up airflow-init
```

Wait for the container to exit with code 0, then start all services:

```bash
docker compose up -d
```

The Airflow UI will be available at `http://localhost:8080`. Log in with `admin` / `admin`.
Mongo Express runs at `http://localhost:8081` and requires no authentication.

Before triggering the pipeline, create the filesystem connection that the FileSensor depends on. In the Airflow UI, navigate to Admin > Connections, add a new record with Conn Id `fs_default` and Conn Type `File (path)`, and leave all other fields empty.

## Running the pipeline

In the Airflow UI, navigate to DAGs and ensure both `dag1_process` and `dag2_load_mongo` are unpaused. Trigger `dag1_process` manually using the run button. Once the transformation tasks complete, `dag2_load_mongo` will be triggered automatically via the Dataset outlet on the `clean_content` task.

Processed records can be inspected in Mongo Express under the `tiktok_reviews` database, `processed_reviews` collection.

## DAG reference

### dag1_process

| Task | Type | Description |
|---|---|---|
| wait_for_file | FileSensor | Waits for the input CSV to appear in `data/` |
| check_empty_file | BranchPythonOperator | Routes to transform group or empty file log |
| log_empty_file | BashOperator | Logs that the file is empty and halts |
| transform_group.replace_nulls | PythonOperator | Replaces NaN and string null values with `-` |
| transform_group.sort_by_date | PythonOperator | Sorts records by the `at` column ascending |
| transform_group.clean_content | PythonOperator | Removes emojis and special characters from `content` |

### dag2_load_mongo

| Task | Type | Description |
|---|---|---|
| load_to_mongo | PythonOperator | Reads processed CSV and inserts records into MongoDB |

## MongoDB queries

Once data is loaded, the following aggregation queries can be run in MongoDB Compass under the Aggregations tab.

Top 5 most frequently occurring comments:

```javascript
[
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
]
```

All entries where the content field is fewer than 5 characters:

```javascript
[
  { $project: { content: 1, length: { $strLenCP: "$content" } } },
  { $match: { length: { $lt: 5 } } }
]
```

Average rating per day as a timestamp:

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

## Project structure

```
.
├── dags/
│   ├── dag1_process.py
│   └── dag2_load_mongo.py
├── data/                  # mounted into the Airflow containers, git-ignored
├── logs/                  # git-ignored
├── config.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```
