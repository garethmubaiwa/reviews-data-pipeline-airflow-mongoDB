FROM apache/airflow:2.9.2

USER airflow

# Install providers using Airflow's constraints file.
# This prevents pip from accidentally upgrading/downgrading airflow itself,
# which would break the airflow binary and cause "command not found".
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.11.txt" \
    apache-airflow-providers-mongo \
    pymongo