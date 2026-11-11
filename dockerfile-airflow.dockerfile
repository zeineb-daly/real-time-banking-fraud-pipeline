# Airflow image with dbt (Python 3.12, same dbt versions as requirements.txt)
FROM apache/airflow:2.9.3

USER airflow

RUN pip install --no-cache-dir "dbt-core==1.12.6" "dbt-snowflake==1.12.5"
