from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -----------------------------
# Default arguments for DAG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=20),  # Retry delay in case of failure
}

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2 and build marts",
    schedule_interval="@daily",     # Run daily (adjust if needed)
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

    # -----------------------------
    # Task 1: Run DBT snapshots (SCD Type-2)
    # -----------------------------
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt snapshot --profiles-dir /home/airflow/.dbt"
        ),
        execution_timeout=timedelta(minutes=60),  # Increased for 100k+ records
    )

    # -----------------------------
    # Task 2: Run DBT marts (fact & dimension models)
    # -----------------------------
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt run --select marts --profiles-dir /home/airflow/.dbt"
        ),
        execution_timeout=timedelta(minutes=90),  # Longer timeout for large data
    )

    # -----------------------------
    # Task dependencies
    # -----------------------------
    dbt_snapshot >> dbt_run_marts
