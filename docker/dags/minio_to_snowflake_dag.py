import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Tables to process
TABLES = ["customers", "accounts", "transactions"]

# Parallelism for uploading files
MAX_WORKERS = 6

# -----------------------------
# Python Callables
# -----------------------------
def download_from_minio():
    """Download all files from MinIO for each table."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    local_files = {}

    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []

        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)

    return local_files


def upload_files_parallel(cur, table, files, max_workers=MAX_WORKERS):
    """Upload files to Snowflake in parallel."""
    def upload(f):
        cur.execute(f"PUT file://{f} @%{table}")
        print(f"Uploaded {f} -> @{table} stage")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(upload, files)


def load_to_snowflake(**kwargs):
    """Load downloaded files into Snowflake using parallel uploads."""
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        # -----------------------------
        # Parallel upload
        # -----------------------------
        upload_files_parallel(cur, table, files)

        # -----------------------------
        # Copy into table
        # -----------------------------
        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table}")

    cur.close()
    conn.close()

# -----------------------------
# Airflow DAG
# -----------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables with parallel uploads",
    schedule_interval="*/1 * * * *",  # Every minute
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
        execution_timeout=timedelta(minutes=30),  # Timeout for large files
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
        execution_timeout=timedelta(minutes=60),  # Timeout for loading 100k+ records
    )

    task1 >> task2
