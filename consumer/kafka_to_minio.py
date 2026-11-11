import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import time
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

TOPICS = [
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
]
BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "500"))         # flush when a table buffer reaches this size...
FLUSH_SECONDS = float(os.getenv("CONSUMER_FLUSH_SECONDS", "30"))  # ...or when this much time has passed

# Kafka consumer settings
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    # Offsets are committed manually, only AFTER the buffered records are safely in MinIO.
    # With auto-commit, a crash could commit offsets of records still in memory -> data loss.
    enable_auto_commit=False,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)


def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    stamp = now.strftime("%H%M%S%f")
    file_path = f'{table_name}_{date_str}_{stamp}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{stamp}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'Uploaded {len(records)} records to s3://{bucket}/{s3_key}')


buffer = {topic: [] for topic in TOPICS}
last_flush = time.monotonic()


def flush_all(reason):
    """Write every non-empty buffer to MinIO, then commit offsets (at-least-once delivery)."""
    global last_flush
    pending = sum(len(records) for records in buffer.values())
    if pending:
        print(f"Flushing {pending} records ({reason})")
        for topic, records in buffer.items():
            write_to_minio(topic.split('.')[-1], records)
            buffer[topic] = []
        consumer.commit()
    last_flush = time.monotonic()


print(f"Connected to Kafka. Listening for messages (flush every {BATCH_SIZE} records or {FLUSH_SECONDS:g}s)...")

try:
    while True:
        # poll() returns after at most 1s even with no messages, so the time-based flush still happens
        batches = consumer.poll(timeout_ms=1000)
        for tp, messages in batches.items():
            for message in messages:
                event = message.value
                if event is None:  # tombstone
                    continue
                # With schemas disabled the event IS the payload; with schemas it's wrapped in {"schema", "payload"}
                payload = event.get("payload", event)
                record = payload.get("after")  # Only take the actual row (deletes have after = null)
                if record:
                    buffer[tp.topic].append(record)

        if any(len(records) >= BATCH_SIZE for records in buffer.values()):
            flush_all(f"batch of {BATCH_SIZE} reached")
        elif time.monotonic() - last_flush >= FLUSH_SECONDS:
            flush_all(f"{FLUSH_SECONDS:g}s elapsed")

except KeyboardInterrupt:
    print("\nStopping: flushing remaining records before exit...")
    flush_all("shutdown")
finally:
    consumer.close()
    print("Consumer closed.")
