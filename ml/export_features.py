"""
One-off export of the ML dataset from Snowflake to a local Parquet file.

    docker compose run --rm jupyter python /home/jovyan/ml/export_features.py

Reads features_transactions joined with fraud_rules_baseline (dataset_split +
rule columns, used only to compare the model with the rules). If the Parquet
file already exists, nothing is queried (no Snowflake credits); use --force to
re-export.
"""
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector

ML_DIR = Path(os.getenv("ML_DIR", Path(__file__).resolve().parent))
OUT = ML_DIR / "data" / "features_transactions.parquet"

QUERY = """
select f.*,
       b.dataset_split,
       b.rule_burst, b.rule_micro_amount, b.rule_new_device_night, b.rule_alert
from BANKING.ANALYTICS.FEATURES_TRANSACTIONS f
join BANKING.ANALYTICS.FRAUD_RULES_BASELINE b using (transaction_id)
"""

NUMERIC = [
    "amount", "avg_amount_past", "amount_vs_avg", "nb_tx_past", "hour_of_day", "is_night",
    "is_new_device", "device_age_hours", "is_new_country", "nb_tx_10min", "nb_tx_1h",
    "is_new_recipient", "nb_senders_to_recipient_24h",
    "rule_burst", "rule_micro_amount", "rule_new_device_night", "rule_alert",
]


def main() -> None:
    if OUT.exists() and "--force" not in sys.argv:
        print(f"{OUT} already exists: nothing exported (use --force to re-export).")
        return

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="BANKING",
        schema="ANALYTICS",
    )
    try:
        df = conn.cursor().execute(QUERY).fetch_pandas_all()
    finally:
        conn.close()

    df.columns = [c.lower() for c in df.columns]
    for col in NUMERIC:  # Snowflake NUMBER(p,s) can arrive as Decimal objects
        df[col] = pd.to_numeric(df[col])
    df["is_fraud"] = df["is_fraud"].astype(bool)
    df["transaction_time"] = pd.to_datetime(df["transaction_time"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f"Exported {len(df):,} rows to {OUT}")
    print(df.groupby("dataset_split")["is_fraud"].agg(["size", "sum"]).rename(columns={"size": "rows", "sum": "frauds"}))


if __name__ == "__main__":
    main()
