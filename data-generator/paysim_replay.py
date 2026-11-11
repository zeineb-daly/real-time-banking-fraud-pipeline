"""
Replay the PaySim dataset into Postgres, in step order, at a controlled rate,
so it flows through the CDC pipeline (Postgres -> Debezium -> Kafka -> ...).

    python data-generator/paysim_replay.py --rate 200
    python data-generator/paysim_replay.py --from-step 1 --to-step 24 --rate 0   # first day, max speed

- Reads the CSV with the standard csv module (no pandas), one step at a time.
- Accounts are created on the fly from nameOrig / nameDest (C... = customer, M... = merchant).
- step (hours since the start of the simulation) becomes created_at.
- isFraud is stored as is_fraud (the label). isFlaggedFraud is deliberately NOT stored:
  it is the output of PaySim's own rule-based detector, not an input.
- Idempotent: each row keeps its CSV row number (paysim_row, unique), so replaying
  the same steps again inserts nothing twice. Safe to resume after Ctrl+C.
"""
import argparse
import csv
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "paysim.csv"
EXPECTED_HEADER = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]

INSERT_ACCOUNTS = """
    INSERT INTO accounts (account_number, account_type, balance, currency, data_source)
    VALUES %s
    ON CONFLICT (account_number) DO NOTHING
"""

# Account ids are resolved by joining on account_number, so the script needs no in-memory cache
INSERT_TRANSACTIONS = """
    INSERT INTO transactions (
        account_id, related_account_id, txn_type, amount, status, created_at, step,
        old_balance_orig, new_balance_orig, old_balance_dest, new_balance_dest,
        is_fraud, data_source, paysim_row
    )
    SELECT o.id, d.id, v.txn_type, v.amount, 'COMPLETED', v.created_at, v.step,
           v.obo, v.nbo, v.obd, v.nbd, v.is_fraud, 'paysim', v.paysim_row
    FROM (VALUES %s) AS v(orig, dest, txn_type, amount, created_at, step,
                          obo, nbo, obd, nbd, is_fraud, paysim_row)
    JOIN accounts o ON o.account_number = v.orig
    JOIN accounts d ON d.account_number = v.dest
    ORDER BY v.paysim_row
    ON CONFLICT (paysim_row) DO NOTHING
"""


def parse_args():
    p = argparse.ArgumentParser(description="Replay PaySim into Postgres for the CDC pipeline")
    p.add_argument("--file", type=Path, default=DEFAULT_CSV, help=f"PaySim CSV (default: {DEFAULT_CSV})")
    p.add_argument("--rate", type=float, default=200, help="transactions per second, 0 = as fast as possible (default: 200)")
    p.add_argument("--from-step", type=int, default=1, help="first step to replay, inclusive (default: 1)")
    p.add_argument("--to-step", type=int, default=10**9, help="last step to replay, inclusive (default: all)")
    p.add_argument("--start-date", default="2026-01-01T00:00:00+00:00",
                   help="timestamp of step 1 (ISO 8601, default: 2026-01-01T00:00:00+00:00)")
    p.add_argument("--batch-size", type=int, default=None, help="rows per insert (default: rate/10, max 1000)")
    args = p.parse_args()
    if args.from_step > args.to_step:
        p.error("--from-step must be <= --to-step")
    if args.rate < 0:
        p.error("--rate must be >= 0")
    args.start = datetime.fromisoformat(args.start_date)
    if args.start.tzinfo is None:
        args.start = args.start.replace(tzinfo=UTC)
    if args.batch_size is None:
        args.batch_size = 1000 if args.rate == 0 else max(1, min(1000, int(args.rate // 10)))
    return args


def iter_steps(path: Path, from_step: int, to_step: int):
    """Yield (step, [(paysim_row, csv_row), ...]) one step at a time. The file must be sorted by step."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != EXPECTED_HEADER:
            raise ValueError(f"Unexpected CSV header: {header}")
        current, rows = None, []
        for paysim_row, row in enumerate(reader, start=1):  # 1 = first data row
            step = int(row[0])
            if current is not None and step < current:
                raise ValueError(f"CSV not sorted by step (row {paysim_row}: step {step} after {current})")
            if step < from_step:
                continue
            if step > to_step:
                break
            if step != current:
                if rows:
                    yield current, rows
                current, rows = step, []
            rows.append((paysim_row, row))
        if rows:
            yield current, rows


def to_records(step: int, rows, start: datetime):
    """Spread the step's rows evenly over its hour, keeping the CSV order."""
    hour_start = start + timedelta(hours=step - 1)
    n = len(rows)
    records = []
    for i, (paysim_row, r) in enumerate(rows):
        records.append({
            "orig": r[3], "dest": r[6], "txn_type": r[1],
            "amount": Decimal(r[2]),
            "created_at": hour_start + timedelta(seconds=i * 3600 / n),
            "step": step,
            "obo": Decimal(r[4]), "nbo": Decimal(r[5]),
            "obd": Decimal(r[7]), "nbd": Decimal(r[8]),
            "is_fraud": r[9] == "1",
            # r[10] = isFlaggedFraud: intentionally ignored
            "paysim_row": paysim_row,
        })
    return records


def insert_batch(cur, batch) -> int:
    # Accounts first (FK), deduplicated; first-seen balance becomes the account balance
    accounts = {}
    for rec in batch:
        for name, balance in ((rec["orig"], rec["obo"]), (rec["dest"], rec["obd"])):
            if name not in accounts:
                kind = "MERCHANT" if name.startswith("M") else "CUSTOMER"
                accounts[name] = (name, kind, balance, "USD", "paysim")
    execute_values(cur, INSERT_ACCOUNTS, list(accounts.values()), page_size=len(accounts))

    values = [(r["orig"], r["dest"], r["txn_type"], r["amount"], r["created_at"], r["step"],
               r["obo"], r["nbo"], r["obd"], r["nbd"], r["is_fraud"], r["paysim_row"]) for r in batch]
    execute_values(cur, INSERT_TRANSACTIONS, values, page_size=len(values))
    return cur.rowcount  # rows actually inserted (0 for rows already replayed)


def main():
    args = parse_args()
    if not args.file.exists():
        sys.exit(f"CSV not found: {args.file}")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    rate_txt = "max speed" if args.rate == 0 else f"{args.rate:g} tx/s"
    print(f"Replaying {args.file.name} steps {args.from_step}..{args.to_step if args.to_step < 10**9 else 'end'} "
          f"at {rate_txt}, batches of {args.batch_size}")

    started = time.monotonic()
    last_report = started
    read = inserted = frauds = 0
    last_step_done = None
    try:
        for step, rows in iter_steps(args.file, args.from_step, args.to_step):
            records = to_records(step, rows, args.start)
            for i in range(0, len(records), args.batch_size):
                batch = records[i:i + args.batch_size]
                inserted += insert_batch(cur, batch)
                conn.commit()  # one DB transaction per batch: accounts + transactions together
                read += len(batch)
                frauds += sum(r["is_fraud"] for r in batch)

                if args.rate:  # throttle: keep the average at --rate
                    ahead = read / args.rate - (time.monotonic() - started)
                    if ahead > 0:
                        time.sleep(ahead)

                now = time.monotonic()
                if now - last_report >= 5:
                    print(f"step {step:>3} | read {read:,} | inserted {inserted:,} | frauds {frauds:,} "
                          f"| {read / (now - started):,.0f} tx/s")
                    last_report = now
            last_step_done = step
    except KeyboardInterrupt:
        conn.rollback()
        resume = (last_step_done + 1) if last_step_done else args.from_step
        print(f"\nInterrupted. Steps completed up to {last_step_done}. "
              f"Resume with: --from-step {resume} (already inserted rows are skipped anyway).")
    finally:
        cur.close()
        conn.close()

    elapsed = time.monotonic() - started
    print(f"Done: read {read:,}, inserted {inserted:,} (skipped {read - inserted:,} already present), "
          f"frauds {frauds:,}, in {elapsed:,.0f}s.")


if __name__ == "__main__":
    main()
