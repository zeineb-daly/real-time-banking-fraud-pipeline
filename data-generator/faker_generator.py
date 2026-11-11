import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Project configuration for 100k+ records
# -----------------------------
NUM_CUSTOMERS = 2000
ACCOUNTS_PER_CUSTOMER = 5
NUM_TRANSACTIONS = 10   # per account → 2k*5*10 = 100k transactions
MAX_TXN_AMOUNT = 1000.00
CURRENCY = "USD"
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")
DEFAULT_LOOP = True
SLEEP_SECONDS = 2

parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# -----------------------------
# Connect to Postgres
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
conn.autocommit = True
cur = conn.cursor()

# -----------------------------
# Core generation logic (batch inserts)
# -----------------------------
def run_iteration():
    customers_list = []
    accounts_list = []
    transactions_list = []

    # 1️.Generate customers
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        customers_list.append((first_name, last_name, email))

    # Batch insert customers
    cur.executemany(
        "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
        customers_list
    )
    customer_ids = [cur.fetchone()[0] for _ in range(NUM_CUSTOMERS)]

    # 2️.Generate accounts
    for customer_id in customer_ids:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            accounts_list.append((customer_id, account_type, initial_balance, CURRENCY))

    # Batch insert accounts
    cur.executemany(
        "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
        accounts_list
    )
    account_ids = [cur.fetchone()[0] for _ in range(len(accounts_list))]

    # 3️⃣ Generate transactions
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for account_id in account_ids:
        for _ in range(NUM_TRANSACTIONS):
            txn_type = random.choice(txn_types)
            amount = round(random.uniform(1, MAX_TXN_AMOUNT), 2)
            related_account = None
            if txn_type == "TRANSFER" and len(account_ids) > 1:
                related_account = random.choice([a for a in account_ids if a != account_id])
            transactions_list.append((account_id, txn_type, amount, related_account, "COMPLETED"))

    # Batch insert transactions
    cur.executemany(
        "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, %s)",
        transactions_list
    )

    print(f"Generated {NUM_CUSTOMERS} customers, {len(accounts_list)} accounts, {len(transactions_list)} transactions.")

# -----------------------------
# Main loop
# -----------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:
            break
        time.sleep(SLEEP_SECONDS)

except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

finally:
    cur.close()
    conn.close()
    sys.exit(0)
