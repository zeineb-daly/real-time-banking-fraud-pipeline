"""
Synthetic banking data generator with labelled fraud scenarios.

Every customer gets a "normal behaviour" profile (home city, usual devices,
typical amount, personal active hours) and a 60-day history. Honest customers
also travel, change phones, make big purchases, go on shopping sprees or pay at
night. Fraud events are built from signals (new_device, foreign, night, burst,
high_amount); some only show 1-2 of them, and a small share is left unlabelled,
so fraud and legit overlap like in real data. See config.yaml for every knob.
"""
import argparse
import math
import os
import random
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal

import psycopg2
from dotenv import load_dotenv
from faker import Faker
from psycopg2.extras import execute_values

from config import load_config

load_dotenv()

DATA_SOURCE = "faker"
TXN_COLUMNS = (
    "account_id", "txn_type", "amount", "related_account_id", "status", "created_at",
    "channel", "merchant_category", "country", "city", "device_id", "ip_address",
    "is_fraud", "fraud_type", "data_source",
)
IDX_CREATED_AT, IDX_IS_FRAUD, IDX_FRAUD_TYPE = 5, 12, 13


def money(value: float) -> Decimal:
    return max(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_DOWN), Decimal("0.01"))


def weighted_choice(weights: dict):
    return random.choices(list(weights), weights=list(weights.values()), k=1)[0]


def randint_range(pair) -> int:
    return random.randint(int(pair[0]), int(pair[1]))


def uniform_range(pair) -> float:
    return random.uniform(float(pair[0]), float(pair[1]))


@dataclass
class Account:
    id: int
    customer_id: int
    balance: Decimal


@dataclass
class Customer:
    id: int
    city: str
    avg_amount: float
    devices: list[tuple[str, str]]          # (device_id, ip) used before any phone change
    active_start: float                     # hour of day, e.g. 7.5
    active_end: float
    created_at: datetime
    trip: tuple | None = None               # (start, end, country, city)
    device_change_at: datetime | None = None
    new_phone: tuple[str, str] | None = None
    accounts: list[Account] = field(default_factory=list)


class Generator:
    def __init__(self, cfg: dict, cur):
        self.cfg = cfg
        self.cur = cur
        self.fake = Faker()
        self.tx = cfg["transactions"]
        self.prof = cfg["customer_profile"]
        self.noise = cfg["legit_noise"]
        self.fraud = cfg["fraud"]
        self.max_amount = float(self.tx["max_amount"])
        self.truth: dict[int, tuple[str, bool]] = {}  # id(row) -> (fraud kind, labelled) — diagnostics only

    # ---------- time helpers ----------
    def _random_time(self, cust: Customer, night: bool = False) -> datetime:
        """Random timestamp in the history window, inside the customer's active hours (or at night)."""
        day0 = self.window_start.replace(hour=0, minute=0, second=0, microsecond=0)
        days = (self.now - day0).days
        for _ in range(20):
            hour = random.uniform(0, 5) if night else random.uniform(cust.active_start, cust.active_end)
            ts = day0 + timedelta(days=random.randint(0, days), hours=hour)
            if self.window_start <= ts <= self.now:
                return ts
        return self.now - timedelta(minutes=random.uniform(1, 60))

    def _burst_times(self, cust: Customer, n: int, window_minutes: float, night: bool) -> list[datetime]:
        start = self._random_time(cust, night=night)
        times = [start + timedelta(minutes=random.uniform(0, window_minutes)) for _ in range(n)]
        return sorted(min(t, self.now) for t in times)

    # ---------- attribute helpers ----------
    def _new_device(self) -> tuple[str, str]:
        return str(uuid.uuid4()), self.fake.ipv4_public()

    def _foreign_location(self) -> tuple[str, str]:
        return random.choice(self.tx["foreign_countries"]), self.fake.city()

    def _usual_device(self, cust: Customer, ts: datetime) -> tuple[str, str]:
        if cust.device_change_at and ts >= cust.device_change_at and random.random() < 0.8:
            return cust.new_phone
        return random.choice(cust.devices)

    def _normal_amount(self, cust: Customer) -> float:
        mu = math.log(cust.avg_amount)
        return min(random.lognormvariate(mu, self.prof["amount_sigma"]), self.max_amount)

    def _channel(self, txn_type: str) -> str:
        return random.choice(self.tx["channels_by_type"][txn_type])

    def _row(self, acct, txn_type, amount, ts, channel, device, ip, country, city,
             related=None, merchant=None, fraud_type=None):
        return (acct.id, txn_type, money(amount), related, "COMPLETED", ts,
                channel, merchant, country, city, device, ip,
                fraud_type is not None, fraud_type, DATA_SOURCE)

    # ---------- entity generation ----------
    def _make_profile(self, cid: int) -> Customer:
        span = self.now - self.window_start
        cust = Customer(
            id=cid,
            city=self.fake.city(),
            avg_amount=uniform_range([self.prof["avg_amount_min"], self.prof["avg_amount_max"]]),
            devices=[self._new_device()
                     for _ in range(randint_range([self.prof["devices_min"], self.prof["devices_max"]]))],
            active_start=uniform_range(self.prof["active_start_hour"]),
            active_end=uniform_range(self.prof["active_end_hour"]),
            created_at=self.window_start - timedelta(days=uniform_range(self.prof["account_age_days"])),
        )
        if random.random() < self.noise["traveler_rate"]:
            length = timedelta(days=randint_range(self.noise["trip_days"]))
            start = self.window_start + (span - length) * random.random()
            cust.trip = (start, start + length, *self._foreign_location())
        if random.random() < self.noise["device_change_rate"]:
            cust.device_change_at = self.window_start + span * random.uniform(0.1, 0.9)
            cust.new_phone = self._new_device()
        return cust

    def _create_customers(self) -> list[Customer]:
        n = self.cfg["volume"]["num_customers"]
        profiles_tmp = [self._make_profile(0) for _ in range(n)]  # created_at needed for the insert
        rows = []
        for p in profiles_tmp:
            first, last = self.fake.first_name(), self.fake.last_name()
            # uuid suffix keeps emails unique across iterations and restarts
            email = f"{first}.{last}.{uuid.uuid4().hex[:8]}@{self.fake.free_email_domain()}".lower()
            rows.append((first, last, email, p.created_at))
        ids = execute_values(
            self.cur,
            "INSERT INTO customers (first_name, last_name, email, created_at) VALUES %s RETURNING id",
            rows, fetch=True, page_size=1000,
        )
        for (cid,), p in zip(ids, profiles_tmp):
            p.id = cid
        return profiles_tmp

    def _create_accounts(self, customers: list[Customer]) -> list[Account]:
        acc_cfg = self.cfg["accounts"]
        rows, owners = [], []
        for cust in customers:
            for _ in range(self.cfg["volume"]["accounts_per_customer"]):
                balance = money(uniform_range([acc_cfg["initial_balance_min"], acc_cfg["initial_balance_max"]]))
                opened = cust.created_at + timedelta(days=random.uniform(0, 10))
                rows.append((cust.id, random.choice(acc_cfg["types"]), balance, acc_cfg["currency"],
                             opened, DATA_SOURCE))
                owners.append(cust)
        ids = execute_values(
            self.cur,
            "INSERT INTO accounts (customer_id, account_type, balance, currency, created_at, data_source) "
            "VALUES %s RETURNING id",
            rows, fetch=True, page_size=1000,
        )
        accounts = []
        for (aid,), row, cust in zip(ids, rows, owners):
            acct = Account(id=aid, customer_id=cust.id, balance=row[2])
            cust.accounts.append(acct)
            accounts.append(acct)
        return accounts

    # ---------- legit transactions ----------
    def _legit_context(self, cust: Customer, ts: datetime):
        device, ip = self._usual_device(cust, ts)
        if random.random() < self.noise["one_off_device_rate"]:
            device, ip = self._new_device()
        country, city = self.tx["home_country"], cust.city
        if cust.trip and cust.trip[0] <= ts <= cust.trip[1]:
            country, city = cust.trip[2], cust.trip[3]
        return device, ip, country, city

    def _legit_txn(self, cust: Customer, acct: Account, all_accounts: list[Account]):
        txn_type = weighted_choice(self.tx["type_weights"])
        ts = self._random_time(cust, night=random.random() < self.noise["night_rate"])
        device, ip, country, city = self._legit_context(cust, ts)
        amount = self._normal_amount(cust)
        if random.random() < self.noise["large_purchase_rate"]:
            amount = min(amount * random.uniform(3, 10), self.max_amount)

        related = None
        if txn_type == "TRANSFER":
            related = random.choice(all_accounts).id
            while related == acct.id:
                related = random.choice(all_accounts).id
        merchant = random.choice(self.tx["merchant_categories"]) if txn_type == "PAYMENT" else None
        return self._row(acct, txn_type, amount, ts, self._channel(txn_type),
                         device, ip, country, city, related=related, merchant=merchant)

    def _shopping_spree(self, cust: Customer) -> list[tuple]:
        """Legit burst: several payments in a few minutes (same pattern as card testing, but honest)."""
        acct = random.choice(cust.accounts)
        times = self._burst_times(cust, randint_range(self.noise["spree_txns"]),
                                  self.noise["spree_window_minutes"], night=False)
        rows = []
        for ts in times:
            device, ip, country, city = self._legit_context(cust, ts)
            rows.append(self._row(acct, "PAYMENT", self._normal_amount(cust), ts, random.choice(["POS", "ONLINE"]),
                                  device, ip, country, city,
                                  merchant=random.choice(self.tx["merchant_categories"])))
        return rows

    # ---------- fraud building blocks ----------
    def _pick_signals(self, sc: dict) -> tuple[set, bool]:
        """Full event: each signal with its configured probability. Partial event: only 1-2 signals."""
        probs = sc["signals"]
        if random.random() < self.fraud["partial_rate"]:
            k = min(randint_range(self.fraud["partial_signals"]), len(probs))
            return set(random.sample(list(probs), k)), True
        return {s for s, p in probs.items() if random.random() < p}, False

    def _fraud_context(self, cust: Customer, signals: set, ts: datetime):
        device, ip = self._new_device() if "new_device" in signals else self._usual_device(cust, ts)
        if "foreign" in signals:
            country, city = self._foreign_location()
        else:
            country, city = self.tx["home_country"], cust.city
        return device, ip, country, city

    def _fraud_times(self, cust: Customer, n: int, sc: dict, signals: set) -> list[datetime]:
        window = sc["window_minutes"] if "burst" in signals else self.fraud["spread_window_minutes"]
        return self._burst_times(cust, n, window, night="night" in signals)

    def _fraud_amount(self, cust: Customer, sc: dict, signals: set) -> float:
        if "high_amount" in signals:
            return min(cust.avg_amount * uniform_range(sc["amount_multiplier"]), self.max_amount)
        return self._normal_amount(cust)

    # ---------- fraud scenarios ----------
    def _account_takeover(self, cust, acct, mules, sc, signals, label):
        times = self._fraud_times(cust, randint_range(sc["txns"]), sc, signals)
        device, ip, country, city = self._fraud_context(cust, signals, times[0])  # one attacker, one context
        rows = []
        for ts in times:
            txn_type = random.choice(["WITHDRAWAL", "TRANSFER"])
            related = random.choice(mules).id if txn_type == "TRANSFER" else None
            channel = "ATM" if txn_type == "WITHDRAWAL" else random.choice(["ONLINE", "MOBILE"])
            rows.append(self._row(acct, txn_type, self._fraud_amount(cust, sc, signals), ts, channel,
                                  device, ip, country, city, related=related, fraud_type=label))
        return rows

    def _card_testing(self, cust, acct, mules, sc, signals, label):
        times = self._fraud_times(cust, randint_range(sc["txns"]) + 1, sc, signals)
        device, ip, country, city = self._fraud_context(cust, signals, times[0])
        rows = []
        for i, ts in enumerate(times):
            is_last = i == len(times) - 1
            amount = self._fraud_amount(cust, sc, signals) if is_last else uniform_range(sc["small_amount"])
            merchant = random.choice(["ONLINE_RETAIL", "DIGITAL_GOODS", "ELECTRONICS"])
            rows.append(self._row(acct, "PAYMENT", amount, ts, "ONLINE", device, ip, country, city,
                                  merchant=merchant, fraud_type=label))
        return rows

    def _balance_draining(self, cust, acct, mules, sc, signals, label):
        splits = randint_range(sc["splits"])
        times = self._fraud_times(cust, splits, sc, signals)
        device, ip, country, city = self._fraud_context(cust, signals, times[0])
        mule = random.choice(mules)
        total = float(acct.balance) * uniform_range(sc["drain_ratio"])
        rows = []
        for ts in times:
            amount = (min(total / splits, self.max_amount) if "high_amount" in signals
                      else self._normal_amount(cust))
            rows.append(self._row(acct, "TRANSFER", amount, ts, random.choice(["ONLINE", "MOBILE"]),
                                  device, ip, country, city, related=mule.id, fraud_type=label))
        return rows

    def _rapid_transfers(self, cust, acct, mules, sc, signals, label):
        times = self._fraud_times(cust, randint_range(sc["txns"]), sc, signals)
        device, ip, country, city = self._fraud_context(cust, signals, times[0])
        mule = random.choice(mules)
        rows = []
        for ts in times:
            rows.append(self._row(acct, "TRANSFER", self._fraud_amount(cust, sc, signals), ts,
                                  random.choice(["ONLINE", "MOBILE"]), device, ip, country, city,
                                  related=mule.id, fraud_type=label))
        return rows

    def _inject_fraud(self, customers: list[Customer], accounts: list[Account]) -> list[tuple]:
        if not self.fraud["enabled"]:
            return []
        scenarios = self.fraud["scenarios"]
        handlers = {
            "account_takeover": self._account_takeover,
            "card_testing": self._card_testing,
            "balance_draining": self._balance_draining,
            "rapid_transfers": self._rapid_transfers,
        }
        weights = {name: sc["weight"] for name, sc in scenarios.items() if name in handlers}
        mules = random.sample(accounts, min(self.fraud["mule_pool_size"], len(accounts)))
        mule_ids = {m.id for m in mules}

        rows = []
        for cust in customers:
            for acct in cust.accounts:
                if acct.id in mule_ids or random.random() >= self.fraud["account_rate"]:
                    continue
                name = weighted_choice(weights)
                signals, partial = self._pick_signals(scenarios[name])
                labelled = random.random() >= self.fraud["unlabeled_rate"]
                event = handlers[name](cust, acct, mules, scenarios[name], signals,
                                       name if labelled else None)  # unlabelled: stored as legit
                kind = "partial" if partial else "full"
                for r in event:
                    self.truth[id(r)] = (kind, labelled)
                rows.extend(event)
        return rows

    # ---------- iteration ----------
    def run_iteration(self) -> list[tuple]:
        self.now = datetime.now(UTC)
        self.window_start = self.now - timedelta(days=self.cfg["run"]["history_days"])
        self.truth = {}

        customers = self._create_customers()
        accounts = self._create_accounts(customers)

        per_account = self.cfg["volume"]["transactions_per_account"]
        txns = [self._legit_txn(cust, acct, accounts)
                for cust in customers for acct in cust.accounts for _ in range(per_account)]
        for cust in customers:
            if random.random() < self.noise["spree_rate"]:
                txns.extend(self._shopping_spree(cust))
        txns.extend(self._inject_fraud(customers, accounts))
        txns.sort(key=lambda r: r[IDX_CREATED_AT])  # chronological ids, like a real event stream

        execute_values(
            self.cur,
            f"INSERT INTO transactions ({', '.join(TXN_COLUMNS)}) VALUES %s",
            txns, page_size=1000,
        )

        fraud_counts = Counter(r[IDX_FRAUD_TYPE] for r in txns if r[IDX_IS_FRAUD])
        n_fraud = sum(fraud_counts.values())
        kinds = Counter(self.truth.values())
        print(f"Generated {len(customers)} customers, {len(accounts)} accounts, {len(txns)} transactions "
              f"({n_fraud} labelled fraudulent, {n_fraud / len(txns):.2%}).")
        for name, count in sorted(fraud_counts.items()):
            print(f"  - {name}: {count}")
        print(f"  fraud txns by kind: full {kinds[('full', True)]}, partial {kinds[('partial', True)]}, "
              f"unlabelled (stored as legit) {kinds[('full', False)] + kinds[('partial', False)]}")
        return txns


def main() -> None:
    parser = argparse.ArgumentParser(description="Run fake banking data generator")
    parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
    parser.add_argument("--config", help="Path to the YAML config (default: config.yaml next to this script)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if cfg.get("seed") is not None:
        random.seed(cfg["seed"])
        Faker.seed(cfg["seed"])
    loop = cfg["run"]["loop"] and not args.once

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    conn.autocommit = True
    cur = conn.cursor()
    generator = Generator(cfg, cur)

    try:
        iteration = 0
        while True:
            iteration += 1
            print(f"\n--- Iteration {iteration} started ---")
            generator.run_iteration()
            print(f"--- Iteration {iteration} finished ---")
            if not loop:
                break
            time.sleep(cfg["run"]["sleep_seconds"])
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting gracefully...")
    finally:
        cur.close()
        conn.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
