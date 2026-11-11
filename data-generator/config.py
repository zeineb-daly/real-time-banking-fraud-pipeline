"""Load and validate the generator's YAML configuration."""
import os
from pathlib import Path

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yaml")
REQUIRED_SECTIONS = ("volume", "run", "accounts", "transactions", "customer_profile", "legit_noise", "fraud")
KNOWN_SIGNALS = {"new_device", "foreign", "night", "burst", "high_amount"}


def load_config(path: str | None = None) -> dict:
    path = Path(path or os.getenv("GENERATOR_CONFIG") or DEFAULT_CONFIG_PATH)
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    errors = [f"missing section '{s}'" for s in REQUIRED_SECTIONS if s not in cfg]
    if errors:
        raise ValueError("Invalid generator config:\n - " + "\n - ".join(errors))

    def check_rate(name, value):
        if not 0 <= value <= 1:
            errors.append(f"{name} must be between 0 and 1 (got {value})")

    def check_range(name, pair):
        if len(pair) != 2 or pair[0] > pair[1]:
            errors.append(f"{name} must be [min, max] with min <= max (got {pair})")

    def check_section(prefix, section):
        """Keys ending in _rate are probabilities; lists are [min, max] ranges."""
        for key, value in section.items():
            if key.endswith("_rate"):
                check_rate(f"{prefix}.{key}", value)
            elif isinstance(value, list) and len(value) == 2 and all(isinstance(v, (int, float)) for v in value):
                check_range(f"{prefix}.{key}", value)

    for key, value in cfg["volume"].items():
        if value < 1:
            errors.append(f"volume.{key} must be >= 1")
    if cfg["run"]["history_days"] <= 0:
        errors.append("run.history_days must be > 0")

    acc = cfg["accounts"]
    check_range("accounts.initial_balance", [acc["initial_balance_min"], acc["initial_balance_max"]])

    tx = cfg["transactions"]
    missing_channels = set(tx["type_weights"]) - set(tx["channels_by_type"])
    if missing_channels:
        errors.append(f"transactions.channels_by_type missing: {sorted(missing_channels)}")

    prof = cfg["customer_profile"]
    check_range("customer_profile.avg_amount", [prof["avg_amount_min"], prof["avg_amount_max"]])
    check_range("customer_profile.devices", [prof["devices_min"], prof["devices_max"]])
    check_section("customer_profile", prof)
    if not (0 <= prof["active_start_hour"][0] and prof["active_end_hour"][1] <= 24
            and prof["active_start_hour"][1] < prof["active_end_hour"][0]):
        errors.append("customer_profile: active hours must satisfy 0 <= start < end <= 24")

    check_section("legit_noise", cfg["legit_noise"])

    fraud = cfg["fraud"]
    check_section("fraud", fraud)
    if fraud["enabled"] and fraud["mule_pool_size"] < 1:
        errors.append("fraud.mule_pool_size must be >= 1")
    for name, sc in fraud["scenarios"].items():
        check_section(f"fraud.scenarios.{name}", sc)
        signals = sc.get("signals", {})
        if not signals:
            errors.append(f"fraud.scenarios.{name}.signals must list at least one signal")
        for sig, prob in signals.items():
            if sig not in KNOWN_SIGNALS:
                errors.append(f"fraud.scenarios.{name}.signals: unknown signal '{sig}' (known: {sorted(KNOWN_SIGNALS)})")
            check_rate(f"fraud.scenarios.{name}.signals.{sig}", prob)

    if errors:
        raise ValueError("Invalid generator config:\n - " + "\n - ".join(errors))
