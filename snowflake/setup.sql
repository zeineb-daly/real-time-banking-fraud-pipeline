-- =============================================================
-- Snowflake bootstrap — idempotent, safe to run on every DAG run.
-- Executed by the `setup_snowflake` task of minio_to_snowflake_banking.
-- Names must match banking_dbt/models/sources.yml (BANKING.RAW).
-- =============================================================

CREATE DATABASE IF NOT EXISTS BANKING;
CREATE SCHEMA IF NOT EXISTS BANKING.RAW;

-- Raw CDC rows land as a single VARIANT column `v` (parsed by dbt staging models)
CREATE TABLE IF NOT EXISTS BANKING.RAW.CUSTOMERS    (v VARIANT);
CREATE TABLE IF NOT EXISTS BANKING.RAW.ACCOUNTS     (v VARIANT);
CREATE TABLE IF NOT EXISTS BANKING.RAW.TRANSACTIONS (v VARIANT);
