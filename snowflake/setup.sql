-- =============================================================
-- Snowflake bootstrap — idempotent, safe to run on every DAG run.
-- Executed by the `setup_snowflake` task of minio_to_snowflake_banking.
-- Names must match banking_dbt/models/sources.yml (BANKING.RAW)
-- and SNOWFLAKE_WAREHOUSE in .env (COMPUTE_WH).
-- =============================================================

-- Cost control: smallest warehouse, suspended after 60 s of inactivity,
-- resumed automatically when a query arrives. Created suspended (no credits).
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
-- Enforce the same settings if the warehouse already existed (e.g. trial default)
ALTER WAREHOUSE COMPUTE_WH SET
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS BANKING;
CREATE SCHEMA IF NOT EXISTS BANKING.RAW;

-- Raw CDC rows land as a single VARIANT column `v` (parsed by dbt staging models)
CREATE TABLE IF NOT EXISTS BANKING.RAW.CUSTOMERS    (v VARIANT);
CREATE TABLE IF NOT EXISTS BANKING.RAW.ACCOUNTS     (v VARIANT);
CREATE TABLE IF NOT EXISTS BANKING.RAW.TRANSACTIONS (v VARIANT);
