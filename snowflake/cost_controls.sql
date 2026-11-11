-- =============================================================
-- Snowflake cost guardrail — run ONCE, by hand, in a Snowsight worksheet.
-- Resource monitors can only be created by ACCOUNTADMIN, so this is NOT
-- run by Airflow or CI.
--
-- Project budget: 20 credits in total (FREQUENCY = NEVER: the counter never
-- resets, it is a one-off budget for the whole project, not a monthly one).
--
-- Why IF NOT EXISTS and not CREATE OR REPLACE:
--   CREATE OR REPLACE drops and recreates the monitor, which resets the credits
--   already consumed to 0 (with FREQUENCY = NEVER the budget would silently start
--   over) and detaches it from the warehouse. IF NOT EXISTS leaves an existing
--   monitor untouched, so this script is safe to re-run.
-- =============================================================

USE ROLE ACCOUNTADMIN;

-- 1. Check first: an existing monitor must be listed as BUDGET_PROJET
--    (unquoted names are stored in upper case; a name created with quotes,
--    e.g. "budget_projet", would NOT match and a second monitor would be created).
SHOW RESOURCE MONITORS;

-- 2. Create the budget only if it does not exist yet:
--   75 %  -> email notification to account admins
--   90 %  -> suspend the warehouse once running queries finish
--   100 % -> suspend immediately (running queries are cancelled)
CREATE RESOURCE MONITOR IF NOT EXISTS budget_projet
    WITH CREDIT_QUOTA = 20
         FREQUENCY = NEVER
         START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 90 PERCENT DO SUSPEND
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- 3. Attach it to the project's warehouse (idempotent; created by setup.sql)
ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = budget_projet;

-- 4. Verify: quota 20, used credits, and COMPUTE_WH -> BUDGET_PROJET
SHOW RESOURCE MONITORS LIKE 'BUDGET_PROJET';
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

-- To change the budget later WITHOUT resetting the consumed credits:
--   ALTER RESOURCE MONITOR budget_projet SET CREDIT_QUOTA = 30;
