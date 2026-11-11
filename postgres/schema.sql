-- Banking OLTP initial schema (simplified)


CREATE TABLE IF NOT EXISTS customers (
id SERIAL PRIMARY KEY,
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL,
email VARCHAR(255) UNIQUE NOT NULL,
created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);


CREATE TABLE IF NOT EXISTS accounts (
id SERIAL PRIMARY KEY,
customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
account_type VARCHAR(50) NOT NULL,
balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
currency CHAR(3) NOT NULL DEFAULT 'USD',
created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);


CREATE TABLE IF NOT EXISTS transactions (
id BIGSERIAL PRIMARY KEY,
account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
txn_type VARCHAR(50) NOT NULL, -- DEPOSIT | WITHDRAWAL | TRANSFER | PAYMENT
amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
related_account_id INT NULL, -- for transfers
status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
channel VARCHAR(20) NULL, -- POS | ONLINE | MOBILE | ATM | BRANCH
merchant_category VARCHAR(50) NULL, -- PAYMENT only
country CHAR(2) NULL,
city VARCHAR(100) NULL,
device_id VARCHAR(64) NULL,
ip_address VARCHAR(45) NULL,
is_fraud BOOLEAN NOT NULL DEFAULT FALSE, -- ML label (synthetic ground truth)
fraud_type VARCHAR(50) NULL -- scenario name; do NOT use as a model feature
);


-- Migration for databases created before the fraud columns existed
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS channel VARCHAR(20) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS merchant_category VARCHAR(50) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS country CHAR(2) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS city VARCHAR(100) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS device_id VARCHAR(64) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS ip_address VARCHAR(45) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS is_fraud BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fraud_type VARCHAR(50) NULL;


-- Simple indexed columns for performance in queries
CREATE INDEX IF NOT EXISTS idx_transactions_account_created ON transactions(account_id, created_at);


-- =============================================================
-- PaySim support (data-generator/paysim_replay.py)
-- Additive and idempotent: no existing row or column is removed.
-- =============================================================

-- PaySim has no customers: an account can exist without one
ALTER TABLE accounts ALTER COLUMN customer_id DROP NOT NULL;
-- PaySim identifier (nameOrig / nameDest, e.g. C1231006815 or M1979787155); NULL for Faker rows
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS account_number VARCHAR(20) NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_account_number ON accounts(account_number);
-- Which generator produced the row ('faker' | 'paysim'); existing rows get 'faker'
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS data_source VARCHAR(10) NOT NULL DEFAULT 'faker';

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS step INT NULL;                       -- PaySim hour (1..743)
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS old_balance_orig NUMERIC(18,2) NULL; -- oldbalanceOrg
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS new_balance_orig NUMERIC(18,2) NULL; -- newbalanceOrig
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS old_balance_dest NUMERIC(18,2) NULL; -- oldbalanceDest
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS new_balance_dest NUMERIC(18,2) NULL; -- newbalanceDest
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS data_source VARCHAR(10) NOT NULL DEFAULT 'faker';
-- Data row number in the PaySim CSV: makes replays idempotent (ON CONFLICT DO NOTHING); NULL for Faker rows
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS paysim_row BIGINT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_transactions_paysim_row ON transactions(paysim_row);

-- PaySim contains 16 frauds with amount = 0: allow 0, still forbid negatives
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_amount_check;
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_amount_non_negative;
ALTER TABLE transactions ADD CONSTRAINT transactions_amount_non_negative CHECK (amount >= 0);
