{{ config(materialized='incremental', unique_key='transaction_id') }}

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    t.channel,
    t.merchant_category,
    t.country,
    t.city,
    t.device_id,
    t.ip_address,
    t.is_fraud,
    t.fraud_type,
    t.step,
    t.old_balance_orig,
    t.new_balance_orig,
    t.old_balance_dest,
    t.new_balance_dest,
    t.data_source,
    CURRENT_TIMESTAMP AS load_timestamp
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id