{{ config(materialized='view') }}

SELECT
    v:id::string                 AS transaction_id,
    v:account_id::string         AS account_id,
    v:amount::float              AS amount,
    v:txn_type::string           AS transaction_type,
    v:related_account_id::string AS related_account_id,
    v:status::string             AS status,
    v:created_at::timestamp      AS transaction_time,
    v:channel::string            AS channel,
    v:merchant_category::string  AS merchant_category,
    v:country::string            AS country,
    v:city::string               AS city,
    v:device_id::string          AS device_id,
    v:ip_address::string         AS ip_address,
    v:is_fraud::boolean          AS is_fraud,
    v:fraud_type::string         AS fraud_type,
    v:step::int                  AS step,
    v:old_balance_orig::float    AS old_balance_orig,
    v:new_balance_orig::float    AS new_balance_orig,
    v:old_balance_dest::float    AS old_balance_dest,
    v:new_balance_dest::float    AS new_balance_dest,
    v:data_source::string        AS data_source,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM {{ source('raw', 'transactions') }}