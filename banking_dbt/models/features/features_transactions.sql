{{ config(materialized='table') }}

-- =============================================================================
-- features_transactions: one row per Faker transaction, with fraud-detection
-- features for ML training.
--
-- Anti-leakage rule: every feature only looks at the customer's PAST, i.e. rows
-- strictly before the current one, or up to the current row when the current
-- transaction is legitimately known at decision time (burst counters, device age).
-- Ties on transaction_time are broken by tx_seq (numeric transaction id).
--
-- Label: is_fraud. fraud_type is kept ONLY for per-scenario evaluation and must
-- NEVER be used as a model feature. Raw identifiers (device_id, ip, recipient id)
-- are not exposed: the model gets behaviour features, not IDs to memorise.
-- =============================================================================

with transactions as (
    select
        transaction_id,
        transaction_id::number as tx_seq,          -- numeric tie-breaker ('10' < '9' as text)
        account_id,
        related_account_id,
        transaction_time,
        amount,
        transaction_type,
        channel,
        merchant_category,
        country,
        device_id,
        is_fraud,
        fraud_type
    from {{ ref('stg_transactions') }}
    where data_source = 'faker'
    -- one row per transaction even if a CDC file was loaded twice
    qualify row_number() over (partition by transaction_id order by transaction_time desc) = 1
),

with_customer as (
    select
        t.*,
        a.customer_id
    from transactions t
    left join {{ ref('stg_accounts') }} a
        on a.account_id = t.account_id
),

-- "Mule account" indicator: distinct customers who sent money to the same
-- recipient in the previous 24h, up to and including the current transaction.
-- COUNT(DISTINCT) is not allowed in a RANGE window, hence the self-join.
recipient_senders as (
    select
        cur.transaction_id,
        count(distinct prev.customer_id) as nb_senders_to_recipient_24h
    from with_customer cur
    join with_customer prev
        on  prev.related_account_id = cur.related_account_id
        and prev.transaction_time >= dateadd('hour', -24, cur.transaction_time)
        and (   prev.transaction_time < cur.transaction_time
             or (prev.transaction_time = cur.transaction_time and prev.tx_seq <= cur.tx_seq))
    where cur.related_account_id is not null
    group by cur.transaction_id
),

features as (
    select
        -- keys and context (not features)
        c.transaction_id,
        c.customer_id,
        c.account_id,
        c.transaction_time,

        -- categorical features known at transaction time
        c.transaction_type,
        c.channel,
        c.merchant_category,
        c.amount,

        -- amount vs. the customer's past: strictly previous rows only
        avg(c.amount) over (
            partition by c.customer_id
            order by c.transaction_time, c.tx_seq
            rows between unbounded preceding and 1 preceding
        ) as avg_amount_past,

        -- history size: 0 for the customer's first transaction
        row_number() over (
            partition by c.customer_id
            order by c.transaction_time, c.tx_seq
        ) - 1 as nb_tx_past,

        -- time of day (UTC, same clock as the customers' active hours)
        hour(c.transaction_time)                   as hour_of_day,
        iff(hour(c.transaction_time) < 5, 1, 0)    as is_night,

        -- device: first use by this customer, and hours since that first use
        iff(row_number() over (
                partition by c.customer_id, c.device_id
                order by c.transaction_time, c.tx_seq) = 1, 1, 0) as is_new_device,
        datediff('second',
            min(c.transaction_time) over (
                partition by c.customer_id, c.device_id
                order by c.transaction_time, c.tx_seq
                rows between unbounded preceding and current row),
            c.transaction_time) / 3600.0           as device_age_hours,

        -- country: first transaction of this customer from this country
        iff(row_number() over (
                partition by c.customer_id, c.country
                order by c.transaction_time, c.tx_seq) = 1, 1, 0) as is_new_country,

        -- bursts: customer's transactions in the last 10 min / 1 h, current one included
        count(*) over (
            partition by c.customer_id
            order by c.transaction_time
            range between interval '10 minutes' preceding and current row
        ) as nb_tx_10min,
        count(*) over (
            partition by c.customer_id
            order by c.transaction_time
            range between interval '1 hour' preceding and current row
        ) as nb_tx_1h,

        -- recipient: first transfer from this customer to this account (0 if no recipient)
        iff(c.related_account_id is null, 0,
            iff(row_number() over (
                    partition by c.customer_id, c.related_account_id
                    order by c.transaction_time, c.tx_seq) = 1, 1, 0)) as is_new_recipient,

        -- label + evaluation-only column
        c.is_fraud,
        c.fraud_type                               -- evaluation only, NEVER a feature
    from with_customer c
)

select
    f.transaction_id,
    f.customer_id,
    f.account_id,
    f.transaction_time,
    f.transaction_type,
    f.channel,
    f.merchant_category,
    f.amount,
    f.avg_amount_past,
    f.amount / nullif(f.avg_amount_past, 0)        as amount_vs_avg,   -- NULL without history
    f.nb_tx_past,
    f.hour_of_day,
    f.is_night,
    f.is_new_device,
    f.device_age_hours,
    f.is_new_country,
    f.nb_tx_10min,
    f.nb_tx_1h,
    f.is_new_recipient,
    coalesce(r.nb_senders_to_recipient_24h, 0)     as nb_senders_to_recipient_24h,
    f.is_fraud,
    f.fraud_type
from features f
left join recipient_senders r
    on r.transaction_id = f.transaction_id
