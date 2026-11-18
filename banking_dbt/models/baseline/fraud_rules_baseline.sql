{{ config(materialized='table') }}

-- =============================================================================
-- fraud_rules_baseline: rule-based fraud detection, the reference an ML model
-- must beat. One row per transaction, one column per rule, rule_alert = at
-- least one rule fired.
--
-- Rules only use features from features_transactions (past-only by design).
-- They NEVER use is_fraud or fraud_type: those are kept for evaluation only.
--
-- Thresholds were chosen on the TRAIN period only (days 1-40), by comparing
-- precision / recall / F1 of candidate thresholds; test (41-50) and demo (51-60)
-- were not looked at. Change them here, not in the rule expressions.
-- =============================================================================

{% set burst_min_tx_10min      = 5 %}     -- rule_burst: at least 5 transactions in 10 minutes
{% set micro_max_amount_vs_avg = 0.05 %}  -- rule_micro_amount: amount <= 5% of the customer's past average
{% set new_device_max_hours    = 6 %}     -- rule_new_device_night: device first seen < 6 h ago...
                                          -- ...and transaction between 00:00 and 04:59

with bounds as (
    -- same time split as the ML model: computed from the first transaction
    select min(transaction_time) as t0
    from {{ ref('features_transactions') }}
),

rules as (
    select
        f.transaction_id,
        f.customer_id,
        f.transaction_time,
        case
            when f.transaction_time < dateadd('day', 40, b.t0) then 'train'   -- days 1-40
            when f.transaction_time < dateadd('day', 50, b.t0) then 'test'    -- days 41-50
            else 'demo'                                                       -- days 51-60
        end as dataset_split,

        iff(f.nb_tx_10min >= {{ burst_min_tx_10min }}, 1, 0)                      as rule_burst,
        iff(coalesce(f.amount_vs_avg <= {{ micro_max_amount_vs_avg }}, false), 1, 0) as rule_micro_amount,
        iff(f.device_age_hours < {{ new_device_max_hours }} and f.is_night = 1, 1, 0) as rule_new_device_night,

        -- evaluation only: never used by the rules above
        f.is_fraud,
        f.fraud_type
    from {{ ref('features_transactions') }} f
    cross join bounds b
)

select
    transaction_id,
    customer_id,
    transaction_time,
    dataset_split,
    rule_burst,
    rule_micro_amount,
    rule_new_device_night,
    greatest(rule_burst, rule_micro_amount, rule_new_device_night) as rule_alert,
    is_fraud,
    fraud_type
from rules
