-- Customer lifetime value mart
-- Final table that depends on the full intermediate view chain:
--   stg_customers + stg_orders -> int_customer_order_details -> int_order_patterns -> int_customer_scoring -> HERE

{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    order_count,
    total_spent,
    avg_order_amount,
    first_order_date,
    last_order_date,
    order_span_days,
    completed_revenue,
    frequency_score,
    monetary_score,
    reliability_score,
    completion_rate,

    -- Combined CLV score (weighted average of component scores)
    round(
        (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20)::decimal,
        2
    ) as clv_score,

    -- CLV tier based on combined score
    case
        when (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20) >= 4.0 then 'platinum'
        when (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20) >= 3.0 then 'gold'
        when (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20) >= 2.0 then 'silver'
        else 'bronze'
    end as clv_tier

from {{ ref('int_customer_scoring') }}
