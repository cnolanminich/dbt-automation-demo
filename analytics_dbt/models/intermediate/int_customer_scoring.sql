-- Intermediate view: score customers based on order patterns
-- View L4 - depends on int_order_patterns (view)

{{ config(materialized='view') }}

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
    completed_orders,
    completed_revenue,

    -- Frequency score (1-5)
    case
        when order_count >= 5 then 5
        when order_count >= 3 then 4
        when order_count >= 2 then 3
        when order_count = 1 then 2
        else 1
    end as frequency_score,

    -- Monetary score (1-5)
    case
        when total_spent >= 500 then 5
        when total_spent >= 300 then 4
        when total_spent >= 150 then 3
        when total_spent >= 50 then 2
        else 1
    end as monetary_score,

    -- Completion rate
    case
        when order_count > 0
        then round(completed_orders::decimal / order_count, 2)
        else 0
    end as completion_rate,

    -- Reliability score (1-5) based on completion rate
    case
        when order_count = 0 then 1
        when completed_orders::decimal / order_count >= 0.9 then 5
        when completed_orders::decimal / order_count >= 0.7 then 4
        when completed_orders::decimal / order_count >= 0.5 then 3
        when completed_orders::decimal / order_count >= 0.3 then 2
        else 1
    end as reliability_score

from {{ ref('int_order_patterns') }}
