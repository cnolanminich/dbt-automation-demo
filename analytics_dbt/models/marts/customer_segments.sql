-- Customer segmentation mart
-- This is a TABLE - will use on_cron daily automation

{{ config(materialized='table') }}

with customer_metrics as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        total_orders,
        total_revenue,
        avg_order_value
    from {{ ref('customer_orders') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    total_orders,
    total_revenue,
    avg_order_value,
    case
        when total_revenue >= 500 then 'high_value'
        when total_revenue >= 200 then 'medium_value'
        when total_revenue > 0 then 'low_value'
        else 'no_purchases'
    end as customer_segment,
    case
        when total_orders >= 3 then 'frequent'
        when total_orders >= 1 then 'occasional'
        else 'inactive'
    end as purchase_frequency
from customer_metrics
