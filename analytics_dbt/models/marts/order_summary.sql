-- Order summary mart - daily order aggregates
-- This is a TABLE - will use on_cron daily automation
-- Depends on stg_orders_enriched (table, 2min refresh)

{{ config(materialized='table') }}

select
    order_date,
    count(distinct customer_id) as unique_customers,
    count(order_id) as total_orders,
    sum(order_amount) as daily_revenue,
    avg(order_amount) as avg_order_value,
    count(case when order_status = 'completed' then 1 end) as completed_orders,
    count(case when order_status = 'pending' then 1 end) as pending_orders,
    count(case when order_status = 'shipped' then 1 end) as shipped_orders,
    -- From enriched table
    count(case when order_size_category = 'large' then 1 end) as large_orders,
    count(case when order_size_category = 'medium' then 1 end) as medium_orders,
    count(case when order_size_category = 'small' then 1 end) as small_orders
from {{ ref('stg_orders_enriched') }}
group by order_date
order by order_date desc
