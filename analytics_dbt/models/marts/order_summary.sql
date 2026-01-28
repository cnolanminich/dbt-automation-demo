-- Order summary mart - daily order aggregates
-- This is a TABLE - will use on_cron daily automation

{{ config(materialized='table') }}

select
    order_date,
    count(distinct customer_id) as unique_customers,
    count(order_id) as total_orders,
    sum(order_amount) as daily_revenue,
    avg(order_amount) as avg_order_value,
    count(case when order_status = 'completed' then 1 end) as completed_orders,
    count(case when order_status = 'pending' then 1 end) as pending_orders,
    count(case when order_status = 'shipped' then 1 end) as shipped_orders
from {{ ref('stg_orders') }}
group by order_date
order by order_date desc
