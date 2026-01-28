-- Customer orders mart - aggregated order data per customer
-- This is a TABLE - will use on_cron daily automation

{{ config(materialized='table') }}

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.signup_date,
    count(o.order_id) as total_orders,
    coalesce(sum(o.order_amount), 0) as total_revenue,
    coalesce(avg(o.order_amount), 0) as avg_order_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
group by 1, 2, 3, 4, 5
