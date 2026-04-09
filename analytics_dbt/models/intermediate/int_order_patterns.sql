-- Intermediate view: analyze ordering patterns per customer
-- View L3 - depends on int_customer_order_details (view)

{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    count(order_id) as order_count,
    sum(order_amount) as total_spent,
    avg(order_amount) as avg_order_amount,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    max(order_date) - min(order_date) as order_span_days,
    count(case when order_status = 'completed' then 1 end) as completed_orders,
    sum(case when order_status = 'completed' then order_amount else 0 end) as completed_revenue
from {{ ref('int_customer_order_details') }}
group by customer_id, first_name, last_name, email, signup_date
