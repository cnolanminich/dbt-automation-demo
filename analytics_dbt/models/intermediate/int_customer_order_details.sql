-- Intermediate view: join customers with their orders
-- View L2 - depends on stg_customers (view) and stg_orders (view)

{{ config(materialized='view') }}

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.signup_date,
    o.order_id,
    o.order_date,
    o.order_amount,
    o.order_status,
    o.order_date - c.signup_date as days_from_signup_to_order
from {{ ref('stg_customers') }} c
inner join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
