-- Intermediate view: join customers with their orders and demographics
-- View L2 - depends on stg_customers, stg_orders, stg_customer_demographics (views)
-- stg_customer_demographics sources from non-dbt asset, threading it through the full lineage

{{ config(materialized='view') }}

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.signup_date,
    d.country,
    d.region,
    d.age_group,
    d.household_income,
    o.order_id,
    o.order_date,
    o.order_amount,
    o.order_status,
    o.order_date - c.signup_date as days_from_signup_to_order
from {{ ref('stg_customers') }} c
inner join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
left join {{ ref('stg_customer_demographics') }} d on c.customer_id = d.customer_id
