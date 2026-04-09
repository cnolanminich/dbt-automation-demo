-- Staging view for customer demographics (from non-dbt upstream asset)
-- Threads through: int_customer_order_details → int_order_patterns → int_customer_scoring → customer_lifetime_value

{{ config(materialized='view') }}

select
    customer_id,
    country,
    region,
    age_group,
    gender,
    household_income::decimal(10,2) as household_income
from {{ source('external', 'customer_demographics') }}
