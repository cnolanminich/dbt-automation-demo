-- Staging view for orders
-- This is a VIEW - will use on_code_version_changed automation

{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    order_date::date as order_date,
    amount::decimal(10,2) as order_amount,
    status as order_status
from {{ ref('raw_orders') }}
