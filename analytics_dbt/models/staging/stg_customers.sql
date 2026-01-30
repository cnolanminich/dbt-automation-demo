-- Staging view for customers
-- This is a VIEW - will use on_code_version_changed automation

{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at::date as signup_date
from {{ ref('raw_customers') }}
