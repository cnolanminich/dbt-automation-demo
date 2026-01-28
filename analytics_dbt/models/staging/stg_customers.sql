-- Staging view for customers
-- This is a VIEW - will use on_code_version_changed automation

{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    last_name as last_name_2,
    email,
    created_at::date as signup_date
from {{ ref('raw_customers') }}
