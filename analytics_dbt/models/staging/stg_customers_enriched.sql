-- Enriched customers staging table
-- This is a TABLE (not view) that refreshes every 2 minutes
-- Used as a dependency for customer_orders

{{ config(materialized='table', tags=['refresh_2min']) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at::date as signup_date,
    -- Enrichment: calculate days since signup
    current_date - created_at::date as days_since_signup,
    -- Enrichment: email domain
    split_part(email, '@', 2) as email_domain
from {{ ref('raw_customers') }}
