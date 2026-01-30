-- Enriched orders staging table
-- This is a TABLE (not view) that refreshes every 2 minutes
-- Used as a dependency for order_summary

{{ config(materialized='table', tags=['refresh_2min']) }}

select
    order_id,
    customer_id,
    order_date::date as order_date,
    amount::decimal(10,2) as order_amount,
    status as order_status,
    -- Enrichment: days since order
    current_date - order_date::date as days_since_order,
    -- Enrichment: order size category
    case
        when amount >= 300 then 'large'
        when amount >= 100 then 'medium'
        else 'small'
    end as order_size_category
from {{ ref('raw_orders') }}
