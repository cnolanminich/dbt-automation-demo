-- Staging view for enriched web sessions (from non-dbt mid-pipeline asset)
-- Sits downstream of the enriched_web_sessions Python asset

{{ config(materialized='view') }}

select
    customer_id,
    session_start,
    session_end,
    event_count,
    page_views,
    add_to_carts,
    purchases,
    converted
from {{ source('external', 'enriched_web_sessions') }}
