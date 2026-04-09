-- Staging view for web events (from non-dbt upstream asset)

{{ config(materialized='view') }}

select
    event_id,
    customer_id,
    event_type,
    page_url,
    event_timestamp::timestamp as event_timestamp
from {{ source('external', 'web_events') }}
