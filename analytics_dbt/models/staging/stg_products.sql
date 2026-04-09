-- Staging view for product catalog (from non-dbt upstream asset)

{{ config(materialized='view') }}

select
    product_id,
    name as product_name,
    category,
    price::decimal(10,2) as price
from {{ source('external', 'product_catalog') }}
