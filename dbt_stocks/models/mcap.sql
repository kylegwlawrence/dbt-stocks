{{ config(materialized='table') }}

select 
    ticker
    , market_cap
    , latest_quarter as latest_quarter_end
from {{ source('staging','mcap') }}