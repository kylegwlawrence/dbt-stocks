{{ config(materialized='table') }}

select 
    ticker
    , metric
    , value
    , load_timestamp as load_to_staging_timestamp_local
	, now() as load_to_dbt_timestamp_utc
from {{ source('staging','overview') }}