{{ config(materialized='table') }}

select 
	ticker
	, date as business_date
	, open
	, close
	, adjusted_close
	, volume
	, high
	, low
	, load_timestamp as load_to_staging_timestamp_local
	, now() as load_to_dbt_timestamp_utc
from {{ source('staging','stocks') }}