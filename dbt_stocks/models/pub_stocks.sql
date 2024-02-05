{{ config(schema='serving', materialized='table') }}

select * from {{ ref('fact_stocks') }}  