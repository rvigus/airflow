{{ config(materialized='view',
   schema='staging'
) }}
select * from {{ source('landing', 'tickers') }}