{{ config(materialized='view',
   schema='staging',
   alias='tickers'
) }}
select * from {{ source('landing', 'tickers') }}