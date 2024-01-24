{{ config(materialized='table',
   schema='marts'
) }}

select
{{ dbt_utils.generate_surrogate_key(['ticker']) }} AS dim_ticker_id,
ticker,
symbol,
longname as name,
country
industry,
website,
sector,
upper(currency) as currency,
exchange,
current_timestamp::timestamp as row_created_ts
from {{ ref('stg_tickers') }}
where exchange = 'LSE'
