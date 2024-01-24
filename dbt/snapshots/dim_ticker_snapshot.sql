{% snapshot dim_ticker_snapshot %}

{{
    config(
      target_database='postgres',
      target_schema='marts',
      unique_key='dim_ticker_id',
      strategy='check',
      check_cols=['dim_ticker_id'],
      invalidate_hard_deletes=False
    )
}}

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
exchange
from {{ ref('stg_tickers') }}
where exchange = 'LSE'

{% endsnapshot %}