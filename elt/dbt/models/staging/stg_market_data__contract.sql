{{ config(materialized='view') }}

select
  symbol,
  datetime_utc,
  open,
  high,
  low,
  close,
  volume
from {{ ref('stg_market_data') }}
