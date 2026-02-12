{{ config(materialized='view') }}

select
  symbol,
  datetime as datetime_utc,
  open::double precision as open,
  high::double precision as high,
  low::double precision as low,
  close::double precision as close,
  volume::bigint as volume
from {{ source('market_data_raw', 'market_data') }}
