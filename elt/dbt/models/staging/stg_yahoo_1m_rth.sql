{{ config(materialized='view') }}

select
  symbol,
  datetime,
  open,
  high,
  low,
  close,
  volume,
  'yahoo'::text as vendor
from public.market_data
where
  (datetime at time zone 'America/New_York')::time >= time '09:30'
  and (datetime at time zone 'America/New_York')::time <  time '16:00'