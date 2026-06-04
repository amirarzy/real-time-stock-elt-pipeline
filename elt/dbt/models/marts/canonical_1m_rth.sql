{{ config(materialized='view') }}

with p as (
  select * from {{ ref('stg_polygon_1m_rth') }}
),
y as (
  select * from {{ ref('stg_yahoo_1m_rth') }}
)
select
  coalesce(p.symbol, y.symbol) as symbol,
  coalesce(p.datetime, y.datetime) as datetime,
  coalesce(p.open,  y.open)  as open,
  coalesce(p.high,  y.high)  as high,
  coalesce(p.low,   y.low)   as low,
  coalesce(p.close, y.close) as close,
  coalesce(p.volume, y.volume) as volume,
  case when p.symbol is not null then 'polygon' else 'yahoo' end as vendor_used
from p
full outer join y
  on p.symbol = y.symbol
 and p.datetime = y.datetime