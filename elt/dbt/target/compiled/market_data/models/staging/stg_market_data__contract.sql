

select
  symbol,
  datetime_utc,
  open,
  high,
  low,
  close,
  volume
from "market_data"."staging"."stg_market_data"