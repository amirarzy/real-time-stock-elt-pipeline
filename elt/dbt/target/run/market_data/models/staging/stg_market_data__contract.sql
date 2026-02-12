
  create view "market_data"."staging"."stg_market_data__contract__dbt_tmp"
    
    
  as (
    

select
  symbol,
  datetime_utc,
  open,
  high,
  low,
  close,
  volume
from "market_data"."staging"."stg_market_data"
  );