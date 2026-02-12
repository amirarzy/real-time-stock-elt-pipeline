
  create view "market_data"."staging"."stg_market_data__dbt_tmp"
    
    
  as (
    

select
  symbol,
  datetime as datetime_utc,
  open::double precision as open,
  high::double precision as high,
  low::double precision as low,
  close::double precision as close,
  volume::bigint as volume
from "market_data"."public"."market_data"
  );