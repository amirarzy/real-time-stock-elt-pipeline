
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        symbol, datetime_utc
    from "market_data"."staging"."stg_market_data"
    group by symbol, datetime_utc
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test