
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select datetime_utc
from "market_data"."staging"."stg_market_data"
where datetime_utc is null



  
  
      
    ) dbt_internal_test