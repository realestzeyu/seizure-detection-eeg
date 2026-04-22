
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ALERT_DATE
from "eeg"."main"."alerts_daily"
where ALERT_DATE is null



  
  
      
    ) dbt_internal_test