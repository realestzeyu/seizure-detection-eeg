
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CHANNEL
from "eeg"."main"."alerts_daily"
where CHANNEL is null



  
  
      
    ) dbt_internal_test