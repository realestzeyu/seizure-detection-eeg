
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ALERT_RATE
from "eeg"."main"."patient_summary"
where ALERT_RATE is null



  
  
      
    ) dbt_internal_test