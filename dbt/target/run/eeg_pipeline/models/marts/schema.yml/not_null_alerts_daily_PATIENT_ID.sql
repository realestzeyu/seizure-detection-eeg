
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PATIENT_ID
from "eeg"."main"."alerts_daily"
where PATIENT_ID is null



  
  
      
    ) dbt_internal_test