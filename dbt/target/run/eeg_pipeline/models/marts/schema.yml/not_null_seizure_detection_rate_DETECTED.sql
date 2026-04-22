
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DETECTED
from "eeg"."main"."seizure_detection_rate"
where DETECTED is null



  
  
      
    ) dbt_internal_test