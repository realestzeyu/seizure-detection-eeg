
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DETECTION_RATE
from "eeg"."main"."seizure_detection_rate"
where DETECTION_RATE is null



  
  
      
    ) dbt_internal_test