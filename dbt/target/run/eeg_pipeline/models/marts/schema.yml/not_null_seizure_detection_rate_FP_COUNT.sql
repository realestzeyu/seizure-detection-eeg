
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select FP_COUNT
from "eeg"."main"."seizure_detection_rate"
where FP_COUNT is null



  
  
      
    ) dbt_internal_test