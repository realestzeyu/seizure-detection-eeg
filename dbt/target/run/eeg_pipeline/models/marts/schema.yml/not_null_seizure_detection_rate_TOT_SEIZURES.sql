
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TOT_SEIZURES
from "eeg"."main"."seizure_detection_rate"
where TOT_SEIZURES is null



  
  
      
    ) dbt_internal_test