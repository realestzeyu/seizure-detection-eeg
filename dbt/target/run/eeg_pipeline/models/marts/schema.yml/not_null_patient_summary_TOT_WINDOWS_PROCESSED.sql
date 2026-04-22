
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TOT_WINDOWS_PROCESSED
from "eeg"."main"."patient_summary"
where TOT_WINDOWS_PROCESSED is null



  
  
      
    ) dbt_internal_test