
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TOT_ALERTS_FIRED
from "eeg"."main"."patient_summary"
where TOT_ALERTS_FIRED is null



  
  
      
    ) dbt_internal_test