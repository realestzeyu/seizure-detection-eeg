
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PATIENT_ID as unique_field,
    count(*) as n_records

from "eeg"."main"."patient_summary"
where PATIENT_ID is not null
group by PATIENT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test