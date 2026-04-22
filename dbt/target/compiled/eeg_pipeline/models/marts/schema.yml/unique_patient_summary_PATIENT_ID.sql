
    
    

select
    PATIENT_ID as unique_field,
    count(*) as n_records

from "eeg"."main"."patient_summary"
where PATIENT_ID is not null
group by PATIENT_ID
having count(*) > 1


