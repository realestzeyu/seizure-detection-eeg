
    
    

with all_values as (

    select
        alert_reason as value_field,
        count(*) as n_records

    from "eeg"."main"."stg_eeg_alerts"
    group by alert_reason

)

select *
from all_values
where value_field not in (
    'spike_threshold','high_variance'
)


