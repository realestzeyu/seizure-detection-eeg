
  
  create view "eeg"."main"."stg_eeg_alerts__dbt_tmp" as (
    SELECT *
FROM delta_scan('../data/delta/eeg_alerts/')
  );
