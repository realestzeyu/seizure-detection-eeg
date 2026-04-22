
  
  create view "eeg"."main"."stg_eeg_features__dbt_tmp" as (
    SELECT *
FROM delta_scan('../data/delta/eeg_features/')
  );
