
  
    
    

    create  table
      "eeg"."main"."patient_summary__dbt_tmp"
  
    as (
      -- patient_summary — one row per patient:

-- total windows processed
-- total alerts fired
-- alert rate (alerts / windows as a percentage)
SELECT
features.patient_id,
COUNT(DISTINCT(features.window_start)) AS TOT_WINDOWS_PROCESSED,
COUNT(alerts.alert_reason) AS TOT_ALERTS,
CONCAT(ROUND(COUNT(alerts.alert_reason)*100.0/COUNT(DISTINCT(features.window_start)),2), '%') AS ALERT_RATE
FROM "eeg"."main"."stg_eeg_features" AS features
LEFT JOIN "eeg"."main"."stg_eeg_alerts" AS alerts
GROUP BY features.patient_id
    );
  
  