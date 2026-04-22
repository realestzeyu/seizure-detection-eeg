
  
    
    

    create  table
      "eeg"."main"."alerts_daily__dbt_tmp"
  
    as (
      -- alerts_daily — one row per patient + channel + date:

-- count of alerts
-- breakdown by alert_reason

WITH alert_counts AS (
    SELECT patient_id AS PATIENT_ID,
    channel AS CHANNEL,
    event_date AS ALERT_DATE,
    COUNT(CASE WHEN alert_reason = 'spike_threshold' THEN 1 ELSE NULL END) AS SPIKE_THRESHOLD_CNT,
    COUNT(CASE WHEN alert_reason = 'high_variance' THEN 1 ELSE NULL END) AS HIGH_VAR_CNT
    FROM "eeg"."main"."stg_eeg_alerts"
    GROUP BY patient_id, channel, event_date
)
SELECT 
PATIENT_ID,
CHANNEL,
ALERT_DATE,
SPIKE_THRESHOLD_CNT,
HIGH_VAR_CNT
FROM alert_counts
    );
  
  