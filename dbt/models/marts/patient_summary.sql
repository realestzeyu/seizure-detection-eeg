-- patient_summary — one row per patient:

-- total windows processed
-- total alerts fired
-- alert rate (alerts / windows as a percentage)
WITH feature_counts AS (
    SELECT patient_id AS PATIENT_ID,
    COUNT(*) AS TOT_WINDOWS_PROCESSED
    FROM {{ ref('stg_eeg_features') }}
    GROUP BY patient_id
), alert_counts AS (
    SELECT patient_id AS PATIENT_ID,
    COUNT(alert_reason) AS TOT_ALERTS_FIRED
    FROM {{ ref('stg_eeg_alerts') }} 
    GROUP BY patient_id
)
SELECT 
feature_counts.PATIENT_ID,
feature_counts.TOT_WINDOWS_PROCESSED,
alert_counts.TOT_ALERTS_FIRED,
CONCAT(ROUND(COALESCE(alert_counts.TOT_ALERTS_FIRED * 100.0 / feature_counts.TOT_WINDOWS_PROCESSED, 0.00),2),'%') AS ALERT_RATE
FROM feature_counts
LEFT JOIN alert_counts
ON feature_counts.PATIENT_ID = alert_counts.PATIENT_ID