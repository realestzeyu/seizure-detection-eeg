-- seizure_detection_rate — the headline metric:

-- join stg_eeg_alerts against CHB-MIT annotation data
-- how many labeled seizure windows had at least one alert overlap
-- detection rate as a percentage
-- false positive count
-- (
--     col("alerts.window_start") < col("annotations.seizure_end_ms")
-- )  # if alert window is within seizure start and end time, then we consider it as detected
-- & (col("alerts.window_end") > col("annotations.seizure_start_ms"))
-- & (col("alerts.patient_id") == col("annotations.patient_id")),

WITH fp AS(
    SELECT alerts.* -- select all the alerts that are alerted but not in a window
    FROM "eeg"."main"."stg_eeg_alerts" alerts
    WHERE NOT EXISTS( -- if the below returns 1, means there is an overlap/ we actually detected it. so we exclude those. NOT EXISTS returns true false row by row
        SELECT 1 -- select everything that is true
        FROM "eeg"."main"."stg_eeg_annotations" ann -- so any alerts that do not exist within the seizure window will return 1
        WHERE alerts.window_start < ann.seizure_end_ms
        AND alerts.window_end > ann.seizure_start_ms
        AND alerts.patient_id = ann.patient_id
    )
)
SELECT 
COUNT(DISTINCT ann.patient_id) AS DETECTED,
(SELECT COUNT(DISTINCT seizure_start_ms || patient_id) FROM "eeg"."main"."stg_eeg_annotations") AS TOT_SEIZURES,
CONCAT(ROUND(COUNT(DISTINCT ann.patient_id) * 100.0 / (SELECT COUNT(DISTINCT seizure_start_ms || patient_id) FROM "eeg"."main"."stg_eeg_annotations") , 2), '%') AS DETECTION_RATE,
(SELECT COUNT(*) FROM fp WHERE fp.patient_id = alerts.patient_id) AS FP_COUNT -- need WHERE patient_id match cuz we group by patient_id below
FROM "eeg"."main"."stg_eeg_alerts" alerts
JOIN "eeg"."main"."stg_eeg_annotations" ann
ON alerts.window_start < ann.seizure_end_ms
AND alerts.window_end > ann.seizure_start_ms
AND alerts.patient_id = ann.patient_id 
GROUP BY alerts.patient_id