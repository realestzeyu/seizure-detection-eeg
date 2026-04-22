-- Fails if any detection_rate value falls outside 0-100.
-- DETECTION_RATE is stored as "14.29%" so we strip the % before casting.
SELECT *
FROM {{ ref('seizure_detection_rate') }}
WHERE CAST(REPLACE(DETECTION_RATE, '%', '') AS FLOAT) < 0
   OR CAST(REPLACE(DETECTION_RATE, '%', '') AS FLOAT) > 100
