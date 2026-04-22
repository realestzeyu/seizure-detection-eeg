-- Fails if any (PATIENT_ID, CHANNEL, ALERT_DATE) combination appears more than once.
SELECT PATIENT_ID, CHANNEL, ALERT_DATE, COUNT(*) AS cnt
FROM "eeg"."main"."alerts_daily"
GROUP BY PATIENT_ID, CHANNEL, ALERT_DATE
HAVING COUNT(*) > 1