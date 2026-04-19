#!/bin/bash
# Run this when swapping machines or starting fresh.
# Clears all stateful data so the pipeline starts clean.

set -e

echo "Stopping Kafka..."
echo "docker compose down if needed"

echo "Clearing Delta tables..."
rm -rf ./data/delta/eeg_features
rm -rf ./data/delta/eeg_alerts

echo "Clearing Spark checkpoints..."
rm -rf ./data/checkpoints

# powershell commands for windows users

Write-Host "Clearing Delta tables..."
Remove-Item -Recurse -Force ./data/delta/eeg_features -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force ./data/delta/eeg_alerts -ErrorAction SilentlyContinue

Write-Host "Clearing Spark checkpoints..."
Remove-Item -Recurse -Force ./data/checkpoints -ErrorAction SilentlyContinue

echo "Done. Now run:"
echo "  1. docker compose up -d        (start Kafka)"
docker compose up -d
echo "  2. python producer/eeg_producer.py   (need to wait for about 10 seconds before sending EEG data)"
sleep 10s
python producer/eeg_producer.py
echo "  3. python streaming/eeg_stream_processor.py  (process stream)"
python streaming/eeg_stream_processor.py
echo "  4. python batch/eeg_daily_aggregator.py      (run batch join)"
python batch/eeg_daily_aggregator.py
