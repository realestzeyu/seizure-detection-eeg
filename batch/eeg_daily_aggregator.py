# Read eeg_alerts Delta table
# Parse the annotation file into a Spark DataFrame
# Join them on patient_id and overlapping time windows
# Compute and print:

# Total labeled seizures in the annotation file
# How many had at least one alert overlap (detected)
# Detection rate as a percentage
# Any alerts that didn't overlap a seizure (false positives)

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
)
import mne
from datetime import datetime, timezone
import os

patient_id = "chb01"


def parse_summary_txt(file_path) -> pd.DataFrame:
    with open(file_path, "r") as file:
        lines = file.readlines()
    blocks = []
    current_block = None
    for line in lines:
        # each block starts with File Name
        if line.startswith("File Name"):
            # if we are in a block and the new line is File Name, append it to data and start a new block
            if current_block is not None:
                blocks.append(current_block)
            current_block = [line]  # start a new block
        # if we are in a block and the new line is not File Name, keep appending lines to it until we hit the next block
        elif current_block is not None:
            current_block.append(line)
    # if last block, the if startswith does not work. so just append whatever thats in current_block into data
    blocks.append(current_block)
    seizure_data = []
    for block in blocks:
        print(
            block
        )  # this is to debug if the parsing is correct, not necessary in prod
        numberofseizures = block[3].split(":", 1)[1].strip()
        if numberofseizures != "0":
            filename = block[0].split(":", 1)[1].strip()
            seizurestarttime = int((block[4].split(":", 1)[1]).split()[0]) * 1000
            seizureendtime = int((block[5].split(":", 1)[1]).split()[0]) * 1000

            edf_dir = os.path.dirname(file_path)
            # comment this if part out (till continue) if you dont have the EDF files downloaded
            if not os.path.exists(f"{edf_dir}/{filename}"):
                print(
                    f"EDF file {filename} not found in {edf_dir}, skipping this record."
                )
                continue
            raw = mne.io.read_raw_edf(f"{edf_dir}/{filename}", preload=False)
            meas_date_ms = int(raw.info["meas_date"].timestamp() * 1000)

            seizure_data.append(
                {
                    "patient_id": filename[:5],
                    "recording_file": filename,
                    "seizure_start_ms": datetime.fromtimestamp(
                        (meas_date_ms + seizurestarttime) / 1000, tz=timezone.utc
                    ),
                    "seizure_end_ms": datetime.fromtimestamp(
                        (meas_date_ms + seizureendtime) / 1000, tz=timezone.utc
                    ),
                }
            )

    return pd.DataFrame(seizure_data)


spark = (
    SparkSession.builder.appName("eeg-daily-aggregator")
    .config(
        "spark.jars.packages",  # java archives is a bundled library file. Spark is built on top of JVM, so need these jars.
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "io.delta:delta-spark_2.12:3.2.0",
    )
    .config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )  # delta lake extension
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )  # table metadata for delta lake
    .getOrCreate()
)

# read from delta table
eeg_alerts_spark_df = spark.read.format("delta").load("./data/delta/eeg_alerts")
# read from txt file
seizure_annotations_spark_df = spark.createDataFrame(
    parse_summary_txt(
        f"data/raw/physionet.org/files/chbmit/1.0.0/{patient_id}/{patient_id}-summary.txt"
    )
)

alerts = eeg_alerts_spark_df.alias("alerts")  # to give alias to alert tables
annotations = seizure_annotations_spark_df.alias(
    "annotations"
)  # alias for annotations table

annotations.write.format("delta").mode("overwrite").save(
    "./data/delta/eeg_annotations"
)  # write annotations to delta lake for future use

# if alert window overlaps with seizure start and end time, then its detected
joined_df = alerts.join(
    annotations,
    (
        col("alerts.window_start") < col("annotations.seizure_end_ms")
    )  # if alert window is within seizure start and end time, then we consider it as detected
    & (col("alerts.window_end") > col("annotations.seizure_start_ms"))
    & (col("alerts.patient_id") == col("annotations.patient_id")),
)

# detected_df contains the unique seizures that were detected by at least 1 alert
detected_df = joined_df.select(
    "annotations.patient_id",
    "annotations.recording_file",
    "annotations.seizure_start_ms",
    "annotations.seizure_end_ms",
).distinct()
detected_count = detected_df.count()
total_seizures = seizure_annotations_spark_df.count()
detection_rate = (detected_count / total_seizures) * 100
print(f"Total seizures: {total_seizures}")
print(f"Detected seizures: {detected_count}")
print(f"Detection rate: {detection_rate:.2f}%")

false_positives_df = alerts.join(
    annotations,
    (col("alerts.window_start") < col("annotations.seizure_end_ms"))
    & (col("alerts.window_end") > col("annotations.seizure_start_ms"))
    & (col("alerts.patient_id") == col("annotations.patient_id")),
    how="left_anti",  # left_anti join returns only NO MATCH
)
print(f"False positives: {false_positives_df.count()}")
print(
    f"False positive rate: {(false_positives_df.count() / alerts.count()) * 100:.2f}%"
)

eeg_alerts_spark_df.show(5)
joined_df.show(5)
seizure_annotations_spark_df.show(5)
detected_df.show(5)
false_positives_df.show(5)
