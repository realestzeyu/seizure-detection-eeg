from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_date, count_distinct, count, when, lag
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta, date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    DateType,
)

expected_schema = StructType(
    [
        StructField("patient_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField("mean_v", DoubleType(), True),
        StructField("stddev_v", DoubleType(), True),
        StructField("min_v", DoubleType(), True),
        StructField("max_v", DoubleType(), True),
        StructField("spike_count", LongType(), True),
        # StructField("alert_reason", StringType(), True),
        StructField("event_date", DateType(), True),
    ]
)

# i asked claude to give me the possible checks then i implemented them. 14 checks total
"""
Cannot use count(when(...)) — require a separate job:                         

- Check 2 — needs groupBy("window_start").count() to see per-group counts     
- Check 12 — needs groupBy(patient_id, channel, window_start).count() to find
duplicates                                                                    
- Check 13 — needs a window function (lag) to compare consecutive rows      
                                                                            
Can use count(when(...)) — fold into single .agg():                           
                                                                            
- Check 1 — null check (row-level condition)                                  
- Check 3 — channel name check (row-level condition)
- Check 4 — µV range (row-level condition)                                    
- Check 5 — min ≤ mean ≤ max (row-level condition)
- Check 6 — stddev ≤ range (row-level condition)                              
- Check 7 — non-negative check (row-level condition)
- Check 8 — spike_count vs window duration (row-level condition)              
- Check 9 — window_end > window_start (row-level condition)                   
- Check 11 — event_date matches window_start date (row-level condition)       
- Check 14 — patient_id format (row-level condition)   
"""


def validate_eeg_features(df):
    errors = []

    # start off with schema check
    actual = {f.name: type(f.dataType) for f in df.schema.fields}
    for field in expected_schema.fields:
        if field.name not in actual:
            errors.append(f"schema_check: missing column {field.name}")
        elif type(field.dataType) != actual[field.name]:
            errors.append(
                f"schema_check: {field.name} expected {field.dataType} got {actual[field.name]}"
            )

    # 2, 12 and 13 cannot be expressed as count(when())
    # 2. Every window_start has exactly 23 rows
    window_check = df.groupBy("window_start").count().filter(col("count") != 23).count()
    # 12. No duplicate (patient_id, channel, window_start) rows
    duplicate_check = (
        df.groupBy(["patient_id", "channel", "window_start"])
        .count()
        .filter(col("count") > 1)
        .count()
    )
    # 13. For each (patient_id, channel), no unexplained gaps between consecutive windows
    w = Window.partitionBy("patient_id", "channel").orderBy("window_start")
    gap_check = (
        df.withColumn("prev_end", lag("window_end").over(w))
        .filter(col("prev_end").isNotNull() & (col("window_start") != col("prev_end")))
        .count()
    )

    # 2
    if window_check > 0:
        errors.append(
            f"window_start_count: {window_check} rows do not have 23 channels"
        )
    # 12
    if duplicate_check > 0:
        errors.append(f"duplicate_check: {duplicate_check} rows have duplicates")
    # 13
    if gap_check > 0:
        errors.append(f"gap_check: {gap_check} rows have a gap before them")

    # remaining checks can be expressed as count(when()) and they are aggregated into one row result, and we will extract the values and print them out if there are any errors, then raise exception at the end if there are any errors.
    result = df.agg(
        # 1. No nulls in any column
        count(
            when(
                col("patient_id").isNull()
                | col("window_start").isNull()
                | col("window_end").isNull()
                | col("channel").isNull()
                | col("mean_v").isNull()
                | col("stddev_v").isNull()
                | col("min_v").isNull()
                | col("max_v").isNull()
                | col("spike_count").isNull()
                | col("event_date").isNull(),
                1,
            )
        ).alias("null_check"),
        # 3. Channel names match the expected set exactly (no extras, no unknowns)
        count(
            when(
                ~col("channel").isin(
                    [
                        "FP1-F7",
                        "F7-T7",
                        "T7-P7",
                        "P7-O1",
                        "FP1-F3",
                        "F3-C3",
                        "C3-P3",
                        "P3-O1",
                        "FP2-F4",
                        "F4-C4",
                        "C4-P4",
                        "P4-O2",
                        "FP2-F8",
                        "F8-T8",
                        "T8-P8-0",
                        "P8-O2",
                        "FZ-CZ",
                        "CZ-PZ",
                        "P7-T7",
                        "T7-FT9",
                        "FT9-FT10",
                        "FT10-T8",
                        "T8-P8-1",
                    ]
                ),
                1,
            )
        ).alias("channel_check"),
        # 4. min_v, mean_v, max_v all within [-500, +500] µV
        count(
            when(
                (col("min_v") < -500)
                | (col("min_v") > 500)
                | (col("mean_v") < -500)
                | (col("mean_v") > 500)
                | (col("max_v") < -500)
                | (col("max_v") > 500),
                1,
            )
        ).alias("v_range_check"),
        # 5. min_v ≤ mean_v ≤ max_v for every row
        count(
            when((col("min_v") > col("mean_v")) | (col("mean_v") > col("max_v")), 1)
        ).alias("v_order_check"),
        # 6. stddev_v ≤ (max_v - min_v) for every row
        count(when(col("stddev_v") > (col("max_v") - col("min_v")), 1)).alias(
            "stddev_check"
        ),
        # 7. spike_count ≥ 0 and stddev_v ≥ 0
        count(when((col("spike_count") < 0) | (col("stddev_v") < 0), 1)).alias(
            "spike_stddev_check"
        ),
        # 8. spike_count ≤ samples_per_window (256 Hz × window duration in seconds)
        count(
            when(
                col("spike_count")
                > (
                    256
                    * (
                        col("window_end").cast("long")
                        - col("window_start").cast("long")
                    )
                ),
                1,
            )
        ).alias("spike_count_check"),
        # 9. window_end > window_start
        count(when(col("window_end") <= col("window_start"), 1)).alias(
            "window_time_check"
        ),
        # 10. Window duration is consistent — window_end - window_start is the same value across all rows
        count_distinct(
            (col("window_end").cast("long") - col("window_start").cast("long"))
        ).alias("distinct_duration_check"),
        # 11. event_date == date(window_start) for every row
        count(
            when(
                col("event_date") != to_date(col("window_start")), 1
            )  # we will reuse the event_date column here, even though it is not in the dataframe, but we can still use it in the validation since it is derived from the window_start column which is in the dataframe, and if there are any errors then it means there is an issue with the window_start column which is already captured in other checks
        ).alias("event_date_check"),
        # 13. For each (patient_id, channel), no unexplained gaps between consecutive windows
        # idk how to do this help
        # 14. patient_id matches expected format (e.g. chb\d{2})
        count(when(~col("patient_id").rlike(r"^chb\d{2}$"), 1)).alias(
            "patient_id_format_check"
        ),
    ).collect()[
        0
    ]  # result is a row obj, and we will extract the first row since there is only 1 row after aggregation

    # 1
    if result.null_check > 0:
        errors.append(f"null_check: {result['null_check']} rows")
    # 3
    if result.channel_check > 0:
        errors.append(f"channel_check: {result['channel_check']} rows")
    # 4
    if result.v_range_check > 0:
        errors.append(f"v_range_check: {result['v_range_check']} rows")
    # 5
    if result.v_order_check > 0:
        errors.append(f"v_order_check: {result['v_order_check']} rows")
    # 6
    if result.stddev_check > 0:
        errors.append(f"stddev_check: {result['stddev_check']} rows")
    # 7
    if result.spike_stddev_check > 0:
        errors.append(f"spike_stddev_check: {result['spike_stddev_check']} rows")
    # 8
    if result.spike_count_check > 0:
        errors.append(f"spike_count_check: {result['spike_count_check']} rows")
    # 9
    if result.window_time_check > 0:
        errors.append(f"window_time_check: {result['window_time_check']} rows")
    # 10
    if result.distinct_duration_check != 1:
        errors.append(
            f"distinct_duration_check: {result['distinct_duration_check']} distinct durations found, expected 1"
        )
    # 11
    if result.event_date_check > 0:
        errors.append(f"event_date_check: {result['event_date_check']} rows")
    # 14
    if result.patient_id_format_check > 0:
        errors.append(
            f"patient_id_format_check: {result['patient_id_format_check']} rows"
        )

    if errors:
        for e in errors:
            print(f"VALIDATION FAILED: {e}")
        raise ValueError(f"Validation failed with {len(errors)} error(s)")

    """
    below is old validation code, less efficient because of many .count() calls, but more readable. kept for understanding
    """
    # 1. No nulls in any column
    # assert (
    #     df.filter(
    #         col("patient_id").isNull()
    #         | col("window_start").isNull()
    #         | col("window_end").isNull()
    #         | col("channel").isNull()
    #         | col("mean_v").isNull()
    #         | col("stddev_v").isNull()
    #         | col("min_v").isNull()
    #         | col("max_v").isNull()
    #         | col("spike_count").isNull()
    #         | col("event_date").isNull()
    #     ).count()
    #     == 0
    # ), "There are null values in the dataframe"

    # # 2. Every window_start has exactly 23 rows
    # assert (
    #     df.groupBy("window_start").count().filter(col("count") != 23).count() == 0
    # ), "Not every window_start has 23 rows for the channels"

    # # 3. Channel names match the expected set exactly (no extras, no unknowns)
    # assert set(
    #     [row.channel for row in df.select("channel").distinct().collect()]
    # ) == set(
    #     [
    #         "FP1-F7",
    #         "F7-T7",
    #         "T7-P7",
    #         "P7-O1",
    #         "FP1-F3",
    #         "F3-C3",
    #         "C3-P3",
    #         "P3-O1",
    #         "FP2-F4",
    #         "F4-C4",
    #         "C4-P4",
    #         "P4-O2",
    #         "FP2-F8",
    #         "F8-T8",
    #         "T8-P8-0",
    #         "P8-O2",
    #         "FZ-CZ",
    #         "CZ-PZ",
    #         "P7-T7",
    #         "T7-FT9",
    #         "FT9-FT10",
    #         "FT10-T8",
    #         "T8-P8-1",
    #     ]
    # ), "Channel names do not match the expected set"

    # # 4. min_v, mean_v, max_v all within [-500, +500] µV
    # assert (
    #     df.filter(
    #         (col("min_v") < -500)
    #         | (col("min_v") > 500)
    #         | (col("mean_v") < -500)
    #         | (col("mean_v") > 500)
    #         | (col("max_v") < -500)
    #         | (col("max_v") > 500)
    #     ).count()
    #     == 0
    # ), "min_v, mean_v, or max_v values are out of the expected range [-500, +500] µV"

    # # 5. min_v ≤ mean_v ≤ max_v for every row
    # assert (
    #     df.filter(
    #         (col("min_v") > col("mean_v")) | (col("mean_v") > col("max_v"))
    #     ).count()
    #     == 0
    # ), "min_v ≤ mean_v ≤ max_v does not hold for some rows"

    # # 6. stddev_v ≤ (max_v - min_v) for every row
    # assert (
    #     df.filter(col("stddev_v") > (col("max_v") - col("min_v"))).count() == 0
    # ), "stddev_v ≤ (max_v - min_v) does not hold for some rows"

    # # 7. spike_count ≥ 0 and stddev_v ≥ 0
    # assert (
    #     df.filter((col("spike_count") < 0) | (col("stddev_v") < 0)).count() == 0
    # ), "spike_count ≥ 0 and stddev_v ≥ 0 does not hold for some rows"

    # # 8. spike_count ≤ samples_per_window (256 Hz × window duration in seconds)
    # assert (
    #     df.filter(
    #         col("spike_count")
    #         > (
    #             256
    #             * (col("window_end").cast("long") - col("window_start").cast("long"))
    #         )
    #     ).count()
    #     == 0
    # ), "spike_count < window duration does not hold for some rows"

    # # 9. window_end > window_start
    # assert (
    #     df.filter(col("window_end") <= col("window_start")).count() == 0
    # ), "window_end > window_start does not hold for some rows"

    # # 10. Window duration is consistent — window_end - window_start is the same value across all rows
    # window_durations = (
    #     df.select(
    #         (col("window_end").cast("long") - col("window_start").cast("long")).alias(
    #             "window_duration"
    #         )
    #     )
    #     .distinct()
    #     .collect()
    # )
    # assert (
    #     len(window_durations) == 1
    # ), "Window duration is not consistent across all rows"

    # # 11. event_date == date(window_start) for every row

    # # 12. No duplicate (patient_id, channel, window_start) rows
    # assert (
    #     df.groupBy(["patient_id", "channel", "window_start"])
    #     .count()
    #     .filter(col("count") > 1)
    #     .count()
    #     == 0
    # ), "Duplicated rows for column patient_id, channel, window_start"

    # # 13. For each (patient_id, channel), no unexplained gaps between consecutive windows
    # # not sure how to do this

    # # 14. patient_id matches expected format (e.g. chb\d{2})
    # assert (
    #     df.filter(~col("patient_id").rlike(r"^chb\d{2}$")).count() == 0
    # ), "patient_id does not match format of chb\d{2}"


def main():
    # lets run it now
    spark = (
        SparkSession.builder.appName("eeg-validation")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    # test — run your validation, confirm it catches null_check and v_range_check
    df = spark.read.format("delta").load("./data/delta/eeg_features")
    validate_eeg_features(df)
    """
    Uncomment below to insert a BAD ROW to test the validation pipeline works.
    """
    # bad_row = spark.createDataFrame(
    #     [
    #         Row(
    #             patient_id="chb1234",
    #             channel="FP1-F7",
    #             window_start=datetime.fromtimestamp(
    #                 1776664741
    #             ),  # timestamp of when i edit this lol
    #             window_end=datetime.fromtimestamp(1776664741)
    #             + timedelta(seconds=5),  # 5 seconds later
    #             mean_v=999.0,
    #             stddev_v=0.0,
    #             min_v=999.0,
    #             max_v=999.0,
    #             spike_count=0,
    #             # alert_reason=None,
    #             event_date=date(2023, 1, 1),
    #         )
    #     ],
    #     schema=expected_schema,
    # )
    # bad_row.write.format("delta").mode("append").save("./data/delta/eeg_features")
    # delete
    dt = DeltaTable.forPath(spark, "./data/delta/eeg_features")
    dt.delete(col("mean_v") == 999.0)  # delete the bad row we just inserted for testing
    print(f"Validation passed. {df.count()} rows checked.")
    spark.stop()


if __name__ == "__main__":
    main()
