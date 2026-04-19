from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count_distinct

def validate_eeg_features(df):
    # 1. No nulls in any column
    assert df.filter(
        col("patient_id").isNull()
        | col("window_start").isNull()
        | col("window_end").isNull()
        | col("channel").isNull()
        | col("mean_uv").isNull()
        | col("stddev_uv").isNull()
        | col("min_uv").isNull()
        | col("max_uv").isNull()
        | col("spike_count").isNull()
        | col("event_date").isNull()
    ).count() == 0, "There are null values in the dataframe"

    # 2. Every window_start has exactly 23 rows
    assert count_distinct(df.groupBy("window_start").count()) == 23, "Not every window_start has 23 rows for the channels"

    # 3. Channel names match the expected set exactly (no extras, no unknowns)
    assert df.select("channel").distinct().collect() == set(['FP1-F7', 'F7-T7', 'T7-P7', 'P7-O1', 'FP1-F3', 'F3-C3', 'C3-P3', 'P3-O1', 'FP2-F4', 'F4-C4', 'C4-P4', 'P4-O2', 'FP2-F8', 'F8-T8', 'T8-P8-0', 'P8-O2', 'FZ-CZ', 'CZ-PZ', 'P7-T7', 'T7-FT9', 'FT9-FT10', 'FT10-T8', 'T8-P8-1']), "Channel names do not match the expected set"

    # 4. min_uv, mean_uv, max_uv all within [-500, +500] µV
    assert df.filter(
        (col("min_uv") < -500) | (col("min_uv") > 500) |
        (col("mean_uv") < -500) | (col("mean_uv") > 500) |
        (col("max_uv") < -500) | (col("max_uv") > 500)
    ).count() == 0, "min_uv, mean_uv, or max_uv values are out of the expected range [-500, +500] µV"

    # 5. min_uv ≤ mean_uv ≤ max_uv for every row
    assert df.filter(
        (col("min_uv") > col("mean_uv")) | (col("mean_uv") > col("max_uv"))
    ).count() == 0, "min_uv ≤ mean_uv ≤ max_uv does not hold for some rows"

    # 6. stddev_uv ≤ (max_uv - min_uv) for every row
    assert df.filter(
        col("stddev_uv") > (col("max_uv") - col("min_uv"))
    ).count() == 0, "stddev_uv ≤ (max_uv - min_uv) does not hold for some rows"

    # 7. spike_count ≥ 0 and stddev_uv ≥ 0
    assert df.filter(
        col("spike_count") < 0 | col("stddev_uv") < 0
    ).count() == 0, "spike_count ≥ 0 and stddev_uv ≥ 0 does not hold for some rows"

    # 8. spike_count ≤ samples_per_window (256 Hz × window duration in seconds)
    assert df.filter(
        col("spike_count") > (256 * (col("window_end").cast("long") - col("window_start").cast("long")) / 1000)
    ).count() == 0, "spike_count < window duration does not hold for some rows"
    
    # 9. window_end > window_start
    assert df.filter(
        col("window_end") <= col("window_start")
    ).count() == 0, "window_end > window_start does not hold for some rows"

    # 10. Window duration is consistent — window_end - window_start is the same value across all rows
    window_durations = df.select((col("window_end").cast("long") - col("window_start").cast("long")).alias("window_duration")).distinct().collect()
    assert len(window_durations) == 1, "Window duration is not consistent across all rows"

    # 11. event_date == date(window_start) for every row
    # there is no event_date column, unless in the file path

    # 12. No duplicate (patient_id, channel, window_start) rows
    # 13. For each (patient_id, channel), no unexplained gaps between consecutive windows
    # 14. patient_id matches expected format (e.g. chb\d{2})