from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    when,
    abs,
    min,
    max,
    sum,
    mean,
    stddev,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

spark = (
    SparkSession.builder.appName("eeg-stream-processor")
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

# reading from kafka topic eeg-raw
eeg_raw_df = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", "localhost:29092"
    )  # connect to EXTERNAL listener
    .option("subscribe", "eeg-raw")  # subscribe tells spark which topic to read from
    .load()
)

# schema to match the JSON message
schema = StructType(
    [
        StructField("patient_id", StringType(), True),
        StructField("timestamp_ms", DoubleType(), True),
        StructField("channel", StringType(), True),
        StructField("microvolt_value", DoubleType(), True),
        StructField("sample_index", IntegerType(), True),
    ]
)

# column value from kafka is in bytes, so cast to string then parse the JSON using the schema
eeg_parsed_df = eeg_raw_df.select(
    from_json(col=col("value").cast("string"), schema=schema).alias("data")
).select(
    "data.*"
)  # this selects all the fields from the parsed JSON and flattens the dataframe

# withColumn creates a new column event_time that is just in seconds unit
eeg_parsed_df = eeg_parsed_df.withColumn(
    "event_time", to_timestamp(col("timestamp_ms") / 1000)
)

# a little confusing here, but we are extracting the mean, stddev, min, max microvolt values per channel, per patient, in a tumbling window of 5 seconds.
eeg_analytic_df = (
    eeg_parsed_df.withWatermark(
        "event_time", "10 seconds"
    )  # watermark is the threshold for how late data can arrive
    .groupBy(
        window("event_time", "5 seconds"), "patient_id", "channel"
    )  # window is 5s, which means we are grouping data in 5s intervals
    .agg(
        mean("microvolt_value").alias("mean_uv"),
        stddev("microvolt_value").alias("stddev_uv"),
        min("microvolt_value").alias("min_uv"),
        max("microvolt_value").alias("max_uv"),
        sum(
            when(abs(col("microvolt_value")) > 100, 1).otherwise(0)
        ).alias(  # we take the absolute value and check if its > 100mv, if yes then its a spike.
            "spike_count"
        ),
    )
)
