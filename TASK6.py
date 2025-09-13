
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, current_timestamp, to_json, struct, lit, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("IoT-Alerts-VSCode")
    # Add connector Kafka
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bootstrap = "localhost:9092"      # broker
src_topic = "iot-topic"           # sensor
dst_topic = "alerts-topic"        # alerts
checkpoint_dir = "./chk"          # checkpoints
alerts_path = "alerts_conditions_6.csv"  # CSV 

# JSON
in_schema = StructType([
    StructField("id",         StringType(),  True),
    StructField("temperature",DoubleType(),  True),
    StructField("humidity",   DoubleType(),  True),
    StructField("timestamp",  StringType(),  True)  # ISO рядок
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", src_topic)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), in_schema).alias("j"))
       .select(
           col("j.id").alias("id"),
           col("j.temperature").alias("temperature"),
           col("j.humidity").alias("humidity"),
           col("j.timestamp").cast(TimestampType()).alias("event_ts")
       )
       .withWatermark("event_ts", "10 seconds")
)

agg = (
    parsed
    .groupBy(window(col("event_ts"), "1 minute", "30 seconds"))
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )
)

alerts_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(alerts_path)
    .selectExpr(
        "cast(id as string) as rule_id",
        "humidity_min", "humidity_max",
        "temperature_min", "temperature_max",
        "cast(code as string) as code",
        "message"
    )
)

# CrossJoin
cond = (
    ((col("temperature_min") == lit(-999)) | (col("t_avg") >= col("temperature_min"))) &
    ((col("temperature_max") == lit(-999)) | (col("t_avg") <= col("temperature_max"))) &
    ((col("humidity_min")    == lit(-999)) | (col("h_avg") >= col("humidity_min"))) &
    ((col("humidity_max")    == lit(-999)) | (col("h_avg") <= col("humidity_max")))
)

alerts = (
    agg.crossJoin(broadcast(alerts_df))
       .where(cond)
       .select(
           col("window.start").alias("start"),
           col("window.end").alias("end"),
           col("t_avg"),
           col("h_avg"),
           col("code"),
           col("message"),
           current_timestamp().alias("timestamp")
       )
)

# JSON_end
out_json = alerts.select(
    to_json(
        struct(
            struct(col("start"), col("end")).alias("window"),
            col("t_avg"), col("h_avg"),
            col("code"), col("message"),
            col("timestamp")
        )
    ).alias("value")
)

query = (
    out_json
    .selectExpr("CAST(NULL AS STRING) AS key", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("topic", dst_topic)
    .option("checkpointLocation", checkpoint_dir)
    .outputMode("append")
    .start()
)

query.awaitTermination()

