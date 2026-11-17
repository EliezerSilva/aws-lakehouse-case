"""Spark Kafka Structured Streaming -> Bronze Parquet; ingestao isolada."""
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

MESSAGE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("amount_str", T.StringType()),
    T.StructField("event_ts_str", T.StringType()),
    T.StructField("ingested_at_str", T.StringType()),
    T.StructField("status", T.StringType()),
])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="synthetic_transactions_raw")
    parser.add_argument("--output", required=True)
    parser.add_argument("--checkpoint", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("synthetic-kafka-bronze").getOrCreate()
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic)
              .option("startingOffsets", "earliest").load())
    bronze = (
        stream.select(
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_payload"),
        )
        .withColumn("payload", F.from_json("raw_payload", MESSAGE_SCHEMA))
        .select("*", "payload.*").drop("payload")
    )
    query = (bronze.writeStream.format("parquet")
             .option("path", args.output)
             .option("checkpointLocation", args.checkpoint)
             .outputMode("append").start())
    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
