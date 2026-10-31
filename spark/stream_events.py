"""
Real-time path: Kafka -> Spark Structured Streaming -> bronze event store.

This is the streaming half of the lambda-style design. The batch path owns
history and correctness; this path owns freshness, writing micro-batches of live
match events so the gold layer can serve an in-play view minutes after kickoff.

Checkpointing is what makes the job restartable: Spark records Kafka offsets in
the checkpoint directory, so a restart resumes where it stopped instead of
replaying the topic or skipping it. Deleting the checkpoint is therefore a
destructive act, not a cleanup.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        spark/stream_events.py --mode append
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

EVENT_JSON_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("match_id", StringType(), False),
        StructField("season", StringType(), True),
        StructField("minute", IntegerType(), True),
        StructField("team_code", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("emitted_at", StringType(), True),
    ]
)


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("football-stream-events")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "match-events"))
    ap.add_argument("--sink", default=os.getenv("STREAM_SINK", "data/bronze/events_stream"))
    ap.add_argument("--checkpoint", default=os.getenv("STREAM_CHECKPOINT", "data/_checkpoints/events"))
    ap.add_argument("--trigger-seconds", type=int, default=30)
    ap.add_argument("--once", action="store_true", help="Process available data and exit (for CI and Airflow)")
    args = ap.parse_args()

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        # Bounding the per-trigger volume keeps one slow batch from snowballing
        # after an outage, which is the usual cause of a stream never catching up.
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    parsed = (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(F.col("value").cast("string"), EVENT_JSON_SCHEMA).alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("kafka_key", "kafka_timestamp", "payload.*")
        .withColumn("emitted_at", F.to_timestamp("emitted_at"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    # Watermark + dropDuplicates gives at-least-once delivery an effectively
    # exactly-once shape: a redelivered event inside the 10 minute window is
    # discarded, and state older than that is evicted so the job does not grow
    # without bound.
    deduped = parsed.withWatermark("kafka_timestamp", "10 minutes").dropDuplicates(
        ["event_id", "kafka_timestamp"]
    )

    writer = (
        deduped.writeStream.format("parquet")
        .option("path", args.sink)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("season")
        .outputMode("append")
    )

    query = writer.trigger(availableNow=True).start() if args.once else writer.trigger(
        processingTime=f"{args.trigger_seconds} seconds"
    ).start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
