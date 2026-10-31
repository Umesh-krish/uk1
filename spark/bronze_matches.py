"""
Bronze loader: raw CSV in the landing zone -> partitioned Parquet in the lake.

Reads from an S3A-compatible object store (MinIO locally, S3 in AWS, GCS via the
connector on GCP) and writes Parquet partitioned by season. Schemas are declared
explicitly rather than inferred: schema inference on CSV is a silent source of
type drift between runs, and a job that reprocesses history needs the same types
every time.

Run:
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        spark/bronze_matches.py --dataset matches --season 2025-26
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

MATCH_SCHEMA = StructType(
    [
        StructField("match_id", StringType(), False),
        StructField("season", StringType(), False),
        StructField("matchday", IntegerType(), True),
        StructField("kickoff_utc", StringType(), True),
        StructField("home_team_code", StringType(), True),
        StructField("away_team_code", StringType(), True),
        StructField("home_goals", IntegerType(), True),
        StructField("away_goals", IntegerType(), True),
        StructField("home_shots", IntegerType(), True),
        StructField("away_shots", IntegerType(), True),
        StructField("home_corners", IntegerType(), True),
        StructField("away_corners", IntegerType(), True),
        StructField("home_fouls", IntegerType(), True),
        StructField("away_fouls", IntegerType(), True),
        StructField("attendance", IntegerType(), True),
        StructField("referee", StringType(), True),
        StructField("home_xg", DoubleType(), True),
        StructField("away_xg", DoubleType(), True),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("match_id", StringType(), False),
        StructField("season", StringType(), False),
        StructField("minute", IntegerType(), True),
        StructField("team_code", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("player_id", StringType(), True),
    ]
)

TEAM_SCHEMA = StructType(
    [
        StructField("team_code", StringType(), False),
        StructField("team_name", StringType(), True),
        StructField("city", StringType(), True),
    ]
)

DATASETS = {
    "matches": (MATCH_SCHEMA, ["season"]),
    "events": (EVENT_SCHEMA, ["season"]),
    "teams": (TEAM_SCHEMA, []),
}


def build_session(app_name: str) -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Small files kill read performance on object storage far more often
        # than they kill writes, so coalesce on the way in.
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    )
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    return builder.getOrCreate()


def add_audit_columns(df: DataFrame, source_path: str) -> DataFrame:
    """Every bronze row carries where it came from and when it landed.

    Without this, a bad upstream file is untraceable once it is merged into a
    partition, and a targeted replay becomes a full reload.
    """
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_source_row", F.monotonically_increasing_id())
    )


def load(spark: SparkSession, dataset: str, season: str | None, landing: str, bronze: str) -> int:
    schema, partitions = DATASETS[dataset]

    source = f"{landing}/{dataset}"
    if season and partitions:
        source = f"{source}/season={season}"

    df = spark.read.option("header", True).schema(schema).csv(source)

    # Reject rows missing a primary key rather than letting them poison the
    # downstream joins; they are counted and logged, not silently dropped.
    key = "match_id" if dataset == "matches" else "event_id" if dataset == "events" else "team_code"
    total = df.count()
    df = df.filter(F.col(key).isNotNull())
    kept = df.count()
    if kept < total:
        print(f"[warn] {dataset}: dropped {total - kept} rows with a null {key}", file=sys.stderr)

    if dataset == "matches":
        df = df.withColumn("kickoff_utc", F.to_timestamp("kickoff_utc"))

    df = add_audit_columns(df, source)

    writer = df.repartition(2).write.mode("overwrite").format("parquet")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(f"{bronze}/{dataset}")

    print(f"[ok] {dataset}: wrote {kept} rows to {bronze}/{dataset}")
    return kept


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", choices=sorted(DATASETS), required=True)
    ap.add_argument("--season", default=None, help="Load one season only; omit for a full reload")
    ap.add_argument("--landing", default=os.getenv("LANDING_URI", "data/landing"))
    ap.add_argument("--bronze", default=os.getenv("BRONZE_URI", "data/bronze"))
    args = ap.parse_args()

    spark = build_session(f"matchlake-bronze-{args.dataset}")
    try:
        rows = load(spark, args.dataset, args.season, args.landing, args.bronze)
        if rows == 0:
            raise SystemExit(f"no rows loaded for {args.dataset}; failing the task")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
