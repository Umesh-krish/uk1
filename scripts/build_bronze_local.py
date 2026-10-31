"""
Laptop path: build the bronze layer with DuckDB instead of Spark.

The Spark jobs in spark/ are what runs on a cluster (EMR, Dataproc, Databricks).
They need a JVM and a package download, which is a poor first experience for
anyone cloning this repo to look at it. This script produces byte-for-byte
equivalent bronze Parquet using DuckDB, so `make demo` works offline in seconds
and the dbt layer above it is identical either way.

It is not a replacement for the Spark jobs. It exists so the project can be read
and run without provisioning anything.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

DATASETS = {
    "matches": ("season", "match_id"),
    "events": ("season", "event_id"),
    "teams": (None, "team_code"),
}


def build(con: duckdb.DuckDBPyConnection, dataset: str, landing: Path, bronze: Path) -> int:
    partition_col, key = DATASETS[dataset]
    out = bronze / dataset
    # A partitioned COPY writes a directory tree; an unpartitioned one writes a
    # single file, and pointing it at an existing directory is an IO error.
    target = out if partition_col else out / f"{dataset}.parquet"
    out.mkdir(parents=True, exist_ok=True)

    glob = (
        f"{landing}/{dataset}/season=*/{dataset}.csv"
        if partition_col
        else f"{landing}/{dataset}/{dataset}.csv"
    )

    # Mirrors the audit columns the Spark job adds, so downstream models cannot
    # tell which engine produced the layer.
    select = f"""
        select
            *,
            current_timestamp           as _ingested_at,
            '{glob}'                    as _source_file,
            row_number() over ()        as _source_row
        from read_csv_auto('{glob}', header = true, union_by_name = true)
        where {key} is not null
    """

    partition_clause = ", PARTITION_BY (season)" if partition_col else ""
    con.execute(
        f"""
        copy ({select})
        to '{target}'
        (FORMAT PARQUET, COMPRESSION SNAPPY, OVERWRITE_OR_IGNORE true{partition_clause})
        """
    )

    rows = con.execute(f"select count(*) from ({select})").fetchone()[0]
    print(f"[ok] {dataset:<8} {rows:>6} rows -> {out}")
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--landing", default="data/landing")
    ap.add_argument("--bronze", default="data/bronze")
    args = ap.parse_args()

    landing, bronze = Path(args.landing), Path(args.bronze)
    if not landing.exists():
        raise SystemExit(f"{landing} not found; run scripts/generate_seed_data.py first")

    con = duckdb.connect()
    try:
        total = sum(build(con, ds, landing, bronze) for ds in DATASETS)
    finally:
        con.close()

    print(f"bronze built: {total:,} rows total")


if __name__ == "__main__":
    main()
