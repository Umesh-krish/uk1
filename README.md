# matchlake

A batch and streaming lakehouse for football match data, built with Spark, Kafka, Airflow and dbt.

Raw season files land in object storage untouched. Spark builds a partitioned Parquet bronze layer. A Kafka stream carries live in-match events into the same lake. dbt turns both into a tested star schema, and Airflow runs the whole thing nightly with freshness checks every half hour.

The transformation layer runs offline on DuckDB, so the entire project can be built and inspected with no cloud account and no containers:

```bash
pip install -r requirements.txt
make demo
```

That generates three seasons of source data, builds bronze, runs the models and runs the tests. It takes about twenty seconds.

```
  2025-26 league table

  #   TEAM   P    W   D   L   GF   GA   GD    PTS   PPG    xG +/-
  ----------------------------------------------------------------
  1   MCI    38   27  6   5   73   25   +48   87    2.29   -6.53
  2   MUN    38   24  7   7   69   34   +35   79    2.08   +9.01
  3   ARS    38   22  8   8   63   27   +36   74    1.95   -11.6
  4   CHE    38   23  5   10  66   40   +26   74    1.95   +1.83
  5   TOT    38   21  9   8   71   39   +32   72    1.89   +8.95
  6   LIV    38   20  8   10  81   41   +40   68    1.79   +6.21
```

## Architecture

```
                   ┌──────────────┐
   season CSVs ───▶│   LANDING    │  raw bytes, never modified
                   │  (S3 / GCS)  │  partitioned by season + extract date
                   └──────┬───────┘
                          │  Spark (EMR / Dataproc / Databricks)
                          ▼
   live events      ┌──────────────┐
   ──▶ Kafka ──────▶│    BRONZE    │  Parquet, snappy, partitioned by season
       Structured   │              │  + _ingested_at, _source_file, _source_row
       Streaming    └──────┬───────┘
                          │  dbt
                          ▼
                   ┌──────────────┐
                   │   STAGING    │  typed, renamed, no business logic
                   └──────┬───────┘
                          ▼
                   ┌──────────────┐
                   │    MARTS     │  dim_team · dim_season
                   │  star schema │  fct_match · fct_team_match · fct_match_event
                   └──────┬───────┘
                          ▼
                   ┌──────────────┐
                   │     GOLD     │  agg_team_season_form
                   └──────────────┘

   Airflow orchestrates the batch path nightly and checks freshness every 30 min.
```

## What is in here

| Path | What it does |
| --- | --- |
| `ingestion/extract_matches.py` | Lands raw season files in object storage, unmodified |
| `ingestion/stream_producer.py` | Replays a season's events into Kafka on a compressed clock |
| `spark/bronze_matches.py` | CSV to partitioned Parquet, explicit schemas, audit columns |
| `spark/stream_events.py` | Structured Streaming from Kafka, watermarked and checkpointed |
| `dbt/models/staging/` | Typing and renaming only |
| `dbt/models/marts/` | Star schema: two dimensions, three facts, one gold aggregate |
| `dbt/tests/` | Three custom reconciliation tests |
| `airflow/dags/matchlake_batch.py` | Nightly extract to bronze to dbt to test |
| `airflow/dags/matchlake_freshness.py` | Stream lag and row-count guards, every 30 minutes |
| `scripts/build_bronze_local.py` | DuckDB stand-in for the Spark bronze job, for laptops |

## The data model

`fct_match` is one row per match. `fct_team_match` is one row per team per match, so league tables and home/away splits are a group-by instead of a union written by hand in every query. `fct_match_event` is one row per in-match event, the largest table and the reason the lake is partitioned Parquet rather than CSV.

Surrogate keys are md5 hashes of the natural key, so facts never join on a club code that a source system might reuse or rebrand mid-season.

Both fact tables are incremental with `delete+insert` on the season, so a nightly run reprocesses only the current season and a full rebuild is still one `--full-refresh` away.

## Data quality

41 tests run on every build: uniqueness and not-null on every key, referential integrity from facts to dimensions, accepted values on every enum column, plus three custom tests that catch what schema tests cannot.

**`assert_goals_reconcile_with_events`** is the one that earns its keep. The scoreline comes from the match feed; the goal events come from the event feed, usually a different system with a different latency. If the two disagree, something dropped or duplicated events and every in-play metric is wrong while the league table still looks fine.

**`assert_every_team_plays_full_season`** catches a fixture lost during ingestion, which quietly distorts every per-team aggregate.

**`assert_no_future_matches_in_facts`** catches a bad date parse, the most common failure when a source changes its date format mid-season.

## Running the full stack

```bash
cp .env.example .env
make up          # MinIO, Kafka, Spark master + worker, Airflow
make stream      # replay five matches into Kafka
open http://localhost:8080   # Airflow, trigger matchlake_batch
```

`make demo` and `make up` build the same models. The difference is only which engine writes bronze.

## How this maps to a cloud

The local stack is chosen so every component has a direct managed equivalent. Nothing in the dbt layer changes between them; only the target in `dbt/profiles.yml`.

| Local | AWS | GCP |
| --- | --- | --- |
| MinIO | S3 | Cloud Storage |
| Spark batch | EMR or Glue | Dataproc |
| Spark Structured Streaming | EMR, or Glue streaming jobs | Dataflow |
| Kafka | MSK or Kinesis | Pub/Sub |
| Parquet + Hive partitions | Glue Data Catalog | BigQuery external tables |
| DuckDB warehouse | Redshift (Spectrum over S3) | BigQuery |
| Airflow | MWAA | Cloud Composer |
| dbt | dbt on either | dbt on either |

## Notes on the design

**The landing zone is deliberately dumb.** Files arrive exactly as the source sent them. Nothing is parsed or cleaned, so a bad transform can always be replayed from the original bytes rather than re-fetched from a source that may have changed underneath.

**Schemas are declared, not inferred.** Schema inference on CSV is a silent source of type drift between runs, and a job that reprocesses history needs the same types every time.

**Every bronze row carries its provenance.** Without `_source_file`, a bad upstream file is untraceable once merged into a partition and a targeted replay becomes a full reload.

**The stream is watermarked and deduplicated.** Kafka gives at-least-once delivery; a 10 minute watermark plus `dropDuplicates` gives it an effectively exactly-once shape without unbounded state growth.

**`dbt test` is its own Airflow task.** A failed test should leave the built tables in place for inspection and fail the DAG loudly, not roll the models back.

**Source paths come from `MATCHLAKE_DATA`, not a relative path.** Airflow runs tasks from a different working directory than a developer does, and a relative source path silently resolves to nothing.

## License

MIT
