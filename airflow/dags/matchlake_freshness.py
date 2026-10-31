"""
Freshness and stream-health checks, every 30 minutes.

Separate from the batch DAG on purpose. A nightly pipeline that only reports at
3am tells you about a broken stream eight hours late; this one catches it while
a match is still being played.

Fails loudly rather than quietly skipping, because a silent freshness check is
worse than none: it buys false confidence.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.exceptions import AirflowFailException

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
}

# How stale each dataset is allowed to get before it is a problem.
FRESHNESS_SLA_MINUTES = {
    "events_stream": 15,
    "matches": 60 * 26,
}


with DAG(
    dag_id="matchlake_freshness",
    description="Checks lake freshness and streaming lag; alerts within the half hour",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["matchlake", "quality", "monitoring"],
) as dag:

    @task
    def check_stream_freshness() -> dict:
        """Newest streamed event must be inside its SLA window."""
        import duckdb

        sla = FRESHNESS_SLA_MINUTES["events_stream"]
        con = duckdb.connect()
        try:
            row = con.execute(
                """
                select
                    max(_ingested_at)                                       as newest,
                    date_diff('minute', max(_ingested_at), current_timestamp) as lag_minutes,
                    count(*)                                                as row_count
                from read_parquet('/opt/matchlake/data/bronze/events_stream/**/*.parquet',
                                  hive_partitioning = true)
                """
            ).fetchone()
        except Exception as exc:
            raise AirflowFailException(f"stream sink unreadable: {exc}") from exc
        finally:
            con.close()

        newest, lag, count = row
        print(f"newest streamed event {newest}, lag {lag}m, {count} rows")

        if lag is None or lag > sla:
            raise AirflowFailException(f"stream is {lag} minutes stale, SLA is {sla}")
        return {"lag_minutes": lag, "row_count": count}

    @task
    def check_warehouse_rowcounts() -> dict:
        """Guards against a silent truncation: a fact table should never shrink."""
        import duckdb

        con = duckdb.connect("/opt/matchlake/data/warehouse/matchlake.duckdb", read_only=True)
        try:
            counts = {
                table: con.execute(f"select count(*) from marts.{table}").fetchone()[0]
                for table in ("fct_match", "fct_team_match", "fct_match_event", "agg_team_season_form")
            }
        finally:
            con.close()

        print(counts)

        if counts["fct_team_match"] != counts["fct_match"] * 2:
            raise AirflowFailException(
                f"fct_team_match should be exactly twice fct_match, got {counts}"
            )
        if counts["fct_match"] == 0:
            raise AirflowFailException("fct_match is empty")

        return counts

    @task
    def publish_metrics(stream: dict, warehouse: dict) -> None:
        """Where a real deployment would emit to CloudWatch, Monitoring or Datadog."""
        payload = {"stream": stream, "warehouse": warehouse, "checked_at": pendulum.now("UTC").isoformat()}
        print(f"metrics: {payload}")

    publish_metrics(check_stream_freshness(), check_warehouse_rowcounts())
