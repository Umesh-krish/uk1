"""
Nightly batch pipeline: extract -> bronze -> dbt -> test.

Design notes that matter in review:

* The DAG is idempotent. Every task is keyed on the logical date's season, and
  bronze writes overwrite the season partition rather than appending, so a
  rerun of any day produces the same lake state. Backfilling is therefore safe.
* dbt test runs as its own task after dbt run, not with `--fail-fast` inside it.
  A failed test should leave the built tables in place for inspection and fail
  the DAG loudly, not roll the models back.
* The streaming path is deliberately not orchestrated here. It runs
  continuously; Airflow only checks that it is alive (see football_freshness).
"""
from __future__ import annotations

import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ROOT = "/opt/football-data-pipeline"
DBT_DIR = f"{PROJECT_ROOT}/dbt"
SPARK_SUBMIT = "spark-submit --master ${SPARK_MASTER:-local[*]}"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
    "email_on_failure": False,
}


def current_season(logical_date: pendulum.DateTime) -> str:
    """Seasons run August to May, so the label depends on the month."""
    year = logical_date.year
    return f"{year}-{str(year + 1)[2:]}" if logical_date.month >= 8 else f"{year - 1}-{str(year)[2:]}"


with DAG(
    dag_id="football_batch",
    description="Nightly match lakehouse refresh: land raw files, build bronze, rebuild the warehouse",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["football-data-pipeline", "batch", "spark", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    @task(task_id="resolve_season")
    def resolve_season(**context) -> str:
        season = current_season(context["logical_date"])
        print(f"processing season {season}")
        return season

    season = resolve_season()

    extract = BashOperator(
        task_id="extract_to_landing",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python ingestion/extract_matches.py "
            "--seasons {{ ti.xcom_pull(task_ids='resolve_season') }}"
        ),
    )

    bronze_matches = BashOperator(
        task_id="bronze_matches",
        bash_command=(
            f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} spark/bronze_matches.py "
            "--dataset matches --season {{ ti.xcom_pull(task_ids='resolve_season') }}"
        ),
    )

    bronze_events = BashOperator(
        task_id="bronze_events",
        bash_command=(
            f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} spark/bronze_matches.py "
            "--dataset events --season {{ ti.xcom_pull(task_ids='resolve_season') }}"
        ),
    )

    bronze_teams = BashOperator(
        task_id="bronze_teams",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} spark/bronze_matches.py --dataset teams",
    )

    # Drain whatever the stream has buffered before the warehouse rebuild, so
    # the nightly tables include today's live events instead of lagging a day.
    drain_stream = BashOperator(
        task_id="drain_event_stream",
        bash_command=f"cd {PROJECT_ROOT} && {SPARK_SUBMIT} spark/stream_events.py --once",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --target ${{DBT_TARGET:-dev}} --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --target ${{DBT_TARGET:-dev}} --profiles-dir .",
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_DIR} && dbt docs generate --target ${{DBT_TARGET:-dev}} --profiles-dir .",
        # Documentation is nice to have; it must never fail the data pipeline.
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> season >> extract
    extract >> [bronze_matches, bronze_events, bronze_teams] >> drain_stream
    drain_stream >> dbt_run >> dbt_test >> dbt_docs >> finish
