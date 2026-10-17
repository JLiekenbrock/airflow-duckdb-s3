from datetime import date, datetime, timedelta

from airflow.sdk import DAG, get_current_context, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="delta_s3_to_clickhouse_backfill",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "start_date": "2026-10-12",
        "end_date": "2026-10-19",
        "tenant_ids": list(range(1, 801)),
        "days_per_batch": 7,
        "delta_s3_path": None,
    },
    doc_md=(
        "Split a historical date range into windows, then trigger delta_s3_to_clickhouse for each one. "
        "All selected tenants are passed to every window. The main DAG serializes loads and retries dbt tasks. "
        "Failed windows do not block other windows; this backfill run fails if any child run fails. "
        "Open each mapped task's Triggered DAG link to inspect its dbt models and tests. "
        "Unpause the main DAG first. Waiting is deferrable so it does not occupy the worker slot."
    ),
    tags=["dbt", "clickhouse", "backfill"],
) as dag:
    @task
    def make_windows():
        params = get_current_context()["params"]
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"])
        days = params["days_per_batch"]
        if start >= end:
            raise ValueError("end_date must be later than start_date")
        if type(days) is not int or days < 1:
            raise ValueError("days_per_batch must be a positive integer")
        return [
            {
                "start_date": (start + timedelta(days=offset)).isoformat(),
                "end_date": (start + timedelta(days=min(offset + days, (end - start).days))).isoformat(),
                "tenant_ids": params["tenant_ids"],
                "delta_s3_path": params["delta_s3_path"],
            }
            for offset in range(0, (end - start).days, days)
        ]

    TriggerDagRunOperator.partial(
        task_id="load_window",
        trigger_dag_id="delta_s3_to_clickhouse",
        trigger_run_id="backfill__{{ run_id }}__{{ ti.map_index }}",
        reset_dag_run=True,
        wait_for_completion=True,
        deferrable=True,
        retries=0,
    ).expand(conf=make_windows())
