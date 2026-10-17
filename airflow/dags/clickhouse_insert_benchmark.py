"""Benchmark actual dbt incremental strategies with visible Airflow phases."""

from datetime import datetime
import runpy
from types import SimpleNamespace

from airflow.sdk import DAG, get_current_context, task, task_group


def runner():
    return SimpleNamespace(**runpy.run_path("/opt/dbt/benchmarks/runner.py"))


with DAG(
    dag_id="clickhouse_insert_benchmark",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "source": "http://minio:9000/poc/delta/nyc_taxi_trips/2025-01/",
        "start_date": "2025-01-01",
        "end_date": "2025-02-01",
        "repeats": 3,
    },
    doc_md=(
        "Compare actual dbt append, delete+insert, default, and insert_overwrite strategies. "
        "Flyway creates isolated poc_insert_benchmark tables. Each strategy has visible initial, replay, "
        "and partial-update tasks. A report compares timing and correctness. Replay/correction correctness "
        "failures are recorded as benchmark findings, not task errors. Only benchmark tables are truncated. "
        "Run after preparing NYC taxi Delta data and while regular imports are idle."
    ),
    tags=["dbt", "clickhouse", "benchmark"],
) as dag:
    @task
    def prepare():
        return runner().prepare(get_current_context()["params"])

    @task
    def measure(config, case, phase):
        return runner().run_phase(config, case, phase)

    @task
    def collect(initial, replay, partial):
        return [initial, replay, partial]

    @task_group
    def strategy(config, case):
        initial = measure.override(task_id="initial_load")(config, case, "initial")
        replay = measure.override(task_id="replay")(config, case, "replay")
        partial = measure.override(task_id="partial_update")(config, case, "partial_update")
        initial >> replay >> partial
        return collect(initial, replay, partial)

    @task
    def report(config, groups):
        return runner().report(config, list(groups))

    config = prepare()
    groups = strategy.partial(config=config).expand(case=["append", "delete_insert", "legacy", "insert_overwrite"])
    report(config, groups)
