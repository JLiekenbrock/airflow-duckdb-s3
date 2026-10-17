"""Visible sequential scenarios, each with bounded concurrent dbt loads."""

from datetime import datetime
import runpy
from types import SimpleNamespace

from airflow.sdk import DAG, get_current_context, task


def runner():
    return SimpleNamespace(**runpy.run_path('/opt/dbt/benchmarks/batching.py'))


with DAG(
    dag_id='clickhouse_batching_benchmark',
    start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
    max_active_runs=1, max_active_tasks=1,
    params={'source': 'http://minio:9000/poc/delta/nyc_taxi_trips/2025-01/',
            'start_date': '2025-01-01', 'end_date': '2025-02-01', 'repeats': 3},
    doc_md='Compare whole-range, weekly and daily dbt loads at 1/2/4 workers. '
           'Scenarios run sequentially; workers run inside each scenario task. '
           'Only poc_insert_benchmark.batching is truncated. Run with other ingestion idle. '
           'Per-window logs are under /tmp/batching; aggregate metrics appear in the report task.',
    tags=['dbt', 'clickhouse', 'benchmark'],
) as dag:
    @task
    def prepare():
        return runner().prepare(get_current_context()['params'])

    @task(retries=0)
    def measure(config, days, concurrency):
        return runner().measure(config, days, concurrency)

    @task
    def report(config, groups):
        return runner().report(config, groups)

    config = prepare()
    previous = config
    groups = []
    for label, days, workers in [('whole_range', 0, 1),
                                ('weekly_1', 7, 1), ('weekly_2', 7, 2), ('weekly_4', 7, 4),
                                ('daily_1', 1, 1), ('daily_2', 1, 2), ('daily_4', 1, 4)]:
        result = measure.override(task_id=label)(config, days, workers)
        previous >> result
        previous = result
        groups.append(result)
    report(config, groups)
