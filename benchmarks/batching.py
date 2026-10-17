"""Compare date-window size and bounded parallel dbt operations."""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
import json
import os
from pathlib import Path
import statistics
import subprocess
import time
import uuid

import runpy

BASE = runpy.run_path(str(Path(__file__).with_name('runner.py')))
query, literal = BASE['query'], BASE['literal']
ROOT, DBT = BASE['ROOT'], BASE['DBT']
OUTPUT = Path('/opt/airflow/logs/batching_benchmark')


def prepare(params):
    # Reuse migration setup, source validation, full-range staging and version metadata.
    return BASE['prepare'](params)


def windows(config, days):
    start, end = date.fromisoformat(config['start_date']), date.fromisoformat(config['end_date'])
    result = []
    while start < end:
        stop = min(start + timedelta(days=days), end) if days else end
        result.append({'start_date': start.isoformat(), 'end_date': stop.isoformat()})
        start = stop
    return result


def load(config, window, tag):
    invocation = uuid.uuid4().hex
    command = [DBT, 'run-operation', 'load_window', '--project-dir', str(ROOT),
               '--profiles-dir', str(ROOT), '--target-path', f'/tmp/batching/{invocation}/target',
               '--log-path', f'/tmp/batching/{invocation}/logs', '--args', json.dumps(window),
               '--vars', json.dumps({'delta_s3_path': config['source'],
                                    'start_date': config['start_date'], 'end_date': config['end_date']})]
    try:
        subprocess.run(command, env={**os.environ, 'BENCHMARK_TAG': tag},
                       check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f'{window}: {error.stdout[-3000:]}\n{error.stderr[-1000:]}') from error


def measure(config, days, concurrency):
    batches = windows(config, days)
    results = []
    expected = {k: config['expected'][k] for k in ('rows', 'distinct_keys', 'total_cents')}
    for repetition in range(1, config['repeats'] + 1):
        query('TRUNCATE TABLE poc_insert_benchmark.batching')
        tag = 'batching_' + uuid.uuid4().hex
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=concurrency) as workers:
            # Resolve every future; a failed load fails the scenario after workers finish.
            futures = [workers.submit(load, config, window, tag) for window in batches]
            for future in futures:
                future.result()
        elapsed = time.monotonic() - started
        actual = query('SELECT count() AS rows, uniqExact(trip_id) AS distinct_keys, '
                       'sum(toInt64(total_amount * 100)) AS total_cents '
                       'FROM poc_insert_benchmark.batching FORMAT JSONEachRow')[0]
        actual = {k: int(v) for k, v in actual.items()}
        query('SYSTEM FLUSH LOGS')
        metrics = query('SELECT sum(query_duration_ms) AS sql_ms, max(memory_usage) AS peak_query_bytes '
                        'FROM system.query_log WHERE type=\'QueryFinish\' AND is_initial_query=1 '
                        f'AND log_comment={literal(tag)} FORMAT JSONEachRow')[0]
        parts = query("SELECT count() AS active_parts FROM system.parts WHERE database='poc_insert_benchmark' "
                      "AND table='batching' AND active FORMAT JSONEachRow")[0]
        row = {'days': days, 'concurrency': concurrency, 'windows': len(batches),
               'repetition': repetition, 'wall_seconds': round(elapsed, 3),
               'rows_per_second': round(expected['rows'] / elapsed), 'correct': actual == expected,
               'actual': actual, **{k: int(v) for k, v in {**metrics, **parts}.items()}}
        print(json.dumps(row), flush=True)
        results.append(row)
    return results


def report(config, groups):
    rows = [row for group in groups for row in group]
    lines = ['# Date-window batching benchmark', '',
             f"Source rows: {config['expected']['rows']:,}; repeats: {config['repeats']}.", '',
             '| Window | Workers | Loads | Median wall s | Rows/s | Sum SQL ms | Max query MiB | Active parts* | Correct |',
             '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |']
    for days, workers in dict.fromkeys((r['days'], r['concurrency']) for r in rows):
        samples = [r for r in rows if (r['days'], r['concurrency']) == (days, workers)]
        median = lambda key: statistics.median(r[key] for r in samples)
        label = f'{days} days' if days else 'Whole range'
        lines.append(f"| {label} | {workers} | {samples[0]['windows']} | {median('wall_seconds'):.3f} | "
                     f"{median('rows_per_second'):,.0f} | {median('sql_ms'):.0f} | "
                     f"{max(r['peak_query_bytes'] for r in samples)/2**20:.1f} | "
                     f"{median('active_parts'):.0f} | {sum(r['correct'] for r in samples)}/{len(samples)} |")
    lines.extend(['', 'Every trial starts empty and loads the same full date range. '
                  'Each dbt operation deletes its disjoint pickup-date window then inserts from the shared full-range staging view. '
                  'This tests window replacement, not the adapter incremental materialization or replay recovery.', '',
                  'Wall time includes worker scheduling and dbt startup, but excludes Airflow queue time, setup and checks. '
                  'SQL time sums tagged queries and can exceed wall time with concurrent workers. '
                  'Memory is the largest individual query, not concurrent peak server memory. '
                  '*Active parts are a post-load snapshot; background merges race with measurement. '
                  'This does not measure parts created, merge cost or sustained ingestion pressure.', '',
                  'Each query uses max_threads=4 and max_insert_threads=1, so more workers can use more total CPU/memory. '
                  'Scenarios run sequentially, use warm caches and share background merges. '
                  'Windows have variable row counts; these are S3 INSERT SELECT loads, not 100k-row client batches. '
                  'No rate limiter or async-on/off comparison is included. Run while other imports are idle.', ''])
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / 'results.json').write_text(json.dumps({'config': config, 'results': rows}, indent=2) + '\n')
    (OUTPUT / 'results.md').write_text('\n'.join(lines))
    print('\n'.join(lines), flush=True)
    return str(OUTPUT / 'results.md')
