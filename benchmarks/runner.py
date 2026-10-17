"""Benchmark isolated taxi tables, using Flyway for schema setup."""

import base64
from datetime import date, timedelta
import json
import os
from pathlib import Path
import statistics
import subprocess
import time
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parent
DBT = "/home/airflow/dbt-venv/bin/dbt"
DATABASE = "poc_insert_benchmark"
OUTPUT = Path("/opt/airflow/logs/insert_benchmark")
STRATEGIES = {"append": "append", "delete_insert": "delete+insert", "legacy": "default", "insert_overwrite": "insert_overwrite"}


def literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def query(sql):
    endpoint = f"http://{os.environ['CLICKHOUSE_HOST']}:{os.environ.get('CLICKHOUSE_PORT', '8123')}"
    settings = urllib.parse.urlencode({"max_threads": 4, "max_insert_threads": 1})
    credentials = base64.b64encode(f"{os.environ.get('CLICKHOUSE_USER', 'default')}:{os.environ.get('CLICKHOUSE_PASSWORD', '')}".encode()).decode()
    request = urllib.request.Request(endpoint + "/?" + settings, data=sql.encode(), headers={"Authorization": "Basic " + credentials})
    with urllib.request.urlopen(request, timeout=600) as response:
        result = response.read().decode()
    return [json.loads(line) for line in result.splitlines()] if "FORMAT JSONEachRow" in sql else result


def prepare(params):
    config = {key: params[key] for key in ("source", "start_date", "end_date", "repeats")}
    start, end = date.fromisoformat(config["start_date"]), date.fromisoformat(config["end_date"])
    if start >= end or type(config["repeats"]) is not int or config["repeats"] < 1:
        raise ValueError("Provide an increasing date range and a positive integer repeats")
    config["partial_end"] = min(start + timedelta(days=7), end).isoformat()
    if config["partial_end"] == config["end_date"]:
        raise ValueError("Use a range longer than seven days to exercise partial partition updates")
    env = os.environ.copy()
    env.update({
        "environments_default_url": f"jdbc:clickhouse://{env['CLICKHOUSE_HOST']}:{env.get('CLICKHOUSE_PORT', '8123')}/default",
        "environments_default_user": env.get("CLICKHOUSE_USER", "default"),
        "environments_default_password": env.get("CLICKHOUSE_PASSWORD", ""),
    })
    subprocess.run(["flyway", "migrate"], cwd=ROOT.parent, env=env, check=True)
    subprocess.run(["flyway", f"-locations=filesystem:{ROOT / 'migrations'}", "-table=flyway_insert_benchmark_history", "-baselineOnMigrate=true", "-baselineVersion=0", "migrate"], cwd=ROOT.parent, env=env, check=True)
    source = "deltaLake(" + ",".join(literal(v) for v in (config["source"], env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"])) + ")"
    expected = query(
        "SELECT count() AS rows, uniqExact(trip_id) AS distinct_keys, sum(toInt64(total_amount * 100)) AS total_cents, "
        f"countIf(pickup_date < toDate({literal(config['partial_end'])}) AND isNotNull(total_amount)) AS partial_rows FROM {source} "
        f"WHERE pickup_date >= toDate({literal(config['start_date'])}) AND pickup_date < toDate({literal(config['end_date'])}) FORMAT JSONEachRow"
    )[0]
    config["expected"] = {key: int(value) for key, value in expected.items()}
    if not config["expected"]["rows"] or config["expected"]["rows"] != config["expected"]["distinct_keys"]:
        raise ValueError("Benchmark source must be nonempty with unique trip IDs")
    dbt_run(config, "delete_insert", config["end_date"], 0, "benchmark_stage", model="stg_nyc_taxi_trips")
    config["clickhouse_version"] = query("SELECT version() FORMAT JSONEachRow")[0]
    config["dbt_version"] = subprocess.check_output([DBT, "--version"], text=True)
    return config


def dbt_run(config, case, end_date, revision, tag, model="taxi_insert_benchmark"):
    env = {**os.environ, "BENCHMARK_TAG": tag}
    variables = {"case": case, "strategy": STRATEGIES[case], "start_date": config["start_date"], "end_date": end_date, "revision": revision, "delta_s3_path": config["source"]}
    subprocess.run(
        [DBT, "run", "--project-dir", str(ROOT), "--profiles-dir", str(ROOT), "--target-path", "/tmp/insert_benchmark/target", "--log-path", "/tmp/insert_benchmark/logs", "--select", model, "--vars", json.dumps(variables)],
        env=env, check=True, capture_output=True, text=True,
    )


def run_phase(config, case, phase):
    if case not in STRATEGIES or phase not in {"initial", "replay", "partial_update"}:
        raise ValueError("Unknown benchmark case or phase")
    results = []
    for repetition in range(1, config["repeats"] + 1):
        # Each measured trial starts from the same data, even if other tasks interleave.
        query(f"TRUNCATE TABLE {DATABASE}.{case}")
        if phase != "initial":
            dbt_run(config, case, config["end_date"], 0, "benchmark_setup_" + uuid.uuid4().hex)
        tag = "benchmark_" + uuid.uuid4().hex
        partial = phase == "partial_update"
        started = time.monotonic()
        try:
            dbt_run(config, case, config["partial_end"] if partial else config["end_date"], int(partial), tag)
        except subprocess.CalledProcessError as error:
            raise RuntimeError(f"{case}/{phase} dbt failed:\n{error.stdout[-3000:]}\n{error.stderr[-1000:]}") from error
        elapsed = time.monotonic() - started
        actual = query(f"SELECT count() AS rows, uniqExact(trip_id) AS distinct_keys, sum(toInt64(total_amount * 100)) AS total_cents FROM {DATABASE}.{case} FORMAT JSONEachRow")[0]
        expected = {key: config["expected"][key] for key in ("rows", "distinct_keys", "total_cents")}
        if partial:
            expected["total_cents"] += config["expected"]["partial_rows"] * 100
        actual = {key: int(value) for key, value in actual.items()}
        query("SYSTEM FLUSH LOGS")
        metrics = query(
            "SELECT sum(query_duration_ms) AS sql_ms, max(memory_usage) AS peak_query_bytes, sum(written_rows) AS written_rows "
            f"FROM system.query_log WHERE type='QueryFinish' AND is_initial_query=1 AND log_comment={literal(tag)} FORMAT JSONEachRow"
        )[0]
        parts = query(f"SELECT count() AS active_parts FROM system.parts WHERE database={literal(DATABASE)} AND table={literal(case)} AND active FORMAT JSONEachRow")[0]
        result = {"case": case, "phase": phase, "repetition": repetition, "wall_seconds": round(elapsed, 3), "correct": actual == expected, "actual": actual, "expected": expected, **{key: int(value) for key, value in {**metrics, **parts}.items()}}
        print(json.dumps(result), flush=True)
        results.append(result)
    return results


def report(config, groups):
    results = [row for group in groups for phase in group for row in phase]
    lines = ["# Local dbt insert strategy benchmark", "", f"Source rows: {config['expected']['rows']:,}; repeats: {config['repeats']}; max_threads=4; max_insert_threads=1.", "", "Insert settings: async_insert=1, wait_for_async_insert=1, lightweight_deletes_sync=2 (matching the measured server defaults, pinned in the benchmark profile). This is not an async-on/off comparison.", "", "| Strategy | Phase | Median dbt wall s | Median SQL ms | Max query MiB | Correct trials |", "| --- | --- | ---: | ---: | ---: | ---: |"]
    for case, strategy in STRATEGIES.items():
        for phase in ("initial", "replay", "partial_update"):
            samples = [row for row in results if row["case"] == case and row["phase"] == phase]
            if samples:
                lines.append(f"| {strategy} | {phase} | {statistics.median(r['wall_seconds'] for r in samples):.3f} | {statistics.median(r['sql_ms'] for r in samples):.0f} | {max(r['peak_query_bytes'] for r in samples) / 2**20:.1f} | {sum(r['correct'] for r in samples)}/{len(samples)} |")
    lines.extend(["", "Fastest correct result per measured scenario (median dbt wall time):", ""])
    for phase in ("initial", "replay", "partial_update"):
        candidates = []
        for case in STRATEGIES:
            samples = [row for row in results if row["case"] == case and row["phase"] == phase]
            if samples and all(row["correct"] for row in samples):
                candidates.append((statistics.median(row["wall_seconds"] for row in samples), STRATEGIES[case]))
        if candidates:
            elapsed, strategy = min(candidates)
            lines.append(f"- {phase}: `{strategy}` ({elapsed:.3f} s).")
    lines.extend(["", "Initial = full source into an empty precreated table. Replay = identical full source over a full baseline. Partial update = first seven days over a full baseline, increasing each non-null total_amount by USD 1.00; other days must survive.", "", "Correctness checks row count, exact distinct trip IDs, and total_amount summed as integer cents. Expected failures are benchmark outcomes, not Airflow execution failures. No fault injection or concurrent writers were tested.", "", "Warm local caches; phases run sequentially with repeated source scans. Wall time includes dbt process startup and materialization; SQL ms sums tagged initial queries and excludes setup and validation. Peak memory is the largest individual measured query, not total server memory. Background merges and cache effects are not isolated. Results apply to the 24-column NYC taxi schema and local versions only.", ""])
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / "results.json").write_text(json.dumps({"config": config, "results": results}, indent=2) + "\n")
    markdown = "\n".join(lines)
    (OUTPUT / "results.md").write_text(markdown)
    print(markdown, flush=True)
    return {"report": str(OUTPUT / "results.md"), "raw_results": str(OUTPUT / "results.json")}
