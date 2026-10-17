# NYC taxi: S3 Delta → dbt → ClickHouse

A small NYC yellow-taxi POC based on the original Airflow + DuckDB S3 repository. A standalone script prepares the actual trip data in local MinIO as Delta. Airflow imports it through dbt into a Flyway-managed ClickHouse table. The import DAG never writes to S3.

## Run locally

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T airflow /usr/local/bin/python /opt/dbt/scripts/prepare_nyc_taxi.py --month 2025-01
```

The writer downloads the [official NYC TLC yellow-taxi Parquet file](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), processes it in 250,000-row chunks, and writes `s3://poc/delta/nyc_taxi_trips/2025-01`, partitioned by `pickup_date`. Running it again replaces only that month's prepared source. January 2025 contains 3,475,226 source trips; 22 records have missing or out-of-month pickup dates and are excluded, leaving 3,475,204 trips. No tenant, user, or generic event mapping is used.

Open [Airflow](http://localhost:8083) and sign in as `admin`. Retrieve its generated password with:

```sh
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Unpause and trigger `delta_s3_to_clickhouse` with its defaults, or provide:

```json
{"start_date": "2025-01-01", "end_date": "2025-02-01", "delta_s3_path": "http://minio:9000/poc/delta/nyc_taxi_trips/2025-01/"}
```

Dates refer to NYC pickup dates and are start-inclusive/end-exclusive. One run imports the whole requested window. MinIO console: [localhost:9003](http://localhost:9003); S3 API: `localhost:9002`. ClickHouse SQL UI: [localhost:8123/play](http://localhost:8123/play), user `default`, password from `.env` (`poc` by default). These credentials are for local development.

## How the tools work together

| Tool | Responsibility |
| --- | --- |
| Standalone Python writer | Downloads monthly taxi files and prepares Delta outside Airflow. |
| Airflow | Validates dates, runs Flyway, orchestrates dbt tasks, retries failures, and limits concurrency. |
| Flyway | Creates and changes ClickHouse tables through versioned migrations. |
| dbt | Defines the taxi SQL models, their `ref()` dependencies, tests, and incremental loading. |
| Cosmos | Turns dbt's dependencies into visible Airflow model/test tasks. |
| ClickHouse | Reads Delta from MinIO directly, executes transformations, and stores trip facts. |

The main DAG, `airflow/dags/delta_s3_to_clickhouse.py`, is:

```text
validate_run_config → flyway_migrate → dbt_lineage
                                       stg_nyc_taxi_trips.run
                                                ↓
                                       stg_nyc_taxi_trips.test
                                                ↓
                                       nyc_taxi_trips.run
                                                ↓
                                       nyc_taxi_trips.test
```

`nyc_taxi_trips.sql` uses `ref('stg_nyc_taxi_trips')`, which defines model order. Cosmos places tests after each model; failed input tests block the destination load. Each dbt task retries twice with exponential backoff. ClickHouse processes rows directly from Delta; Airflow does not collect trip rows in Python during an import. The staging model remains a normal ClickHouse view, recreated for each run.

## Actual taxi schema

`analytics.nyc_taxi_trips` has 24 columns:

| Fields | Storage |
| --- | --- |
| `trip_id`, `source_month`, `source_row_number` | Reproducible surrogate key and source provenance. |
| `pickup_date`, `pickup_at`, `dropoff_at` | NYC calendar date and microsecond timestamps with `America/New_York` timezone. |
| `vendor_id`, `passenger_count`, `trip_distance`, `rate_code_id`, `store_and_fwd_flag` | Trip details, preserving source nulls. |
| `pickup_location_id`, `dropoff_location_id`, `payment_type` | Actual TLC zone and payment codes. |
| `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee` | Nullable `Decimal(18,2)` USD amounts. |

Naive TLC timestamps are interpreted as NYC local time before writing Delta; ambiguous/nonexistent daylight-saving times use Arrow's `earliest` policy. Currency floats are rounded to cents on conversion to Decimal. Negative monetary corrections, zero distance, missing passenger counts, and other source anomalies are preserved. Fields absent in older source files become null.

TLC does not publish a unique trip ID. The surrogate combines source `YYYYMM` and original zero-based row position, assigned before date filtering. It is stable when the same source file ordering is replayed, including across different preparation chunk sizes. A revised/reordered upstream file is not guaranteed to represent the same trip IDs: reconcile a complete affected source month instead of assuming arbitrary updated files are safe row-level upserts. This POC's `delete+insert` removes matching incoming IDs; it does not remove source rows that disappeared.

Input tests check required provenance/date fields, unique trip IDs, and agreement between pickup date, NYC timestamp, and source month. The final table has a uniqueness test. Optional nullable taxi attributes are not replaced with invented values.

## Table names and migrations

A ClickHouse name is **`database.table`**. `CLICKHOUSE_DATABASE` in `compose.yaml` is `analytics`; `profiles.yml` supplies it as dbt's `schema`. Model filenames determine relation names:

| dbt file | ClickHouse relation |
| --- | --- |
| `models/stg_nyc_taxi_trips.sql` | `analytics.stg_nyc_taxi_trips` view |
| `models/nyc_taxi_trips.sql` | `analytics.nyc_taxi_trips` table |

To override a name, add `alias='another_name'` to the model's existing `config()`. `ref()` still uses the model name. Update the Flyway destination through a new migration to match; changing an alias or `CLICKHOUSE_DATABASE` alone does not rename hardcoded migration targets.

`migrations/V3__replace_events_with_nyc_taxi_trips.sql` defines the taxi destination and retires the previous `analytics.delta_events` table and staging view. V1/V2 remain only as immutable migration history so existing installations validate; they are superseded on fresh installs too. Never edit applied migrations. Keep dbt's engine, sorting, and partition configuration consistent with Flyway. Flyway JDBC and dbt use ClickHouse HTTP port `8123`.

## Backfills

`airflow/dags/delta_s3_to_clickhouse_backfill.py` defines a separate `delta_s3_to_clickhouse_backfill` DAG. It splits dates and triggers the main DAG with parameters; it contains no duplicate ingestion logic.

```json
{"start_date": "2025-01-01", "end_date": "2025-02-01", "days_per_batch": 7, "delta_s3_path": "http://minio:9000/poc/delta/nyc_taxi_trips/2025-01/"}
```

`days_per_batch=7` is the default; use `1` for daily windows. Windows start at the requested date and the final one stops at `end_date`. Each includes all trips in that pickup-date range. A prepared source path represents one source month: prepare and submit other months using their respective paths.

Each mapped `load_window` task has a **Triggered DAG** link to its main-DAG run and dbt tasks. Failed windows do not stop other windows; the backfill finishes failed if any child fails. After fixing the issue, clear a failed `load_window` task to rerun that child with the same parameters. Successful windows are unaffected.

Unpause the main DAG first. Waiting backfill tasks defer to Airflow's triggerer, releasing worker slots. The main DAG allows one active run and one task at a time across regular loads and backfills; a queued backfill can delay regular loads. Airflow normally limits mapped tasks to 1,024, so split longer backfills into smaller submissions.

## Insert strategy benchmark

Prepare the taxi source, then trigger `clickhouse_insert_benchmark`. Its separate dbt project in `benchmarks/` reuses the real `stg_nyc_taxi_trips` model and all 24 trip columns. Flyway creates isolated `poc_insert_benchmark` tables. It never truncates the main taxi table.

Four mapped groups compare `append` (index 0), `delete+insert` (1), `default` (2), and `insert_overwrite` (3). Each has visible `initial_load` → `replay` → `partial_update` tasks with three trials per phase:

- Initial: load a complete month into an empty table.
- Replay: reload that month over an existing baseline.
- Partial update: add USD 1.00 to each non-null `total_amount` in the first week while preserving all other days.

Each trial resets only its benchmark table and restores the baseline outside the measured interval. Checks compare row count, exact distinct trip IDs, and total amounts in integer cents. Expected correctness failures are benchmark findings; dbt execution errors fail the Airflow task. The final `report` task prints results and writes `/opt/airflow/logs/insert_benchmark/results.md` and `results.json`.

```sh
docker compose cp airflow:/opt/airflow/logs/insert_benchmark/results.md benchmarks/results.md
docker compose cp airflow:/opt/airflow/logs/insert_benchmark/results.json benchmarks/results.json
```

The [recorded results](benchmarks/results.md) cover all 3,475,204 trips with the full taxi schema and replace the old event-projection results. Both `delete+insert` and `default` passed all scenarios. `append` duplicated replayed rows; `insert_overwrite` lost the rest of the month during a partial-week update. The main model uses `delete+insert`.

Benchmarks use dbt `threads=1`, ClickHouse `max_threads=4`, `max_insert_threads=1`, `async_insert=1`, `wait_for_async_insert=1`, and `lightweight_deletes_sync=2`. Run while other ingestion is idle. Caches are warm and background merges are not isolated. Wall time includes dbt startup; SQL time sums tagged initial queries and excludes setup/checks. Reported memory is the largest individual query, not total server memory. There is no concurrent-writer, crash-recovery, or async-on/off test.

## Bulk loads, partitions, and concurrency

Keep one date-window load at a time initially. ClickHouse can parallelize work inside a query even when Airflow permits only one task and dbt has `threads=1`. Benchmark additional concurrent loads only when CPU, memory, disk, and merges have spare capacity. Other ingestion DAGs need a shared Airflow pool to coordinate access to the same server. Task limits do not cancel queries that remain server-side after a disconnect.

The destination uses monthly partitions (`toYYYYMM(pickup_date)`). Daily or weekly imports can write into them; window boundaries need not match partition boundaries. Concurrent inserts into the same partition create separate parts, but overlapping `trip_id` values can race under `delete+insert`. Plain `MergeTree` does not enforce business-key uniqueness or remove duplicate trip IDs during merges. Replacing an entire partition requires its complete intended contents, so `insert_overwrite` is unsuitable for a partial-week correction of a monthly partition.

For clients sending individual inserts, ClickHouse recommends at least 1,000 rows per insert, ideally 10,000–100,000, and around one synchronous insert per second. This is not a maximum row count for the dbt bulk `INSERT … SELECT` queries. Airflow windows control scope and retry cost; ClickHouse processes the query internally in blocks. See [bulk insert guidance](https://clickhouse.com/docs/concepts/features/operations/insert/bulkinserts) and [query parallelism](https://clickhouse.com/docs/concepts/core-concepts/query-parallelism).

```sql
SELECT count() AS trips, uniqExact(trip_id) AS distinct_trips,
       sum(total_amount) AS total_usd
FROM analytics.nyc_taxi_trips;

SELECT partition, sum(rows) AS rows, count() AS active_parts
FROM system.parts
WHERE database = 'analytics' AND table = 'nyc_taxi_trips' AND active
GROUP BY partition ORDER BY partition;
```
