# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. Airflow uses Flyway and Cosmos-rendered dbt tasks to validate and import existing Delta data from S3/MinIO into ClickHouse. A separate manual script supplies optional local demo data.

## How the tools work together

| Tool | Responsibility in this repo |
| --- | --- |
| Airflow | Runs the workflow, passes dates and tenant IDs to dbt, and controls retries and concurrency. The separate backfill DAG reuses the main DAG with different date parameters. |
| Flyway | Applies versioned migrations that create the destination database and table before dbt runs. |
| dbt + Cosmos | dbt defines SQL models, dependencies through `ref()`, and data tests. Cosmos turns those dependencies into Airflow model and test tasks. |
| ClickHouse | Executes the SQL, reads the Delta data from S3/MinIO, and stores the loaded rows. |

The staging model creates a ClickHouse view over `deltaLake(...)`, filtered by the requested dates and tenants. The final incremental model reads it using `ref('stg_delta_events')`. dbt submits the SQL to ClickHouse; ClickHouse reads and transforms the data directly. Transaction rows do not pass through the Airflow worker's Python memory during this load.

## ClickHouse table names

A fully qualified name is **`database.table`**. `CLICKHOUSE_DATABASE` in `compose.yaml` is currently `analytics`; `profiles.yml` reads that value as dbt's `schema`, which the ClickHouse adapter uses as the database name. By default, the model filename determines the table or view name:

| dbt model | ClickHouse name | Type |
| --- | --- | --- |
| `models/stg_delta_events.sql` | `analytics.stg_delta_events` | View managed by dbt |
| `models/delta_events.sql` | `analytics.delta_events` | Destination table created by Flyway and loaded incrementally by dbt |

To override a model's table name, add an `alias` to its existing `config()` block, for example `alias='transactions'`. The model `delta_events` would then target `analytics.transactions`. References still use the model name, such as `ref('delta_events')`; dbt resolves the alias automatically.

Flyway defines the destination table in `migrations/V2__create_delta_events.sql`. Its database and table name must match dbt's resolved destination name. Changing `CLICKHOUSE_DATABASE` or a model alias does not rename the hardcoded destination in an existing migration. Add a new Flyway migration for an existing deployment and update the dbt configuration together; do not edit an already applied migration. Keep the Flyway table definition and dbt's engine, sorting, and partition settings consistent.

## Requirements

- Docker Compose

## Run

```sh
cp .env.example .env
docker compose up -d --build
```

Open Airflow 3 at <http://localhost:8083>. Sign in as `admin`; Airflow 3 generates its password on first startup. Retrieve it with `docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`. The local setup uses Airflow 3's `LocalExecutor` with SQLite and a single task slot.

For a fresh local demo, run the standalone writer once before triggering a load:

```sh
docker compose exec -T airflow /usr/local/bin/python /opt/dbt/scripts/seed_local_delta.py
```

This manually creates or overwrites the demo Delta table at `s3://poc/delta/events`, partitioned by `event_date`, with one event for each of 800 tenants during October 12–18, 2026. It uses the container's Python environment only; it is not an Airflow task. Skip it when importing your own existing Delta table. Both DAGs only read the S3 source, including on retries and backfills. MinIO console: <http://localhost:9003>; S3 API: <http://localhost:9002>.

## Larger demo: NYC yellow taxis

The standalone `scripts/seed_nyc_taxi.py` downloads a month from the [official NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), projects trips into the existing POC event schema, and writes a separate date-partitioned Delta table in MinIO. It reads Parquet in 250,000-row chunks rather than building a Python list of millions of trips. January 2025 is about 59 MB compressed and contains 3,475,226 source trips.

```sh
docker compose exec -T airflow /usr/local/bin/python /opt/dbt/scripts/seed_nyc_taxi.py --month 2025-01
```

This creates or replaces only `s3://poc/delta/nyc_taxi/2025-01`; the small demo source is preserved. Source pickup zone IDs become `tenant_id`, pickup dates become `event_date`, vendor IDs become `user_id` (null vendors become 0), and `event_type` is the constant `purchase`. An artificial `event_id` combines source month and original row position. These are benchmark mappings, not actual customer or passenger identities. The writer excludes pickup dates outside the chosen month and zones outside 1–800 and reports the excluded count. This is a narrow event projection, not a complete taxi analytics schema; the source has no official trip ID, and row-position IDs assume unchanged source file ordering.

Trigger the existing `delta_s3_to_clickhouse` DAG with:

```json
{"start_date": "2025-01-01", "end_date": "2025-02-01", "delta_s3_path": "http://minio:9000/poc/delta/nyc_taxi/2025-01/"}
```

Omitting `tenant_ids` processes all default tenant IDs (including every mapped pickup zone). The optional `delta_s3_path` overrides the default `DELTA_S3_PATH` for this run; it is also forwarded by the backfill DAG. To exercise weekly backfills on this dataset, pass the same configuration to `delta_s3_to_clickhouse_backfill` with `days_per_batch: 7`. The destination remains `analytics.delta_events`; taxi event IDs are separate from the small demo IDs. Prepare additional months with `--month YYYY-MM` and load their respective source paths. This one-month example demonstrates millions of projected rows, not a benchmark of hundreds of millions or the full taxi payload.

Local run on October 17, 2026: 3,475,204 rows loaded, 22 source records excluded, 261 pickup zones, and no duplicate `(tenant_id, event_id)` keys. All Airflow tasks and dbt tests passed. Download/conversion took about 5 seconds and the import task sequence about 13 seconds, including about 2.8 seconds for the final dbt model task. These are observations from this local narrow projection, not throughput guarantees.

```sql
SELECT count() AS rows, uniqExact((tenant_id, event_id)) AS distinct_keys
FROM analytics.delta_events
WHERE toYYYYMM(event_date) = 202501;
```

## Insert strategy benchmark in Airflow

After preparing the NYC taxi source, unpause and trigger `clickhouse_insert_benchmark`. It is defined in `airflow/dags/clickhouse_insert_benchmark.py` and runs a separate dbt project in `benchmarks/`. Each mapped strategy group shows `initial_load` → `replay` → `partial_update`, with three measured trials per phase by default. The final `report` task prints the comparison and writes Markdown and JSON results under `/opt/airflow/logs/insert_benchmark/`.

Mapped indexes are `0=append`, `1=delete+insert`, `2=default`, and `3=insert_overwrite`. The normal import DAG continues using `delete+insert`; benchmark choices do not change its configuration.

| Strategy | Question tested |
| --- | --- |
| `append` | How fast is loading new rows, and what happens on replay? |
| `delete+insert` | Can an existing time window be replayed and corrected by unique key? |
| `default` | What is the cost of rebuilding the table with old and new rows? |
| `insert_overwrite` | Is replacing monthly partitions suitable for full-month loads and partial-week corrections? |

The initial load inserts a full month into an empty table. Replay repeats that month over an existing baseline. The partial update increases `user_id` by one for the first seven days; all other days must remain. Each trial resets only its benchmark table and recreates the same baseline outside the timed measurement. Row count, exact distinct keys, and the sum of `user_id` are compared with expected values. A strategy producing duplicates or losing other days is recorded as incorrect, even if it is fast. These expected correctness findings do not fail Airflow tasks; dbt execution errors do.

Flyway creates the isolated `poc_insert_benchmark` tables using the optional migrations in `benchmarks/migrations/` and a separate history table. The benchmark does not change `analytics.delta_events`. It measures dbt wall time, tagged SQL duration, largest individual query memory usage, and active parts. It runs one task at a time with `max_threads=4`, `max_insert_threads=1`, and dbt `threads=1`. Run it while other ingestion is idle. Local cache and background merge effects are not isolated, so results are indicative rather than a universal best strategy. It does not measure Airflow scheduling overhead as part of dbt wall time, concurrent writes, or fault recovery.

This compares dbt incremental strategies for bulk `INSERT … SELECT` loads. Insert settings match the measured server defaults and are pinned in the benchmark profile: `async_insert=1`, `wait_for_async_insert=1`, and `lightweight_deletes_sync=2`. This is not a comparison of client payload sizes or async enabled versus disabled.

To retrieve the report after completion:

```sh
docker compose cp airflow:/opt/airflow/logs/insert_benchmark/results.md benchmarks/results.md
docker compose cp airflow:/opt/airflow/logs/insert_benchmark/results.json benchmarks/results.json
```

The [recorded local report](benchmarks/results.md) and [raw measurements](benchmarks/results.json) contain 36 measured trials on 3,475,204 projected NYC trips. Initial loads favored `append` (2.524 s median dbt wall time); full-month replay favored `insert_overwrite` (2.609 s); partial-week correction favored `delete+insert` (2.558 s). Append produced duplicates on replay. Partition overwrite replaced the whole January partition with only the correction week, losing the remaining days. Keep `delete+insert` for this POC's arbitrary date-window reprocessing. Consider append only with external guarantees against duplicate ingestion, and partition overwrite only when each input contains the complete intended contents of every affected partition. These results do not establish behavior after a crash or the best strategy for a wider or larger dataset.

## Regular loads

Trigger `delta_s3_to_clickhouse`. One run loads the entire requested date range for all selected tenants, without splitting it into batches:

```json
{"start_date": "2026-10-12", "end_date": "2026-10-19", "tenant_ids": [101, 202]}
```

The default tenant selection is all 800 sample tenants (IDs 1–800). The example selects tenants 101 and 202 together. Dates are start-inclusive and end-exclusive.

The main DAG in `airflow/dags/delta_s3_to_clickhouse.py` is:

```text
validate_run_config → flyway_migrate → dbt_lineage
```

Cosmos reads dbt's `ref()` dependencies and renders staging model → input tests → final model → destination uniqueness test. Input failures block the load. Each dbt task retries twice with exponential backoff. Staging remains a normal dbt view (`analytics.stg_delta_events`), recreated for each run; there is no custom cleanup or success gate.

## Backfills

Trigger `delta_s3_to_clickhouse_backfill`, defined separately in `airflow/dags/delta_s3_to_clickhouse_backfill.py`. It only splits dates and triggers the main DAG with parameters; all Flyway and dbt processing is reused.

Set `days_per_batch` to a positive integer: `7` (default) for weekly windows or `1` for daily windows. Windows start at your requested `start_date`; the final window stops at the exclusive `end_date`. This example creates October 1–8, 8–15, and 15–18 windows:

```json
{"start_date": "2026-10-01", "end_date": "2026-10-18", "tenant_ids": [101, 202], "days_per_batch": 7}
```

**Every backfill window contains all selected tenants together.** The backfill DAG splits dates, not tenants.

| Settings | Data processed in each window |
| --- | --- |
| All default tenants, `days_per_batch=1` | One day for all 800 tenants |
| All default tenants, `days_per_batch=7` | Up to seven days for all 800 tenants |
| `tenant_ids=[101, 202]`, `days_per_batch=7` | Up to seven days for tenants 101 and 202 together |

Each mapped `load_window` task waits for its main-DAG run and reports that window's result. Follow its **Triggered DAG** link to see the dbt models, tests, logs, and retry history. A failed window does not stop other windows; the backfill run finishes failed if any window fails. After fixing the cause, clear a failed mapped `load_window` task to rerun that window's child DAG with the same parameters. Successful windows are unaffected.

Unpause the main DAG before starting a backfill. Waiting tasks use Airflow's triggerer (`deferrable=True`) to release worker slots, so the main DAG can run even with one local task slot. The main DAG allows one active run, serializing regular and backfill loads together. A large queued backfill can therefore delay regular runs.

There are two partition controls in this POC:

- **Which data gets processed:** set `start_date`, `end_date`, and `tenant_ids` in the Airflow DAG run configuration. The staging model filters by the half-open date range and the tenant list before the final model runs.
- **How data is stored:** the standalone seed script writes the demo Delta table partitioned by `event_date`; the ClickHouse destination uses monthly partitions with `toYYYYMM(event_date)`. Change the demo source partitioning in `scripts/seed_local_delta.py`. To change an existing ClickHouse table's partitioning, add a Flyway migration and keep the settings in `models/delta_events.sql` consistent.

Column tests in `models/schema.yml` check required values and accepted event types; SQL tests in `tests/` check uniqueness of `(tenant_id, event_id)`. Flyway's JDBC driver and dbt connect over ClickHouse's HTTP port `8123`. Flyway creates the `analytics` database and defines `analytics.delta_events` in `migrations/V2__create_delta_events.sql`; dbt maintains its incremental contents in `models/delta_events.sql`. The sample data and local MinIO credentials are for local development only.

## Production insert sizing

This POC permits one active task at a time. Its incremental model uses the dbt-clickhouse adapter to issue a set-based `INSERT … SELECT` from the Delta-backed staging view; ClickHouse processes the selected rows as a bulk query. dbt does not buffer rows into client-side insert batches in this flow. Bulk sizing matters most when an application or loader sends individual `INSERT` requests: ClickHouse recommends at least 1,000 rows per insert, ideally 10,000–100,000, and around one synchronous insert query per second. Larger batches reduce part creation and background merge work. If the source cannot be batched on the client, consider ClickHouse asynchronous inserts.

Only the backfill DAG creates batches; dbt uses the ordinary `delete+insert` incremental strategy for each main-DAG run. Weekly backfills reduce orchestration overhead but increase each load's runtime and retry cost; benchmark a representative week before using them for large historical loads. Row counts per window are unbounded. Airflow's mapped-task count is also limited by `max_map_length` (normally 1,024); submit longer historical backfills in smaller date ranges. See [ClickHouse bulk insert guidance](https://clickhouse.com/docs/concepts/features/operations/insert/bulkinserts) and [dbt-clickhouse incremental strategies](https://clickhouse.com/docs/integrations/data-ingestion/etl-tools/dbt/materializations).

## Load concurrency

**One load at a time means one main-DAG run, including all selected tenants.** Its `max_active_runs=1` and `max_active_tasks=1` limits apply to regular runs and runs triggered by backfills alike. Mapped backfill tasks can wait concurrently, but their child data loads execute one at a time. ClickHouse can still execute a single load using multiple threads internally; Airflow's task limit and dbt's `threads: 1` do not restrict a ClickHouse query to one CPU thread. See [ClickHouse query parallelism](https://clickhouse.com/docs/concepts/core-concepts/query-parallelism).

Multiple concurrent loads can improve throughput when one load leaves spare capacity, such as while waiting on S3. They can also increase contention for CPU, memory, disk, and background merges. Benchmark one load first, then two, and increase concurrency only if total throughput improves while normal query latency and merge backlog remain acceptable. The DAG limits apply only to this DAG: use a shared Airflow pool across ingestion DAGs targeting the same ClickHouse instance. These limits constrain submitted tasks, so also account for queries that continue server-side after a client disconnect and for background merge work.

ClickHouse supports concurrent inserts into the same monthly partition. Inserts create separate data parts that background merges consolidate; sharing a partition does not cause rows to overwrite each other. Daily or weekly insert windows therefore do not need to match monthly storage partitions. More concurrent inserts can increase resource usage and merge pressure. See [ClickHouse parts and merges](https://clickhouse.com/resources/engineering/clickhouse-optimize-table-final).

If you later enable concurrent window loads, overlapping business keys become a separate concern: concurrent dbt `delete+insert` loads affecting the same `(tenant_id, event_id)` can interfere and produce incorrect results. Different date windows do not guarantee disjoint keys because `event_date` is not part of this model's unique key. The ordinary `MergeTree` table does not enforce business-key uniqueness or remove duplicates during background merges. Before enabling parallel writes, ensure their keys do not overlap and benchmark the resulting load.

To inspect the loaded result, connect to ClickHouse at `localhost:8123` and query `analytics.delta_events`.

To inspect ClickHouse's generated monthly partitions:

```sql
SELECT partition, rows
FROM system.parts
WHERE database = 'analytics' AND table = 'delta_events' AND active
ORDER BY partition;
```
