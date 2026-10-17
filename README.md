# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. Airflow writes sample events to a local MinIO bucket as a Delta Lake table, then uses Flyway and Cosmos-rendered dbt tasks to validate and load a selected date window into ClickHouse.

## Requirements

- Docker Compose

## Run

```sh
cp .env.example .env
docker compose up -d --build
```

Open Airflow 3 at <http://localhost:8083>. Sign in as `admin`; Airflow 3 generates its password on first startup. Retrieve it with `docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`. The local setup uses Airflow 3's `LocalExecutor` with SQLite and a single task slot. Trigger `delta_s3_to_clickhouse` with the default parameters, or pass a weekly half-open date range and the tenant IDs for that week as DAG run configuration, for example:

```json
{"start_date": "2026-10-12", "end_date": "2026-10-19", "tenant_ids": [101, 202]}
```

Set `days_per_batch` to a positive integer: `1` (default) for daily loads or `7` for weekly backfills. Windows start at your requested `start_date`; the final window is shortened to stop at the exclusive `end_date`. For example, this creates October 1–8, 8–15, and 15–18 windows:

```json
{"start_date": "2026-10-01", "end_date": "2026-10-18", "tenant_ids": [101, 202], "days_per_batch": 7}
```

Airflow seeds 800 sample tenants by default and maps the selected date range into independent `process_window` groups. Each contains a Cosmos group rendering `+delta_events`: staging model/input tests → incremental model/destination uniqueness check → cleanup. A failed input test blocks only that window's load; other windows continue. Each dbt task retries twice with exponential backoff. Expand the mapped groups in Airflow to inspect individual windows and retry their failed tasks. One active DAG run and one active task limit ClickHouse concurrency. The date range is start-inclusive and end-exclusive.

The DAG validates the run parameters, writes the Delta table at `http://minio:9000/poc/delta/events/` (MinIO console: <http://localhost:9003>; S3 API: <http://localhost:9002>), runs Flyway, then creates the windows. Each window has its own staging view. Cleanup runs even if a dbt task fails. The final `all_windows_succeeded` task depends on every processing task so cleanup cannot hide a failure: all windows are attempted, but the overall run remains failed if any task exhausts its retries. Validation, seeding, or migration failures block processing because all windows need those prerequisites.

There are two partition controls in this POC:

- **Which data gets processed:** set `start_date`, `end_date`, and `tenant_ids` in the Airflow DAG run configuration. The staging model filters by the half-open date range and the tenant list before the final model runs.
- **How data is stored:** the seed task writes the Delta table partitioned by `event_date`; the ClickHouse model stores data in monthly partitions using `toYYYYMM(event_date)`. Change `partition_by` in `airflow/dags/delta_s3_to_clickhouse.py` or `models/delta_events.sql` to use a different partition key/granularity. For daily ClickHouse partitions, for example, use `toYYYYMMDD(event_date)`.

Basic dbt tests in `models/schema.yml` check that tenant/event IDs and required columns are non-null, `(tenant_id, event_id)` pairs are unique, and event types are among the sample values. Flyway connects over ClickHouse's native TCP port `9000`; dbt connects over HTTP port `8123`. Flyway creates the `analytics` database and defines `analytics.delta_events` in `migrations/V2__create_delta_events.sql`; dbt maintains its incremental contents in `models/delta_events.sql`. The sample data and local MinIO credentials are for local development only.

## Production insert sizing

This POC permits one active task at a time. Its incremental model uses the dbt-clickhouse adapter to issue a set-based `INSERT … SELECT` from the Delta-backed staging view; ClickHouse processes the selected rows as a bulk query. dbt does not buffer rows into client-side insert batches in this flow. Bulk sizing matters most when an application or loader sends individual `INSERT` requests: ClickHouse recommends at least 1,000 rows per insert, ideally 10,000–100,000, and around one synchronous insert query per second. Larger batches reduce part creation and background merge work. If the source cannot be batched on the client, consider ClickHouse asynchronous inserts.

Airflow owns the windows and retries; dbt uses the ordinary `delete+insert` incremental strategy for each window. Each window has visible status and independent retries. Weekly backfills reduce task overhead but increase each load's runtime and retry cost; benchmark a representative week before using them for large historical loads. Row counts per window are unbounded. Airflow's mapped-task count is also limited by `max_map_length` (normally 1,024); submit longer historical backfills in smaller date ranges. See [ClickHouse bulk insert guidance](https://clickhouse.com/docs/concepts/features/operations/insert/bulkinserts) and [dbt-clickhouse incremental strategies](https://clickhouse.com/docs/integrations/data-ingestion/etl-tools/dbt/materializations).

## Load concurrency

Start with one load at a time. The DAG sets `max_active_runs=1` and `max_active_tasks=1`, preserving separate window tasks for visibility and retries while limiting concurrent work. ClickHouse can still execute a single load using multiple threads internally; Airflow's task limit and dbt's `threads: 1` do not restrict a ClickHouse query to one CPU thread. See [ClickHouse query parallelism](https://clickhouse.com/docs/concepts/core-concepts/query-parallelism).

Multiple concurrent loads can improve throughput when one load leaves spare capacity, such as while waiting on S3. They can also increase contention for CPU, memory, disk, and background merges. Benchmark one load first, then two, and increase concurrency only if total throughput improves while normal query latency and merge backlog remain acceptable. The DAG limits apply only to this DAG: use a shared Airflow pool across ingestion DAGs targeting the same ClickHouse instance. These limits constrain submitted tasks, so also account for queries that continue server-side after a client disconnect and for background merge work.

ClickHouse supports concurrent inserts into the same monthly partition. Inserts create separate data parts that background merges consolidate; sharing a partition does not cause rows to overwrite each other. Daily or weekly insert windows therefore do not need to match monthly storage partitions. More concurrent inserts can increase resource usage and merge pressure. See [ClickHouse parts and merges](https://clickhouse.com/resources/engineering/clickhouse-optimize-table-final).

Overlapping business keys are a separate concern: concurrent dbt `delete+insert` loads affecting the same `(tenant_id, event_id)` can interfere and produce incorrect results. Different date windows do not guarantee disjoint keys because `event_date` is not part of this model's unique key. The ordinary `MergeTree` table does not enforce business-key uniqueness or remove duplicates during background merges. Keep one load at a time initially; before enabling parallel writes, ensure their keys do not overlap and benchmark the resulting load.

To inspect the loaded result, connect to ClickHouse at `localhost:8123` and query `analytics.delta_events`.

To inspect ClickHouse's generated monthly partitions:

```sql
SELECT partition, rows
FROM system.parts
WHERE database = 'analytics' AND table = 'delta_events' AND active
ORDER BY partition;
```
