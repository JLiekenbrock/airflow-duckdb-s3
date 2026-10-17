# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. Airflow writes sample events to a local MinIO bucket as a Delta Lake table, then uses Flyway and Cosmos-rendered dbt tasks to validate and load a selected date window into ClickHouse.

## Requirements

- Docker Compose

## Run

```sh
cp .env.example .env
docker compose up -d --build
```

Open Airflow 3 at <http://localhost:8083>. Sign in as `admin`; Airflow 3 generates its password on first startup. Retrieve it with `docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`. The local setup uses Airflow 3's `LocalExecutor` with SQLite and a single task slot.

## Regular loads

Trigger `delta_s3_to_clickhouse`. One run loads the entire requested date range for all selected tenants, without splitting it into batches:

```json
{"start_date": "2026-10-12", "end_date": "2026-10-19", "tenant_ids": [101, 202]}
```

The default tenant selection is all 800 sample tenants (IDs 1–800). The example selects tenants 101 and 202 together. Dates are start-inclusive and end-exclusive.

The main DAG in `airflow/dags/delta_s3_to_clickhouse.py` is:

```text
validate_run_config → seed_local_delta → flyway_migrate → dbt_lineage
```

Cosmos reads dbt's `ref()` dependencies and renders staging model → input tests → final model → destination uniqueness test. Input failures block the load. Each dbt task retries twice with exponential backoff. Staging remains a normal dbt view (`analytics.stg_delta_events`), recreated for each run; there is no custom cleanup or success gate.

The seed task writes a tiny demo Delta table at `http://minio:9000/poc/delta/events/` with one event per sample tenant during October 12–18, 2026. It overwrites that demo source on each run. Production ingestion should read your existing Delta source instead of using this seed task. MinIO console: <http://localhost:9003>; S3 API: <http://localhost:9002>.

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
- **How data is stored:** the seed task writes the Delta table partitioned by `event_date`; the ClickHouse model stores data in monthly partitions using `toYYYYMM(event_date)`. Change `partition_by` in `airflow/dags/delta_s3_to_clickhouse.py` or `models/delta_events.sql` to use a different partition key/granularity. For daily ClickHouse partitions, for example, use `toYYYYMMDD(event_date)`.

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
