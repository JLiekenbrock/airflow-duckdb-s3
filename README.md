# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. Airflow writes sample events to a local MinIO bucket as a Delta Lake table, then runs Flyway and `dbt build` to load a selected date window into ClickHouse.

## Requirements

- Docker Compose

## Run

```sh
cp .env.example .env
docker compose up -d --build
```

Open Airflow at <http://localhost:8080> and sign in with `admin` / `AIRFLOW_PASSWORD` (defaults to `admin`). Trigger `delta_s3_to_clickhouse` with the default parameters, or pass a weekly half-open date range and the tenant IDs for that week as DAG run configuration, for example:

```json
{"start_date": "2026-10-12", "end_date": "2026-10-19", "tenant_ids": [101, 202], "tenant_batch_size": 25}
```

Airflow seeds 800 small sample tenants by default and splits `tenant_ids` into batches of `tenant_batch_size` (default 25). This creates 32 mapped Cosmos task groups. Expand a group in Airflow's Graph view to see the dbt models and tests and their `ref()` dependency order. Each batch gets its own staging view; its Cosmos dbt tasks run the batch, then a cleanup task drops the view. Cosmos dbt tasks retry twice. The local Compose setup uses Airflow's `SequentialExecutor`; it demonstrates mapping and per-task retries without parallel execution. Each mapped group uses a separate dbt target directory. The date range is start-inclusive and end-exclusive.

The DAG seeds a Delta table at `http://minio:9000/poc/delta/events/` (MinIO console: <http://localhost:9003>; S3 API: <http://localhost:9002>), runs Flyway, then processes the selected date window. The high-level Airflow order is `seed_local_delta` → `migrate` → mapped `dbt_tenant_batch` → mapped `drop_stage_view`. Inside each Cosmos group, dbt's `ref('stg_delta_events')` dependency orders the staging view before the final model, and attached tests run after the model.

There are two partition controls in this POC:

- **Which data gets processed:** set `start_date`, `end_date`, and `tenant_ids` in the Airflow DAG run configuration. The staging model filters by both the half-open date range and the tenant batch before the final model runs.
- **How data is stored:** the seed task writes the Delta table partitioned by `event_date`; the ClickHouse model stores data in monthly partitions using `toYYYYMM(event_date)`. Change `partition_by` in `airflow/dags/delta_s3_to_clickhouse.py` or `models/delta_events.sql` to use a different partition key/granularity. For daily ClickHouse partitions, for example, use `toYYYYMMDD(event_date)`.

Basic dbt tests in `models/schema.yml` check that tenant/event IDs and required columns are non-null, `(tenant_id, event_id)` pairs are unique, and event types are among the sample values. Flyway connects over ClickHouse's native TCP port `9000`; dbt connects over HTTP port `8123`. Flyway creates the `analytics` database and defines `analytics.delta_events` in `migrations/V2__create_delta_events.sql`; dbt maintains its incremental contents in `models/delta_events.sql`. The sample data and local MinIO credentials are for local development only.

To inspect the loaded result, connect to ClickHouse at `localhost:8123` and query `analytics.delta_events`.

To inspect ClickHouse's generated monthly partitions:

```sql
SELECT partition, rows
FROM system.parts
WHERE database = 'analytics' AND table = 'delta_events' AND active
ORDER BY partition;
```
