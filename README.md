# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. Airflow writes sample events to a local MinIO bucket as a Delta Lake table, then runs Flyway and `dbt build` to load a selected date window into ClickHouse.

## Requirements

- Docker Compose

## Run

```sh
cp .env.example .env
docker compose up -d
```

Open Airflow at <http://localhost:8080> and sign in with `admin` / `AIRFLOW_PASSWORD` (defaults to `admin`). Trigger `delta_s3_to_clickhouse` with the default date parameters, or pass a half-open date range as DAG run configuration, for example:

```json
{"start_date": "2026-10-16", "end_date": "2026-10-18"}
```

The DAG seeds a tiny Delta table at `http://minio:9000/poc/delta/events/` (MinIO console: <http://localhost:9003>; S3 API: <http://localhost:9002>), runs Flyway, then passes the requested date window to dbt. The date range is start-inclusive and end-exclusive. The model reads only that window and ClickHouse stores rows in monthly partitions using `toYYYYMM(event_date)`. The incremental `delete+insert` strategy uses `event_id`, so rerunning a window replaces matching rows instead of appending duplicates.

Flyway connects over ClickHouse's native TCP port `9000`; dbt connects over HTTP port `8123`. Flyway creates the `analytics` database. `models/delta_events.sql` defines the incremental ClickHouse table. Use `dbt build --full-refresh` for a one-time full rebuild. The sample data and local MinIO credentials are for local development only.

To inspect the loaded result, connect to ClickHouse at `localhost:8123` and query `analytics.delta_events`.
