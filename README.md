# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. The old DuckDB and OpenShift pipeline is removed. ClickHouse reads an existing Delta Lake table in S3; dbt materializes its rows into a ClickHouse `MergeTree` table. Flyway owns ClickHouse database migrations, and Airflow orchestrates Flyway followed by `dbt build`.

## Requirements

- Docker Compose
- An existing Delta Lake table in S3 with `event_id`, `event_date`, `event_type`, and `user_id` columns

## Run

```sh
cp .env.example .env
docker compose up -d
```

Open Airflow at <http://localhost:8080> and sign in with `admin` / `AIRFLOW_PASSWORD` (defaults to `admin`). Trigger the `delta_s3_to_clickhouse` DAG. It runs Flyway first, then `dbt build`.

Set `DELTA_S3_PATH` to the table root, including its `_delta_log` directory (for example `s3://my-bucket/events/`). For public S3 data, leave the AWS credentials blank. For private S3, provide a key and secret; ClickHouse also supports instance roles, which are preferable outside this POC. `AWS_REGION` can be set for the environment where ClickHouse runs. The Compose file reads `.env` to configure ClickHouse; also export the Delta path and S3 credentials in the shell where you run dbt, because dbt executes on the host.

Flyway connects over ClickHouse's native TCP port `9000`. dbt connects over HTTP port `8123`. The Airflow image installs the Flyway CLI with its ClickHouse plugin/JDBC driver and the pinned dbt adapter. The Flyway migration creates the `analytics` database. The table definition lives in `models/delta_events.sql`: dbt creates `analytics.delta_events` as a `MergeTree` table from that model's `SELECT` query. Re-running `dbt build` rebuilds it from the current Delta snapshot; it does not continuously sync the lake or preserve Delta change history.

To run dbt locally instead, install `requirements.txt`, export the connection and S3 variables in `.env` into your shell, then run `dbt build --profiles-dir .` after `flyway migrate`.
