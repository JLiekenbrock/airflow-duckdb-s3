# S3 Delta Lake to ClickHouse with dbt

Small proof of concept based on the original Airflow + DuckDB S3 project. Airflow, DuckDB, OpenShift, and the multi-model pipeline are removed. ClickHouse reads an existing Delta Lake table in S3; dbt materializes its rows into a ClickHouse `MergeTree` table. Flyway owns ClickHouse database migrations.

## Requirements

- Docker Compose
- Python 3.10+
- Flyway CLI with its ClickHouse database plugin and ClickHouse JDBC driver on the classpath
- An existing Delta Lake table in S3 with `event_id`, `event_date`, `event_type`, and `user_id` columns

## Run

```sh
cp .env.example .env
docker compose up -d
python -m venv .venv
# activate the venv, then:
pip install -r requirements.txt
flyway migrate
dbt debug
dbt run
```

Set `DELTA_S3_PATH` to the table root, including its `_delta_log` directory (for example `s3://my-bucket/events/`). For public S3 data, leave the AWS credentials blank. For private S3, provide a key and secret; ClickHouse also supports instance roles, which are preferable outside this POC. `AWS_REGION` can be set for the environment where ClickHouse runs. The Compose file reads `.env` to configure ClickHouse; also export the Delta path and S3 credentials in the shell where you run dbt, because dbt executes on the host.

Flyway connects over ClickHouse's native TCP port `9000`. dbt connects over HTTP port `8123`. The Flyway ClickHouse database plugin and ClickHouse JDBC driver must be installed for the Flyway CLI; see [Flyway ClickHouse support](https://documentation.red-gate.com/flyway/reference/database-driver-reference/clickhouse-database).

The resulting table is `analytics.delta_events`. Re-running `dbt run` rebuilds the table from the current Delta snapshot; it does not continuously sync the lake or preserve Delta change history.
