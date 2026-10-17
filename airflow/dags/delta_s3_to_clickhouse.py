from datetime import date, datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


DEMO_ROWS = [
    {"event_id": 1, "event_date": date(2026, 10, 16), "event_type": "page_view", "user_id": 101},
    {"event_id": 2, "event_date": date(2026, 10, 16), "event_type": "purchase", "user_id": 101},
    {"event_id": 3, "event_date": date(2026, 10, 17), "event_type": "page_view", "user_id": 202},
]


with DAG(
    dag_id="delta_s3_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"start_date": "2026-10-16", "end_date": "2026-10-18"},
    tags=["dbt", "clickhouse", "delta-lake"],
) as dag:
    @task
    def seed_local_delta():
        """Write a tiny repeatable Delta dataset to the local MinIO bucket."""
        import pyarrow as pa
        from deltalake import write_deltalake

        endpoint = "http://minio:9000"
        write_deltalake(
            "s3://poc/delta/events",
            pa.Table.from_pylist(DEMO_ROWS),
            mode="overwrite",
            storage_options={
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ACCESS_KEY_ID": "minio",
                "AWS_SECRET_ACCESS_KEY": "minio123",
                "AWS_REGION": "us-east-1",
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ADDRESSING_STYLE": "path",
            },
        )

    seed = seed_local_delta()

    migrate = BashOperator(
        task_id="flyway_migrate",
        bash_command="flyway -configFiles=/opt/dbt/flyway.toml migrate",
        cwd="/opt/dbt",
        append_env=True,
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build --project-dir /opt/dbt --profiles-dir /opt/dbt "
            "--vars \"{start_date: '${START_DATE}', end_date: '${END_DATE}'}\""
        ),
        env={"START_DATE": "{{ params.start_date }}", "END_DATE": "{{ params.end_date }}"},
        cwd="/opt/dbt",
        append_env=True,
    )

    seed >> migrate >> build
