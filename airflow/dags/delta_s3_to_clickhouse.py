from datetime import date, datetime, timedelta
import json
import subprocess

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context


DEMO_ROWS = [
    {
        "tenant_id": tenant_id,
        "event_id": 1,
        "event_date": date(2026, 10, 12) + timedelta(days=tenant_id % 7),
        "event_type": "purchase" if tenant_id % 2 else "page_view",
        "user_id": tenant_id,
    }
    for tenant_id in range(1, 801)
]

DEFAULT_TENANT_BATCH_SIZE = 25


def run_dbt(command, batch):
    """Run one isolated dbt invocation for a tenant batch."""
    vars_json = json.dumps(
        {
            "start_date": batch["start_date"],
            "end_date": batch["end_date"],
            "tenant_ids": batch["tenant_ids"],
            "batch_id": batch["batch_id"],
        }
    )
    subprocess.run(
        [
            "dbt", command, "--project-dir", "/opt/dbt", "--profiles-dir", "/opt/dbt",
            "--select", "stg_delta_events" if command == "run" else "delta_events",
            "--vars", vars_json,
            "--target-path", f"/tmp/dbt-target-{batch['batch_id']}",
        ],
        cwd="/opt/dbt",
        check=True,
    )


with DAG(
    dag_id="delta_s3_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "start_date": "2026-10-12",
        "end_date": "2026-10-19",
        "tenant_ids": list(range(1, 801)),
        "tenant_batch_size": DEFAULT_TENANT_BATCH_SIZE,
    },
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
            partition_by=["event_date"],
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

    @task
    def migrate():
        subprocess.run(
            ["flyway", "-configFiles=/opt/dbt/flyway.toml", "migrate"],
            cwd="/opt/dbt",
            check=True,
        )

    @task
    def make_tenant_batches():
        context = get_current_context()
        params = context["params"]
        tenant_ids = list(dict.fromkeys(int(tenant_id) for tenant_id in params["tenant_ids"]))
        if not tenant_ids:
            raise ValueError("Provide at least one tenant_id in the DAG run configuration")
        if any(tenant_id < 0 for tenant_id in tenant_ids):
            raise ValueError("tenant_ids must be non-negative integers")
        if params["start_date"] >= params["end_date"]:
            raise ValueError("end_date must be later than start_date")
        batch_size = int(params["tenant_batch_size"])
        if batch_size < 1:
            raise ValueError("tenant_batch_size must be a positive integer")

        return [
            {
                "batch_id": f"batch_{index:03d}",
                "tenant_ids": tenant_ids[offset : offset + TENANT_BATCH_SIZE],
                "start_date": params["start_date"],
                "end_date": params["end_date"],
            }
            for index, offset in enumerate(range(0, len(tenant_ids), batch_size))
        ]

    @task(retries=2)
    def dbt_stage(batch):
        run_dbt("run", batch)

    @task(retries=2)
    def dbt_build(batch):
        run_dbt("build", batch)

    @task(retries=1)
    def drop_stage_view(batch):
        vars_json = json.dumps({"batch_id": batch["batch_id"]})
        subprocess.run(
            [
                "dbt", "run-operation", "drop_batch_stage_view",
                "--project-dir", "/opt/dbt", "--profiles-dir", "/opt/dbt",
                "--vars", vars_json,
                "--target-path", f"/tmp/dbt-target-{batch['batch_id']}",
            ],
            cwd="/opt/dbt",
            check=True,
        )

    batches = make_tenant_batches()
    stages = dbt_stage.expand(batch=batches)
    builds = dbt_build.expand(batch=batches)
    cleanup = drop_stage_view.expand(batch=batches)

    seed >> migrate() >> stages >> builds >> cleanup
