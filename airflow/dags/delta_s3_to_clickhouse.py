from datetime import date, datetime, timedelta
import json
import shutil
import subprocess
from pathlib import Path

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior


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
DBT_PROJECT_PATH = Path("/opt/dbt")

dbt_project = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=False,
)
dbt_profile = ProfileConfig(
    profile_name="delta_clickhouse",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


def set_batch_vars(context, operator):
    """Pass the mapped tenant/date window into each Cosmos dbt task."""
    batches = context["ti"].xcom_pull(task_ids="make_tenant_batches")
    batch = batches[context["ti"].map_index]
    operator.vars = batch
    operator.dbt_cmd_global_flags = [
        "--target-path",
        f"/tmp/dbt-target-{batch['batch_id']}",
    ]


def cosmos_models(group_id, model):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=dbt_project,
        profile_config=dbt_profile,
        render_config=RenderConfig(
            select=[model],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={
            "interceptors": [set_batch_vars],
            "retries": 2,
        },
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
                "tenant_ids": tenant_ids[offset : offset + batch_size],
                "start_date": params["start_date"],
                "end_date": params["end_date"],
            }
            for index, offset in enumerate(range(0, len(tenant_ids), batch_size))
        ]

    @task(retries=1)
    def drop_stage_view(batch):
        vars_json = json.dumps({"batch_id": batch["batch_id"]})
        subprocess.run(
            [
                "dbt", "--project-dir", "/opt/dbt", "--profiles-dir", "/opt/dbt",
                "--target-path", f"/tmp/dbt-target-{batch['batch_id']}",
                "run-operation", "drop_batch_stage_view",
                "--vars", vars_json,
            ],
            cwd="/opt/dbt",
            check=True,
        )
        shutil.rmtree(f"/tmp/dbt-target-{batch['batch_id']}", ignore_errors=True)

    @task_group(group_id="dbt_stage_batch")
    def dbt_stage_batch(batch):
        cosmos_models("stage", "stg_delta_events")

    @task_group(group_id="dbt_build_batch")
    def dbt_build_batch(batch):
        models = cosmos_models("build", "delta_events")
        cleanup = drop_stage_view(batch)
        models >> cleanup

    batches = make_tenant_batches()
    stages = dbt_stage_batch.expand(batch=batches)
    builds = dbt_build_batch.expand(batch=batches)
    all_batches_validated = EmptyOperator(task_id="all_batches_validated")

    seed >> migrate() >> stages >> all_batches_validated >> builds
