import base64
from datetime import date, datetime, timedelta
import os
import re
import subprocess
import urllib.request
from pathlib import Path

from airflow.sdk import DAG, get_current_context, task, task_group
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
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

DBT_PROJECT_PATH = Path("/opt/dbt")
DBT_EXECUTABLE = "/home/airflow/dbt-venv/bin/dbt"

dbt_project = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=False,
)
dbt_profile = ProfileConfig(
    profile_name="delta_clickhouse",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


def set_dbt_vars(context, operator):
    """Pass this mapped window to each Cosmos task."""
    windows = context["ti"].xcom_pull(task_ids="make_windows")
    operator.vars = windows[context["ti"].map_index]


def cosmos_models():
    return DbtTaskGroup(
        group_id="dbt_lineage",
        project_config=dbt_project,
        profile_config=dbt_profile,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
            select=["+delta_events"],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={
            "interceptors": [set_dbt_vars],
            "retries": 2,
            "retry_exponential_backoff": True,
        },
    )


with DAG(
    dag_id="delta_s3_to_clickhouse",
    doc_md=(
        "## Delta to ClickHouse\n\n"
        "Airflow maps independent windows: days_per_batch=1 for daily loads, or 7 for weekly backfills. "
        "Each window contains a Cosmos dbt lineage: "
        "staging model and input tests → final model and its uniqueness check → stage-view cleanup.\n\n"
        "Each dbt task retries twice. A failed window does not block other windows. One task runs at a time "
        "to limit ClickHouse load. The final success gate requires every window to succeed.\n\n"
        "Flyway applies versioned ClickHouse DDL before dbt runs. Cosmos runs dbt models and "
        "their tests as Airflow tasks. Both read the ClickHouse host and credentials from the "
        "same `CLICKHOUSE_*` environment settings."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "start_date": "2026-10-12",
        "end_date": "2026-10-19",
        "tenant_ids": list(range(1, 801)),
        "days_per_batch": 1,
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
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            },
        )

    @task
    def flyway_migrate():
        flyway_env = os.environ.copy()
        flyway_env.update(
            {
                "environments_default_url": (
                    f"jdbc:clickhouse://{flyway_env.get('CLICKHOUSE_HOST', 'localhost')}"
                    f":{flyway_env.get('CLICKHOUSE_PORT', '8123')}/default"
                ),
                "environments_default_user": flyway_env.get("CLICKHOUSE_USER", "default"),
                "environments_default_password": flyway_env.get("CLICKHOUSE_PASSWORD", ""),
            }
        )
        subprocess.run(
            ["flyway", "migrate"],
            cwd="/opt/dbt",
            env=flyway_env,
            check=True,
        )

    @task
    def validate_run_config():
        """Validate and normalize the single date/tenant selection for dbt."""
        params = get_current_context()["params"]
        days_per_batch = params["days_per_batch"]
        if type(days_per_batch) is not int or days_per_batch < 1:
            raise ValueError("days_per_batch must be a positive integer")
        tenant_ids = params["tenant_ids"]
        if not isinstance(tenant_ids, list):
            raise ValueError("tenant_ids must be a list of integers")
        if any(isinstance(tenant_id, bool) for tenant_id in tenant_ids):
            raise ValueError("tenant_ids must contain integers, not booleans")
        try:
            tenant_ids = list(dict.fromkeys(int(tenant_id) for tenant_id in tenant_ids))
            start_date = date.fromisoformat(params["start_date"])
            end_date = date.fromisoformat(params["end_date"])
        except (TypeError, ValueError) as error:
            raise ValueError("Provide valid ISO dates and integer tenant_ids") from error
        if not tenant_ids:
            raise ValueError("Provide at least one tenant_id in the DAG run configuration")
        if any(tenant_id < 0 for tenant_id in tenant_ids):
            raise ValueError("tenant_ids must be non-negative integers")
        if start_date >= end_date:
            raise ValueError("end_date must be later than start_date")
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "tenant_ids": tenant_ids,
            "days_per_batch": days_per_batch,
        }

    @task
    def make_windows(run_config):
        start = date.fromisoformat(run_config["start_date"])
        end = date.fromisoformat(run_config["end_date"])
        days_per_batch = run_config["days_per_batch"]
        return [
            {
                **run_config,
                "start_date": (start + timedelta(days=offset)).isoformat(),
                "end_date": (start + timedelta(days=min(offset + days_per_batch, (end - start).days))).isoformat(),
                "batch_id": (start + timedelta(days=offset)).strftime("window_%Y%m%d"),
            }
            for offset in range(0, (end - start).days, days_per_batch)
        ]

    @task(retries=1, trigger_rule="all_done")
    def drop_stage_view(window):
        database = os.environ["CLICKHOUSE_DATABASE"]
        batch_id = window["batch_id"]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database) or not re.fullmatch(r"window_\d{8}", batch_id):
            raise ValueError("Invalid database or batch identifier")

        query = f"DROP VIEW IF EXISTS `{database}`.`stg_delta_events_{batch_id}`"
        credentials = base64.b64encode(
            f"{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}".encode()
        ).decode()
        request = urllib.request.Request(
            f"http://{os.environ['CLICKHOUSE_HOST']}:{os.environ['CLICKHOUSE_PORT']}/",
            data=query.encode(),
            headers={"Authorization": f"Basic {credentials}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()

    @task_group
    def process_window(window):
        models = cosmos_models()
        cleanup = drop_stage_view(window)
        models >> cleanup

    @task
    def all_windows_succeeded():
        """Only succeed when every processing task and cleanup succeeded."""

    run_config = validate_run_config()
    seed = seed_local_delta()
    migrate = flyway_migrate()
    windows = make_windows(run_config)
    process_window.expand(window=windows)

    run_config >> seed >> migrate >> windows

    # Include model/test failures directly: all_done cleanup must not mask them.
    processing_tasks = list(dag.tasks)
    completion = all_windows_succeeded()
    for processing_task in processing_tasks:
        processing_task >> completion
