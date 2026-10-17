from datetime import date, datetime
import os
import subprocess
from pathlib import Path

from airflow.sdk import DAG, get_current_context, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior


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
    """Pass the validated date range and tenants to every dbt task."""
    operator.vars = context["ti"].xcom_pull(task_ids="validate_run_config")


with DAG(
    dag_id="delta_s3_to_clickhouse",
    doc_md=(
        "## Delta to ClickHouse\n\n"
        "Import existing Delta data from S3; this DAG never writes to the source. "
        "One run loads one date range for all selected tenants. Cosmos renders the dbt ref() dependencies: "
        "staging model and input tests → final model and its uniqueness check.\n\n"
        "Each dbt task retries twice. One run and one task execute at a time to limit ClickHouse load. "
        "Use delta_s3_to_clickhouse_backfill to split a historical range into independent runs of this DAG.\n\n"
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
    },
    tags=["dbt", "clickhouse", "delta-lake"],
) as dag:
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
        }

    run_config = validate_run_config()
    migrate = flyway_migrate()
    models = DbtTaskGroup(
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

    run_config >> migrate >> models
