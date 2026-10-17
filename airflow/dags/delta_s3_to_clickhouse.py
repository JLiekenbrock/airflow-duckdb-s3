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
    """Pass the validated pickup-date range and source to every dbt task."""
    operator.vars = context["ti"].xcom_pull(task_ids="validate_run_config")


with DAG(
    dag_id="delta_s3_to_clickhouse",
    doc_md=(
        "## Delta to ClickHouse\n\n"
        "Import existing Delta data from S3; this DAG never writes to the source. "
        "One run loads NYC taxi trips for one pickup-date range. Cosmos renders the dbt ref() dependencies: "
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
        "start_date": "2025-01-01",
        "end_date": "2025-02-01",
        "delta_s3_path": "http://minio:9000/poc/delta/nyc_taxi_trips/2025-01/",
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
        """Validate the pickup-date window and existing Delta source."""
        params = get_current_context()["params"]
        try:
            start_date = date.fromisoformat(params["start_date"])
            end_date = date.fromisoformat(params["end_date"])
        except (TypeError, ValueError) as error:
            raise ValueError("Provide valid ISO dates") from error
        if start_date >= end_date:
            raise ValueError("end_date must be later than start_date")
        source_path = params["delta_s3_path"]
        if source_path is not None and (
            not isinstance(source_path, str) or not source_path.startswith(("http://", "https://", "s3://"))
        ):
            raise ValueError("delta_s3_path must be an HTTP(S) or S3 URL, or null for the default source")
        return {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "delta_s3_path": source_path,
        }

    run_config = validate_run_config()
    migrate = flyway_migrate()
    models = DbtTaskGroup(
        group_id="dbt_lineage",
        project_config=dbt_project,
        profile_config=dbt_profile,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
            select=["+nyc_taxi_trips"],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
        operator_args={
            "interceptors": [set_dbt_vars],
            "retries": 2,
            "retry_exponential_backoff": True,
        },
    )

    run_config >> migrate >> models
