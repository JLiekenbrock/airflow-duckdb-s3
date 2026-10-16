from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="delta_s3_to_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "clickhouse", "delta-lake"],
) as dag:
    migrate = BashOperator(
        task_id="flyway_migrate",
        bash_command="flyway -configFiles=/opt/dbt/flyway.toml migrate",
        cwd="/opt/dbt",
        append_env=True,
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --project-dir /opt/dbt --profiles-dir /opt/dbt",
        cwd="/opt/dbt",
        append_env=True,
    )

    migrate >> build
