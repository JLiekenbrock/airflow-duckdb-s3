from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


PIPELINE_IMAGE = os.environ.get(
    "PIPELINE_IMAGE", "registry.example.com/data/airflow-duckdb-s3:0.1.0"
)
NAMESPACE = os.environ.get("OPENSHIFT_NAMESPACE", "data-jobs")
SERVICE_ACCOUNT = os.environ.get("OPENSHIFT_SERVICE_ACCOUNT", "duckdb-s3-runner")
S3_SECRET = os.environ.get("OPENSHIFT_S3_SECRET", "duckdb-s3-credentials")


with DAG(
    dag_id="duckdb_s3_openshift",
    description="Run dbt-duckdb S3 transformations in an OpenShift pod",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=4,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "mandant": Param(
            "m001",
            type="string",
            pattern=r"^[A-Za-z0-9_-]{1,64}$",
            description="Mandant/tenant identifier",
        ),
        "stichtage": Param(
            ["2026-06-30"],
            type="array",
            minItems=1,
            maxItems=366,
            items={"type": "string", "format": "date"},
            description="ISO reporting dates",
        ),
    },
    render_template_as_native_obj=False,
    tags=["duckdb", "dbt", "s3", "openshift"],
) as dag:
    transform = KubernetesPodOperator(
        task_id="build_stichtag_report",
        name="duckdb-s3-stichtag-report",
        namespace=NAMESPACE,
        image=PIPELINE_IMAGE,
        cmds=None,
        arguments=[
            "--mandant",
            "{{ params.mandant }}",
            "--stichtage",
            "{{ params.stichtage | tojson }}",
            "--run-id",
            "{{ run_id }}",
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name=S3_SECRET)
            )
        ],
        service_account_name=SERVICE_ACCOUNT,
        in_cluster=False,
        kubernetes_conn_id=None,
        config_file="/opt/airflow/.kube/config",
        get_logs=True,
        log_events_on_failure=True,
        reattach_on_restart=True,
        on_finish_action="delete_pod",
        startup_timeout_seconds=600,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
        security_context={"runAsNonRoot": True},
        container_security_context={
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "seccompProfile": {"type": "RuntimeDefault"},
        },
    )
