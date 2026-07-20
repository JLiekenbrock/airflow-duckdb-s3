# Local Airflow to OpenShift: dbt + DuckDB + S3

This repository runs Apache Airflow **2.10.4 locally** with its lightweight
`SequentialExecutor` and launches data tasks as
short-lived pods on an existing OpenShift cluster. Each pod runs dbt with the
DuckDB adapter, reads Parquet directly from S3, and writes partitioned Parquet
back to S3.

The DAG exposes two run parameters:

- `stichtage`: one or more ISO dates (`YYYY-MM-DD`)
- `mandant`: the tenant/client identifier

They are passed as JSON to the pod and then as typed dbt vars. SQL is kept in
dbt/Jinja templates in `models/` and `macros/`; the DAG contains no business SQL.

## Architecture

```text
local Airflow 2.10.4
       |
       | Kubernetes API (current kubeconfig / OpenShift service account)
       v
OpenShift KubernetesPodOperator pod
       |
       +-- dbt-duckdb renders SQL with mandant + stichtage
       +-- DuckDB reads s3://.../input/**/*.parquet
       +-- DuckDB writes s3://.../output/mandant=.../*.parquet
```

`KubernetesPodOperator` is used deliberately: Airflow remains local, while only
the data workload runs in OpenShift. This is not an Airflow `KubernetesExecutor`
deployment.

## Quick start

1. Copy `.env.example` to `.env` and edit the image, namespace and S3 values.
2. Log into OpenShift and select the target cluster/project:

   ```shell
   oc login https://api.example.com:6443
   oc project data-jobs
   ```

3. Edit the RoleBinding user in `openshift/rbac.yaml`, edit the secret values,
   then create the OpenShift resources:

   ```shell
   oc apply -f openshift/rbac.yaml
   oc apply -f openshift/s3-secret.example.yaml
   ```

4. Build and push the task image:

   ```shell
   docker build -t registry.example.com/data/airflow-duckdb-s3:0.1.0 .
   docker push registry.example.com/data/airflow-duckdb-s3:0.1.0
   ```

5. Start local Airflow:

   ```shell
   docker compose up --build
   ```

Open <http://localhost:8082> and sign in as `admin`. The `airflow standalone`
command prints its generated password during first initialization; retrieve it
with `docker compose logs airflow`. Then trigger `duckdb_s3_openshift` with, for
example:

```json
{
  "mandant": "m001",
  "stichtage": ["2026-06-30", "2026-07-31"]
}
```

The local Airflow container mounts `${KUBECONFIG}` read-only. On Windows Docker
Desktop, set `KUBECONFIG` to an absolute path that Docker can share. For a local
Kubernetes cluster, change `in_cluster` to `true` only when Airflow itself runs
in that cluster.

## S3 layout

The example expects input below:

```text
s3://<bucket>/<INPUT_PREFIX>/mandant=<mandant>/stichtag=YYYY-MM-DD/*.parquet
```

and publishes to:

```text
s3://<bucket>/<OUTPUT_PREFIX>/mandant=<mandant>/report_<run-id>.parquet
```

Configure the two URI templates with `DBT_INPUT_URI_TEMPLATE` and
`DBT_OUTPUT_URI_TEMPLATE`. The macros validate substituted identifiers before
building a URI. Date values are passed separately as a dbt list and rendered as
SQL date literals.

## Local validation without OpenShift

```shell
python runner/run_dbt.py \
  --mandant m001 \
  --stichtage '["2026-06-30"]' \
  --project-dir . \
  --profiles-dir . \
  --target local
```

The `local` target uses local Parquet paths from the profile environment. The
`openshift` target loads DuckDB's `httpfs` and `parquet` extensions and creates
an S3 secret from environment variables.

## Production notes

- Pin the task image by digest after testing.
- Replace the example static S3 secret with your organization's secret manager
  or OpenShift workload identity.
- The supplied Role only permits pod lifecycle and pod-log access in one
  namespace. Apply it for the identity used by local Airflow.
- OpenShift restricted SCCs commonly require arbitrary UIDs. The image uses a
  group-writable working directory and does not request a fixed UID.
- The example output is immutable per Airflow run. Add a catalog or transactional
  table format if readers require atomic replacement of a logical dataset.
