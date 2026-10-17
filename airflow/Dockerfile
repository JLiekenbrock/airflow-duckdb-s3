FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt" \
    -r /requirements.txt
COPY dbt-requirements.txt /dbt-requirements.txt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt
RUN curl -fsSL https://repo.maven.apache.org/maven2/org/flywaydb/flyway-commandline/10.20.0/flyway-commandline-10.20.0-linux-x64.tar.gz -o /tmp/flyway.tar.gz \
    && mkdir -p /home/airflow/.local/flyway \
    && tar -xzf /tmp/flyway.tar.gz --strip-components=1 -C /home/airflow/.local/flyway \
    && rm /tmp/flyway.tar.gz \
    && mkdir -p /home/airflow/.local/flyway/drivers \
    && curl -fsSL https://repo.maven.apache.org/maven2/org/flywaydb/flyway-database-clickhouse/10.20.0/flyway-database-clickhouse-10.20.0.jar -o /home/airflow/.local/flyway/drivers/flyway-database-clickhouse-10.20.0.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.7/clickhouse-jdbc-0.9.7-all-dependencies.jar -o /home/airflow/.local/flyway/drivers/clickhouse-jdbc-0.9.7-all-dependencies.jar

ENV PATH="/home/airflow/.local/flyway:/home/airflow/dbt-venv/bin:${PATH}"
ENV FLYWAY_LOCATIONS="filesystem:/opt/dbt/migrations"
COPY dbt_project.yml profiles.yml flyway.toml /opt/dbt/
COPY migrations /opt/dbt/migrations
COPY models /opt/dbt/models
COPY airflow/dags /opt/airflow/dags
