FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN curl -fsSL https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/10.20.1/flyway-commandline-10.20.1-linux-x64.tar.gz -o /tmp/flyway.tar.gz \
    && mkdir -p /home/airflow/.local/flyway \
    && tar -xzf /tmp/flyway.tar.gz --strip-components=1 -C /home/airflow/.local/flyway \
    && rm /tmp/flyway.tar.gz \
    && mkdir -p /home/airflow/.local/flyway/drivers \
    && curl -fsSL https://repo1.maven.org/maven2/org/flywaydb/flyway-database-clickhouse/10.20.1/flyway-database-clickhouse-10.20.1.jar -o /home/airflow/.local/flyway/drivers/flyway-database-clickhouse-10.20.1.jar \
    && curl -fsSL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-all.jar -o /home/airflow/.local/flyway/drivers/clickhouse-jdbc-0.6.5-all.jar

ENV PATH="/home/airflow/.local/flyway:${PATH}"
ENV FLYWAY_LOCATIONS="filesystem:/opt/dbt/migrations"
COPY dbt_project.yml profiles.yml flyway.toml /opt/dbt/
COPY migrations /opt/dbt/migrations
COPY models /opt/dbt/models
COPY airflow/dags /opt/airflow/dags
