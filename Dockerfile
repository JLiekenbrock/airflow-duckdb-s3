FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/opt/pipeline

WORKDIR /opt/pipeline
COPY requirements-pipeline.txt .
RUN pip install --no-cache-dir -r requirements-pipeline.txt

COPY dbt_project.yml profiles.yml ./
COPY macros ./macros
COPY models ./models
COPY runner ./runner

# OpenShift may assign an arbitrary UID in group 0.
RUN chgrp -R 0 /opt/pipeline && chmod -R g=u /opt/pipeline

ENTRYPOINT ["python", "/opt/pipeline/runner/run_dbt.py"]

