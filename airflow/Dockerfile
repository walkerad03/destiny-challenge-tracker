FROM apache/airflow:3.2.2

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "dbt-core>=1.8.0,<2.0.0" \
    "dbt-postgres>=1.8.0,<2.0.0" \
    psycopg[binary]

COPY --chown=airflow:root dags/ /opt/airflow/dags/
