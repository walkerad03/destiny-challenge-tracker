FROM apache/airflow:3.2.2

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" dbt-postgres psycopg

COPY --chown=airflow:root dags/ /opt/airflow/dags/
