import datetime
import json
import os
from pathlib import Path

import pendulum
import psycopg
from common.bungie_api import fetch_character_activity_history

from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DAG_NAME = "destiny_ingestion_pipeline"

dag = DAG(
    DAG_NAME,
    schedule="@hourly",
    default_args={"retries": 1, "retry_delay": datetime.timedelta(minutes=5)},
    start_date=pendulum.datetime(2026, 5, 25, tz="UTC"),
    catchup=False,
)


def pull_api_data(**context):
    api_key = Variable.get("secret_bungie_api_key")

    raw_data = fetch_character_activity_history(
        membership_type=1,
        account_id="4611686018457944318",
        character_id="2305843009269336162",
        api_key=api_key,
        staging_dir="/opt/airflow/bronze_staging",
    )
    return raw_data


def load_data_to_postgres(**context):
    ti = context["ti"]
    saved_files = ti.xcom_pull(task_ids="pull_api_data")

    if not saved_files:
        print("No files to process.")
        return

    host = os.environ.get("DBT_HOST", "postgres")
    user = os.environ.get("DBT_USER")
    password = os.environ.get("DBT_PASSWORD")
    dbname = os.environ.get("DBT_DBNAME")

    conn_info = f"host={host} dbname={dbname} user={user} password={password}"

    with psycopg.connect(conn_info) as conn:
        with conn.cursor() as cur:
            for file_path in saved_files:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                parts = Path(file_path).stem.split("_")
                account_id = parts[1]
                character_id = parts[2]
                page = int(parts[5])

                cur.execute(
                    """
                    INSERT INTO bronze.raw_activity_history
                    (account_id, character_id, page_number, payload)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (account_id, character_id, page_number)
                    DO UPDATE SET
                        payload = EXCLUDED.payload,
                        ingested_at = CURRENT_TIMESTAMP
                    """,
                    (account_id, character_id, page, json.dumps(data)),
                )
            conn.commit()


pull_data_task = PythonOperator(
    task_id="pull_api_data",
    python_callable=pull_api_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id="load_data_to_postgres",
    python_callable=load_data_to_postgres,
    dag=dag,
)

run_dbt_silver_task = BashOperator(
    task_id="run_dbt_silver",
    bash_command=(
        "dbt run "
        "--select slv_activity_history "
        "--project-dir /opt/airflow/dbt_transform "
        "--profiles-dir /opt/airflow/dbt_transform"
    ),
    dag=dag,
)

pull_data_task >> load_data_task >> run_dbt_silver_task
