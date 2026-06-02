import datetime
import json
import os
from pathlib import Path

import pendulum
import psycopg
import requests
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


def build_character_roster(**context):
    host = os.environ.get("DBT_HOST", "postgres")
    user = os.environ.get("DBT_USER")
    password = os.environ.get("DBT_PASSWORD")
    dbname = os.environ.get("DBT_DBNAME")
    api_key = Variable.get("secret_bungie_api_key")
    headers = {"X-API-Key": api_key}

    conn_info = f"host={host} dbname={dbname} user={user} password={password}"

    roster = []

    with psycopg.connect(conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT bungie_membership_id, bungie_membership_type FROM config.tracked_users WHERE is_active = TRUE"
            )
            users = cur.fetchall()

            for membership_id, membership_type in users:
                url = f"https://www.bungie.net/Platform/Destiny2/{membership_type}/Profile/{membership_id}/?components=200"
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()

                char_data = (
                    resp.json()
                    .get("Response", {})
                    .get("characters", {})
                    .get("data", {})
                )

                for char_id in char_data.keys():
                    roster.append(
                        {
                            "membership_type": membership_type,
                            "account_id": membership_id,
                            "character_id": char_id,
                        }
                    )

    return roster


def pull_api_data(membership_type, account_id, character_id, **context):
    api_key = Variable.get("secret_bungie_api_key")

    raw_data = fetch_character_activity_history(
        membership_type=membership_type,
        account_id=account_id,
        character_id=character_id,
        api_key=api_key,
        staging_dir="/opt/airflow/bronze_staging",
    )
    return raw_data


def load_data_to_postgres(**context):
    ti = context["ti"]
    mapped_saved_files = ti.xcom_pull(task_ids="pull_api_data")

    if not mapped_saved_files:
        print("No files to process.")
        return

    saved_files: list[str] = []
    for file_list in mapped_saved_files:
        if isinstance(file_list, list):
            saved_files.extend(file_list)

    if not saved_files:
        print("No files to process after flattening.")
        return

    print(f"Number of files to process: {len(saved_files)}")

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

                print(f"File Parts: {parts}")

                account_id = parts[1]
                character_id = parts[2]
                mode = int(parts[3])
                page = int(parts[5])

                cur.execute(
                    """
                    INSERT INTO bronze.raw_activity_history
                    (account_id, character_id, activity_mode, page_number, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (account_id, character_id, activity_mode, page_number)
                    DO UPDATE SET
                        payload = EXCLUDED.payload,
                        ingested_at = CURRENT_TIMESTAMP
                    """,
                    (account_id, character_id, mode, page, json.dumps(data)),
                )
            conn.commit()


build_roster_task = PythonOperator(
    task_id="build_character_roster",
    python_callable=build_character_roster,
    dag=dag,
)

pull_data_task = PythonOperator.partial(
    task_id="pull_api_data",
    python_callable=pull_api_data,
    dag=dag,
).expand(op_kwargs=build_roster_task.output)

load_data_task = PythonOperator(
    task_id="load_data_to_postgres",
    python_callable=load_data_to_postgres,
    dag=dag,
)

run_dbt_task = BashOperator(
    task_id="run_dbt",
    bash_command=(
        "dbt run "
        "--project-dir /opt/airflow/dbt_transform "
        "--profiles-dir /opt/airflow/dbt_transform"
    ),
    dag=dag,
)

build_roster_task >> pull_data_task >> load_data_task >> run_dbt_task
