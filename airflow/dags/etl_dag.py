from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from docker.types import Mount
import subprocess

CONN_ID = ''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


dag = DAG(
    'etl_and_dbt',
    default_args=default_args,
    description='An ETL workflow with dbt',
    start_date=datetime(2024, 6, 22),
    catchup=False
)

task_1 = AirbyteTriggerSyncOperator(
    task_id="airbyte_postgres_postgres",
    airbyte_conn_id='airbyte',
    connection_id=CONN_ID,
    asynchronous=False,
    timeout=3600,
    wait=3,
    dag=dag
)

task_2 = DockerOperator(
    task_id="dbt_run",
    image="ghcr.io/dbt-labs/dbt-postgres:1.4.7",
    command="run --profiles-dir /root --project-dir /dbt --full-refresh",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="etl_etl_network",
    mounts=[
        Mount(source="/home/pratiksinha35/home/ETL/custom_postgres", target="/dbt", type="bind"),
        Mount(source="/home/pratiksinha35/.dbt", target="/root", type="bind"),
    ],
    dag=dag
)

task_1 >> task_2
