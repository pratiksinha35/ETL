from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def run_etl_script(script_path="/opt/airflow/etl/etl_script.py"):
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f'Script failed with error: {result.stderr}')
    else:
        print(result.stdout)

dag = DAG(
    'etl_and_dbt',
    default_args=default_args,
    description='An ETL workflow with dbt',
    start_date=datetime(2024, 6, 20),
    catchup=False
)

task_1 = PythonOperator(
    task_id="run_etl_script",
    python_callable=run_etl_script,
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
