import os
from datetime import datetime, timedelta
from airflow import DAG
from operators.DbtOperator import DbtOperator

DAG_NAME = "build__tickers"

dag = DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
)
dag.doc_md = __doc__

build = DbtOperator(
    task_id="build",
    dbt_command=f"dbt build --select stg_tickers+",
    dag=dag,
)