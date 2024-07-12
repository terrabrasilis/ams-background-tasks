"""A sample DAG."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_KEY = "sample-tool"

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "retries": 0,
    "catchup": False,
}

with DAG("sample", default_args=default_args, schedule_interval=None) as dag:
    cmd = BashOperator(task_id=DAG_KEY, bash_command=DAG_KEY)
    cmd
