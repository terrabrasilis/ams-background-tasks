"""A DAG to load the environment variables."""

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

DAG_KEY = "load-vars"

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "retries": 0,
    "catchup": False,
}


def _set_vars():
    print("setting DB_URL")
    Variable.set(
        "DB_URL",
        os.getenv("DB_URL", "postgresql://postgres:postgres@127.0.0.1:5432/AMS"),
    )


with DAG(DAG_KEY, default_args=default_args, schedule_interval=None) as dag:
    set_vars = PythonOperator(task_id=DAG_KEY, python_callable=_set_vars)
    set_vars
