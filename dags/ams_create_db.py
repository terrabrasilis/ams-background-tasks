"""A DAG to create the AMS database."""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

DAG_KEY = "ams-create-db"

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "retries": 0,
    "catchup": False,
}

with DAG(DAG_KEY, default_args=default_args, schedule_interval=None) as dag:
    db_url = Variable.get("AMS_DB_URL", None)
    aux_db_url = Variable.get("AMS_AUX_DB_URL", None)
    bash_command = f"{DAG_KEY} {db_url} {aux_db_url} --force-recreate"

    create_db = BashOperator(task_id=DAG_KEY, bash_command=bash_command, dag=dag)

    create_db