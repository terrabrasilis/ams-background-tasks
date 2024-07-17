"""A DAG to create the AMS database."""

import random
from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common import default_args, get_variable

AMS__DB_URL = get_variable("AMS__DB_URL")
AMS__AUX_DB_URL = get_variable("AMS__AUX_DB_URL")
AMS__FORCE_RECREATE_DB = get_variable("AMS__FORCE_RECREATE_DB")

DAG_KEY = "ams-create-db"


@task
def create_db():
    bash_command = f"ams-create-db {AMS__DB_URL} {('--force-recreate' if AMS__FORCE_RECREATE_DB else '')}"
    return BashOperator(task_id="ams-create-db", bash_command=bash_command).execute({})


@task
def update_municipalities():
    bash_command = f"ams-update-municipalities {AMS__DB_URL} {AMS__AUX_DB_URL}"
    return BashOperator(
        task_id="update-municipalities", bash_command=bash_command
    ).execute({})


def _sleep():
    sleep(random.random() * 20)


@task
def update_active_fires():
    return PythonOperator(
        task_id="update-active-fires", python_callable=_sleep
    ).execute({})


@task
def update_deter():
    return PythonOperator(task_id="update-deter", python_callable=_sleep).execute({})


@task
def classify_by_land_use():
    return PythonOperator(
        task_id="classify-by-land-use", python_callable=_sleep
    ).execute({})


with DAG(DAG_KEY, default_args=default_args, schedule_interval=None) as dag:
    run_create_db = create_db()
    run_update_municipalities = update_municipalities()
    run_update_active_fires = update_active_fires()
    run_update_deter = update_deter()
    run_classify = classify_by_land_use()

    (
        run_create_db
        >> run_update_municipalities
        >> [run_update_active_fires, run_update_deter]
        >> run_classify
    )
