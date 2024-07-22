"""A DAG to create the AMS database."""

import os
import random
from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common import default_args, get_secrets_env, get_variable

DAG_KEY = "ams-create-db"


def _sleep():
    sleep(random.random() * 20)


@task
def create_db():
    bash_command = f"ams-create-db {('--force-recreate' if get_variable('AMS_FORCE_RECREATE_DB') else '')}"
    return BashOperator(
        task_id="ams-create-db",
        bash_command=bash_command,
        env=get_secrets_env(["AMS_DB_URL"]),
        append_env=True,
    ).execute({})


@task
def update_municipalities():
    return BashOperator(
        task_id="ams-update-municipalities",
        bash_command="ams-update-municipalities",
        env=get_secrets_env(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
    ).execute({})


@task
def update_active_fires():
    return BashOperator(
        task_id="ams-update-active-fires",
        bash_command="ams-update-active-fires",
        env=get_secrets_env(["AMS_DB_URL", "AMS_AF_DB_URL"]),
        append_env=True,
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
