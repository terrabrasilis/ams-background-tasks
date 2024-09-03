"""A DAG to create the AMS database."""

import os
import random
from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from common import default_args, get_secrets_env, get_variable

DAG_KEY = "ams-create-db"


def _sleep():
    sleep(random.random() * 20)


def _get_biomes():
    return " ".join(
        [
            f"--biome={_}"
            for _ in get_variable(name="AMS_BIOMES").split(";")
            if len(_) > 0
        ]
    )


@task(task_id="create-db")
def create_db():
    bash_command = f"ams-create-db {('--force-recreate' if get_variable('AMS_FORCE_RECREATE_DB')=='1' else '')}"

    return BashOperator(
        task_id="ams-create-db",
        bash_command=bash_command,
        env=get_secrets_env(["AMS_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-biome")
def update_biome():
    return BashOperator(
        task_id="ams-update-biome",
        bash_command=f"ams-update-biome {_get_biomes()}",
        env=get_secrets_env(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-spatial-units")
def update_spatial_units():
    return BashOperator(
        task_id="ams-update-spatial-units",
        bash_command=f"ams-update-spatial-units {_get_biomes()}",
        env=get_secrets_env(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-active-fires")
def update_active_fires():
    bash_command = (
        f"ams-update-active-fires {('--all-data' if get_variable('AMS_ALL_DATA_DB')=='1' else '')} "
        f"{_get_biomes()}"
    )
    return BashOperator(
        task_id="ams-update-active-fires",
        bash_command=bash_command,
        env=get_secrets_env(["AMS_DB_URL", "AMS_AF_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-amz-deter")
def update_amz_deter():
    bash_command = (
        f"ams-update-deter"
        f" {('--all-data' if get_variable('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --truncate"
    )

    env = get_secrets_env(["AMS_DB_URL", "AMS_AMZ_DETER_B_DB_URL"])
    env["AMS_DETER_B_DB_URL"] = env["AMS_AMZ_DETER_B_DB_URL"]

    return BashOperator(
        task_id="ams-update-amz-deter",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="update-cer-deter")
def update_cer_deter():
    bash_command = (
        f"ams-update-deter"
        f" {('--all-data' if get_variable('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Cerrado'"
    )

    env = get_secrets_env(["AMS_DB_URL", "AMS_CER_DETER_B_DB_URL"])
    env["AMS_DETER_B_DB_URL"] = env["AMS_CER_DETER_B_DB_URL"]

    return BashOperator(
        task_id="ams-update-cer-deter",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="classify-deter-by-land-use")
def classify_deter_by_land_use():
    bash_command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if get_variable('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado'"
        " --indicator='deter'"
        " --land-use-dir=/opt/airflow/land_use"
    )

    env = get_secrets_env(["AMS_DB_URL"])

    return BashOperator(
        task_id="ams-classify-by-land-use",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="classify-fires-by-land-use")
def classify_active_fires_by_land_use():
    bash_command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if get_variable('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado'"
        " --indicator='focos'"
        " --land-use-dir=/opt/airflow/land_use"
    )

    env = get_secrets_env(["AMS_DB_URL"])

    return BashOperator(
        task_id="ams-classify-by-land-use",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="finalize-classification")
def finalize_classification():
    bash_command = (
        f"ams-finalize-classification"
        f" {('--all-data' if get_variable('AMS_ALL_DATA_DB')=='1' else '')}"
        " --drop-tmp"
    )

    env = get_secrets_env(["AMS_DB_URL"])

    return BashOperator(
        task_id="ams-finalize-classification",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


def _check_recreate_db():
    force_recreate = get_variable("AMS_FORCE_RECREATE_DB") == "1"

    if force_recreate:
        return "create-db"
    return "skip"


with DAG(
    DAG_KEY, default_args=default_args, schedule_interval=None, catchup=False
) as dag:

    run_check_recreate_db = BranchPythonOperator(
        task_id="ams-check-recreate-db",
        python_callable=_check_recreate_db,
    )

    run_skip = EmptyOperator(task_id="skip")
    run_join = EmptyOperator(task_id="join", trigger_rule="none_failed_or_skipped")

    run_create_db = create_db()
    run_update_biome = update_biome()
    run_update_spatial_units = update_spatial_units()
    run_update_active_fires = update_active_fires()
    run_update_amz_deter = update_amz_deter()
    run_update_cer_deter = update_cer_deter()
    run_classify_deter = classify_deter_by_land_use()
    run_classify_active_fires = classify_active_fires_by_land_use()
    run_finalize_classification = finalize_classification()

    run_check_recreate_db >> [run_create_db, run_skip]

    run_create_db >> [run_update_biome, run_update_spatial_units] >> run_join
    run_skip >> run_join

    run_join >> [run_update_active_fires, run_update_amz_deter]
    run_update_amz_deter >> run_update_cer_deter

    run_update_active_fires >> run_classify_active_fires
    run_update_cer_deter >> run_classify_deter
    [run_classify_active_fires, run_classify_deter] >> run_finalize_classification
