"""A DAG to create the AMS database."""

import os, sys

import pathlib

project_dir = str(pathlib.Path(__file__).parent.resolve().absolute())

# Loading project dir files
sys.path.append(project_dir)

from common import *

import random

from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonVirtualenvOperator, ShortCircuitOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

from airflow.models.connection import Connection


land_use_dir = project_dir + "/land_use"

DAG_KEY = "ams-create-db"

AMS_ALL_DATA_DB = None
AMS_FORCE_RECREATE_DB = None
AMS_BIOMES = None

def _sleep():
    sleep(random.random() * 20)


def _get_biomes():
    return " ".join(
        [
            f"--biome={_}"
            for _ in Variable.get("AMS_BIOMES").split(";")
            if len(_) > 0
        ]
    )

@task(task_id="update-environment")
def update_environment():
    bash_command = f"pip install "+ project_dir    

    return BashOperator(
        task_id="update-environment",
        bash_command=bash_command
    ).execute({})


def check_variables():
    
    AMS_ALL_DATA_DB = Variable.get('AMS_ALL_DATA_DB')
    AMS_FORCE_RECREATE_DB = Variable.get('AMS_FORCE_RECREATE_DB')
    AMS_BIOMES = Variable.get('AMS_BIOMES')
   
    ams_db_url = BaseHook.get_connection('AMS_DB_URL')
    ams_aux_db_url = BaseHook.get_connection('AMS_AUX_DB_URL')
    ams_af_db_url = BaseHook.get_connection('AMS_AF_DB_URL')
    ams_amz_deter_b_db_url = BaseHook.get_connection('AMS_AMZ_DETER_B_DB_URL')
    ams_cer_deter_b_db_url = BaseHook.get_connection('AMS_CER_DETER_B_DB_URL')

    if not ams_db_url and not ams_db_url.get_uri(): 
        raise Exception("Missing ams_db_url airflow conection configuration.")
    
    if not ams_aux_db_url and not ams_aux_db_url.get_uri(): 
        raise Exception("Missing ams_aux_db_url airflow conection configuration.")

    if not ams_af_db_url and not ams_af_db_url.get_uri(): 
        raise Exception("Missing ams_af_db_url airflow conection configuration.")
    
    if not ams_amz_deter_b_db_url and not ams_amz_deter_b_db_url.get_uri(): 
        raise Exception("Missing ams_amz_deter_b_db_url airflow conection configuration.")
    
    if not ams_cer_deter_b_db_url and not ams_cer_deter_b_db_url.get_uri(): 
        raise Exception("Missing ams_cer_deter_b_db_url airflow conection configuration.")

    if not AMS_ALL_DATA_DB: 
        raise Exception("Missing AMS_ALL_DATA_DB airflow variable.")
        
    if not AMS_FORCE_RECREATE_DB: 
        raise Exception("Missing AMS_FORCE_RECREATE_DB airflow variable.")

    if not AMS_BIOMES: 
        raise Exception("Missing AMS_BIOMES airflow variable.")

    return True


@task(task_id="create-db")
def create_db():
    bash_command = f"ams-create-db {('--force-recreate' if Variable.get('AMS_FORCE_RECREATE_DB')=='1' else '')}"

    return BashOperator(
        task_id="ams-create-db",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-biome")
def update_biome():
    return BashOperator(
        task_id="ams-update-biome",
        bash_command=f"ams-update-biome {_get_biomes()}",
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-spatial-units")
def update_spatial_units():
    return BashOperator(
        task_id="ams-update-spatial-units",
        bash_command=f"ams-update-spatial-units {_get_biomes()}",
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-active-fires")
def update_active_fires():
    bash_command = (
        f"ams-update-active-fires {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')} "
        f"{_get_biomes()}"
    )
    return BashOperator(
        task_id="ams-update-active-fires",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_AF_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-amz-deter")
def update_amz_deter():
    bash_command = (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --truncate"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL", "AMS_AMZ_DETER_B_DB_URL"])
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
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Cerrado'"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL", "AMS_CER_DETER_B_DB_URL"])
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
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado'"
        " --indicator='deter'"
        " --land-use-dir="+land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

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
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado'"
        " --indicator='focos'"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

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
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --drop-tmp"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id="ams-finalize-classification",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


def _check_recreate_db():
    force_recreate = Variable.get("AMS_FORCE_RECREATE_DB") == "1"

    if force_recreate:
        return "create-db"
    return "skip"


with DAG(
    DAG_KEY, default_args=default_args, schedule_interval=None, catchup=False
) as dag:
    
    run_check_variables = ShortCircuitOperator(
        task_id="check-variables",
        provide_context=True,
        python_callable=check_variables,
        op_kwargs={},
    )

    run_update_environment = update_environment()

    run_check_recreate_db = BranchPythonOperator(
        task_id="check-recreate-db",
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


    #RUNS

    run_check_variables >> run_update_environment

    run_update_environment >> run_check_recreate_db    

    run_check_recreate_db >> [run_create_db, run_skip]    

    run_create_db >> [run_update_biome, run_update_spatial_units] >> run_join
    run_skip >> run_join

    run_join >> [run_update_active_fires, run_update_amz_deter]
    run_update_amz_deter >> run_update_cer_deter

    run_update_active_fires >> run_classify_active_fires
    run_update_cer_deter >> run_classify_deter
    [run_classify_active_fires, run_classify_deter] >> run_finalize_classification
