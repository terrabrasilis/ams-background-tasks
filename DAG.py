"""A DAG to create the AMS database."""

import os
import pathlib
import sys

project_dir = str(pathlib.Path(__file__).parent.resolve().absolute())

# Loading project dir files
sys.path.append(project_dir)

import random
from datetime import datetime
from pathlib import Path
from time import sleep

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator
from dateutil.relativedelta import relativedelta

from common import *

land_use_dir = project_dir + "/land_use"
risk_dir = project_dir + "/risk"

AMS_ALL_DATA_DB = None
AMS_FORCE_RECREATE_DB = None
AMS_BIOMES = None


def _get_biomes():
    return " ".join(
        [f"--biome={_}" for _ in Variable.get("AMS_BIOMES").split(";") if len(_) > 0]
    )


# DAG: ams-create-db


@task(task_id="update-environment")
def update_environment():
    bash_command = f"pip install " + project_dir

    return BashOperator(
        task_id="update-environment", bash_command=bash_command
    ).execute({})


def check_variables():
    AMS_ALL_DATA_DB = Variable.get("AMS_ALL_DATA_DB")
    AMS_FORCE_RECREATE_DB = Variable.get("AMS_FORCE_RECREATE_DB")
    AMS_BIOMES = Variable.get("AMS_BIOMES")
    AMS_STAC_API_URL = Variable.get("AMS_STAC_API_URL")
    AMS_STAC_COLLECTION = Variable.get("AMS_STAC_COLLECTION")

    ams_db_url = BaseHook.get_connection("AMS_DB_URL")
    ams_aux_db_url = BaseHook.get_connection("AMS_AUX_DB_URL")
    ams_af_db_url = BaseHook.get_connection("AMS_AF_DB_URL")
    ams_amz_deter_b_db_url = BaseHook.get_connection("AMS_AMZ_DETER_B_DB_URL")
    ams_cer_deter_b_db_url = BaseHook.get_connection("AMS_CER_DETER_B_DB_URL")
    # ams_ftp_url = BaseHook.get_connection("AMS_FTP_URL")

    if not ams_db_url and not ams_db_url.get_uri():
        raise Exception("Missing ams_db_url airflow conection configuration.")

    # if not ams_ftp_url and not ams_ftp_url.get_uri():
    #    raise Exception("Missing ams_ftp_url airflow conection configuration.")

    if not ams_aux_db_url and not ams_aux_db_url.get_uri():
        raise Exception("Missing ams_aux_db_url airflow conection configuration.")

    if not ams_af_db_url and not ams_af_db_url.get_uri():
        raise Exception("Missing ams_af_db_url airflow conection configuration.")

    if not ams_amz_deter_b_db_url and not ams_amz_deter_b_db_url.get_uri():
        raise Exception(
            "Missing ams_amz_deter_b_db_url airflow conection configuration."
        )

    if not ams_cer_deter_b_db_url and not ams_cer_deter_b_db_url.get_uri():
        raise Exception(
            "Missing ams_cer_deter_b_db_url airflow conection configuration."
        )

    if not AMS_ALL_DATA_DB:
        raise Exception("Missing AMS_ALL_DATA_DB airflow variable.")

    if not AMS_FORCE_RECREATE_DB:
        raise Exception("Missing AMS_FORCE_RECREATE_DB airflow variable.")

    if not AMS_BIOMES:
        raise Exception("Missing AMS_BIOMES airflow variable.")

    if not AMS_STAC_API_URL:
        raise Exception("Missing AMS_STAC_API_URL airflow variable.")

    if not AMS_STAC_COLLECTION:
        raise Exception("Missing AMS_STAC_COLLECTION airflow variable.")

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


# prepare classification


def _prepare_classification(land_use_type: str):
    bash_command = f"ams-prepare-classification --land-use-type {land_use_type}"

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"ams-prepare-classification-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="prepare-classification-AMS")
def prepare_classification_ams():
    return _prepare_classification(land_use_type="ams")


@task(task_id="prepare-classification-PPCDAM")
def prepare_classification_ppcdam():
    return _prepare_classification(land_use_type="ppcdam")


# deter data classfication


def _classify_deter_by_land_use(land_use_type: str):
    bash_command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado'"
        " --indicator='deter'"
        f" --land-use-type={land_use_type}"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"ams-classify-deter-by-land-use-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="classify-deter-by-land-use-AMS")
def classify_deter_by_land_use_ams():
    return _classify_deter_by_land_use(land_use_type="ams")


@task(task_id="classify-deter-by-land-use-PPCDAM")
def classify_deter_by_land_use_ppcdam():
    return _classify_deter_by_land_use(land_use_type="ppcdam")


# active fires data classification


def _classify_active_fires_by_land_use(land_use_type: str):
    bash_command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado'"
        " --indicator='focos'"
        f" --land-use-type={land_use_type}"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"ams-classify-fires-by-land-use-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="classify-fires-by-land-use-AMS")
def classify_active_fires_by_land_use_ams():
    return _classify_active_fires_by_land_use(land_use_type="ams")


@task(task_id="classify-fires-by-land-use-PPCDAM")
def classify_active_fires_by_land_use_ppcdam():
    return _classify_active_fires_by_land_use(land_use_type="ppcdam")


# finalize classification


def finalize_classification(land_use_type: str):
    bash_command = (
        f"ams-finalize-classification"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --land-use-type={land_use_type}"
        " --drop-tmp"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"ams-finalize-classification-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="finalize-classification-AMS")
def finalize_classification_ams():
    return finalize_classification(land_use_type="ams")


@task(task_id="finalize-classification-PPCDAM")
def finalize_classification_ppcdam():
    return finalize_classification(land_use_type="ppcdam")


# risk


@task(task_id="download-ibama-risk-file")
def download_ibama_risk_file():
    bash_command = (
        "ams-download-ibama-risk-file --days-until-expiration=15 --save-dir=" + risk_dir
    )

    return BashOperator(
        task_id="ams-download-ibama-risk-file",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_FTP_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-ibama-risk")
def update_ibama_risk():
    bash_command = "ams-update-ibama-risk --risk-threshold=0.85 --biome='Amazônia'"

    return BashOperator(
        task_id="ams-update-ibama-risk",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="download-inpe-risk-file")
def download_inpe_risk_file():
    beg = (datetime.now() - relativedelta(days=15)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")

    Path(risk_dir).mkdir(parents=False, exist_ok=True)

    bash_command = (
        "ams-download-inpe-risk-file"
        f" --stac-api-url={Variable.get('AMS_STAC_API_URL')}"
        f" --collection={Variable.get('AMS_STAC_COLLECTION')}"
        f" --save-dir={risk_dir}"
        f" --days-until-expiration=15"
        f" --begin='{beg}'"
        f" --end='{end}'"
    )

    return BashOperator(
        task_id="ams-download-inpe-risk-file",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
    ).execute({})


@task(task_id="update-inpe-risk")
def update_inpe_risk():
    bash_command = "ams-update-inpe-risk --risk-threshold=0. --biome='Amazônia'"

    return BashOperator(
        task_id="ams-update-inpe-risk",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
    ).execute({})


# risk data classification


def _classify_risk_by_land_use(land_use_type: str):
    bash_command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia'"
        # " --indicator='risco'"
        " --indicator='risco-inpe'"
        f" --land-use-type={land_use_type}"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"ams-classify-risk-by-land-use-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="classify-risk-by-land-use-AMS")
def classify_risk_by_land_use_ams():
    return _classify_risk_by_land_use(land_use_type="ams")


@task(task_id="classify-risk-by-land-use-PPCDAM")
def classify_risk_by_land_use_ppcdam():
    return _classify_risk_by_land_use(land_use_type="ppcdam")


# others


def _check_recreate_db():
    force_recreate = Variable.get("AMS_FORCE_RECREATE_DB") == "1"

    if force_recreate:
        return "create-db"
    return "skip-create-db"


with DAG(
    "ams-create-db",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    catchup=False,
    concurrency=3,
    max_active_runs=1,
) as dag:
    run_check_variables = ShortCircuitOperator(
        task_id="check-variables",
        provide_context=True,
        python_callable=check_variables,
        op_kwargs={},
    )

    run_update_environment = update_environment()

    run_check_recreate_db = BranchPythonOperator(
        task_id="check-create-db",
        python_callable=_check_recreate_db,
    )

    run_skip = EmptyOperator(task_id="skip-create-db")
    run_join = EmptyOperator(
        task_id="join-create-db", trigger_rule="none_failed_or_skipped"
    )

    # database creation
    run_create_db = create_db()

    # biomes and spatial units
    run_update_biome = update_biome()
    run_update_spatial_units = update_spatial_units()

    # indicators
    run_update_active_fires = update_active_fires()
    run_update_amz_deter = update_amz_deter()
    run_update_cer_deter = update_cer_deter()
    # run_download_ibama_risk_file = download_ibama_risk_file()
    run_download_risk_file = download_inpe_risk_file()
    # run_update_ibama_risk = update_ibama_risk()
    run_update_risk = update_inpe_risk()

    # preparing to classify
    run_prepare_classification = EmptyOperator(task_id="prepare-classification")
    run_prepare_classification_ams = prepare_classification_ams()
    run_prepare_classification_ppcdam = prepare_classification_ppcdam()

    # classification
    run_classify_deter_ams = classify_deter_by_land_use_ams()
    run_classify_deter_ppcdam = classify_deter_by_land_use_ppcdam()

    run_classify_active_fires_ams = classify_active_fires_by_land_use_ams()
    run_classify_active_fires_ppcdam = classify_active_fires_by_land_use_ppcdam()

    run_classify_risk_ams = classify_risk_by_land_use_ams()
    run_classify_risk_ppcdam = classify_risk_by_land_use_ppcdam()

    # finalize classification
    run_finalize_classification_ams = finalize_classification_ams()
    run_finalize_classification_ppcdam = finalize_classification_ppcdam()

    # running

    run_check_variables >> run_update_environment

    run_update_environment >> run_check_recreate_db

    run_check_recreate_db >> [run_create_db, run_skip]

    run_create_db >> [run_update_biome, run_update_spatial_units] >> run_join
    run_skip >> run_join

    run_join >> [
        run_update_active_fires,
        run_update_amz_deter,
        run_download_risk_file,
    ]
    run_update_amz_deter >> run_update_cer_deter
    run_download_risk_file >> run_update_risk

    [
        run_update_active_fires,
        run_update_cer_deter,
        run_update_risk,
    ] >> run_prepare_classification

    run_prepare_classification >> [
        run_prepare_classification_ams,
        run_prepare_classification_ppcdam,
    ]

    # ams
    run_prepare_classification_ams >> [
        run_classify_active_fires_ams,
        run_classify_deter_ams,
        run_classify_risk_ams,
    ]

    [
        run_classify_active_fires_ams,
        run_classify_deter_ams,
        run_classify_risk_ams,
    ] >> run_finalize_classification_ams

    # ppcdam
    run_prepare_classification_ppcdam >> [
        run_classify_active_fires_ppcdam,
        run_classify_deter_ppcdam,
        run_classify_risk_ppcdam,
    ]

    [
        run_classify_active_fires_ppcdam,
        run_classify_deter_ppcdam,
        run_classify_risk_ppcdam,
    ] >> run_finalize_classification_ppcdam


# DAG: ams-calculate-land-use-area


@task(task_id="calculate-amz-land-use-area")
def calculate_amz_land_use_area():
    bash_command = (
        f"ams-calculate-land-use-area {('--force-recreate' if Variable.get('AMS_FORCE_RECREATE_DB')=='1' else '')} "
        " --biome='Amazônia'"
        " --land-use-type='ams'"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id="ams-calculate-amz-land-use-area",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


@task(task_id="calculate-cer-land-use-area")
def calculate_cer_land_use_area():
    bash_command = (
        f"ams-calculate-land-use-area --biome='Cerrado'"
        " --land-use-type='ams'"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id="ams-calculate-cer-land-use-area",
        bash_command=bash_command,
        env=env,
        append_env=True,
    ).execute({})


with DAG(
    "ams-calculate-land-use-area",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    run_check_variables = ShortCircuitOperator(
        task_id="check-variables",
        provide_context=True,
        python_callable=check_variables,
        op_kwargs={},
    )

    run_update_environment = update_environment()

    run_calculate_amz_land_use_area = calculate_amz_land_use_area()
    run_calculate_cer_land_use_area = calculate_cer_land_use_area()

    (
        run_check_variables
        >> run_update_environment
        >> run_calculate_amz_land_use_area
        >> run_calculate_cer_land_use_area
    )
