"""A DAG to create the AMS database."""

import json
import pathlib
import sys

project_dir = str(pathlib.Path(__file__).parent.resolve().absolute())

venv_dir = pathlib.Path(f"/tmp/venvs/")
venv_dir.mkdir(exist_ok=True, parents=False)
venv_path = venv_dir / pathlib.Path(project_dir).name

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
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from dateutil.relativedelta import relativedelta

from ams_background_tasks.tools.common import BIOMES
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


def _get_all_biomes():
    return " ".join([f"--biome='{_}'" for _ in BIOMES])


# DAG: ams-create-db


def update_environment(dag):
    bash_command = f"python3 -m venv {venv_path} && " if not venv_path.exists() else ""
    bash_command += f"source {venv_path}/bin/activate && " f"pip install " + project_dir

    return BashOperator(
        task_id="update-environment", bash_command=bash_command, dag=dag
    )


def check_variables(**context):
    context["ti"].xcom_push(
        key="start_process", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    AMS_ALL_DATA_DB = Variable.get("AMS_ALL_DATA_DB")
    AMS_FORCE_RECREATE_DB = Variable.get("AMS_FORCE_RECREATE_DB")
    AMS_BIOMES = Variable.get("AMS_BIOMES")
    AMS_STAC_API_URL = Variable.get("AMS_STAC_API_URL")
    AMS_STAC_COLLECTION = Variable.get("AMS_STAC_COLLECTION")
    AMS_EMAIL_TO = Variable.get("AMS_EMAIL_TO")
    AMS_FREQUENCY_TO_UPDATE_DETER = Variable.get("AMS_FREQUENCY_TO_UPDATE_DETER")
    AMS_FREQUENCY_TO_UPDATE_FOCOS = Variable.get("AMS_FREQUENCY_TO_UPDATE_FOCOS")
    AMS_FREQUENCY_TO_UPDATE_RISCO = Variable.get("AMS_FREQUENCY_TO_UPDATE_RISCO")

    ams_db_url = BaseHook.get_connection("AMS_DB_URL")
    ams_aux_db_url = BaseHook.get_connection("AMS_AUX_DB_URL")
    ams_af_db_url = BaseHook.get_connection("AMS_AF_DB_URL")
    ams_amz_deter_b_db_url = BaseHook.get_connection("AMS_AMZ_DETER_B_DB_URL")
    ams_cer_deter_b_db_url = BaseHook.get_connection("AMS_CER_DETER_B_DB_URL")
    ams_pan_deter_b_db_url = BaseHook.get_connection("AMS_PAN_DETER_B_DB_URL")
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

    if not ams_pan_deter_b_db_url and not ams_pan_deter_b_db_url.get_uri():
        raise Exception(
            "Missing ams_pan_deter_b_db_url airflow conection configuration."
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

    if not AMS_EMAIL_TO:
        raise Exception("Missing AMS_EMAIL_TO airflow variable.")

    if not AMS_FREQUENCY_TO_UPDATE_DETER:
        raise Exception("Missing AMS_FREQUENCY_TO_UPDATE_DETER airflow variable.")

    if not AMS_FREQUENCY_TO_UPDATE_FOCOS:
        raise Exception("Missing AMS_FREQUENCY_TO_UPDATE_FOCOS airflow variable.")

    if not AMS_FREQUENCY_TO_UPDATE_RISCO:
        raise Exception("Missing AMS_FREQUENCY_TO_UPDATE_RISCO airflow variable.")

    return True


def create_db(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += f"ams-create-db {('--force-recreate' if Variable.get('AMS_FORCE_RECREATE_DB')=='1' else '')}"

    return BashOperator(
        task_id="create-db",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
        dag=dag,
    )


def update_biome(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += f"ams-update-biome {_get_all_biomes()}"

    return BashOperator(
        task_id="update-biome",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
        dag=dag,
    )


def update_spatial_units(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += f"ams-update-spatial-units {_get_all_biomes()}"

    return BashOperator(
        task_id="update-spatial-units",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_AUX_DB_URL"]),
        append_env=True,
        dag=dag,
    )


def update_active_fires(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-update-active-fires {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')} "
        f"{_get_all_biomes()} "
        f"--limit={Variable.get('AMS_LIMIT', 0)}"
    )
    return BashOperator(
        task_id="update-active-fires",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_AF_DB_URL"]),
        append_env=True,
        dag=dag,
    )


def update_amz_deter(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --biome='Amazônia' --truncate --limit={Variable.get('AMS_LIMIT', 0)}"
        f" --create-processing-flag"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL", "AMS_AMZ_DETER_B_DB_URL"])
    env["AMS_DETER_B_DB_URL"] = env["AMS_AMZ_DETER_B_DB_URL"]

    return BashOperator(
        task_id="update-amz-deter",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def update_cer_deter(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --biome='Cerrado' --limit={Variable.get('AMS_LIMIT', 0)}"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL", "AMS_CER_DETER_B_DB_URL"])
    env["AMS_DETER_B_DB_URL"] = env["AMS_CER_DETER_B_DB_URL"]

    return BashOperator(
        task_id="update-cer-deter",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def update_pan_deter(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --biome='Pantanal' --limit={Variable.get('AMS_LIMIT', 0)}"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL", "AMS_PAN_DETER_B_DB_URL"])
    env["AMS_DETER_B_DB_URL"] = env["AMS_PAN_DETER_B_DB_URL"]

    return BashOperator(
        task_id="update-pan-deter",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def finalize_deter_update(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-finalize-deter-update"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"finalize-deter-update",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


# prepare classification


def _prepare_classification(dag, land_use_type: str):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += f"ams-prepare-classification --land-use-type {land_use_type}"

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"prepare-classification-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def prepare_classification_ams(dag):
    return _prepare_classification(dag=dag, land_use_type="ams")


def prepare_classification_ppcdam(dag):
    return _prepare_classification(dag=dag, land_use_type="ppcdam")


# deter data classfication


def _classify_deter_by_land_use(dag, land_use_type: str):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado' --biome='Pantanal'"
        " --indicator='deter'"
        f" --land-use-type={land_use_type}"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"classify-deter-by-land-use-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def classify_deter_by_land_use_ams(dag):
    return _classify_deter_by_land_use(dag=dag, land_use_type="ams")


def classify_deter_by_land_use_ppcdam(dag):
    return _classify_deter_by_land_use(dag=dag, land_use_type="ppcdam")


# active fires data classification


def _classify_fires_by_land_use(dag, land_use_type: str):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-classify-by-land-use "
        f"{('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')} "
        f"{_get_all_biomes()} "
        "--indicator='focos' "
        f"--land-use-type={land_use_type} "
        "--land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"classify-fires-by-land-use-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def classify_fires_by_land_use_ams(dag):
    return _classify_fires_by_land_use(dag=dag, land_use_type="ams")


def classify_fires_by_land_use_ppcdam(dag):
    return _classify_fires_by_land_use(dag=dag, land_use_type="ppcdam")


# finalize classification


def finalize_classification(dag, land_use_type: str):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-finalize-classification"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --land-use-type={land_use_type}"
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"finalize-classification-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
        trigger_rule="all_done",
    )


def finalize_classification_ams(dag):
    return finalize_classification(dag=dag, land_use_type="ams")


def finalize_classification_ppcdam(dag):
    return finalize_classification(dag=dag, land_use_type="ppcdam")


# risk


def download_ibama_risk_file(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        "ams-download-ibama-risk-file --days-until-expiration=15 --save-dir=" + risk_dir
    )

    return BashOperator(
        task_id="download-ibama-risk-file",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL", "AMS_FTP_URL"]),
        append_env=True,
        dag=dag,
    )


def update_ibama_risk(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += "ams-update-ibama-risk --risk-threshold=0.85 --biome='Amazônia'"

    return BashOperator(
        task_id="update-ibama-risk",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
        dag=dag,
    )


def download_inpe_risk_file(dag):
    beg = (datetime.now() - relativedelta(days=30)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")

    Path(risk_dir).mkdir(parents=False, exist_ok=True)

    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        "ams-download-inpe-risk-file"
        f" --stac-api-url={Variable.get('AMS_STAC_API_URL')}"
        f" --collection={Variable.get('AMS_STAC_COLLECTION')}"
        f" --save-dir={risk_dir}"
        f" --days-until-expiration=15"
        f" --begin='{beg}'"
        f" --end='{end}'"
    )

    return BashOperator(
        task_id="download-inpe-risk-file",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
        dag=dag,
    )


def update_inpe_risk(dag):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += "ams-update-inpe-risk --risk-threshold=0. --biome='Amazônia'"

    return BashOperator(
        task_id="update-inpe-risk",
        bash_command=bash_command,
        env=get_conn_secrets_uri(["AMS_DB_URL"]),
        append_env=True,
        dag=dag,
    )


# risk data classification


def _classify_risk_by_land_use(dag, land_use_type: str):
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia'"
        " --indicator='risco'"
        f" --land-use-type={land_use_type}"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id=f"classify-risk-by-land-use-{land_use_type}",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def classify_risk_by_land_use_ams(dag):
    return _classify_risk_by_land_use(dag=dag, land_use_type="ams")


def classify_risk_by_land_use_ppcdam(dag):
    return _classify_risk_by_land_use(dag=dag, land_use_type="ppcdam")


# others


def _check_recreate_db():
    force_recreate = Variable.get("AMS_FORCE_RECREATE_DB") == "1"

    if force_recreate:
        return "create-db"

    return "skip-create-db"


def _need_update_indicator(dag, indicator: str):
    env = get_conn_secrets_uri(["AMS_DB_URL"])

    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += f"ams-need-update-indicator --indicator={indicator} "
    bash_command += (
        f"--frequency={Variable.get(f'AMS_FREQUENCY_TO_UPDATE_{indicator.upper()}')}"
    )

    return BashOperator(
        task_id=f"need-update-{indicator}",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
    )


def decide_update_indicator(**context):
    indicator = context["indicator"]

    bash_result = context["ti"].xcom_pull(task_ids=f"need-update-{indicator}")

    bash_result = bash_result.strip().lower()

    if bash_result == "true":
        if indicator == "deter":
            return "update-amz-deter"
        elif indicator == "focos":
            return "update-active-fires"
        elif indicator == "risco":
            return "download-inpe-risk-file"
        assert False

    return f"skip-update-{indicator}"


def need_update_deter(dag):
    return _need_update_indicator(dag, indicator="deter")


def need_update_fires(dag):
    return _need_update_indicator(dag=dag, indicator="focos")


def need_update_risk(dag):
    return _need_update_indicator(dag=dag, indicator="risco")


def prepare_status_email(**context):
    bash_result = context["ti"].xcom_pull(task_ids=f"retrieve-process-status")

    res = json.loads(bash_result)

    context["ti"].xcom_push(key="email_subject", value=res["subject"])
    context["ti"].xcom_push(key="email_html_content", value=res["html_content"])

    print(context)


def send_status_email():
    return EmailOperator(
        task_id="send-status-email",
        mime_charset="utf-8",
        to=Variable.get("AMS_EMAIL_TO"),
        subject="{{ ti.xcom_pull(task_ids='prepare-status-email', key='email_subject') }}",
        html_content="{{ ti.xcom_pull(task_ids='prepare-status-email', key='email_html_content') }}",
    )


def retrieve_process_status(dag: DAG):
    env = get_conn_secrets_uri(["AMS_DB_URL"])

    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += f"ams-print-process-status --start=\"{{{{ ti.xcom_pull(task_ids='check-variables', key='start_process') }}}}\""

    return BashOperator(
        task_id=f"retrieve-process-status",
        bash_command=bash_command,
        env=env,
        append_env=True,
        dag=dag,
        trigger_rule="all_done",
    )


with DAG(
    "ams-create-db",
    default_args=default_args,
    schedule_interval="0 4 * * *",
    catchup=False,
    concurrency=3,
    max_active_runs=1,
) as dag:
    run_check_variables = ShortCircuitOperator(
        task_id="check-variables",
        provide_context=True,
        python_callable=check_variables,
        op_kwargs={},
        dag=dag,
    )

    run_update_environment = update_environment(dag=dag)

    run_check_recreate_db = BranchPythonOperator(
        task_id="check-create-db",
        python_callable=_check_recreate_db,
    )

    run_skip = EmptyOperator(task_id="skip-create-db")
    run_join = EmptyOperator(
        task_id="join-create-db", trigger_rule="none_failed_or_skipped"
    )

    # database creation
    run_create_db = create_db(dag=dag)

    # biomes and spatial units
    run_update_biome = update_biome(dag=dag)
    run_update_spatial_units = update_spatial_units(dag=dag)

    # indicators
    run_update_active_fires = update_active_fires(dag=dag)
    run_update_amz_deter = update_amz_deter(dag=dag)
    run_update_cer_deter = update_cer_deter(dag=dag)
    run_update_pan_deter = update_pan_deter(dag=dag)
    run_finalize_deter_update = finalize_deter_update(dag=dag)
    # run_download_ibama_risk_file = download_ibama_risk_file()
    run_download_risk_file = download_inpe_risk_file(dag=dag)
    # run_update_ibama_risk = update_ibama_risk()
    run_update_risk = update_inpe_risk(dag=dag)

    # preparing to classify
    run_prepare_classification_ams = prepare_classification_ams(dag=dag)
    run_prepare_classification_ppcdam = prepare_classification_ppcdam(dag=dag)

    # classification
    run_classify_deter_ams = classify_deter_by_land_use_ams(dag=dag)
    run_classify_deter_ppcdam = classify_deter_by_land_use_ppcdam(dag=dag)

    run_classify_fires_ams = classify_fires_by_land_use_ams(dag=dag)
    run_classify_fires_ppcdam = classify_fires_by_land_use_ppcdam(dag=dag)

    run_classify_risk_ams = classify_risk_by_land_use_ams(dag=dag)
    run_classify_risk_ppcdam = classify_risk_by_land_use_ppcdam(dag=dag)

    # finalize classification
    run_finalize_classification_ams = finalize_classification_ams(dag=dag)
    run_finalize_classification_ppcdam = finalize_classification_ppcdam(dag=dag)

    run_retrieve_process_status = retrieve_process_status(dag=dag)

    # running

    run_check_variables >> run_update_environment

    run_update_environment >> run_check_recreate_db

    run_check_recreate_db >> [run_create_db, run_skip]

    run_create_db >> [run_update_biome, run_update_spatial_units] >> run_join
    run_skip >> run_join

    run_join2 = EmptyOperator(task_id="join-prepare-classification")

    (
        run_join
        >> [run_prepare_classification_ams, run_prepare_classification_ppcdam]
        >> run_join2
    )

    run_check_update_deter = need_update_deter(dag=dag)
    run_check_update_fires = need_update_fires(dag=dag)
    run_check_update_risk = need_update_risk(dag=dag)

    run_join2 >> [run_check_update_risk, run_check_update_deter, run_check_update_fires]

    decide_deter = BranchPythonOperator(
        task_id="decide-update-deter",
        python_callable=decide_update_indicator,
        provide_context=True,
        op_kwargs={
            "indicator": "deter",
        },
    )

    run_skip_update_deter = EmptyOperator(task_id="skip-update-deter")

    (
        run_check_update_deter
        >> decide_deter
        >> [
            run_update_amz_deter,
            run_skip_update_deter,
        ]
    )

    decide_fires = BranchPythonOperator(
        task_id="decide-update-fires",
        python_callable=decide_update_indicator,
        provide_context=True,
        op_kwargs={
            "indicator": "focos",
        },
    )

    run_skip_update_active_fires = EmptyOperator(task_id="skip-update-focos")

    (
        run_check_update_fires
        >> decide_fires
        >> [
            run_update_active_fires,
            run_skip_update_active_fires,
        ]
    )

    decide_risk = BranchPythonOperator(
        task_id="decide-update-risk",
        python_callable=decide_update_indicator,
        provide_context=True,
        op_kwargs={
            "indicator": "risco",
        },
    )

    run_skip_update_risk = EmptyOperator(task_id="skip-update-risco")

    (
        run_check_update_risk
        >> decide_risk
        >> [
            run_download_risk_file,
            run_skip_update_risk,
        ]
    )

    (
        run_download_risk_file
        >> run_update_risk
        >> [run_classify_risk_ams, run_classify_risk_ppcdam]
    )

    run_update_active_fires >> [
        run_classify_fires_ams,
        run_classify_fires_ppcdam,
    ]

    (
        run_update_amz_deter
        >> run_update_cer_deter
        >> run_update_pan_deter
        >> run_finalize_deter_update
    )

    run_finalize_deter_update >> [run_classify_deter_ams, run_classify_deter_ppcdam]

    (
        run_classify_deter_ams,
        run_classify_fires_ams,
        run_classify_risk_ams,
        run_skip_update_deter,
        run_skip_update_active_fires,
        run_skip_update_risk,
    ) >> run_finalize_classification_ams

    [
        run_classify_deter_ppcdam,
        run_classify_fires_ppcdam,
        run_classify_risk_ppcdam,
        run_skip_update_deter,
        run_skip_update_active_fires,
        run_skip_update_risk,
    ] >> run_finalize_classification_ppcdam

    [
        run_finalize_classification_ams,
        run_finalize_classification_ppcdam,
    ] >> run_retrieve_process_status

    run_prepare_status_email = PythonOperator(
        task_id="prepare-status-email",
        python_callable=prepare_status_email,
        provide_context=True,
        dag=dag,
    )

    run_send_status_email = send_status_email()

    run_retrieve_process_status >> run_prepare_status_email >> run_send_status_email

# DAG: ams-calculate-land-use-area


@task(task_id="calculate-biomes-land-use-area")
def calculate_biomes_land_use_area():
    bash_command = f"source {venv_path}/bin/activate && "
    bash_command += (
        f"ams-calculate-land-use-area {('--force-recreate' if Variable.get('AMS_FORCE_RECREATE_DB')=='1' else '')} "
        f"{_get_all_biomes()}"
        " --land-use-type='ams'"
        " --land-use-dir=" + land_use_dir
    )

    env = get_conn_secrets_uri(["AMS_DB_URL"])

    return BashOperator(
        task_id="calculate-biomes-land-use-area",
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

    run_update_environment = update_environment(dag=dag)

    run_calculate_land_use_area = calculate_biomes_land_use_area()

    (run_check_variables >> run_update_environment >> run_calculate_land_use_area)
