from datetime import datetime

from airflow.models import Variable
from dateutil.relativedelta import relativedelta

from ams_background_tasks.airflow.common.env import LAND_USE_DIR, RISK_DIR
from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import (
    CONN_DB_URL,
    VAR_ALL_DATA_DB,
    VAR_FORCE_UPDATE_AT,
    VAR_FREQUENCY_UPDATE_RISK,
    VAR_STAC_API_URL,
    VAR_STAC_COLLECTION,
)


def download_inpe_risk_file(dag):
    beg = (datetime.now() - relativedelta(days=30)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")

    RISK_DIR.mkdir(parents=False, exist_ok=True)

    command = (
        "ams-download-inpe-risk-file"
        f" --stac-api-url={Variable.get(VAR_STAC_API_URL)}"
        f" --collection={Variable.get(VAR_STAC_COLLECTION)}"
        f" --save-dir={RISK_DIR}"
        f" --days-until-expiration=15"
        f" --begin='{beg}'"
        f" --end='{end}'"
    )

    return bash_task(
        dag=dag,
        task_id="download-inpe-risk-file",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def update_inpe_risk(dag):
    command = "ams-update-inpe-risk --risk-threshold=0. --biome='Amazônia'"

    return bash_task(
        dag=dag,
        task_id="update-inpe-risk",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def _classify_risk_by_land_use(dag, land_use_type: str):
    command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get(VAR_ALL_DATA_DB)=='1' else '')}"
        " --biome='Amazônia'"
        " --indicator='risco'"
        f" --land-use-type={land_use_type}"
        f" --land-use-dir={LAND_USE_DIR}"
    )

    return bash_task(
        dag=dag,
        task_id=f"classify-risk-by-land-use-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def classify_risk_by_land_use_ams(dag):
    return _classify_risk_by_land_use(dag=dag, land_use_type="ams")


def classify_risk_by_land_use_ppcdam(dag):
    return _classify_risk_by_land_use(dag=dag, land_use_type="ppcdam")


def need_update_risk(dag):
    command = (
        f"ams-need-update-indicator --indicator=risco "
        f"--frequency={Variable.get(VAR_FREQUENCY_UPDATE_RISK)} "
        f"--hour-force-update={Variable.get(VAR_FORCE_UPDATE_AT)}"
    )

    return bash_task(
        dag=dag,
        command=command,
        task_id="need-update-risk",
        env_keys=[CONN_DB_URL],
    )


def decide_update_risk(**context):
    bash_result = context["ti"].xcom_pull(task_ids="need-update-risk")

    bash_result = bash_result.strip().lower()

    if bash_result == "true":
        return "download-inpe-risk-file"

    return "skip-update-risk"
