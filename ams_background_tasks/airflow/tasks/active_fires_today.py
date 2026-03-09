from airflow.models import Variable

from ams_background_tasks.airflow.common.biomes import get_all_biomes
from ams_background_tasks.airflow.common.env import LAND_USE_DIR
from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import (
    CONN_DB_URL,
    VAR_ALL_DATA_DB,
    VAR_FREQUENCY_UPDATE_FIRES_TODAY,
    VAR_LIMIT,
)


def update_active_fires_today(dag):
    command = (
        "ams-update-active-fires-today "
        f"{get_all_biomes()} "
        f"--limit={Variable.get(VAR_LIMIT, 0)}"
    )

    return bash_task(
        dag=dag,
        task_id="update-active-fires-today",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def _classify_fires_today_by_land_use(dag, land_use_type: str):
    command = (
        f"ams-classify-by-land-use "
        f"{('--all-data' if Variable.get(VAR_ALL_DATA_DB)=='1' else '')} "
        f"{get_all_biomes()} "
        "--indicator='focos-hoje' "
        f"--land-use-type={land_use_type} "
        f"--land-use-dir={LAND_USE_DIR}"
    )

    return bash_task(
        dag=dag,
        task_id=f"classify-fires-today-by-land-use-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def classify_fires_today_by_land_use_ams(dag):
    return _classify_fires_today_by_land_use(dag=dag, land_use_type="ams")


def classify_fires_today_by_land_use_ppcdam(dag):
    return _classify_fires_today_by_land_use(dag=dag, land_use_type="ppcdam")


def need_update_fires_today(dag):
    command = (
        f"ams-need-update-indicator --indicator=focos-hoje "
        f"--frequency={Variable.get(VAR_FREQUENCY_UPDATE_FIRES_TODAY)}"
    )

    return bash_task(
        dag=dag,
        command=command,
        task_id="need-update-fires-today",
        env_keys=[CONN_DB_URL],
    )


def decide_update_fires_today(**context):
    bash_result = context["ti"].xcom_pull(task_ids="need-update-fires-today")

    bash_result = bash_result.strip().lower()

    if bash_result == "true":
        return "update-active-fires-today"

    return "skip-update-fires-today"
