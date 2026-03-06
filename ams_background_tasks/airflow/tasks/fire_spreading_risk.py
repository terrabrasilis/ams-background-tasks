from airflow.models import Variable

from ams_background_tasks.airflow.common.env import FIRE_SR_DIR, LAND_USE_DIR
from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import (
    CONN_DB_URL,
    VAR_ALL_DATA_DB,
    VAR_FREQUENCY_UPDATE_FIRE_SPREADING_RISK,
)


def download_fire_sr_file(dag):
    command = f"ams-download-fire-spreading-risk-file --save-dir {FIRE_SR_DIR}"

    return bash_task(
        dag=dag,
        task_id="download-fire-sr-file",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def process_fire_sr_file(dag):
    command = f"ams-process-fire-spreading-risk-file --save-dir {FIRE_SR_DIR}"

    return bash_task(
        dag=dag,
        task_id="process-fire-sr-file",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def import_fire_sr(dag):
    command = f"ams-import-fire-spreading-risk-file --save-dir {FIRE_SR_DIR}"

    return bash_task(
        dag=dag,
        task_id="import-fire-sr-file",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def update_fire_sr(dag):
    command = f"ams-process-fire-spreading-risk-file --save-dir {FIRE_SR_DIR}"

    return bash_task(
        dag=dag,
        task_id="update-fire-sr",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def _classify_fire_sr_by_land_use(dag, land_use_type: str):
    command = (
        f"ams-classify-by-land-use "
        f"{('--all-data' if Variable.get(VAR_ALL_DATA_DB)=='1' else '')} "
        "--biome='Cerrado' "
        "--indicator='risco-espalhamento-fogo' "
        f"--land-use-type={land_use_type} "
        f"--land-use-dir={LAND_USE_DIR}"
    )

    return bash_task(
        dag=dag,
        task_id=f"classify-fire-sr-by-land-use-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def classify_fire_sr_by_land_use_ams(dag):
    return _classify_fire_sr_by_land_use(dag=dag, land_use_type="ams")


def classify_fire_sr_by_land_use_ppcdam(dag):
    return _classify_fire_sr_by_land_use(dag=dag, land_use_type="ppcdam")


def need_update_fire_sr(dag):
    command = (
        f"ams-need-update-indicator --indicator=risco-espalhamento-fogo "
        f"--frequency={Variable.get(VAR_FREQUENCY_UPDATE_FIRE_SPREADING_RISK)}"
    )

    return bash_task(
        dag=dag,
        command=command,
        task_id="need-update-fire-sr",
        env_keys=[CONN_DB_URL],
    )


def decide_update_fire_sr(**context):
    bash_result = context["ti"].xcom_pull(task_ids="need-update-fire-sr")

    bash_result = bash_result.strip().lower()

    if bash_result == "true":
        return "import-fire-sr-file"

    return "skip-update-fire-sr"
