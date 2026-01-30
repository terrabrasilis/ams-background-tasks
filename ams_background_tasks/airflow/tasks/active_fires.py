from airflow.models import Variable

from ams_background_tasks.airflow.common.biomes import get_all_biomes
from ams_background_tasks.airflow.common.env import LAND_USE_DIR
from ams_background_tasks.airflow.common.tasks import bash_task


def update_active_fires(dag):
    command = (
        "ams-update-active-fires "
        f"{'--all-data' if Variable.get('AMS_ALL_DATA_DB') == '1' else ''} "
        f"{get_all_biomes()} "
        f"--limit={Variable.get('AMS_LIMIT', 0)}"
    )

    return bash_task(
        dag=dag,
        task_id="update-active-fires",
        command=command,
        env_keys=[
            "AMS_DB_URL",
            "AMS_AF_DB_URL",
            "AMS_FC_DB_URL",
        ],
    )


def _classify_fires_by_land_use(dag, land_use_type: str):
    command = (
        f"ams-classify-by-land-use "
        f"{('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')} "
        f"{get_all_biomes()} "
        "--indicator='focos' "
        f"--land-use-type={land_use_type} "
        f"--land-use-dir={LAND_USE_DIR}"
    )

    return bash_task(
        dag=dag,
        task_id=f"classify-fires-by-land-use-{land_use_type}",
        command=command,
        env_keys=["AMS_DB_URL"],
    )


def classify_fires_by_land_use_ams(dag):
    return _classify_fires_by_land_use(dag=dag, land_use_type="ams")


def classify_fires_by_land_use_ppcdam(dag):
    return _classify_fires_by_land_use(dag=dag, land_use_type="ppcdam")


def classify_fires_by_land_use_prodes(dag):
    return _classify_fires_by_land_use(dag=dag, land_use_type="prodes")


def need_update_fires(dag):
    command = (
        f"ams-need-update-indicator --indicator=focos "
        f"--frequency={Variable.get('AMS_FREQUENCY_TO_UPDATE_ACTIVE_FIRES')}"
    )

    return bash_task(
        dag=dag,
        command=command,
        task_id="need-update-fires",
        env_keys=["AMS_DB_URL"],
    )


def decide_update_fires(**context):
    bash_result = context["ti"].xcom_pull(task_ids="need-update-fires")

    bash_result = bash_result.strip().lower()

    if bash_result == "true":
        return "update-active-fires"

    return "skip-update-fires"
