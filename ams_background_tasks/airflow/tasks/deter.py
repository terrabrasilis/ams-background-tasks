from airflow.models import Variable

from ams_background_tasks.airflow.common.env import LAND_USE_DIR
from ams_background_tasks.airflow.common.secrets import get_conn_secrets_uri
from ams_background_tasks.airflow.common.tasks import bash_task


def update_amz_deter(dag):
    command = (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --biome='Amazônia' --truncate --limit={Variable.get('AMS_LIMIT', 0)}"
        f" --create-processing-flag"
    )

    _env = get_conn_secrets_uri(["AMS_AMZ_DETER_B_DB_URL"])
    env_dict = {"AMS_DETER_B_DB_URL": _env["AMS_AMZ_DETER_B_DB_URL"]}

    return bash_task(
        dag=dag,
        command=command,
        task_id="update-amz-deter",
        env_keys=["AMS_DB_URL"],
        env_dict=env_dict,
    )


def update_cer_deter(dag):
    command = (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --biome='Cerrado' --limit={Variable.get('AMS_LIMIT', 0)}"
    )

    _env = get_conn_secrets_uri(["AMS_CER_DETER_B_DB_URL"])
    env_dict = {"AMS_DETER_B_DB_URL": _env["AMS_CER_DETER_B_DB_URL"]}

    return bash_task(
        dag=dag,
        command=command,
        task_id="update-cer-deter",
        env_keys=["AMS_DB_URL"],
        env_dict=env_dict,
    )


def update_pan_deter(dag):
    command = (
        f"ams-update-deter"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        f" --biome='Pantanal' --limit={Variable.get('AMS_LIMIT', 0)}"
    )

    _env = get_conn_secrets_uri(["AMS_PAN_DETER_B_DB_URL"])
    env_dict = {"AMS_DETER_B_DB_URL": _env["AMS_PAN_DETER_B_DB_URL"]}

    return bash_task(
        dag=dag,
        command=command,
        task_id="update-pan-deter",
        env_keys=["AMS_DB_URL"],
        env_dict=env_dict,
    )


def finalize_deter_update(dag):
    command = (
        f"ams-finalize-deter-update"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
    )

    return bash_task(
        dag=dag,
        task_id="finalize-deter-update",
        command=command,
        env_keys=["AMS_DB_URL"],
    )


def _classify_deter_by_land_use(dag, land_use_type: str):
    command = (
        f"ams-classify-by-land-use"
        f" {('--all-data' if Variable.get('AMS_ALL_DATA_DB')=='1' else '')}"
        " --biome='Amazônia' --biome='Cerrado' --biome='Pantanal'"
        " --indicator='deter'"
        f" --land-use-type={land_use_type}"
        f" --land-use-dir={LAND_USE_DIR}"
    )

    return bash_task(
        dag=dag,
        task_id=f"classify-deter-by-land-use-{land_use_type}",
        command=command,
        env_keys=["AMS_DB_URL"],
    )


def classify_deter_by_land_use_ams(dag):
    return _classify_deter_by_land_use(dag=dag, land_use_type="ams")


def classify_deter_by_land_use_ppcdam(dag):
    return _classify_deter_by_land_use(dag=dag, land_use_type="ppcdam")


def need_update_deter(dag):
    command = (
        f"ams-need-update-indicator --indicator=deter "
        f"--frequency={Variable.get('AMS_FREQUENCY_TO_UPDATE_DETER')}"
    )

    return bash_task(
        dag=dag,
        command=command,
        task_id="need-update-deter",
        env_keys=["AMS_DB_URL"],
    )


def decide_update_deter(**context):
    bash_result = context["ti"].xcom_pull(task_ids="need-update-deter")

    bash_result = bash_result.strip().lower()

    if bash_result == "true":
        return "update-amz-deter"

    return "skip-update-deter"
