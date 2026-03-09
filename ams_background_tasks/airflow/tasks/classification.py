from airflow.models import Variable

from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import (
    CONN_DB_URL,
    VAR_ALL_DATA_DB,
    VAR_FORCE_RECREATE_DB,
)


def _prepare_classification(dag, land_use_type: str):
    command = f"ams-prepare-classification --land-use-type {land_use_type}"
    command += (
        f" {('--force-recreate' if Variable.get(VAR_FORCE_RECREATE_DB)=='1' else '')}"
    )

    return bash_task(
        dag=dag,
        task_id=f"prepare-classification-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def prepare_classification_ams(dag):
    return _prepare_classification(dag=dag, land_use_type="ams")


def prepare_classification_ppcdam(dag):
    return _prepare_classification(dag=dag, land_use_type="ppcdam")


def prepare_classification_prodes(dag):
    return _prepare_classification(dag=dag, land_use_type="prodes")


def finalize_classification(dag, land_use_type: str):
    command = (
        f"ams-finalize-classification"
        f" {('--all-data' if Variable.get(VAR_ALL_DATA_DB)=='1' else '')}"
        f" --land-use-type={land_use_type}"
    )

    return bash_task(
        dag=dag,
        task_id=f"finalize-classification-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
        trigger_rule="all_done",
    )


def finalize_classification_ams(dag):
    return finalize_classification(dag=dag, land_use_type="ams")


def finalize_classification_ppcdam(dag):
    return finalize_classification(dag=dag, land_use_type="ppcdam")


def finalize_classification_prodes(dag):
    return finalize_classification(dag=dag, land_use_type="prodes")
