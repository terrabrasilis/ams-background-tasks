from ams_background_tasks.airflow.common.env import (
    LAND_USE_DIR,
    PRODES_CACHE_DIR,
    PRODES_CHUNK_DIR,
    PRODES_COUNT_DIR,
    PRODES_DIR,
    PRODES_REPROJECT_DIR,
    PRODES_ROOT_DIR,
)
from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import CONN_DB_URL
from ams_background_tasks.tools.common import AMAZONIA


def _update_prodes(dag, land_use_type: str):
    PRODES_DIR.mkdir(parents=True, exist_ok=True)
    PRODES_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PRODES_CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    PRODES_REPROJECT_DIR.mkdir(parents=True, exist_ok=True)
    PRODES_COUNT_DIR.mkdir(parents=True, exist_ok=True)

    command = (
        "ams-update-prodes"
        f" --biome='{AMAZONIA}'"
        f" --years 2000 2025"
        f" --land-use-dir={LAND_USE_DIR}"
        f" --land-use-type={land_use_type}"
        f" --prodes-root-dir={PRODES_ROOT_DIR}"
        " --chunk-size=1000"
        " --save-indicators"
        " --force-recreate"
    )

    return bash_task(
        dag=dag,
        task_id=f"update-prodes-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def update_prodes_ams(dag):
    return _update_prodes(dag=dag, land_use_type="ams")


def update_prodes_ppcdam(dag):
    return _update_prodes(dag=dag, land_use_type="ppcdam")


def _finalize_prodes(dag, land_use_type: str):
    command = f"ams-finalize-prodes --land-use-type={land_use_type}"

    return bash_task(
        dag=dag,
        task_id=f"finalize-prodes-{land_use_type}",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def finalize_prodes_ams(dag):
    return _finalize_prodes(dag=dag, land_use_type="ams")


def finalize_prodes_ppcdam(dag):
    return _finalize_prodes(dag=dag, land_use_type="ppcdam")
