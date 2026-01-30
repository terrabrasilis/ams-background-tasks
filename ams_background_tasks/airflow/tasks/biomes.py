from airflow.models import Variable

from ams_background_tasks.airflow.common.biomes import get_all_biomes
from ams_background_tasks.airflow.common.env import LAND_USE_DIR
from ams_background_tasks.airflow.common.tasks import bash_task


def update_biome(dag):
    command = f"ams-update-biome {get_all_biomes()}"

    return bash_task(
        dag=dag,
        task_id="update-biome",
        command=command,
        env_keys=["AMS_DB_URL", "AMS_AUX_DB_URL"],
    )


def calculate_biomes_land_use_area(dag):
    command = (
        f"ams-calculate-land-use-area {('--force-recreate' if Variable.get('AMS_FORCE_RECREATE_DB')=='1' else '')} "
        f"{get_all_biomes()}"
        " --land-use-type='ams'"
        f" --land-use-dir={LAND_USE_DIR}"
    )

    return bash_task(
        dag=dag,
        task_id="calculate-biomes-land-use-area",
        command=command,
        env_keys=["AMS_DB_URL"],
    )
