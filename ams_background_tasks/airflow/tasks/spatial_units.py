from ams_background_tasks.airflow.common.biomes import get_all_biomes
from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import CONN_AUX_DB_URL, CONN_DB_URL


def update_spatial_units(dag):
    command = f"ams-update-spatial-units {get_all_biomes()}"

    return bash_task(
        dag=dag,
        task_id="update-spatial-units",
        command=command,
        env_keys=[CONN_DB_URL, CONN_AUX_DB_URL],
    )
