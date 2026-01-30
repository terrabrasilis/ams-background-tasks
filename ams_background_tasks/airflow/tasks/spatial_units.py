from ams_background_tasks.airflow.common.biomes import get_all_biomes
from ams_background_tasks.airflow.common.tasks import bash_task


def update_spatial_units(dag):
    command = f"ams-update-spatial-units {get_all_biomes()}"

    return bash_task(
        dag=dag,
        task_id="update-spatial-units",
        command=command,
        env_keys=["AMS_DB_URL", "AMS_AUX_DB_URL"],
    )
