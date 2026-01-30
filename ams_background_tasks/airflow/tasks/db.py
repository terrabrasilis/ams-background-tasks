from airflow.models import Variable

from ams_background_tasks.airflow.common.tasks import bash_task


def create_db(dag):
    command = f"ams-create-db {('--force-recreate' if Variable.get('AMS_FORCE_RECREATE_DB')=='1' else '')}"

    return bash_task(
        dag=dag,
        task_id="create-db",
        command=command,
        env_keys=["AMS_DB_URL"],
    )


def drop_temp_tables(dag):
    command = "ams-drop-temp-tables"

    return bash_task(
        dag=dag,
        task_id="drop-temp-tables",
        command=command,
        env_keys=["AMS_DB_URL"],
        trigger_rule="all_success",
    )


def check_recreate_db():
    force_recreate = Variable.get("AMS_FORCE_RECREATE_DB") == "1"

    if force_recreate:
        return "create-db"

    return "skip-create-db"
