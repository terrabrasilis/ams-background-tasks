from airflow.models import Variable

from ams_background_tasks.airflow.common.tasks import bash_task
from ams_background_tasks.airflow.common.vars import CONN_DB_URL, VAR_FORCE_RECREATE_DB


def create_db(dag):
    command = (
        f"ams-create-db "
        f"{('--force-recreate' if Variable.get(VAR_FORCE_RECREATE_DB)=='1' else '')}"
    )

    return bash_task(
        dag=dag,
        task_id="create-db",
        command=command,
        env_keys=[CONN_DB_URL],
    )


def drop_temp_tables(dag):
    command = "ams-drop-temp-tables"

    return bash_task(
        dag=dag,
        task_id="drop-temp-tables",
        command=command,
        env_keys=[CONN_DB_URL],
        trigger_rule="all_success",
    )


def check_recreate_db():
    force_recreate = Variable.get(VAR_FORCE_RECREATE_DB) == "1"

    if force_recreate:
        return "create-db"

    return "skip-create-db"
