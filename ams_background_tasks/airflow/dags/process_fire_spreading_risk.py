from airflow import DAG
from airflow.operators.python import ShortCircuitOperator

from ams_background_tasks.airflow.common.env import update_environment
from ams_background_tasks.airflow.common.vars import check_variables
from ams_background_tasks.airflow.dags import default_args
from ams_background_tasks.airflow.tasks.fire_spreading_risk import (
    download_fire_sr_file,
    process_fire_sr_file,
)


def build_process_fire_sr_dag():
    with DAG(
        "ams-process-fire-spreading-risk-file",
        default_args=default_args,
        schedule_interval="0 2 * * *",
        catchup=False,
    ) as dag:
        run_check_variables = ShortCircuitOperator(
            task_id="check-variables",
            provide_context=True,
            python_callable=check_variables,
            op_kwargs={},
        )
        run_update_environment = update_environment(dag=dag)
        run_download_fire_sr_file = download_fire_sr_file(dag)
        run_process_fire_sr_file = process_fire_sr_file(dag)

        (  # type: ignore
            run_check_variables
            >> run_update_environment
            >> run_download_fire_sr_file
            >> run_process_fire_sr_file
        )

        return dag
