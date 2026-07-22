from airflow import DAG
from airflow.operators.python import ShortCircuitOperator

from ams_background_tasks.airflow.common.env import update_environment
from ams_background_tasks.airflow.common.vars import check_variables
from ams_background_tasks.airflow.dags import default_args
from ams_background_tasks.airflow.tasks.prodes import (
    update_prodes_ams,
    update_prodes_ppcdam,
)


def build_update_prodes_dag():
    with DAG(
        "ams-update-prodes",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
    ) as dag:
        run_check_variables = ShortCircuitOperator(
            task_id="check-variables",
            provide_context=True,
            python_callable=check_variables,
            op_kwargs={},
        )
        run_update_environment = update_environment(dag=dag)
        run_update_prodes_ams = update_prodes_ams(dag=dag)
        run_update_prodes_ppcdam = update_prodes_ppcdam(dag=dag)

        run_check_variables >> run_update_environment >> run_update_prodes_ams >> run_update_prodes_ppcdam  # type: ignore

        return dag
