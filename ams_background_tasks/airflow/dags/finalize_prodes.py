from airflow import DAG
from airflow.operators.python import ShortCircuitOperator

from ams_background_tasks.airflow.common.env import update_environment
from ams_background_tasks.airflow.common.vars import check_variables
from ams_background_tasks.airflow.dags import default_args
from ams_background_tasks.airflow.tasks.prodes import (
    finalize_prodes_ams,
    finalize_prodes_ppcdam,
)


def build_finalize_prodes_dag():
    with DAG(
        "ams-finalize-prodes",
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
        run_finalize_prodes_ams = finalize_prodes_ams(dag=dag)
        run_finalize_prodes_ppcdam = finalize_prodes_ppcdam(dag=dag)

        run_check_variables >> run_update_environment >> run_finalize_prodes_ams >> run_finalize_prodes_ppcdam  # type: ignore

        return dag
