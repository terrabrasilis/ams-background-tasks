from airflow import DAG
from airflow.operators.python import ShortCircuitOperator

from ams_background_tasks.airflow.common.env import update_environment
from ams_background_tasks.airflow.common.vars import check_variables
from ams_background_tasks.airflow.dags import default_args
from ams_background_tasks.airflow.tasks.biomes import calculate_biomes_land_use_area


def build_calculate_land_use_area_dag():
    with DAG(
        "ams-calculate-land-use-area",
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
        run_calculate_land_use_area = calculate_biomes_land_use_area(dag=dag)

        (  # type: ignore
            run_check_variables >> run_update_environment >> run_calculate_land_use_area
        )

        return dag
