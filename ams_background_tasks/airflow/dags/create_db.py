from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)

from ams_background_tasks.airflow.common.env import update_environment
from ams_background_tasks.airflow.common.vars import check_variables
from ams_background_tasks.airflow.dags import default_args
from ams_background_tasks.airflow.tasks.active_fires import (
    classify_fires_by_land_use_ams,
    classify_fires_by_land_use_ppcdam,
    classify_fires_by_land_use_prodes,
    decide_update_fires,
    need_update_fires,
    update_active_fires,
)
from ams_background_tasks.airflow.tasks.biomes import update_biome
from ams_background_tasks.airflow.tasks.classification import (
    finalize_classification_ams,
    finalize_classification_ppcdam,
    finalize_classification_prodes,
    prepare_classification_ams,
    prepare_classification_ppcdam,
    prepare_classification_prodes,
)
from ams_background_tasks.airflow.tasks.db import (
    check_recreate_db,
    create_db,
    drop_temp_tables,
)
from ams_background_tasks.airflow.tasks.deter import (
    classify_deter_by_land_use_ams,
    classify_deter_by_land_use_ppcdam,
    decide_update_deter,
    finalize_deter_update,
    need_update_deter,
    update_amz_deter,
    update_cer_deter,
    update_pan_deter,
)
from ams_background_tasks.airflow.tasks.fire_spreading_risk import (
    classify_fire_sr_by_land_use_ams,
    classify_fire_sr_by_land_use_ppcdam,
    decide_update_fire_sr,
    import_fire_sr,
    need_update_fire_sr,
    update_fire_sr,
)
from ams_background_tasks.airflow.tasks.mailer import (
    prepare_status_email,
    retrieve_process_status,
    send_status_email,
)
from ams_background_tasks.airflow.tasks.risk import (
    classify_risk_by_land_use_ams,
    classify_risk_by_land_use_ppcdam,
    decide_update_risk,
    download_inpe_risk_file,
    need_update_risk,
    update_inpe_risk,
)
from ams_background_tasks.airflow.tasks.spatial_units import update_spatial_units


def build_ams_create_db_dag():
    with DAG(
        "ams-create-db",
        default_args=default_args,
        schedule_interval="0 4 * * *",
        catchup=False,
        concurrency=4,
        max_active_runs=1,
    ) as dag:
        # preparing the environment

        run_check_variables = ShortCircuitOperator(
            task_id="check-variables",
            provide_context=True,
            python_callable=check_variables,
            op_kwargs={},
            dag=dag,
        )

        run_update_environment = update_environment(dag=dag)

        run_check_variables >> run_update_environment  # type: ignore

        # creating the database

        run_check_recreate_db = BranchPythonOperator(
            task_id="check-create-db",
            python_callable=check_recreate_db,
        )

        run_skip = EmptyOperator(task_id="skip-create-db")

        run_create_db = create_db(dag=dag)

        run_update_environment >> run_check_recreate_db  # type: ignore

        run_check_recreate_db >> [run_create_db, run_skip]  # type: ignore

        # creating biomes and spatial units

        run_join = EmptyOperator(
            task_id="join-create-db", trigger_rule="none_failed_or_skipped"
        )

        run_update_biome = update_biome(dag=dag)
        run_update_spatial_units = update_spatial_units(dag=dag)

        run_create_db >> [run_update_biome, run_update_spatial_units] >> run_join  # type: ignore
        run_skip >> run_join  # type: ignore

        # preparing the database to classify

        run_join2 = EmptyOperator(task_id="join-prepare-classification")

        (
            run_join  # type: ignore
            >> [
                prepare_classification_ams(dag=dag),
                prepare_classification_ppcdam(dag=dag),
                prepare_classification_prodes(dag=dag),
            ]
            >> run_join2
        )

        # checking the need for an upddate
        run_check_update_fires = need_update_fires(dag=dag)
        run_check_update_deter = need_update_deter(dag=dag)
        run_check_update_risk = need_update_risk(dag=dag)
        run_check_update_fire_sr = need_update_fire_sr(dag=dag)

        run_join2 >> [run_check_update_fires, run_check_update_deter, run_check_update_risk, run_check_update_fire_sr]  # type: ignore

        # active fires
        run_decide_update_fires = BranchPythonOperator(
            task_id="decide-update-fires",
            python_callable=decide_update_fires,
            provide_context=True,
            op_kwargs={},
        )

        run_skip_update_fires = EmptyOperator(task_id="skip-update-fires")

        run_update_fires = update_active_fires(dag=dag)

        (  # type: ignore
            run_check_update_fires
            >> run_decide_update_fires
            >> [run_skip_update_fires, run_update_fires]
        )

        # deter
        run_decide_update_deter = BranchPythonOperator(
            task_id="decide-update-deter",
            python_callable=decide_update_deter,
            provide_context=True,
            op_kwargs={},
        )

        run_skip_update_deter = EmptyOperator(task_id="skip-update-deter")

        run_update_amz_deter = update_amz_deter(dag=dag)
        run_update_cer_deter = update_cer_deter(dag=dag)
        run_update_pan_deter = update_pan_deter(dag=dag)

        (  # type: ignore
            run_check_update_deter
            >> run_decide_update_deter
            >> [
                run_update_amz_deter,
                run_skip_update_deter,
            ]
        )

        run_finalize_deter_update = finalize_deter_update(dag=dag)

        (  # type: ignore
            run_update_amz_deter
            >> run_update_cer_deter
            >> run_update_pan_deter
            >> run_finalize_deter_update
        )

        # risk
        run_decide_update_risk = BranchPythonOperator(
            task_id="decide-update-risk",
            python_callable=decide_update_risk,
            provide_context=True,
            op_kwargs={},
        )

        run_skip_update_risk = EmptyOperator(task_id="skip-update-risk")

        run_download_risk_file = download_inpe_risk_file(dag=dag)

        (  # type: ignore
            run_check_update_risk
            >> run_decide_update_risk
            >> [
                run_download_risk_file,
                run_skip_update_risk,
            ]
        )

        run_update_risk = update_inpe_risk(dag=dag)

        (run_download_risk_file >> run_update_risk)  # type: ignore

        # fire spreading risk

        run_decide_update_fire_sr = BranchPythonOperator(
            task_id="decide-update-fire-sr",
            python_callable=decide_update_fire_sr,
            provide_context=True,
            op_kwargs={},
        )

        run_skip_update_fire_sr = EmptyOperator(task_id="skip-update-fire-sr")
        run_import_fire_sr = import_fire_sr(dag=dag)

        (  # type: ignore
            run_check_update_fire_sr
            >> run_decide_update_fire_sr
            >> [
                run_import_fire_sr,
                run_skip_update_fire_sr,
            ]
        )

        run_update_fire_sr = update_fire_sr(dag=dag)

        run_import_fire_sr >> run_update_fire_sr  # type: ignore

        # classification

        # activate fires
        run_classify_fires_ams = classify_fires_by_land_use_ams(dag=dag)
        run_classify_fires_ppcdam = classify_fires_by_land_use_ppcdam(dag=dag)
        run_classify_fires_prodes = classify_fires_by_land_use_prodes(dag=dag)

        run_update_fires >> [  # type: ignore
            run_classify_fires_ams,
            run_classify_fires_ppcdam,
            run_classify_fires_prodes,
        ]

        # deter
        run_classify_deter_ams = classify_deter_by_land_use_ams(dag=dag)
        run_classify_deter_ppcdam = classify_deter_by_land_use_ppcdam(dag=dag)

        run_finalize_deter_update >> [run_classify_deter_ams, run_classify_deter_ppcdam]  # type: ignore

        # risk
        run_classify_risk_ams = classify_risk_by_land_use_ams(dag=dag)
        run_classify_risk_ppcdam = classify_risk_by_land_use_ppcdam(dag=dag)

        run_update_risk >> [run_classify_risk_ams, run_classify_risk_ppcdam]  # type: ignore

        # fire spreading risk
        run_classify_fire_sr_ams = classify_fire_sr_by_land_use_ams(dag=dag)
        run_classify_fire_sr_ppcdam = classify_fire_sr_by_land_use_ppcdam(dag=dag)

        run_update_fire_sr >> [  # type: ignore
            run_classify_fire_sr_ams,
            run_classify_fire_sr_ppcdam,
        ]

        # finalize classification

        run_finalize_classification_ams = finalize_classification_ams(dag=dag)
        run_finalize_classification_ppcdam = finalize_classification_ppcdam(dag=dag)
        run_finalize_classification_prodes = finalize_classification_prodes(dag=dag)

        (  # type: ignore
            run_classify_deter_ams,
            run_classify_fires_ams,
            run_classify_risk_ams,
            run_skip_update_deter,
            run_skip_update_fires,
            run_skip_update_risk,
            run_classify_fire_sr_ams,
            run_skip_update_fire_sr,
        ) >> run_finalize_classification_ams

        [  # type: ignore
            run_classify_deter_ppcdam,
            run_classify_fires_ppcdam,
            run_classify_risk_ppcdam,
            run_skip_update_deter,
            run_skip_update_fires,
            run_skip_update_risk,
            run_classify_fire_sr_ppcdam,
            run_skip_update_fire_sr,
        ] >> run_finalize_classification_ppcdam

        [  # type: ignore
            run_classify_fires_prodes,
            run_skip_update_fires,
        ] >> run_finalize_classification_prodes

        # dropping temp tables
        run_drop_temp_tables = drop_temp_tables(dag=dag)

        (  # type: ignore
            run_finalize_classification_ams,
            run_finalize_classification_ppcdam,
            run_finalize_classification_prodes,
        ) >> run_drop_temp_tables

        # sending status mail
        run_retrieve_process_status = retrieve_process_status(dag=dag)

        run_drop_temp_tables >> run_retrieve_process_status  # type: ignore

        run_prepare_status_email = PythonOperator(
            task_id="prepare-status-email",
            python_callable=prepare_status_email,
            provide_context=True,
            dag=dag,
        )

        run_send_status_email = send_status_email()

        run_retrieve_process_status >> run_prepare_status_email >> run_send_status_email  # type: ignore

        return dag
