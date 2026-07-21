import sys
from pathlib import Path

project_dir = str(Path(__file__).parent.resolve().absolute())

sys.path.append(project_dir)


from ams_background_tasks.airflow.dags.calculate_land_use_area import (
    build_calculate_land_use_area_dag,
)
from ams_background_tasks.airflow.dags.create_db import build_ams_create_db_dag
from ams_background_tasks.airflow.dags.finalize_prodes import build_finalize_prodes_dag
from ams_background_tasks.airflow.dags.process_fire_spreading_risk import (
    build_process_fire_sr_dag,
)
from ams_background_tasks.airflow.dags.update_prodes import build_update_prodes_dag

dag1 = build_ams_create_db_dag()
dag2 = build_process_fire_sr_dag()
dag3 = build_calculate_land_use_area_dag()
dag4 = build_update_prodes_dag()
dag5 = build_finalize_prodes_dag()
