from datetime import datetime

from airflow.hooks.base import BaseHook  # type: ignore
from airflow.models import Variable  # type: ignore

REQUIRED_VARS = [
    "AMS_ALL_DATA_DB",
    "AMS_FORCE_RECREATE_DB",
    "AMS_BIOMES",
    "AMS_STAC_API_URL",
    "AMS_STAC_COLLECTION",
    "AMS_EMAIL_TO",
    "AMS_FREQUENCY_TO_UPDATE_DETER",
    "AMS_FREQUENCY_TO_UPDATE_FOCOS",
    "AMS_FREQUENCY_TO_UPDATE_RISCO",
    "AMS_FREQUENCY_TO_UPDATE_RISCO-ESPALHAMENTO-FOGO",
    "AMS_LIMIT",
]


REQUIRED_CONNS = [
    "AMS_DB_URL",
    "AMS_AUX_DB_URL",
    "AMS_AF_DB_URL",
    "AMS_FC_DB_URL",
    "AMS_AMZ_DETER_B_DB_URL",
    "AMS_CER_DETER_B_DB_URL",
    "AMS_PAN_DETER_B_DB_URL",
]


def validate_variables():
    for var in REQUIRED_VARS:
        if not Variable.get(var, default_var=None):
            raise ValueError(f"Missing Airflow Variable: {var}")


def validate_conns():
    for name in REQUIRED_CONNS:
        conn = BaseHook.get_connection(name)
        if not conn or not conn.get_uri():
            raise ValueError(f"Missing connection: {name}")


def check_variables(**context):
    context["ti"].xcom_push(
        key="start_process", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    validate_variables()
    validate_conns()

    return True
