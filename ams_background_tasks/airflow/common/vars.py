from datetime import datetime

from airflow.hooks.base import BaseHook  # type: ignore
from airflow.models import Variable  # type: ignore

VAR_ALL_DATA_DB = "AMS_ALL_DATA_DB"
VAR_FORCE_RECREATE_DB = "AMS_FORCE_RECREATE_DB"
VAR_BIOMES = "AMS_BIOMES"
VAR_STAC_API_URL = "AMS_STAC_API_URL"
VAR_STAC_COLLECTION = "AMS_STAC_COLLECTION"
VAR_EMAIL_TO = "AMS_EMAIL_TO"
VAR_FREQUENCY_UPDATE_DETER = "AMS_FREQUENCY_TO_UPDATE_DETER"
VAR_FREQUENCY_UPDATE_FIRES = "AMS_FREQUENCY_TO_UPDATE_FIRES"
VAR_FREQUENCY_UPDATE_FIRES_TODAY = "AMS_FREQUENCY_TO_UPDATE_FIRES_TODAY"
VAR_FREQUENCY_UPDATE_RISK = "AMS_FREQUENCY_TO_UPDATE_RISK"
VAR_FREQUENCY_UPDATE_FIRE_SPREADING_RISK = "AMS_FREQUENCY_TO_UPDATE_FIRE_SR"
VAR_FORCE_UPDATE_AT = "AMS_FORCE_UPDATE_AT"
VAR_LIMIT = "AMS_LIMIT"

CONN_DB_URL = "AMS_DB_URL"
CONN_AUX_DB_URL = "AMS_AUX_DB_URL"
CONN_AF_DB_URL = "AMS_AF_DB_URL"
CONN_FC_DB_URL = "AMS_FC_DB_URL"
CONN_AMZ_DETER_B_DB_URL = "AMS_AMZ_DETER_B_DB_URL"
CONN_CER_DETER_B_DB_URL = "AMS_CER_DETER_B_DB_URL"
CONN_PAN_DETER_B_DB_URL = "AMS_PAN_DETER_B_DB_URL"

REQUIRED_VARS = [
    VAR_ALL_DATA_DB,
    VAR_FORCE_RECREATE_DB,
    VAR_BIOMES,
    VAR_STAC_API_URL,
    VAR_STAC_COLLECTION,
    VAR_EMAIL_TO,
    VAR_FREQUENCY_UPDATE_DETER,
    VAR_FREQUENCY_UPDATE_FIRES,
    VAR_FREQUENCY_UPDATE_FIRES_TODAY,
    VAR_FREQUENCY_UPDATE_RISK,
    VAR_FREQUENCY_UPDATE_FIRE_SPREADING_RISK,
    VAR_LIMIT,
]


REQUIRED_CONNS = [
    CONN_DB_URL,
    CONN_AUX_DB_URL,
    CONN_AF_DB_URL,
    CONN_FC_DB_URL,
    CONN_AMZ_DETER_B_DB_URL,
    CONN_CER_DETER_B_DB_URL,
    CONN_PAN_DETER_B_DB_URL,
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
