"""A DAG to load the environment variables."""

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.secrets import environment_variables
from common import default_args, get_variable

DAG_KEY = "load-vars"


def _set_vars():
    Variable.set("AMS__DB_URL", get_variable("AMS__DB_URL"))
    Variable.set("AMS__AUX_DB_URL", get_variable("AMS__AUX_DB_URL"))


with DAG(DAG_KEY, default_args=default_args, schedule_interval=None) as dag:
    set_vars = PythonOperator(task_id=DAG_KEY, python_callable=_set_vars)
    set_vars
