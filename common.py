"""Common definitions."""

import os
from datetime import datetime
from airflow.hooks.base import BaseHook

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "retries": 0,
    "catchup": False,
}

def get_conn_secrets_uri(names: list):
    """Return a dict with airflow connection secrets uri."""
    env = {}
    for name in names:
        url = BaseHook.get_connection(name).get_uri()        
        if "?__extra__=" not in url:
            env[name] = url
        else:
            env[name] = url.split("?__extra__=")[0]
        print("Connection secret: " + env[name])
    return env
