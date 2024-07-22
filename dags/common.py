"""Common definitions."""

import os
from datetime import datetime

default_args = {
    "start_date": datetime(2020, 1, 1),
    "owner": "airflow",
    "retries": 0,
    "catchup": False,
}


def get_variable(name: str):
    """Return the variable value."""
    assert name in os.environ
    if "_secret" in name:
        with open(os.environ[name], "r") as src:
            return src.read().strip()
    return os.environ[name]


def get_secrets_env(names: list):
    """Return a dict with secrets variables."""
    env = {}
    for name in names:
        env[name] = get_variable(f"{name}_secret")
    return env
