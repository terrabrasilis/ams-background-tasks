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
    """Load the environment variable."""
    assert name in os.environ
    value = os.environ[name]
    if "/run/secrets/" in value:
        with open(value, "r") as src:
            return src.read().strip()
    return value
