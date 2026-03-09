from typing import Optional

from airflow.operators.bash import BashOperator  # type: ignore

from ams_background_tasks.airflow.common.env import VENV_CMD
from ams_background_tasks.airflow.common.secrets import get_conn_secrets_uri


def bash_task(  # pylint: disable=dangerous-default-value
    *,
    dag,
    task_id: str,
    command: str,
    env_keys: Optional[list] = None,
    trigger_rule="all_success",
    env_dict: dict = {},
):
    bash_command = f"{VENV_CMD} {command}"

    env_keys = env_keys or []

    env = get_conn_secrets_uri(env_keys)

    for key, value in env_dict.items():
        print(key, value)
        env[key] = value

    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        env=env,
        append_env=True,
        trigger_rule=trigger_rule,
        dag=dag,
    )
