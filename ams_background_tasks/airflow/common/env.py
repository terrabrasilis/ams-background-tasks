from pathlib import Path

from airflow.operators.bash import BashOperator  # type: ignore

PROJECT_DIR = Path(__file__).resolve().parents[3]
print(f"project_dir: {PROJECT_DIR}")

VENV_DIR = Path("/tmp/venvs")
VENV_DIR.mkdir(exist_ok=True, parents=False)
VENV_PATH = VENV_DIR / PROJECT_DIR.name
VENV_CMD = f"source {VENV_PATH}/bin/activate && "


LAND_USE_DIR = PROJECT_DIR / "land_use"
RISK_DIR = PROJECT_DIR / "risk"
FIRE_SR_DIR = PROJECT_DIR / "fire_spreading_risk"


def update_environment(dag):
    bash_command = f"python3 -m venv {VENV_PATH} && " if not VENV_PATH.exists() else ""
    bash_command += f"source {VENV_PATH}/bin/activate && pip install {PROJECT_DIR}"

    return BashOperator(
        task_id="update-environment", bash_command=bash_command, dag=dag
    )
