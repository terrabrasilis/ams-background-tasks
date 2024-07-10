from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 1, 1),
    'owner': 'airflow',
    'retries': 0,
    'catchup': False,
}

dag = DAG(
    'sample_dag',
    default_args=default_args,
    schedule_interval=None
)

DAG_KEY = 'sample-tool'

cmd = BashOperator(
    task_id=DAG_KEY,
    bash_command=DAG_KEY,
    dag=dag
)

cmd