import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator

from ams_background_tasks.airflow.common.tasks import bash_task


def prepare_status_email(**context):
    bash_result = context["ti"].xcom_pull(task_ids="retrieve-process-status")

    res = json.loads(bash_result)

    context["ti"].xcom_push(key="email_subject", value=res["subject"])
    context["ti"].xcom_push(key="email_html_content", value=res["html_content"])

    print(context)


def send_status_email():
    return EmailOperator(
        task_id="send-status-email",
        mime_charset="utf-8",
        to=Variable.get("AMS_EMAIL_TO"),
        subject="{{ ti.xcom_pull(task_ids='prepare-status-email', key='email_subject') }}",
        html_content="{{ ti.xcom_pull(task_ids='prepare-status-email', key='email_html_content') }}",
    )


def retrieve_process_status(dag: DAG):
    command = f"ams-print-process-status --start=\"{{{{ ti.xcom_pull(task_ids='check-variables', key='start_process') }}}}\""

    return bash_task(
        dag=dag,
        task_id="retrieve-process-status",
        command=command,
        env_keys=["AMS_DB_URL"],
        trigger_rule="all_done",
    )
