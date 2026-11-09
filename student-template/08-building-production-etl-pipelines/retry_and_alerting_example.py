"""Lab 08 - Failure handling (Workflow Orchestrator, Airflow 3.3.2).

Task-level callbacks run in the worker that ran the task, so a callback
defined in this file works on the standard image. For a real alert, the
Slack and SMTP providers are pre-installed (see the commented example).
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())


def alert_on_failure(context):
    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    exception = context.get("exception")
    print(f"ALERT: {dag_id}.{task_instance.task_id} failed. Exception: {exception}")


# Real alert instead of print (needs a Slack webhook connection named slack_alerts):
# from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
# alert_on_failure = SlackWebhookNotifier(
#     slack_webhook_conn_id="slack_alerts",
#     text="Task {{ ti.task_id }} in {{ dag.dag_id }} failed",
# )

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10),          # first retry after ~10s
    "retry_exponential_backoff": True,             # then ~20s, ~40s
    "max_retry_delay": timedelta(minutes=1),       # never wait more than 1 min
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id=f"retry_and_alerting_example_{STUDENT}",
    default_args=default_args,
    schedule=None,                                 # runs only when triggered
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "retries"],
) as dag:

    def flaky_task():
        import random

        if random.random() < 0.7:                  # 70% chance of failure
            raise Exception("Simulated transient failure")
        print("Task succeeded")

    PythonOperator(task_id="flaky_task", python_callable=flaky_task)
