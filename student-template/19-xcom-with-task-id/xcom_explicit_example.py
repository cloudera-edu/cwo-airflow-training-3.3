"""Lab 19 - XCom with explicit task_ids (Workflow Orchestrator, Airflow 3.3.2)."""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"xcom_explicit_example_{STUDENT}",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["training", "xcom"],
):

    @task
    def push_a(ti=None):
        ti.xcom_push(key="result", value="from task A")

    @task
    def push_b(ti=None):
        ti.xcom_push(key="result", value="from task B")

    @task
    def pull_explicit(ti=None):
        # task_ids is required to pull from anything other than the calling task
        value_a = ti.xcom_pull(task_ids="push_a", key="result")
        value_b = ti.xcom_pull(task_ids="push_b", key="result")
        print(f"from A: {value_a}")
        print(f"from B: {value_b}")
        # pulling from several task_ids returns a list, in order
        both = ti.xcom_pull(task_ids=["push_a", "push_b"], key="result")
        print(f"both: {both}")

    [push_a(), push_b()] >> pull_explicit()
