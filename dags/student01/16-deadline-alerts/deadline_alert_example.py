"""Lab 16 - Time limits on runs and tasks (Workflow Orchestrator, Airflow 3.3.2).

Two DAGs, each with a limit that is deliberately too short:

  task_timeout_example_<student>  - execution_timeout on one task: Airflow stops
                                    the task after 1 minute and marks it failed.
  run_timeout_example_<student>   - dagrun_timeout on the whole run: Airflow marks
                                    the run failed after 2 minutes, even though a
                                    task is still working.

Deadline Alerts (the Airflow 3 replacement for SLAs) only *alert* and need a
notifier such as Slack or email; they are covered in the lecture.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"task_timeout_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "timeouts"],
):
    quick_task = BashOperator(task_id="quick_task", bash_command="sleep 10")

    # Needs 3 minutes but is only allowed 1: stopped and marked failed.
    slow_task = BashOperator(
        task_id="slow_task",
        bash_command="sleep 180",
        execution_timeout=timedelta(minutes=1),
        retries=0,
    )

    quick_task >> slow_task


with DAG(
    dag_id=f"run_timeout_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),   # the whole run gets 2 minutes
    tags=["training", "timeouts"],
):
    # Needs 5 minutes, so the run times out while this task is still running.
    BashOperator(task_id="long_running_task", bash_command="sleep 300")
