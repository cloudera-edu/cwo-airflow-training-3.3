"""Lab 12 - Deferrable operator + trigger, minimal (Workflow Orchestrator, Airflow 3.3.2).

Workflow Orchestrator runs a triggerer, so the task defers, frees its worker
slot, and resumes on its own - no manual kill or separate local cluster as
in the CDE version.

The operator is defined in this file (it runs on the worker, which loads the
DAG file). The trigger is the built-in TimeDeltaTrigger, which the triggerer
can import. A trigger class defined in a DAG file would NOT load in the
triggerer - see the README.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG, BaseOperator, Context
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())


class SelfContainedWaitOperator(BaseOperator):
    """Waits, then resumes. Demonstrates defer/resume with no external system."""

    def __init__(self, wait_seconds: int = 15, **kwargs):
        super().__init__(**kwargs)
        self.wait_seconds = wait_seconds

    def execute(self, context: Context):
        self.log.info("Deferring for %s seconds - worker slot released now", self.wait_seconds)
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=self.wait_seconds)),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event=None):
        self.log.info("Resumed! No worker slot was held during the wait.")
        return "done"


with DAG(
    dag_id=f"deferrable_local_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "deferrable"],
) as dag:

    SelfContainedWaitOperator(task_id="wait_task", wait_seconds=15)
