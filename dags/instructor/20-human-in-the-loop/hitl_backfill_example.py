"""Lab 20 - Human-in-the-loop (Workflow Orchestrator, Airflow 3.3.2).

The run pauses at backfill_parameters (deferred, no worker slot held) until
someone fills in the form under Required Actions in the UI, or through the
HITL REST API. dagrun_timeout is set so the platform's 1-hour default
doesn't end the run while it waits. The responder needs a role that can act
on the DAG.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, task
from airflow.providers.standard.operators.hitl import HITLEntryOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"hitl_backfill_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=8),
    tags=["training", "hitl"],
) as dag:

    parameters = HITLEntryOperator(
        task_id="backfill_parameters",
        subject="Enter backfill parameters",
        body="Specify the date range and target environment.",
        params={
            "start_date": Param("2026-08-01", type="string", format="date"),
            "end_date": Param("2026-08-07", type="string", format="date"),
            "environment": Param("staging", type="string", enum=["staging", "production"]),
        },
    )

    @task
    def run_backfill_job(hitl_output):
        p = hitl_output["params_input"]
        print(f"backfilling {p['start_date']} to {p['end_date']} in {p['environment']}")

    run_backfill = run_backfill_job(parameters.output)
    parameters >> run_backfill
