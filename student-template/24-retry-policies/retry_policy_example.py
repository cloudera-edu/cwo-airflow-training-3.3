"""Lab 24 - Smarter retries with a retry policy (Workflow Orchestrator, Airflow 3.3.2).

Lab 08 used a fixed number of retries: every failure was retried the same
way. Airflow 3.3 (AIP-105) adds retry policies, which look at the actual
exception and decide whether a retry can help:

  TimeoutError (a transient problem, e.g. a busy API)  -> retry after 10 s
  ValueError   (bad input; retrying won't fix it)      -> fail at once
  anything else                                         -> normal retry rules

Trigger the DAG with failure_type = "transient" or "permanent" and compare
the Task Tries. The policy can fail a task early, but it never retries more
than `retries` allows.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG, ExceptionRetryPolicy, Param, RetryAction, RetryRule, task

# Your student name is taken from your folder: dags/<your_name>/<lab>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

retry_policy = ExceptionRetryPolicy(
    rules=[
        RetryRule(
            exception=TimeoutError,
            action=RetryAction.RETRY,
            retry_delay=timedelta(seconds=10),
            reason="Transient error: the source may recover",
        ),
        RetryRule(
            exception=ValueError,
            action=RetryAction.FAIL,
            reason="Bad input: retrying won't help",
        ),
    ],
)

with DAG(
    dag_id=f"retry_policy_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "failure_type": Param("transient", type="string", enum=["transient", "permanent"],
                              description="transient = TimeoutError on the first 2 tries; permanent = ValueError"),
    },
    tags=["training", "retries"],
):

    @task(retries=3, retry_delay=timedelta(minutes=5), retry_policy=retry_policy)
    def call_source(params=None, ti=None):
        kind = params["failure_type"]
        print(f"Try {ti.try_number}, failure_type={kind}")
        if kind == "permanent":
            raise ValueError("Record 42 has no customer_id")
        if ti.try_number <= 2:
            raise TimeoutError("Source API did not answer within 30 seconds")
        print("Source answered; data loaded")

    call_source()
