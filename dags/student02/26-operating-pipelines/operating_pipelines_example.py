"""Lab 26 - Operating pipelines in Airflow 3.3 (Workflow Orchestrator, Airflow 3.3.2).

Two DAGs for practising day-to-day operations:

  rerun_version_demo_<student>
      Prints MESSAGE. In Airflow 3.3, clearing an old run re-runs it on the
      DAG version it originally ran with, not the latest code - unless the
      DAG (or the clear request) sets rerun_with_latest_version=True. You
      change MESSAGE in GitHub, clear the old run, and see which message it
      prints; then switch the setting and clear again.

  regional_load_<student>
      Six mapped tasks, one per region. About 1 in 3 fails (no retries), so
      most runs have something to clear, mark or investigate: clear failed
      tasks with a note, bulk-mark runs, search task logs, and read the Gantt
      chart.
"""
import random
import re
import time
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, task

# Your student name is taken from your folder: dags/<your_name>/<lab>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

MESSAGE = "Hello from version 1"

# ---------------- Part A: which version does a rerun use? ----------------
with DAG(
    dag_id=f"rerun_version_demo_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    rerun_with_latest_version=False,   # the 3.3 default: reruns keep their original version
    tags=["training", "operations"],
):

    @task
    def say_message():
        print(f"MESSAGE = {MESSAGE}")

    say_message()


# ---------------- Part B: clear, mark, search and Gantt ----------------
REGIONS = ["north", "south", "east", "west", "central", "islands"]

with DAG(
    dag_id=f"regional_load_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "operations"],
):

    @task(retries=0)
    def load(region: str):
        seconds = random.randint(5, 25)
        print(f"Loading region {region}, expected {seconds} s")
        time.sleep(seconds)
        if seconds > 18:
            print(f"WARNING slow partition: {region} took {seconds} s")
        if random.random() < 0.33:
            raise RuntimeError(f"Load failed for region {region}: upstream file incomplete")
        print(f"Region {region} loaded")

    @task(trigger_rule="all_done")
    def summary():
        print("Regional load finished (some regions may have failed)")

    load.expand(region=REGIONS) >> summary()
