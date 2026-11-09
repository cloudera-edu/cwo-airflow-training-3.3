"""Lab 23 - State that survives retries and runs (Workflow Orchestrator, Airflow 3.3.2).

Two DAGs, both using the state store added in Airflow 3.3 (AIP-103):

  resumable_batches_<student>   Part A - task state store.
      process_batches works through 5 batches and records the last finished
      batch in task_state_store after each one. Its first try fails during
      batch 3. The retry reads the saved value and resumes from batch 3
      instead of starting again. Task state belongs to one task instance
      (one task in one run) and survives its retries.

  incremental_orders_<student>  Part B - asset state store.
      load_new_orders keeps a "high-water mark" (the last order ID it loaded)
      on its asset. Each run loads only orders after that mark, then moves the
      mark forward. Asset state belongs to the asset, so it survives across
      runs.

Nothing is read from or written to Object Store; the "source" is simulated.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task

# Your student name is taken from your folder: dags/<your_name>/<lab>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

TOTAL_BATCHES = 5
FAIL_AT_BATCH = 3

# ---------------- Part A: task state store ----------------
with DAG(
    dag_id=f"resumable_batches_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "state-store"],
):

    @task(retries=1, retry_delay=timedelta(seconds=10))
    def process_batches(ti=None, task_state_store=None):
        done = task_state_store.get("last_batch_done", 0)
        if done:
            print(f"Try {ti.try_number}: resuming after batch {done} (read from task_state_store)")
        else:
            print(f"Try {ti.try_number}: no saved progress, starting from batch 1")

        for batch in range(done + 1, TOTAL_BATCHES + 1):
            if batch == FAIL_AT_BATCH and ti.try_number == 1:
                raise RuntimeError(f"Simulated crash while processing batch {batch}")
            print(f"Processed batch {batch} of {TOTAL_BATCHES}")
            task_state_store.set("last_batch_done", batch)

        print(f"All {TOTAL_BATCHES} batches done")

    process_batches()


# ---------------- Part B: asset state store ----------------
orders = Asset(name=f"orders_{STUDENT}", uri=f"s3://cloudera-edu-cwo-airflow-training/{STUDENT}/orders/")

with DAG(
    dag_id=f"incremental_orders_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "state-store"],
):

    @task(outlets=[orders])
    def load_new_orders(asset_state_store=None):
        # The task has one outlet, so asset_state_store reads and writes that asset's state.
        watermark = asset_state_store.get("last_order_id", 0)
        new_ids = list(range(watermark + 1, watermark + 6))   # simulated: 5 new orders since last run
        print(f"High-water mark before this run: {watermark}")
        print(f"Loading orders {new_ids[0]} to {new_ids[-1]}")
        asset_state_store.set("last_order_id", new_ids[-1])
        print(f"High-water mark after this run: {new_ids[-1]}")

    load_new_orders()
