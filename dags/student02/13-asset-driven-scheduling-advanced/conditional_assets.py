"""Lab 13 - Asset-driven scheduling, advanced (Workflow Orchestrator, Airflow 3.3.2).

Lab 09 covered one producer and one consumer. This lab adds:
  1. Conditional asset expressions: run when orders AND customers have
     both updated, or when EITHER has.
  2. AssetAlias: the producer decides the exact asset (a dated path) at
     run time, and consumers schedule on the alias.

Changes from the earlier version: HDFS and the `hdfs` CLI -> Object Store
through the Amazon provider; student name in every URI and DAG ID.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, Asset, AssetAlias, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "cloudera-edu-cwo-airflow-training"  # TODO: bucket provided by your instructor
S3_CONN_ID = "object_store"
BASE = f"s3://{BUCKET}/{STUDENT}/assets"

orders = Asset(uri=f"{BASE}/orders.csv", name=f"orders_{STUDENT}")
customers = Asset(uri=f"{BASE}/customers.csv", name=f"customers_{STUDENT}")
daily_export_alias = AssetAlias(f"daily_export_{STUDENT}")


def _write(key: str, body: str) -> None:
    S3Hook(aws_conn_id=S3_CONN_ID).load_string(body, key=key, bucket_name=BUCKET, replace=True)


# --- Producers -------------------------------------------------------------

with DAG(dag_id=f"produce_orders_{STUDENT}", schedule=None, start_date=datetime(2024, 1, 1),
         catchup=False, tags=["training", "assets"]):

    @task(outlets=[orders])
    def write_orders():
        _write(f"{STUDENT}/assets/orders.csv", "order_id,amount\n1,120\n2,75\n")

    write_orders()

with DAG(dag_id=f"produce_customers_{STUDENT}", schedule=None, start_date=datetime(2024, 1, 1),
         catchup=False, tags=["training", "assets"]):

    @task(outlets=[customers])
    def write_customers():
        _write(f"{STUDENT}/assets/customers.csv", "customer_id,name\n1,Asha\n2,Ravi\n")

    write_customers()

# --- Conditional consumers -----------------------------------------------

with DAG(dag_id=f"needs_both_{STUDENT}", schedule=(orders & customers), start_date=datetime(2024, 1, 1),
         catchup=False, tags=["training", "assets"]):

    @task
    def join_orders_and_customers():
        print("Both orders and customers have updated - running the join")

    join_orders_and_customers()

with DAG(dag_id=f"needs_either_{STUDENT}", schedule=(orders | customers), start_date=datetime(2024, 1, 1),
         catchup=False, tags=["training", "assets"]):

    @task
    def refresh_dashboard():
        print("Orders or customers changed - refreshing the dashboard")

    refresh_dashboard()

# --- AssetAlias: the asset is decided at run time ---------------------------

with DAG(dag_id=f"daily_export_{STUDENT}", schedule=None, start_date=datetime(2024, 1, 1),
         catchup=False, tags=["training", "assets", "alias"]):

    @task(outlets=[daily_export_alias])
    def export(*, outlet_events, ds=None):
        key = f"{STUDENT}/exports/{ds}/export.csv"
        _write(key, "id,value\n1,42\n")
        # Attach the real, dated asset to the alias for this run
        outlet_events[daily_export_alias].add(Asset(f"s3://{BUCKET}/{key}"))

    export()

with DAG(dag_id=f"on_daily_export_{STUDENT}", schedule=[daily_export_alias], start_date=datetime(2024, 1, 1),
         catchup=False, tags=["training", "assets", "alias"]):

    @task
    def process_export(*, triggering_asset_events=None):
        for asset, events in (triggering_asset_events or {}).items():
            print(f"Triggered by {asset}: {len(events)} event(s)")

    process_export()
