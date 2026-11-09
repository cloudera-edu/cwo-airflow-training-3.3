"""Lab 14 - Asset event extras (Workflow Orchestrator, Airflow 3.3.2).

A producer attaches metadata (row_count) to the asset event it emits.
The consumer is scheduled on the asset and reads that metadata back
through inlet_events. Runs on the standard image; no Object Store needed.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, Asset, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

sales_asset = Asset(f"sales_data_{STUDENT}")

with DAG(
    dag_id=f"producer_with_extras_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "assets", "extras"],
):

    @task(outlets=[sales_asset])
    def produce_sales_data(*, outlet_events):
        row_count = 128  # stand-in for however many rows you actually wrote
        outlet_events[sales_asset].extra = {"row_count": row_count}

    produce_sales_data()

with DAG(
    dag_id=f"consumer_reads_extras_{STUDENT}",
    schedule=[sales_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "assets", "extras"],
):

    @task(inlets=[sales_asset])
    def read_sales_extras(*, inlet_events):
        last_event = inlet_events[sales_asset][-1]
        print(f"Last row_count from producer: {last_event.extra['row_count']}")

    read_sales_extras()
