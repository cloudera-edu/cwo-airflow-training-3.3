"""Lab 08 - Dynamic task generation (Workflow Orchestrator, Airflow 3.3.2)."""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
TABLES = ["customers", "orders", "products", "inventory"]


def process_table(table_name):
    print(f"Processing table: {table_name}")


with DAG(
    dag_id=f"dynamic_table_processing_{STUDENT}",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "dynamic-tasks"],
) as dag:

    start = PythonOperator(task_id="start", python_callable=lambda: print("Starting"))
    end = PythonOperator(task_id="end", python_callable=lambda: print("Done"))

    for table in TABLES:
        task = PythonOperator(
            task_id=f"process_{table}",
            python_callable=process_table,
            op_kwargs={"table_name": table},
        )
        start >> task >> end
