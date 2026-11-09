"""Lab 11 - Debugging a broken DAG, part 1 solution (Workflow Orchestrator, Airflow 3.3.2)."""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"debug_exercise_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "debug"],
) as dag:

    def step_one():
        return {"status": "ready", "count": 100}

    def step_two(**kwargs):
        ti = kwargs["ti"]
        # FIXED: use the actual task_id ('task_one'), not the function name
        data = ti.xcom_pull(task_ids="task_one")
        print(f'Count: {data["count"]}')
        # push the count explicitly so step_three can use the real number
        ti.xcom_push(key="count", value=data["count"])
        return data["count"] > 50

    def step_three(**kwargs):
        ti = kwargs["ti"]
        passed = ti.xcom_pull(task_ids="task_two")               # boolean
        count = ti.xcom_pull(task_ids="task_two", key="count")   # actual number
        # FIXED: no arithmetic on the boolean, and guard the zero denominator
        if passed and count and count != 1:
            print(f"Ratio: {count / (count - 1)}")
        else:
            print("Skipping ratio calculation - no valid count or count == 1")

    t1 = PythonOperator(task_id="task_one", python_callable=step_one)
    t2 = PythonOperator(task_id="task_two", python_callable=step_two)
    t3 = PythonOperator(task_id="task_three", python_callable=step_three)

    # FIXED: linear dependency, no cycle
    t1 >> t2 >> t3
