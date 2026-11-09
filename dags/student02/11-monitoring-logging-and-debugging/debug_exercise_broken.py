"""Lab 11 - Debugging a broken DAG, part 1 (Workflow Orchestrator, Airflow 3.3.2).

Deploy, trigger, and use the task logs to find the bugs. Task logs are
written to remote storage, so they stay available after the worker pod is gone.
"""
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
        data = {"status": "ready", "count": 100}
        return data

    def step_two(**kwargs):
        ti = kwargs["ti"]
        # BUG 1: Wrong task_id in xcom_pull
        data = ti.xcom_pull(task_ids="step_1")
        print(f'Count: {data["count"]}')
        return data["count"] > 50

    def step_three(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="task_two")
        if result:
            # BUG 2: Division by zero
            ratio = result / (result - True)
            print(f"Ratio: {ratio}")

    t1 = PythonOperator(task_id="task_one", python_callable=step_one)
    t2 = PythonOperator(task_id="task_two", python_callable=step_two)
    t3 = PythonOperator(task_id="task_three", python_callable=step_three)

    # BUG 3: Circular dependency
    t1 >> t2 >> t3
    # Uncomment below to introduce the circular dependency (the DAG processor
    # will report it as a DAG import error instead of running the DAG):
    # t3 >> t1
