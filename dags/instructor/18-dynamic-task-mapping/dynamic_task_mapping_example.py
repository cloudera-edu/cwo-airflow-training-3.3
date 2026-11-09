"""Lab 18 - Dynamic task mapping (Workflow Orchestrator, Airflow 3.3.2).

Each mapped task instance is its own worker pod on Workflow Orchestrator,
so large fan-outs consume pool slots - see the pools lab.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"dynamic_task_mapping_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "dynamic-task-mapping"],
) as dag:

    # 1. Simple expand() over a static list
    @task
    def add_one(x: int) -> int:
        return x + 1

    @task
    def sum_it(values: list[int]) -> None:
        print(f"Total was {sum(values)}")

    sum_it(add_one.expand(x=[1, 2, 3]))

    # 2. Mapping over a list produced at run time
    @task
    def list_files() -> list[str]:
        # Stand-in for listing files that landed in an Object Store prefix
        return ["file_a.csv", "file_b.csv", "file_c.csv"]

    @task
    def process_file(filename: str) -> str:
        print(f"Processing {filename}")
        return f"{filename}: done"

    process_file.expand(filename=list_files())

    # 3. partial() + expand(): fixed and mapped arguments together
    @task
    def scale(value: int, factor: int) -> int:
        return value * factor

    scale.partial(factor=10).expand(value=[1, 2, 3])

    # 4. Mapping over several parameters gives the cross product (6 instances)
    @task
    def combine(x: int, y: int) -> int:
        return x + y

    combine.expand(x=[2, 4, 8], y=[5, 10])
