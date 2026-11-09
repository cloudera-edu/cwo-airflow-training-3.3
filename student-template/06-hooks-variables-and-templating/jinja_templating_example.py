"""Lab 06 - Jinja templating (Workflow Orchestrator, Airflow 3.3.2).

Airflow 3 removed these template variables: execution_date, next_ds, prev_ds,
next_execution_date, prev_execution_date, tomorrow_ds, yesterday_ds.
Use logical_date and data_interval_start / data_interval_end instead.
ds and ds_nodash still work (they are derived from logical_date).
"""
import re
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"jinja_templating_example_{STUDENT}",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "jinja"],
) as dag:

    # 1. Basic variable substitution
    print_date = BashOperator(
        task_id="print_date",
        bash_command='echo "Run date: {{ ds }}"',
    )

    # 2. Formatting a date (was execution_date in Airflow 2)
    print_formatted = BashOperator(
        task_id="print_formatted",
        bash_command="echo \"Pretty date: {{ logical_date.strftime('%B %d, %Y') }}\"",
    )

    # 3. Date arithmetic with macros.timedelta
    print_next_week = BashOperator(
        task_id="print_next_week",
        bash_command="echo \"One week later: {{ (logical_date + macros.timedelta(days=7)).strftime('%Y-%m-%d') }}\"",
    )

    # 4. The data interval replaces prev_ds / next_ds
    print_interval = BashOperator(
        task_id="print_interval",
        bash_command='echo "This run covers {{ data_interval_start }} to {{ data_interval_end }}"',
    )

    # 5. Conditional logic inside a template
    conditional_message = BashOperator(
        task_id="conditional_message",
        bash_command=(
            "{% if logical_date.weekday() >= 5 %}"
            'echo "Weekend run - skipping heavy processing"'
            "{% else %}"
            'echo "Weekday run - full processing"'
            "{% endif %}"
        ),
    )

    # 6. Looping inside a template - build a multi-file command from a params list
    process_files = BashOperator(
        task_id="process_files",
        bash_command=(
            "{% for f in params.files %}"
            'echo "Processing {{ f }} for {{ ds }}"; '
            "{% endfor %}"
        ),
        params={"files": ["sales.csv", "inventory.csv", "returns.csv"]},
    )

    # 7. Templating inside a PythonOperator via op_kwargs
    def report_summary(run_date, environment):
        print(f"Generating report for {run_date} in {environment}")

    python_templated = PythonOperator(
        task_id="python_templated",
        python_callable=report_summary,
        op_kwargs={"run_date": "{{ ds }}", "environment": "{{ params.env }}"},
        params={"env": "production"},
    )

    # 8. New on Workflow Orchestrator: files shipped in the Git repo (the DAG bundle)
    #    sit next to this DAG on the worker, so a script or SQL file can be read
    #    relative to the DAG file. (The Workflow Orchestrator docs also use the
    #    template {{ task_instance.bundle_instance.path }} for the bundle root.)
    DAG_DIR = Path(__file__).parent

    list_bundle_files = BashOperator(
        task_id="list_bundle_files",
        bash_command=f'ls -la "{DAG_DIR}"',
    )

    (
        print_date >> print_formatted >> print_next_week >> print_interval
        >> conditional_message >> process_files >> python_templated >> list_bundle_files
    )
