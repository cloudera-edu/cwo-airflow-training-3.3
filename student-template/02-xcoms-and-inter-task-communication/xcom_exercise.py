"""Lab 02 - Passing data between tasks with XComs (Workflow Orchestrator, Airflow 3.3.2)."""
import re
from pathlib import Path
import json
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"xcom_exercise_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "xcom"],
) as dag:

    def generate_config(**kwargs):
        config = {
            "input_path": "/data/raw/sales_2024.csv",
            "output_path": "/data/processed/sales_clean.parquet",
            "filters": {"region": "APAC", "min_amount": 1000},
            "run_timestamp": str(datetime.now()),
        }
        print(f"Generated config: {json.dumps(config, indent=2)}")
        return config

    def validate_config(**kwargs):
        ti = kwargs["ti"]
        config = ti.xcom_pull(task_ids="generate_config")
        assert config["input_path"], "Input path is required"
        assert config["output_path"], "Output path is required"
        print(f'Config validated for region: {config["filters"]["region"]}')
        return True

    def process_data(**kwargs):
        ti = kwargs["ti"]
        config = ti.xcom_pull(task_ids="generate_config")
        is_valid = ti.xcom_pull(task_ids="validate_config")
        if is_valid:
            print(f'Processing: {config["input_path"]} -> {config["output_path"]}')
            print(f'Filters: {config["filters"]}')
            # Simulate processing results
            return {"records_processed": 5420, "records_filtered": 3180}

    def generate_report(**kwargs):
        ti = kwargs["ti"]
        config = ti.xcom_pull(task_ids="generate_config")
        results = ti.xcom_pull(task_ids="process_data")
        print("=== Pipeline Report ===")
        print(f'Input: {config["input_path"]}')
        print(f'Output: {config["output_path"]}')
        print(f'Records processed: {results["records_processed"]}')
        print(f'Records after filter: {results["records_filtered"]}')

    t1 = PythonOperator(task_id="generate_config", python_callable=generate_config)
    t2 = PythonOperator(task_id="validate_config", python_callable=validate_config)
    t3 = PythonOperator(task_id="process_data", python_callable=process_data)
    t4 = PythonOperator(task_id="generate_report", python_callable=generate_report)

    t1 >> t2 >> t3 >> t4
