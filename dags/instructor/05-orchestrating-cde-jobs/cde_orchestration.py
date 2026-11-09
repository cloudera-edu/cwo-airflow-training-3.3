"""Lab 05 - Orchestrating CDE jobs from Workflow Orchestrator (Airflow 3.3.2).

Before running:
  1. In CDE on Anywhere Cloud, create the resource and the Spark job
     `word_count_job` (script: spark/05-orchestrating-cde-jobs/word_count.py).
  2. In the Workflow Orchestrator UI, add the connection `cde_runtime_api`
     (Admin > Connections, type "Cloudera Data Engineering"): Host = the
     virtual cluster Jobs API URL, Schema = default, Access Key / Private Key =
     your Anywhere Cloud OAuth client ID / secret, Extra =
     {"awc_console_url": "<console URL>", "auth_mode": "awc", "cache_dir": "/tmp"}

Changes from the CDE version: new provider import path
(cloudera.airflow.providers.operators.cde) and an explicit connection_id,
because Workflow Orchestrator runs outside the CDE virtual cluster.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
CDE_CONN_ID = "cde_runtime_api"

with DAG(
    dag_id=f"cde_orchestration_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),   # longer than the 1-hour platform default, for Spark runs
    tags=["training", "cde"],
) as dag:

    def pre_check():
        print("Pre-flight checks passed")

    def post_check():
        print("Post-run validation complete")

    pre = PythonOperator(task_id="pre_flight_check", python_callable=pre_check)

    spark_job = CdeRunJobOperator(
        task_id="run_word_count",
        job_name="word_count_job",
        connection_id=CDE_CONN_ID,
        retries=3,
    )

    post = PythonOperator(task_id="post_validation", python_callable=post_check)

    pre >> spark_job >> post
