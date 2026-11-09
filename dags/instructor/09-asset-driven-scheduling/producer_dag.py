"""Lab 09 - Asset-driven scheduling, producer (Workflow Orchestrator, Airflow 3.3.2).

Writes a CSV to the Anywhere Cloud Object Store and, on success, marks the
Asset (identified by its s3:// URI) as updated. consumer_dag.py runs as soon
as that happens.

Changes from the CDE / Airflow 2 version:
  - Dataset -> Asset (airflow.sdk)
  - HDFS and the `hdfs` CLI (not in the Workflow Orchestrator image) ->
    Object Store through the pre-installed Amazon provider
  - Your student name is part of the URI, so your producer only triggers
    your consumer on the shared instance

Needs the Airflow connection `object_store` (type Amazon Web Services) with
the Object Store access key / secret and Extra:
{"endpoint_url": "https://<object-store-endpoint>", "verify": false}
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, Asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "cloudera-edu-cwo-airflow-training"  # TODO: bucket provided by your instructor
S3_CONN_ID = "object_store"
KEY = f"{STUDENT}/assets/my_data.csv"

my_data = Asset(uri=f"s3://{BUCKET}/{KEY}", name=f"my_data_{STUDENT}")


def write_csv():
    S3Hook(aws_conn_id=S3_CONN_ID).load_string(
        string_data="id,value\n1,42\n2,43\n",
        key=KEY,
        bucket_name=BUCKET,
        replace=True,
    )
    print(f"Wrote s3://{BUCKET}/{KEY}")


with DAG(
    dag_id=f"producer_dag_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "assets"],
) as dag:

    PythonOperator(
        task_id="write_to_object_store",
        python_callable=write_csv,
        outlets=[my_data],   # marks the asset updated when this task succeeds
    )
