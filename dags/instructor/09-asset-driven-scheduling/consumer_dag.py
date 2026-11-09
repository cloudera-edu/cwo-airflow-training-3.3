"""Lab 09 - Asset-driven scheduling, consumer (Workflow Orchestrator, Airflow 3.3.2).

Scheduled on the Asset instead of a cron - runs as soon as the producer's
write_to_object_store task succeeds. The URI must match the producer's exactly.
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
BUCKET = "cloudera-edu-cwo-airflow-training"
S3_CONN_ID = "object_store"
KEY = f"{STUDENT}/assets/my_data.csv"

my_data = Asset(uri=f"s3://{BUCKET}/{KEY}", name=f"my_data_{STUDENT}")


def read_csv():
    content = S3Hook(aws_conn_id=S3_CONN_ID).read_key(key=KEY, bucket_name=BUCKET)
    print(content)


with DAG(
    dag_id=f"consumer_dag_{STUDENT}",
    schedule=[my_data],   # asset-driven schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "assets"],
) as dag:

    PythonOperator(task_id="read_from_object_store", python_callable=read_csv)
