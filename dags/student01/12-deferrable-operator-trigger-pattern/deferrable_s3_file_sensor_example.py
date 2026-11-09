"""Lab 12 - Waiting for a file without holding a worker (Workflow Orchestrator, Airflow 3.3.2).

Two DAGs in this file:

  deferrable_s3_file_sensor_example_<student>  - the waiter. It first removes any old copy
      of the file, then three tasks wait for it:
        wait_custom     - your own deferrable operator (defined here) that defers to the
                          built-in S3KeyTrigger
        wait_builtin    - the ready-made S3KeySensor(deferrable=True)
        wait_reschedule - for comparison: a reschedule-mode sensor. It gives its worker
                          slot back between checks too, but starts a worker pod every
                          poke_interval instead of waiting in the triggerer.

  drop_daily_export_<student>  - plays the "external system": it writes the file the
      waiter is looking for, through the same object_store connection.

Trigger the waiter first, watch the tasks wait, then trigger drop_daily_export_<student>.

Note: the deferrable S3 tasks need aiobotocore in the image (an optional extra of the
Amazon provider). If they fail with an aiobotocore import error, the image doesn't
include it - wait_reschedule still works.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, BaseOperator, Context
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.triggers.s3 import S3KeyTrigger
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "cloudera-edu-cwo-airflow-training"
S3_CONN_ID = "object_store"
KEY = f"{STUDENT}/incoming/daily_export.csv"


class WaitForS3KeyOperator(BaseOperator):
    """Defers to S3KeyTrigger instead of polling on a worker."""

    def __init__(self, bucket: str, key: str, aws_conn_id: str, timeout_s: int = 3600, **kwargs):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.key = key
        self.aws_conn_id = aws_conn_id
        self.timeout_s = timeout_s

    def execute(self, context: Context):
        self.defer(
            trigger=S3KeyTrigger(
                bucket_name=self.bucket,
                bucket_key=self.key,
                aws_conn_id=self.aws_conn_id,
                poke_interval=10,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout_s),
        )

    def execute_complete(self, context: Context, event: dict[str, Any]):
        if event.get("status") == "success":
            self.log.info("File arrived: s3://%s/%s", self.bucket, self.key)
            return self.key
        raise AirflowException(f"File wait failed: {event}")


def remove_old_file():
    """Start every run clean, so the sensors really have to wait."""
    S3Hook(aws_conn_id=S3_CONN_ID).delete_objects(bucket=BUCKET, keys=[KEY])
    print(f"Removed s3://{BUCKET}/{KEY} (if it existed). Now trigger drop_daily_export_{STUDENT}.")


def write_file():
    body = "id,amount\n1,250\n2,410\n3,95\n"
    S3Hook(aws_conn_id=S3_CONN_ID).load_string(body, key=KEY, bucket_name=BUCKET, replace=True)
    print(f"Wrote s3://{BUCKET}/{KEY}")


with DAG(
    dag_id=f"deferrable_s3_file_sensor_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["training", "deferrable"],
):

    reset = PythonOperator(task_id="remove_old_file", python_callable=remove_old_file)

    wait_custom = WaitForS3KeyOperator(
        task_id="wait_custom",
        bucket=BUCKET,
        key=KEY,
        aws_conn_id=S3_CONN_ID,
        timeout_s=3600,
    )

    wait_builtin = S3KeySensor(
        task_id="wait_builtin",
        bucket_name=BUCKET,
        bucket_key=KEY,
        aws_conn_id=S3_CONN_ID,
        deferrable=True,
        timeout=3600,
    )

    wait_reschedule = S3KeySensor(
        task_id="wait_reschedule",
        bucket_name=BUCKET,
        bucket_key=KEY,
        aws_conn_id=S3_CONN_ID,
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    reset >> [wait_custom, wait_builtin, wait_reschedule]


with DAG(
    dag_id=f"drop_daily_export_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "deferrable"],
):
    PythonOperator(task_id="write_daily_export", python_callable=write_file)
