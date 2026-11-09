"""Lab 22 - Mapping time partitions to a daily partition (Workflow Orchestrator, Airflow 3.3.2).

The producer runs every 5 minutes on CronPartitionTimetable, so each run gets
a time-slot partition key such as "2026-11-06T06:25:00". (In production this
would be hourly: "0 * * * *". Every 5 minutes keeps the lab short.)

The consumer uses StartOfDayMapper, which turns that key into the daily key
"2026-11-06", so the daily DAG's run is labelled with the day it belongs to.

StartOfDayMapper maps each upstream event on its own, so the daily DAG runs
once PER upstream event - two producer runs give two daily runs, both with the
same daily key. Lab 25 shows the alternative: RollupMapper, which waits for a
complete set of partitions and runs once.

The s3:// URI is only an identifier here; nothing is read or written.
"""
import re
from pathlib import Path
from airflow.sdk import Asset, CronPartitionTimetable, PartitionedAssetTimetable, StartOfDayMapper, dag, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "cloudera-edu-cwo-airflow-training"

hourly_sales = Asset(
    uri=f"s3://{BUCKET}/{STUDENT}/hourly-sales/",
    name=f"hourly_sales_v2_{STUDENT}",
)


@dag(
    dag_id=f"ingest_hourly_sales_{STUDENT}",
    schedule=CronPartitionTimetable("*/5 * * * *", timezone="UTC"),  # keys like 2026-11-06T06:25:00
    catchup=False,
    tags=["training", "partitions"],
)
def ingest_hourly_sales():
    @task(outlets=[hourly_sales])
    def ingest(dag_run=None):
        print(f"Ingested partition {dag_run.partition_key}")

    ingest()


ingest_hourly_sales()


@dag(
    dag_id=f"daily_sales_summary_{STUDENT}",
    schedule=PartitionedAssetTimetable(
        assets=hourly_sales,
        default_partition_mapper=StartOfDayMapper(),  # 2026-11-06T06:25:00 -> 2026-11-06
    ),
    catchup=False,
    tags=["training", "partitions"],
)
def daily_sales_summary():
    @task
    def summarize(dag_run=None):
        print(f"Daily summary for partition: {dag_run.partition_key}")

    summarize()


daily_sales_summary()
