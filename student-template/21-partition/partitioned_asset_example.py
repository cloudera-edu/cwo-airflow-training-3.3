"""Lab 21 - Partitioned assets (Workflow Orchestrator, Airflow 3.3.2).

The producer runs every 5 minutes on CronPartitionTimetable. Each scheduled
run gets a partition key (its time slot), and the asset event it emits
carries that key. The consumer runs once per partition and can see which
partition triggered it.

In production the producer would typically run hourly or daily ("0 * * * *");
every 5 minutes keeps the lab short.

The s3:// URI is only an identifier here; nothing is read or written.
"""
import re
from pathlib import Path
from airflow.sdk import Asset, CronPartitionTimetable, IdentityMapper, PartitionedAssetTimetable, dag, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "cloudera-edu-cwo-airflow-training"

player_stats = Asset(
    uri=f"s3://{BUCKET}/{STUDENT}/player-stats/",
    name=f"player_stats_v2_{STUDENT}",
)


# --- Producer: one partitioned event per 5-minute slot -------------------
@dag(
    dag_id=f"player_stats_etl_{STUDENT}",
    schedule=CronPartitionTimetable("*/5 * * * *", timezone="UTC"),  # every 5 minutes
    catchup=False,
    tags=["training", "partitions"],
)
def player_stats_etl():
    @task(outlets=[player_stats])
    def ingest(*, outlet_events, dag_run=None):
        print(f"Materialising player stats for partition {dag_run.partition_key}")
        outlet_events[player_stats].extra = {"rows_written": 1000}

    ingest()


player_stats_etl()


# --- Consumer: one run per partition, and it knows which one --------------
@dag(
    dag_id=f"player_stats_report_{STUDENT}",
    schedule=PartitionedAssetTimetable(
        assets=player_stats,
        default_partition_mapper=IdentityMapper(),  # pass the upstream key through unchanged
    ),
    catchup=False,
    tags=["training", "partitions"],
)
def player_stats_report():
    @task
    def build_report(dag_run=None):
        print(f"Building report for partition: {dag_run.partition_key}")

    build_report()


player_stats_report()
