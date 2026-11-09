"""Lab 25 - Partition rollups (Workflow Orchestrator, Airflow 3.3.2).

In Lab 22 the daily DAG ran once per upstream event. Airflow 3.3 adds
RollupMapper, which collects upstream partitions and runs the downstream DAG
once for the whole set.

  regional_sales_<student>        producer. PartitionedAtRuntime: no schedule;
                                  each run chooses its partition key at run
                                  time - the region picked in the trigger form.
  all_regions_report_<student>    RollupMapper + WaitForAll: runs once, after
                                  north, south AND east have all arrived.
  early_regions_report_<student>  RollupMapper + MinimumCount(2): runs once,
                                  as soon as any 2 of the 3 regions are in.

FixedKeyMapper maps every region onto one downstream key ("all_regions"), and
SegmentWindow lists the regions that make up a complete set.

The s3:// URI is only an identifier here; nothing is read or written.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import (
    DAG, Asset, FixedKeyMapper, MinimumCount, Param, PartitionedAssetTimetable,
    PartitionedAtRuntime, RollupMapper, SegmentWindow, WaitForAll, task,
)

# Your student name is taken from your folder: dags/<your_name>/<lab>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
REGIONS = ["north", "south", "east"]

regional_sales = Asset(
    name=f"regional_sales_{STUDENT}",
    uri=f"s3://cloudera-edu-cwo-airflow-training/{STUDENT}/regional-sales/",
)

# --- Producer: the region is chosen when you trigger it -------------------
with DAG(
    dag_id=f"regional_sales_{STUDENT}",
    schedule=PartitionedAtRuntime(),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={"region": Param("north", type="string", enum=REGIONS)},
    tags=["training", "partitions"],
):

    @task(outlets=[regional_sales])
    def load_region(params=None, outlet_events=None):
        region = params["region"]
        outlet_events[regional_sales].add_partitions(region)
        print(f"Loaded sales for region {region}; asset event partition key = {region}")

    load_region()


def rollup(wait_policy):
    return RollupMapper(
        upstream_mapper=FixedKeyMapper("all_regions"),
        window=SegmentWindow(REGIONS),
        wait_policy=wait_policy,
    )


# --- Consumer 1: waits for every region ------------------------------------
with DAG(
    dag_id=f"all_regions_report_{STUDENT}",
    schedule=PartitionedAssetTimetable(assets=regional_sales, default_partition_mapper=rollup(WaitForAll())),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "partitions"],
):

    @task
    def build_report(dag_run=None):
        print(f"All regions are in. Building the national report for partition: {dag_run.partition_key}")

    build_report()


# --- Consumer 2: runs as soon as two regions are in ------------------------
with DAG(
    dag_id=f"early_regions_report_{STUDENT}",
    schedule=PartitionedAssetTimetable(assets=regional_sales, default_partition_mapper=rollup(MinimumCount(2))),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "partitions"],
):

    @task
    def build_early_report(dag_run=None):
        print(f"Two regions are in. Building an early estimate for partition: {dag_run.partition_key}")

    build_early_report()
