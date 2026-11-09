"""validate_job - replaces the CDW row-count check from the CDE version.

Fails the job (and so the Airflow task) when no raw events landed for the day.
CDE job arguments: <base_path> [load_date]
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

base_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://airflow-training/your_name/pipeline"
load_date = sys.argv[2] if len(sys.argv) > 2 else "2026-08-27"

spark = SparkSession.builder.appName("validate_job").getOrCreate()

count = spark.read.parquet(f"{base_path}/raw_events").filter(col("load_date") == load_date).count()
print(f"raw_events rows for load_date={load_date}: {count}")
spark.stop()

if count == 0:
    print("FAIL: no rows ingested")
    sys.exit(1)
print("PASS")
