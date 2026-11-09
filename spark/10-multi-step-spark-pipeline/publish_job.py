"""publish_job - publishes one day of transformed events to the curated area.

CDE job arguments: <base_path> [load_date]
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

base_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://airflow-training/your_name/pipeline"
load_date = sys.argv[2] if len(sys.argv) > 2 else "2026-08-27"

spark = SparkSession.builder.appName("publish_job").getOrCreate()
# Overwrite only the partitions being written, so reruns of a day are idempotent
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

curated = (
    spark.read.parquet(f"{base_path}/transformed_events")
    .filter(col("load_date") == load_date)
    .select("event_id", "event_type", "customer_id", "amount", "event_timestamp", "event_hour", "load_date")
)
curated.show(truncate=False)

(curated.write.mode("overwrite")
    .partitionBy("load_date")
    .parquet(f"{base_path}/curated_events"))
print(f"Rows published for load_date={load_date}: {curated.count()}")

# Curated-side summary: total purchase amount per customer
(curated.filter(col("event_type") == "purchase")
    .groupBy("customer_id").sum("amount")
    .withColumnRenamed("sum(amount)", "total_purchase_amount")
    .orderBy("total_purchase_amount", ascending=False)
    .show())
spark.stop()
