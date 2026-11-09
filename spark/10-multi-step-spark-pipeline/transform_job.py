"""transform_job - cleans one day of raw events and writes transformed_events.

CDE job arguments: <base_path> [load_date]
Change from the CDE version: reads and writes Parquet on Object Store instead
of Hive tables, with no hardcoded credentials.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, trim

base_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://airflow-training/your_name/pipeline"
load_date = sys.argv[2] if len(sys.argv) > 2 else "2026-08-27"

spark = SparkSession.builder.appName("transform_job").getOrCreate()
# Overwrite only the partitions being written, so reruns of a day are idempotent
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

raw_events = spark.read.parquet(f"{base_path}/raw_events").filter(col("load_date") == load_date)

# Drop rows missing required identifiers/amount (e.g. evt-1005 has a NULL amount)
transformed = raw_events.dropna(subset=["event_id", "customer_id", "amount"])
for c in ["event_id", "event_type", "customer_id"]:
    transformed = transformed.withColumn(c, trim(col(c)))
transformed = transformed.withColumn("event_hour", hour("event_timestamp")).dropDuplicates(["event_id"])

transformed.show(truncate=False)
print(f"Input rows: {raw_events.count()}, transformed rows: {transformed.count()}")
transformed.groupBy("event_type").count().orderBy("count", ascending=False).show()

(transformed.write.mode("overwrite")
    .partitionBy("load_date")
    .parquet(f"{base_path}/transformed_events"))
spark.stop()
