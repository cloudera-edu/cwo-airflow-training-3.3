"""ingest_job - writes one day of raw events to Object Store as Parquet.

CDE job arguments: <base_path> [load_date]
  e.g.  s3a://airflow-training/your_name/pipeline 2026-08-27

Change from the CDE version: no Hive/CDW connection (and no hardcoded
credentials). Data goes to Object Store over S3A; configure the S3A
endpoint and keys in the CDE job's Spark configuration.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

base_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://airflow-training/your_name/pipeline"
load_date = sys.argv[2] if len(sys.argv) > 2 else "2026-08-27"

spark = SparkSession.builder.appName("ingest_job").getOrCreate()
# Overwrite only the partitions being written, so reruns of a day are idempotent
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

data = [
    ("evt-1001", "purchase", "cust-501", 129.99, f"{load_date} 08:15:00"),
    ("evt-1002", "purchase", "cust-502", 45.50, f"{load_date} 09:02:11"),
    ("evt-1003", "refund", "cust-501", -20.00, f"{load_date} 10:44:37"),
    ("evt-1004", "purchase", "cust-503", 310.25, f"{load_date} 12:10:05"),
    ("evt-1005", "click", "cust-504", None, f"{load_date} 13:59:22"),
]
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_timestamp", StringType(), True),
])

raw_events = (
    spark.createDataFrame(data, schema=schema)
    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
    .withColumn("load_date", lit(load_date))
)
raw_events.show(truncate=False)

# Overwrite just this day's partition, so reruns are idempotent
(raw_events.write.mode("overwrite")
    .partitionBy("load_date")
    .parquet(f"{base_path}/raw_events"))

print(f"Rows ingested for load_date={load_date}: {raw_events.count()}")
spark.stop()
