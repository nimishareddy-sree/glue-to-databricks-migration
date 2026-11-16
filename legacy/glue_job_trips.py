"""
LEGACY (reference only): the AWS Glue job this project migrates.

This is a representative Glue 4.0 PySpark script of the kind found in many
AWS data lakes. It is kept here so reviewers can compare "before" and "after".
It is NOT executed by this repository (the `awsglue` library only exists inside
the Glue runtime).

What it does:
  1. Reads raw taxi-trip CSV files from S3 via the Glue Data Catalog,
     using job bookmarks to pick up only new files.
  2. Renames/casts columns with ApplyMapping.
  3. Drops obviously bad rows with Filter.
  4. Writes partitioned Parquet back to S3 (append only).

Known problems with this design (motivation for the migration):
  * Append-only Parquet: a re-run or a corrected source file creates duplicates.
  * Bad rows are silently dropped - nobody knows how many or why.
  * Job bookmarks hide state inside Glue; reprocessing a day is awkward.
  * No reconciliation: no way to prove the output is complete and correct.
"""
import sys

from awsglue.context import GlueContext  # type: ignore
from awsglue.job import Job  # type: ignore
from awsglue.transforms import ApplyMapping, Filter  # type: ignore
from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_PATH"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw = glue_context.create_dynamic_frame.from_catalog(
    database="raw_db",
    table_name="taxi_trips_csv",
    transformation_ctx="raw",  # enables job bookmarks
)

mapped = ApplyMapping.apply(
    frame=raw,
    mappings=[
        ("trip_id", "string", "trip_id", "string"),
        ("vendor_id", "string", "vendor_id", "int"),
        ("pickup_datetime", "string", "pickup_ts", "timestamp"),
        ("dropoff_datetime", "string", "dropoff_ts", "timestamp"),
        ("passenger_count", "string", "passenger_count", "int"),
        ("trip_distance", "string", "trip_distance", "double"),
        ("pickup_zone", "string", "pickup_zone", "string"),
        ("dropoff_zone", "string", "dropoff_zone", "string"),
        ("payment_type", "string", "payment_type", "string"),
        ("fare_amount", "string", "fare_amount", "double"),
        ("tip_amount", "string", "tip_amount", "double"),
        ("total_amount", "string", "total_amount", "double"),
        ("updated_at", "string", "updated_at", "timestamp"),
    ],
    transformation_ctx="mapped",
)

cleaned = Filter.apply(
    frame=mapped,
    f=lambda r: r["fare_amount"] is not None
    and r["fare_amount"] >= 0
    and r["trip_distance"] is not None
    and r["trip_distance"] > 0,
    transformation_ctx="cleaned",
)

df = cleaned.toDF().withColumn("pickup_date", cleaned.toDF()["pickup_ts"].cast("date"))

(
    df.write.mode("append")  # <- duplicates on re-run
    .partitionBy("pickup_date")
    .parquet(args["TARGET_PATH"])
)

job.commit()
