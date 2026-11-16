"""Explicit schemas. Raw data is read as strings so nothing is lost before validation."""
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_COLUMNS = [
    "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "pickup_zone", "dropoff_zone", "payment_type",
    "fare_amount", "tip_amount", "total_amount", "updated_at",
]

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

SILVER_SCHEMA = StructType([
    StructField("trip_id", StringType(), False),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_ts", TimestampType(), True),
    StructField("dropoff_ts", TimestampType(), True),
    StructField("pickup_date", DateType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("trip_minutes", DoubleType(), True),
    StructField("pickup_zone", StringType(), True),
    StructField("dropoff_zone", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("updated_at", TimestampType(), True),
])
