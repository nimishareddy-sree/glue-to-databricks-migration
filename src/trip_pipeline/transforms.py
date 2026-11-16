"""Pure DataFrame transformations - no I/O, so every function is unit-testable."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"

PAYMENT_TYPES = {"1": "card", "2": "cash", "3": "no_charge", "4": "dispute"}


def add_ingestion_metadata(df: DataFrame, batch_id: str) -> DataFrame:
    """Bronze: keep raw values untouched, add lineage columns."""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def _try_cast(col: str, type_: str):
    """Cast that returns NULL on bad input instead of failing (safe under ANSI mode)."""
    return F.expr(f"try_cast(`{col}` AS {type_})")


def _try_ts(col: str):
    """Parse a timestamp; NULL on bad input instead of failing (safe under ANSI mode)."""
    return F.expr(f"try_to_timestamp(`{col}`, '{TS_FORMAT}')")


def standardize(df: DataFrame) -> DataFrame:
    """Cast, rename and derive columns (replaces Glue ApplyMapping).

    Uses try_cast / try_to_timestamp so malformed values become NULL and are
    caught by the data-quality rules, rather than crashing the job. This matters
    on Databricks, where ANSI SQL mode makes plain casts throw on bad input.
    """
    payment_map = F.create_map(*[F.lit(x) for kv in PAYMENT_TYPES.items() for x in kv])
    out = (
        df.select(
            F.trim("trip_id").alias("trip_id"),
            _try_cast("vendor_id", "INT").alias("vendor_id"),
            _try_ts("pickup_datetime").alias("pickup_ts"),
            _try_ts("dropoff_datetime").alias("dropoff_ts"),
            _try_cast("passenger_count", "INT").alias("passenger_count"),
            _try_cast("trip_distance", "DOUBLE").alias("trip_distance"),
            F.upper(F.trim("pickup_zone")).alias("pickup_zone"),
            F.upper(F.trim("dropoff_zone")).alias("dropoff_zone"),
            F.coalesce(payment_map[F.trim("payment_type")], F.lit("unknown")).alias("payment_type"),
            _try_cast("fare_amount", "DOUBLE").alias("fare_amount"),
            _try_cast("tip_amount", "DOUBLE").alias("tip_amount"),
            _try_cast("total_amount", "DOUBLE").alias("total_amount"),
            _try_ts("updated_at").alias("updated_at"),
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn(
            "trip_minutes",
            F.round((F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0, 2),
        )
    )
    return out


def latest_per_key(df: DataFrame, key: str = "trip_id", order_col: str = "updated_at") -> DataFrame:
    """Keep only the most recent version of each record within a batch.

    Source systems resend corrected trips; without this step a MERGE would see
    several source rows for one target row and fail (or pick one at random).
    """
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def daily_zone_revenue(silver: DataFrame) -> DataFrame:
    """Gold: business-level daily aggregates per pickup zone."""
    return (
        silver.groupBy("pickup_date", "pickup_zone")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_minutes"),
            F.round(
                F.when(F.sum("fare_amount") > 0, F.sum("tip_amount") / F.sum("fare_amount")), 4
            ).alias("tip_rate"),
        )
    )
