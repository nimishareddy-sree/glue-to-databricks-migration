"""SparkSession factory: reuses the Databricks session, or builds a local one with Delta."""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "trip_pipeline", local: bool = True) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:  # Databricks notebooks/jobs already have one
        return active
    if not local:
        return SparkSession.builder.appName(app_name).getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
