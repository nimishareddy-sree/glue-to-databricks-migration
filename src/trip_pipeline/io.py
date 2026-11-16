"""Small helpers so every layer reads/writes Delta the same way, locally or on Databricks."""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .config import PipelineConfig


def read_table(spark: SparkSession, cfg: PipelineConfig, name: str) -> DataFrame:
    ident = cfg.table(name)
    if cfg.uses_paths:
        return spark.read.format("delta").load(ident)
    return spark.read.table(ident)


def table_exists(spark: SparkSession, cfg: PipelineConfig, name: str) -> bool:
    ident = cfg.table(name)
    if cfg.uses_paths:
        return DeltaTable.isDeltaTable(spark, ident)
    return spark.catalog.tableExists(ident)


def delta_table(spark: SparkSession, cfg: PipelineConfig, name: str) -> DeltaTable:
    ident = cfg.table(name)
    return DeltaTable.forPath(spark, ident) if cfg.uses_paths else DeltaTable.forName(spark, ident)


def write_table(df: DataFrame, cfg: PipelineConfig, name: str, mode: str = "append",
                partition_by: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    ident = cfg.table(name)
    if cfg.uses_paths:
        writer.save(ident)
    else:
        writer.saveAsTable(ident)
