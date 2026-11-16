"""Medallion pipeline: bronze (raw) -> silver (clean, deduplicated) -> gold (aggregates).

Key design choices vs. the legacy Glue job:
  * MERGE (upsert) into silver instead of append -> re-runs are idempotent.
  * Late corrections win only if they are newer (updated_at) -> no stale overwrites.
  * Bad rows quarantined with reasons instead of silently dropped.
  * Gold rebuilt only for the dates touched by the batch (replaceWhere).
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config as C
from .config import PipelineConfig
from .io import delta_table, read_table, table_exists, write_table
from .quality import apply_rules, rule_metrics, split_valid_invalid
from .schemas import RAW_SCHEMA, SILVER_SCHEMA
from .transforms import add_ingestion_metadata, daily_zone_revenue, latest_per_key, standardize

log = logging.getLogger("trip_pipeline")

SILVER_COLUMNS = [f.name for f in SILVER_SCHEMA.fields]


@dataclass
class BatchResult:
    batch_id: str
    bronze_rows: int
    silver_candidates: int
    quarantined: int
    affected_dates: int


def ingest_bronze(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> int:
    raw = (
        spark.read.option("header", "true")
        .schema(RAW_SCHEMA)
        .csv(cfg.source_path)
    )
    bronze = add_ingestion_metadata(raw, batch_id)
    write_table(bronze, cfg, C.BRONZE, mode="append")
    return bronze.count()


def build_silver(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> tuple[int, int, DataFrame]:
    batch = read_table(spark, cfg, C.BRONZE).filter(F.col("_batch_id") == batch_id)
    # No .cache(): serverless compute (incl. Databricks Free Edition) does not support it.
    checked = apply_rules(standardize(batch))

    write_table(rule_metrics(checked, batch_id), cfg, C.DQ_RESULTS, mode="append")

    valid, invalid = split_valid_invalid(checked)
    n_bad = invalid.count()
    if n_bad:
        write_table(invalid, cfg, C.QUARANTINE, mode="append")

    updates = latest_per_key(valid).select(*SILVER_COLUMNS)
    n_candidates = updates.count()

    if not table_exists(spark, cfg, C.SILVER):
        write_table(updates, cfg, C.SILVER, mode="overwrite", partition_by=["pickup_date"])
    else:
        (
            delta_table(spark, cfg, C.SILVER).alias("t")
            .merge(updates.alias("s"), "t.trip_id = s.trip_id")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute()
        )

    affected = updates.select("pickup_date").distinct()
    return n_candidates, n_bad, affected


def build_gold(spark: SparkSession, cfg: PipelineConfig, affected_dates: DataFrame) -> int:
    dates = [r.pickup_date for r in affected_dates.collect() if r.pickup_date is not None]
    if not dates:
        return 0
    silver = read_table(spark, cfg, C.SILVER)
    date_list = ", ".join(f"DATE'{d.isoformat()}'" for d in dates)
    predicate = f"pickup_date IN ({date_list})"
    gold = daily_zone_revenue(silver.filter(predicate))

    if not table_exists(spark, cfg, C.GOLD_DAILY):
        write_table(gold, cfg, C.GOLD_DAILY, mode="overwrite", partition_by=["pickup_date"])
    else:
        ident = cfg.table(C.GOLD_DAILY)
        writer = gold.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
        writer.save(ident) if cfg.uses_paths else writer.saveAsTable(ident)
    return len(dates)


def run(spark: SparkSession, cfg: PipelineConfig, batch_id: str | None = None) -> BatchResult:
    batch_id = batch_id or uuid.uuid4().hex[:12]
    log.info("batch %s: ingesting from %s", batch_id, cfg.source_path)
    n_bronze = ingest_bronze(spark, cfg, batch_id)
    n_silver, n_bad, affected = build_silver(spark, cfg, batch_id)
    n_dates = build_gold(spark, cfg, affected)
    result = BatchResult(batch_id, n_bronze, n_silver, n_bad, n_dates)
    log.info("batch result: %s", result)
    return result
