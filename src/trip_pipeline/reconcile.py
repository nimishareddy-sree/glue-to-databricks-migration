"""Reconciliation: prove the new pipeline matches (or knowingly improves on) the legacy output.

Compares two DataFrames keyed by trip_id and reports:
  * row counts on each side
  * keys only in legacy / only in new
  * rows present in both whose business columns differ (via a row hash)
  * column totals (e.g. sum of total_amount) on each side

Typical use during migration: run the legacy Glue output (Parquet on S3) and the
new silver table for the same date range through `reconcile()` and attach the
report to the cut-over sign-off.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

COMPARE_COLUMNS = [
    "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
    "pickup_zone", "dropoff_zone", "fare_amount", "tip_amount", "total_amount",
]
SUM_COLUMNS = ["fare_amount", "tip_amount", "total_amount"]


@dataclass
class ReconReport:
    legacy_rows: int
    new_rows: int
    legacy_distinct_keys: int
    only_in_legacy: int
    only_in_new: int
    mismatched_rows: int
    legacy_sums: dict
    new_sums: dict

    @property
    def passed(self) -> bool:
        return self.only_in_legacy == 0 and self.only_in_new == 0 and self.mismatched_rows == 0

    def as_dict(self) -> dict:
        return {**asdict(self), "passed": self.passed}


def _row_hash(df: DataFrame, cols: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]
    return df.select("trip_id", F.sha2(F.concat_ws("||", *parts), 256).alias("_h"))


def _sums(df: DataFrame) -> dict:
    row = df.agg(*[F.round(F.sum(c), 2).alias(c) for c in SUM_COLUMNS]).first()
    return {c: (float(row[c]) if row[c] is not None else 0.0) for c in SUM_COLUMNS}


def reconcile(legacy: DataFrame, new: DataFrame, key: str = "trip_id",
              cols: list[str] = COMPARE_COLUMNS) -> ReconReport:
    # Legacy append-only output can hold duplicates; compare one row per key.
    legacy_dedup = legacy.dropDuplicates([key])
    lh, nh = _row_hash(legacy_dedup, cols).alias("l"), _row_hash(new, cols).alias("n")
    joined = lh.join(nh, F.col(f"l.{key}") == F.col(f"n.{key}"), "full_outer")

    counts = joined.agg(
        F.sum(F.col(f"n.{key}").isNull().cast("int")).alias("only_legacy"),
        F.sum(F.col(f"l.{key}").isNull().cast("int")).alias("only_new"),
        F.sum((F.col(f"l.{key}").isNotNull() & F.col(f"n.{key}").isNotNull()
               & (F.col("l._h") != F.col("n._h"))).cast("int")).alias("mismatch"),
    ).first()

    return ReconReport(
        legacy_rows=legacy.count(),
        new_rows=new.count(),
        legacy_distinct_keys=legacy_dedup.count(),
        only_in_legacy=int(counts["only_legacy"] or 0),
        only_in_new=int(counts["only_new"] or 0),
        mismatched_rows=int(counts["mismatch"] or 0),
        legacy_sums=_sums(legacy_dedup),
        new_sums=_sums(new),
    )
