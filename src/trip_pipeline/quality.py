"""Declarative data-quality rules with quarantine.

The legacy Glue job silently dropped bad rows. Here every rule is named,
failing rows are kept in a quarantine table with the reasons, and per-batch
metrics are recorded so data loss is visible and auditable.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    valid_when: str        # Spark SQL boolean expression; TRUE means the row passes
    severity: str = "error"  # "error" -> quarantine row; "warn" -> keep row, just count it


RULES: list[Rule] = [
    Rule("trip_id_present", "trip_id IS NOT NULL AND trip_id <> ''"),
    Rule("pickup_ts_parsed", "pickup_ts IS NOT NULL"),
    Rule("updated_at_parsed", "updated_at IS NOT NULL"),  # MERGE ordering depends on it
    Rule("dropoff_after_pickup", "dropoff_ts > pickup_ts"),
    Rule("distance_positive", "trip_distance > 0"),
    Rule("fare_non_negative", "fare_amount >= 0"),
    Rule("total_covers_fare", "total_amount >= fare_amount"),
    Rule("trip_under_6_hours", "trip_minutes <= 360", severity="warn"),
    Rule("passenger_count_range", "passenger_count BETWEEN 1 AND 6", severity="warn"),
]


def apply_rules(df: DataFrame, rules: list[Rule] = RULES) -> DataFrame:
    """Add `_failed_errors` and `_failed_warnings` array columns listing broken rule names."""
    def failed(rs: list[Rule]):
        checks = [F.when(~F.coalesce(F.expr(r.valid_when), F.lit(False)), F.lit(r.name)) for r in rs]
        return F.filter(F.array(*checks), lambda x: x.isNotNull()) if checks else F.array()

    errors = [r for r in rules if r.severity == "error"]
    warns = [r for r in rules if r.severity == "warn"]
    return df.withColumn("_failed_errors", failed(errors)).withColumn("_failed_warnings", failed(warns))


def split_valid_invalid(checked: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = checked.filter(F.size("_failed_errors") == 0).drop("_failed_errors", "_failed_warnings")
    invalid = checked.filter(F.size("_failed_errors") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid


def rule_metrics(checked: DataFrame, batch_id: str, rules: list[Rule] = RULES) -> DataFrame:
    """One row per rule: how many rows failed it in this batch."""
    total = F.count(F.lit(1))
    aggs = [total.alias("rows_checked")]
    for r in rules:
        col = "_failed_errors" if r.severity == "error" else "_failed_warnings"
        aggs.append(F.sum(F.array_contains(col, r.name).cast("int")).alias(r.name))
    wide = checked.agg(*aggs)
    stack_expr = "stack({n}, {pairs}) as (rule, failed_rows)".format(
        n=len(rules), pairs=", ".join(f"'{r.name}', `{r.name}`" for r in rules)
    )
    severities = F.create_map(*[F.lit(x) for r in rules for x in (r.name, r.severity)])
    return (
        wide.selectExpr("rows_checked", stack_expr)
        .withColumn("failed_rows", F.coalesce("failed_rows", F.lit(0)).cast("long"))
        .withColumn("severity", severities[F.col("rule")])
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("checked_at", F.current_timestamp())
    )
