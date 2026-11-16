from conftest import RAW_HEADER, raw_row
from trip_pipeline.quality import apply_rules, rule_metrics, split_valid_invalid
from trip_pipeline.transforms import standardize


def checked(spark, rows):
    return apply_rules(standardize(spark.createDataFrame(rows, RAW_HEADER)))


def test_good_row_passes(spark):
    valid, invalid = split_valid_invalid(checked(spark, [raw_row()]))
    assert valid.count() == 1 and invalid.count() == 0


def test_bad_rows_are_quarantined_with_reasons(spark):
    rows = [
        raw_row("T1", fare="-5", total="-5"),
        raw_row("T2", distance="0"),
        raw_row("T3", dropoff="2026-01-05 07:00:00"),
        raw_row("", fare="10", total="12"),
    ]
    valid, invalid = split_valid_invalid(checked(spark, rows))
    assert valid.count() == 0
    reasons = {r.trip_id: set(r._failed_errors) for r in invalid.collect()}
    assert "fare_non_negative" in reasons["T1"]
    assert "distance_positive" in reasons["T2"]
    assert "dropoff_after_pickup" in reasons["T3"]
    assert "trip_id_present" in reasons[""]


def test_warning_rules_do_not_quarantine(spark):
    valid, invalid = split_valid_invalid(checked(spark, [raw_row(passengers="9")]))
    assert valid.count() == 1 and invalid.count() == 0


def test_rule_metrics_counts_failures(spark):
    m = rule_metrics(checked(spark, [raw_row("T1"), raw_row("T2", distance="0")]), "b1")
    by_rule = {r.rule: r.failed_rows for r in m.collect()}
    assert by_rule["distance_positive"] == 1
    assert by_rule["fare_non_negative"] == 0
