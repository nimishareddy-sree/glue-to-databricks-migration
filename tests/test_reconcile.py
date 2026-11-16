from conftest import RAW_HEADER, raw_row
from trip_pipeline.reconcile import reconcile
from trip_pipeline.transforms import standardize


def df(spark, rows):
    return standardize(spark.createDataFrame(rows, RAW_HEADER))


def test_identical_outputs_pass(spark):
    rows = [raw_row("T1"), raw_row("T2")]
    rep = reconcile(df(spark, rows), df(spark, rows))
    assert rep.passed and rep.mismatched_rows == 0


def test_detects_missing_extra_and_changed_rows(spark):
    legacy = df(spark, [raw_row("T1"), raw_row("T2"), raw_row("T2")])  # duplicate from append
    new = df(spark, [raw_row("T1", total="20.0"), raw_row("T3")])
    rep = reconcile(legacy, new)
    assert rep.legacy_rows == 3 and rep.legacy_distinct_keys == 2
    assert rep.only_in_legacy == 1   # T2
    assert rep.only_in_new == 1      # T3
    assert rep.mismatched_rows == 1  # T1 total changed
    assert not rep.passed
