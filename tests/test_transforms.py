from conftest import RAW_HEADER, raw_row
from trip_pipeline.transforms import daily_zone_revenue, latest_per_key, standardize


def to_df(spark, rows):
    return spark.createDataFrame(rows, RAW_HEADER)


def test_standardize_casts_and_derives(spark):
    out = standardize(to_df(spark, [raw_row()])).first()
    assert out.trip_distance == 3.2
    assert out.pickup_zone == "MIDTOWN"          # trimmed + upper-cased
    assert out.payment_type == "card"            # code mapped to label
    assert out.trip_minutes == 20.0
    assert str(out.pickup_date) == "2026-01-05"


def test_unknown_payment_code_is_labelled(spark):
    out = standardize(to_df(spark, [raw_row(payment="9")])).first()
    assert out.payment_type == "unknown"


def test_unparseable_timestamp_becomes_null_not_error(spark):
    out = standardize(to_df(spark, [raw_row(pickup="31/02/2026 25:61")])).first()
    assert out.pickup_ts is None


def test_latest_per_key_keeps_newest_version(spark):
    rows = [
        raw_row(total="19.0", updated="2026-01-05 08:21:00"),
        raw_row(total="25.0", updated="2026-01-06 09:00:00"),
    ]
    out = latest_per_key(standardize(to_df(spark, rows))).collect()
    assert len(out) == 1 and out[0].total_amount == 25.0


def test_daily_zone_revenue(spark):
    rows = [raw_row("T1", total="10.0"), raw_row("T2", total="20.0")]
    g = daily_zone_revenue(standardize(to_df(spark, rows))).first()
    assert g.trips == 2 and g.revenue == 30.0
