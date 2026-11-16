import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:  # tests need local Spark + Delta; skip cleanly where they are not installed
    import delta  # noqa: F401
    import pyspark  # noqa: F401
except ImportError:
    collect_ignore_glob = ["test_*.py"]


@pytest.fixture(scope="session")
def spark():
    from trip_pipeline.spark import get_spark

    s = get_spark("trip_pipeline_tests", local=True)
    yield s
    s.stop()


@pytest.fixture
def cfg(tmp_path):
    from trip_pipeline.config import PipelineConfig

    return PipelineConfig(env="local", source_path=str(tmp_path / "raw"), base_path=str(tmp_path / "lake"))


RAW_HEADER = ["trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
              "trip_distance", "pickup_zone", "dropoff_zone", "payment_type",
              "fare_amount", "tip_amount", "total_amount", "updated_at"]


def raw_row(trip_id="T1", pickup="2026-01-05 08:00:00", dropoff="2026-01-05 08:20:00",
            distance="3.2", fare="15.0", tip="3.0", total="19.0",
            updated="2026-01-05 08:21:00", zone=" midtown ", payment="1", passengers="1"):
    return [trip_id, "1", pickup, dropoff, passengers, distance, zone, "SOHO", payment,
            fare, tip, total, updated]


@pytest.fixture
def write_csv():
    import csv

    def _write(folder, rows):
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, "trips.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(RAW_HEADER)
            w.writerows(rows)
        return folder

    return _write
