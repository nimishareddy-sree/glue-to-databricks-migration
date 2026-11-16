"""End-to-end: two batches through bronze -> silver -> gold on local Delta tables."""
import os
from dataclasses import replace

from conftest import raw_row
from trip_pipeline import config as C
from trip_pipeline.io import read_table
from trip_pipeline.pipeline import run


def test_initial_load_then_corrections(spark, cfg, write_csv, tmp_path):
    b1 = write_csv(os.path.join(tmp_path, "raw1"), [
        raw_row("T1", total="19.0", updated="2026-01-05 08:21:00"),
        raw_row("T2", total="30.0", updated="2026-01-05 08:21:00"),
        raw_row("T3", fare="-1", total="0"),  # bad -> quarantine
    ])
    r1 = run(spark, replace(cfg, source_path=b1), batch_id="b1")
    assert r1.bronze_rows == 3 and r1.quarantined == 1

    b2 = write_csv(os.path.join(tmp_path, "raw2"), [
        raw_row("T1", total="25.0", updated="2026-01-06 09:00:00"),   # newer -> wins
        raw_row("T2", total="99.0", updated="2026-01-04 00:00:00"),   # stale -> ignored
        raw_row("T4", total="20.0"),                                  # new trip
    ])
    run(spark, replace(cfg, source_path=b2), batch_id="b2")

    silver = {r.trip_id: r.total_amount for r in read_table(spark, cfg, C.SILVER).collect()}
    assert silver == {"T1": 25.0, "T2": 30.0, "T4": 20.0}

    gold = read_table(spark, cfg, C.GOLD_DAILY).collect()
    assert sum(r.trips for r in gold) == 3
    assert round(sum(r.revenue for r in gold), 2) == 75.0

    assert read_table(spark, cfg, C.QUARANTINE).count() == 1


def test_rerunning_same_files_is_idempotent(spark, cfg, write_csv, tmp_path):
    src = write_csv(os.path.join(tmp_path, "raw"), [raw_row("T1"), raw_row("T2")])
    c = replace(cfg, source_path=src)
    run(spark, c, batch_id="first")
    run(spark, c, batch_id="rerun")
    assert read_table(spark, cfg, C.SILVER).count() == 2
    assert sum(r.trips for r in read_table(spark, cfg, C.GOLD_DAILY).collect()) == 2
