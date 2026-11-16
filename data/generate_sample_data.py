"""Generate synthetic taxi-trip CSV files (no downloads, no real data).

Creates two batches that exercise every hard case the pipeline must handle:

  data/raw/batch_1/trips.csv  - initial load, includes deliberately bad rows
  data/raw/batch_2/trips.csv  - new trips + corrections to batch-1 trips
                                (newer updated_at), an exact resend of a trip,
                                and a stale correction that must NOT win

Usage:
  python data/generate_sample_data.py --rows 5000 --out data/raw
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

ZONES = ["MIDTOWN", "UPPER EAST SIDE", "JFK AIRPORT", "LAGUARDIA", "SOHO",
         "HARLEM", "WILLIAMSBURG", "ASTORIA", "FINANCIAL DISTRICT", "CHELSEA"]
COLUMNS = ["trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
           "trip_distance", "pickup_zone", "dropoff_zone", "payment_type",
           "fare_amount", "tip_amount", "total_amount", "updated_at"]
FMT = "%Y-%m-%d %H:%M:%S"


def make_trip(rng: random.Random, n: int, day0: datetime) -> dict:
    pickup = day0 + timedelta(days=rng.randint(0, 6), seconds=rng.randint(0, 86_399))
    minutes = max(2.0, rng.gauss(18, 9))
    distance = round(max(0.3, minutes * rng.uniform(0.15, 0.45)), 2)
    fare = round(3.0 + distance * 2.5 + minutes * 0.5, 2)
    payment = rng.choices(["1", "2", "3", "4"], weights=[70, 25, 3, 2])[0]
    tip = round(fare * rng.uniform(0.1, 0.25), 2) if payment == "1" else 0.0
    return {
        "trip_id": f"T{n:07d}",
        "vendor_id": str(rng.choice([1, 2])),
        "pickup_datetime": pickup.strftime(FMT),
        "dropoff_datetime": (pickup + timedelta(minutes=minutes)).strftime(FMT),
        "passenger_count": str(rng.choices([1, 2, 3, 4, 5, 6], weights=[60, 20, 8, 6, 3, 3])[0]),
        "trip_distance": f"{distance}",
        "pickup_zone": rng.choice(ZONES),
        "dropoff_zone": rng.choice(ZONES),
        "payment_type": payment,
        "fare_amount": f"{fare}",
        "tip_amount": f"{tip}",
        "total_amount": f"{round(fare + tip + 1.0, 2)}",
        "updated_at": (pickup + timedelta(minutes=minutes, seconds=30)).strftime(FMT),
    }


def corrupt(rng: random.Random, row: dict) -> dict:
    """Inject one realistic defect."""
    bad = dict(row)
    kind = rng.choice(["neg_fare", "zero_distance", "bad_ts", "dropoff_before", "missing_id"])
    if kind == "neg_fare":
        bad["fare_amount"] = "-12.5"
    elif kind == "zero_distance":
        bad["trip_distance"] = "0"
    elif kind == "bad_ts":
        bad["pickup_datetime"] = "31/02/2026 25:61"
    elif kind == "dropoff_before":
        bad["dropoff_datetime"] = bad["pickup_datetime"]
    else:
        bad["trip_id"] = ""
    return bad


def write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--bad-rate", type=float, default=0.02)
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    day0 = datetime(2026, 1, 5)

    batch1 = [make_trip(rng, i, day0) for i in range(a.rows)]
    batch1 = [corrupt(rng, r) if rng.random() < a.bad_rate else r for r in batch1]

    good1 = [r for r in batch1 if r["trip_id"]]
    batch2 = [make_trip(rng, a.rows + i, day0) for i in range(a.rows // 5)]

    # Corrections: fare adjusted later -> newer updated_at must win.
    for r in rng.sample(good1, k=max(1, a.rows // 50)):
        c = dict(r)
        c["tip_amount"] = "0.0"
        c["total_amount"] = f"{round(float(c['fare_amount']) + 1.0, 2)}" if c["fare_amount"][0] != "-" else c["total_amount"]
        c["updated_at"] = (datetime.strptime(r["updated_at"], FMT) + timedelta(days=1)).strftime(FMT)
        batch2.append(c)

    # Exact resend of an existing trip (must not duplicate).
    batch2.append(dict(good1[0]))

    # Stale correction: older updated_at than what silver holds -> must be ignored.
    stale = dict(good1[1])
    stale["total_amount"] = "9999.99"
    stale["updated_at"] = (datetime.strptime(good1[1]["updated_at"], FMT) - timedelta(days=1)).strftime(FMT)
    batch2.append(stale)

    # Same trip twice inside one batch with different versions -> latest wins.
    dup = make_trip(rng, a.rows * 2, day0)
    dup_newer = dict(dup, total_amount="42.0",
                     updated_at=(datetime.strptime(dup["updated_at"], FMT) + timedelta(hours=1)).strftime(FMT))
    batch2 += [dup, dup_newer]

    rng.shuffle(batch2)
    out = Path(a.out)
    write(out / "batch_1" / "trips.csv", batch1)
    write(out / "batch_2" / "trips.csv", batch2)
    print(f"batch_1: {len(batch1)} rows -> {out / 'batch_1' / 'trips.csv'}")
    print(f"batch_2: {len(batch2)} rows -> {out / 'batch_2' / 'trips.csv'}")
    print(f"special cases: stale trip_id={stale['trip_id']}, resent trip_id={good1[0]['trip_id']}, "
          f"in-batch duplicate trip_id={dup['trip_id']}")


if __name__ == "__main__":
    main()
