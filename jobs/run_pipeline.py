"""Entry point for local runs and the Databricks job.

Local:
  python jobs/run_pipeline.py --env local --source-path data/raw/batch_1
  python jobs/run_pipeline.py --env local --source-path data/raw/batch_2

Databricks (see databricks.yml):
  --env databricks --catalog workspace --schema trips_demo --source-path /Volumes/workspace/trips_demo/raw/batch_1
"""
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from trip_pipeline.config import from_args  # noqa: E402
from trip_pipeline.pipeline import run  # noqa: E402
from trip_pipeline.spark import get_spark  # noqa: E402


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    cfg = from_args(argv)
    spark = get_spark(local=cfg.env == "local")
    if cfg.env == "databricks":
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    result = run(spark, cfg)
    print(json.dumps(result.__dict__, indent=2))


if __name__ == "__main__":
    main()
