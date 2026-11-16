"""Environment configuration.

The same code runs locally (Delta tables stored as folders on disk) and on
Databricks (Unity Catalog tables). Only this file knows the difference.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    env: str                 # "local" or "databricks"
    source_path: str         # where raw CSV files land
    base_path: str = ""      # local only: root folder for Delta tables
    catalog: str = ""        # databricks only
    schema: str = ""         # databricks only

    def table(self, name: str) -> str:
        """Return a table identifier: a filesystem path locally, or catalog.schema.name on Databricks."""
        if self.env == "local":
            return f"{self.base_path.rstrip('/')}/{name}"
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def uses_paths(self) -> bool:
        return self.env == "local"


BRONZE = "bronze_trips"
SILVER = "silver_trips"
QUARANTINE = "quarantine_trips"
GOLD_DAILY = "gold_daily_zone_revenue"
DQ_RESULTS = "dq_results"


def from_args(argv: list[str] | None = None) -> PipelineConfig:
    p = argparse.ArgumentParser(description="Run the taxi-trip lakehouse pipeline")
    p.add_argument("--env", choices=["local", "databricks"], default="local")
    p.add_argument("--source-path", default="data/raw")
    p.add_argument("--base-path", default="_lakehouse")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="trips_demo")
    a = p.parse_args(argv)
    return PipelineConfig(
        env=a.env,
        source_path=a.source_path,
        base_path=a.base_path,
        catalog=a.catalog,
        schema=a.schema,
    )
