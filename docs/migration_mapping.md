# AWS Glue → Databricks: concept mapping used in this migration

| AWS Glue (legacy) | Databricks / Delta Lake (new) | Why it matters |
| --- | --- | --- |
| `DynamicFrame` | Spark `DataFrame` | Standard API, portable, unit-testable without the Glue runtime |
| `ApplyMapping` | `select` + `try_cast` / `try_to_timestamp` (`transforms.standardize`) | Explicit, typed, and safe under ANSI SQL mode |
| `Filter` (silently drops rows) | Named DQ rules + quarantine table (`quality.py`) | Every rejected row is kept with the reason; per-batch metrics in `dq_results` |
| Job bookmarks | Batch IDs + idempotent `MERGE` | Re-running a batch or a day is safe; state is visible in tables, not hidden in Glue |
| Append-only Parquet on S3 | Delta tables (ACID, time travel) | No duplicates on re-run; `DESCRIBE HISTORY` shows every write |
| Glue Data Catalog | Unity Catalog (`catalog.schema.table`) | Governance, lineage, permissions |
| Glue Workflows / triggers | Databricks Jobs via Asset Bundle (`databricks.yml`) | Job definition lives in Git, deployed with one command |
| CloudWatch logs only | DQ metrics table + reconciliation report | Data correctness is measurable, not assumed |

## Migration checklist (what a real engagement would follow)

1. **Inventory** every Glue job: sources, targets, schedules, bookmarks, Glue-specific features used.
2. **Pick a representative pilot** (this repo's trips job) and migrate it end to end.
3. **Run both pipelines in parallel** on the same input for a defined period.
4. **Reconcile** daily with `trip_pipeline.reconcile.reconcile()` – counts, key differences, row-hash mismatches, column totals.
5. **Explain every difference.** Expected ones here: legacy duplicates from append-only writes, and corrected trips that legacy never updated.
6. **Cut over** once reconciliation is clean, then decommission the Glue job.
7. Reuse the same pattern for the remaining jobs.
