# ADR 0001 — Dual-target version pinning

## Context
The core must behave identically locally (OSS Spark) and on Databricks DBR 16.4
LTS (Spark 3.5.2). delta-spark 3.3.1 requires pyspark>=3.5.3.

## Decision
Local: pyspark==3.5.9 + delta-spark==3.3.1, Python 3.12. Target: DBR 16.4 LTS (Spark 3.5.2).
pyspark 3.5.3 crashes on Windows + Python 3.12 (SPARK-53759, fixed in 3.5.9), so the
local pin is 3.5.9 — same Spark 3.5.x minor and same Delta protocol as the DBR target
(a patch-level delta). A CI smoke test (`opl._version_check.assert_versions`) fails the
build on drift.

## Consequences
Reproducible behavior across targets; version drift caught in CI, not in production.
