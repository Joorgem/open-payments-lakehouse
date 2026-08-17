# ADR 0004 — pyspark/delta-spark as an optional extra

## Status
Accepted

## Context
`opl` is a dual-target library (ADR 0001): OSS Spark locally/CI, Databricks
serverless in production. The Task 6 deployment spike found that declaring
`pyspark`/`delta-spark` as core wheel dependencies breaks the wheel install on
Databricks Free Edition serverless, because serverless compute already
provides Spark natively in its base environment — pip trying to also resolve
and install pyspark on top of it fails. Two failure modes were observed
empirically:
- client `"2"`: rejected the wheel outright — the package's
  `requires-python >=3.12` doesn't match serverless env v2's Python 3.11.10.
- client `"3"`: matched Python 3.12.3, but pip then failed to fetch the
  ~300MB pyspark wheel with a generic "Unable to find or download the
  required package" error, reproducible on repeat runs (Free Edition's
  ~~governed egress/~~serverless install budget can't complete that download).

> **AMENDED 2026-08-17 by F-DB: the egress half of that alternative is ruled out, so the
> remaining explanation is the install budget.** This line offered two causes it could not
> separate. F-API's Task 0 separated them by measurement — a serverless task resolved a public
> host, got HTTP 200 and pulled 192,973 bytes, and `requests` calls now run in production from
> a job task. **Egress is not what failed the ~300 MB pyspark fetch.** The decision below is
> unchanged; only the attributed cause narrows, and it narrows to the half that was always the
> more likely of the two.

A prior fix moved `pyspark`/`delta-spark` into the PEP 735 `dev` dependency
group. Review found this mechanism wrong: dependency groups are a
uv/PEP 735 local-workflow concept and are invisible in the built wheel's
metadata (`Requires-Dist`/`Provides-Extra`) — pip installs and SBOM/dependency
scanners consuming the published wheel would see no indication that Spark is
part of this library's dependency surface at all.

## Decision
Declare `pyspark==3.5.9` and `delta-spark==3.3.1` as the `spark` optional
extra (`[project.optional-dependencies]`) instead of a dev-only group. This
keeps the dependency visible in wheel metadata (`Provides-Extra: spark`,
`Requires-Dist: pyspark==3.5.9; extra == "spark"`) for pip/SBOM consumers,
while still keeping the bare wheel install clean for Databricks:
- Local dev and CI install with `uv sync --all-groups --all-extras` (extras
  pull in pyspark/delta-spark; groups still cover pytest/ruff/etc.).
- Databricks jobs install the bare wheel (no extras) via
  `environments.dependencies`, so the serverless-vs-pyspark conflict from the
  Task 6 spike never recurs.

## Consequences
- `pip install open-payments-lakehouse` (bare) does not pull Spark. Importing
  `opl.spark` or `opl.bronze.*` from a bare install then fails with a normal
  `ImportError` — documented behavior, not a bug; consumers who need the
  Spark-backed modules must install with the `spark` extra
  (`pip install open-payments-lakehouse[spark]`).
- The Databricks-side install stays exactly as clean as the previous
  dev-group approach, but the dependency is now discoverable by anyone
  inspecting the package's published metadata.
