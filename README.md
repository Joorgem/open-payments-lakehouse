# open-payments-lakehouse

![CI](https://github.com/Joorgem/open-payments-lakehouse/actions/workflows/ci.yml/badge.svg)

A lakehouse that ingests **real, messy Brazilian government data** (Receita
Federal's CNPJ registry) through a PySpark/Delta core tested locally and in
CI, dual-targeted at Databricks Free Edition with its real limits documented
instead of glossed over. It's building toward a Data Vault 2.0 silver layer
and a Kimball star schema on top.

**Status:** F1.3 complete (42.8M rows of real Estabelecimentos data in Delta,
ingested incrementally with a batch-scoped DQ gate) — Silver (Data Vault 2.0)
next.

## Why this isn't a tutorial repo (true today)
- **Real, messy source data**, not a clean CSV fixture: RFB's CNPJ registry is
  served over Nextcloud/WebDAV (not a plain file server), `cp1252`-encoded,
  `;`-delimited, headerless, split into multi-part monthly snapshots — and the
  live share is flaky (~50% transient HTTP 500s observed). The extraction
  client handles this for real: PROPFIND directory listing, Range-based
  resume, size-integrity checks, and retry-with-backoff — see
  [`docs/adr/0003-cnpj-extraction-layer.md`](docs/adr/0003-cnpj-extraction-layer.md)
  and a real, verbatim run in
  [`docs/f1-extraction-evidence.md`](docs/f1-extraction-evidence.md).
- **A PySpark/Delta core exercised on two targets, not just described**: unit
  tests + a Delta roundtrip run locally (`uv run pytest`) and in CI on every
  push; the same library also deploys to Databricks Free Edition. Version
  drift between the two targets fails CI (`opl._version_check`) instead of
  surfacing in production — see
  [`docs/adr/0001-dual-target-versions.md`](docs/adr/0001-dual-target-versions.md).
- **Honest Databricks Free Edition constraints, verified, not assumed**:
  serverless compute blocks outbound internet to untrusted domains (no CNPJ
  download from inside Databricks), so extraction and landing run off-platform
  and only reach Databricks through a control-plane PAT upload into a Unity
  Catalog Volume — see
  [`docs/adr/0002-two-layer-topology.md`](docs/adr/0002-two-layer-topology.md).

## Built vs Roadmap

| | Status |
|---|---|
| **Built (F0 + F1.1 + F1.2 + F1.3)** | Autonomous dev harness (uv lockfile, CI, secret scanning, dual-target version guard) · Docker stack (Redpanda + Postgres) · Databricks Unity Catalog Volume landing via a validated control-plane path · **CNPJ extraction → landing → versioned schema contracts** (WebDAV client with resume + retry, real data landed to a UC Volume) · **Bronze via Auto Loader → Delta with a blocking data-quality gate + quarantine**, deployed as a real 5-task Databricks Job (Asset Bundle) and **run for real** on Databricks Free Edition against the landed lookup files, including a deliberately corrupted batch that the gate blocked — verbatim evidence in [`docs/f1.2-bronze-run-evidence.md`](docs/f1.2-bronze-run-evidence.md) · **Multi-gigabyte Estabelecimentos ingestion: 42.8M rows in Delta**, staged incrementally across runs (Auto Loader checkpoint picks up only new files), unzipped in-Volume on Databricks to stay under the Files API single-PUT ceiling, with a **batch-scoped** DQ gate and a triaged-batch re-promotion path — and three real incidents caught and fixed along the way, including a silently short-written upload and a CSV reader that split records on newlines inside quoted fields: [`docs/f1.3-estabelecimentos-run-evidence.md`](docs/f1.3-estabelecimentos-run-evidence.md), [ADR 0005](docs/adr/0005-csv-multiline-parallelism-ceiling.md), [ADR 0006](docs/adr/0006-bronze-dq-gate-policy.md) |
| **Roadmap — next** | Silver Data Vault 2.0 (hubs/links/sats, historized snapshots) |
| **Roadmap — later** | Gold Kimball star (SCD2 dims, event-grain facts) · Unity Catalog governance (RBAC, column masking, lineage) · pipeline observability · an AI-assisted incident-triage (RCA) agent |

The Data Vault / Kimball / Unity Catalog governance / AI-agent items above are
**roadmap, not built** — they are planned, not present in this repo's code
today.

## Topology (two layers, by necessity)

Databricks Free Edition serverless compute blocks outbound internet to
untrusted domains, so extraction can't run on Databricks — it runs off it and
lands data through the control plane instead:

```
EXTRACTION & LANDING (off Databricks: local / GitHub Actions — have internet)
  CNPJ WebDAV share --> extractor (resume/retry/checksum) --> local files
                                                                  |
                                       upload via Databricks SDK (PAT)
                                                                  v
                                          Unity Catalog Volume (control plane)
                                                                  |
TRANSFORMATION (Databricks Free Edition: serverless, UC, Jobs)   v
  Bronze (Auto Loader) --> Silver (Data Vault 2.0) --> Gold (Kimball star)
```

## Run locally

Local Spark needs a JDK. On Windows, it also needs Hadoop native bits that
`uv` does not manage: **JDK Temurin 17** (`JAVA_HOME`) and, on Windows,
Hadoop's `winutils.exe` + `hadoop.dll` (`HADOOP_HOME`, e.g. Hadoop 3.3.6 from
[`cdarlint/winutils`](https://github.com/cdarlint/winutils)) — see
[`CLAUDE.md`](CLAUDE.md#local-environment) for exact versions and how to
export both into a shell that doesn't already have them on `PATH`. Unix-like
systems need only the JDK.

```bash
uv sync --all-groups --all-extras
uv run pytest                       # unit tests (Delta roundtrip needs a JDK; green in CI)

docker compose up -d                # Postgres (host port 5433) + Redpanda
uv run pytest -m integration

uv run python scripts/extract_cnpj.py --month 2026-06   # real download + unzip + UC Volume upload
```

The extraction command downloads and unzips locally either way. Uploading to
the UC Volume needs Databricks credentials in a git-ignored `.env`
(`opl-free` profile); pass `--no-upload` to just download and unzip.

## Engineering approach

Design decisions live as ADRs in [`docs/adr/`](docs/adr/); a real run's
verbatim output is captured in
[`docs/f1-extraction-evidence.md`](docs/f1-extraction-evidence.md). This
project is built with AI-assisted engineering under human review — every ADR
and design call is one I can walk through and defend.
