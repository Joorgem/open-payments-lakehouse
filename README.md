# open-payments-lakehouse

![CI](https://github.com/Joorgem/open-payments-lakehouse/actions/workflows/ci.yml/badge.svg)

A lakehouse that ingests **real, messy Brazilian government data** (Receita
Federal's CNPJ registry) through a PySpark/Delta core tested locally and in
CI, dual-targeted at Databricks Free Edition with its real limits documented
instead of glossed over. It's building toward a Data Vault 2.0 silver layer
and a Kimball star schema on top.

**Status:** F1.3 complete (71.9M rows of real Estabelecimentos data in Delta,
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
- **Honest Databricks Free Edition constraints — and the one this project got wrong
  about itself**: extraction and landing run off-platform and reach Databricks only
  through a control-plane PAT upload into a Unity Catalog Volume. That topology is
  right and it is running. **The reason first given for it was not.** This README
  used to say serverless compute blocks outbound internet to untrusted domains, and
  called it *verified*; a later phase measured a serverless task resolving a public
  host, receiving HTTP 200 and pulling 192,973 bytes, and then ran an API fetch as a
  production job task. **The constraint that survives is network topology** — egress
  out of Databricks creates no route into a database on a laptop behind a home NAT,
  which is the opposite direction — and it is labelled *argued*, not measured, because
  there is no address to point a probe at. See
  [`docs/adr/0002-two-layer-topology.md`](docs/adr/0002-two-layer-topology.md), whose
  Context carries the amendment and the measurement that forced it.

## Built vs Roadmap

| | Status |
|---|---|
| **Built (F0 + F1.1 + F1.2 + F1.3)** | Autonomous dev harness (uv lockfile, CI, secret scanning, dual-target version guard) · Docker stack (Redpanda + Postgres) · Databricks Unity Catalog Volume landing via a validated control-plane path · **CNPJ extraction → landing → versioned schema contracts** (WebDAV client with resume + retry, real data landed to a UC Volume) · **Bronze via Auto Loader → Delta with a blocking data-quality gate + quarantine**, deployed as a real 5-task Databricks Job (Asset Bundle) and **run for real** on Databricks Free Edition against the landed lookup files, including a deliberately corrupted batch that the gate blocked — verbatim evidence in [`docs/f1.2-bronze-run-evidence.md`](docs/f1.2-bronze-run-evidence.md) · **Multi-gigabyte Estabelecimentos ingestion: 71.9M rows in Delta** (all 10 RFB parts, 4.9 GB compressed), staged incrementally across runs (Auto Loader checkpoint picks up only new files), unzipped in-Volume on Databricks so only the compressed third of the bytes crosses the wire ([ADR 0007](docs/adr/0007-multipart-upload.md) — originally forced by a single-PUT ceiling that belonged to the old SDK pin, not to the Files API), with a **batch-scoped** DQ gate and a triaged-batch re-promotion path — and four real incidents caught and fixed along the way, including a silently short-written upload and two CSV-parsing defects in our own reader that corrupted rows while every quality rule passed and the row count reconciled: [`docs/f1.3-estabelecimentos-run-evidence.md`](docs/f1.3-estabelecimentos-run-evidence.md), [ADR 0005](docs/adr/0005-csv-multiline-parallelism-ceiling.md), [ADR 0006](docs/adr/0006-bronze-dq-gate-policy.md) |
| **Roadmap — next** | Silver Data Vault 2.0 (hubs/links/sats, historized snapshots) |
| **Roadmap — later** | Gold Kimball star (SCD2 dims, event-grain facts) · Unity Catalog governance (RBAC, column masking, lineage) · pipeline observability · an AI-assisted incident-triage (RCA) agent |

The Data Vault / Kimball / Unity Catalog governance / AI-agent items above are
**roadmap, not built** — they are planned, not present in this repo's code
today.

## Topology (two layers, by necessity)

Extraction runs off Databricks and lands data through the control plane. **Not
because serverless is cut off from the internet — it is not, and this document
said so for four phases.** A serverless task has reached a public API and run it
in production. What keeps extraction off-platform is per-source and concrete:
the CNPJ share needs Range-resume over multi-gigabyte downloads and unzipping
before landing, and the Postgres source is a container on a development machine
behind a home NAT, which no amount of outbound egress reaches. See
[ADR 0002](docs/adr/0002-two-layer-topology.md).

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

# real download + unzip + UC Volume upload
uv run python scripts/extract_cnpj.py --month 2026-06 \
  --groups Cnaes,Motivos,Municipios,Naturezas,Paises,Qualificacoes
```

The extraction command downloads and unzips locally either way. Uploading to
the UC Volume needs Databricks credentials in a git-ignored `.env`
(`opl-free` profile); pass `--no-upload` to just download and unzip.

`--groups` is spelled out because each landed file goes to the landing directory
of the bronze table that reads it, and that directory comes from
`opl.bronze.registry`. The default group set is the wider dev recorte of
[ADR 0003](docs/adr/0003-cnpj-extraction-layer.md), which also includes `Simples`
— a table bronze does not register yet, so there is nowhere in the Volume for it
to land and the run is refused before anything is downloaded. `--no-upload`
captures the whole recorte, `Simples` included.

## Engineering approach

Design decisions live as ADRs in [`docs/adr/`](docs/adr/); a real run's
verbatim output is captured in
[`docs/f1-extraction-evidence.md`](docs/f1-extraction-evidence.md). This
project is built with AI-assisted engineering under human review — every ADR
and design call is one I can walk through and defend.
