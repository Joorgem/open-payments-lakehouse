# open-payments-lakehouse

![CI](https://github.com/Joorgem/open-payments-lakehouse/actions/workflows/ci.yml/badge.svg)

A lakehouse over **real, messy Brazilian government data** — Receita Federal's CNPJ
registry — plus a synthetic payment stream. It is a PySpark/Delta library that runs
locally and deploys to Databricks Free Edition as Asset Bundle jobs: bronze through Auto
Loader behind a blocking data-quality gate, a **Data Vault 2.0** silver layer, a **Kimball
star** on top, Unity Catalog column masking on real personal data, derived DataOps views,
and a deterministic incident-triage agent that **drafts** a GitHub issue and files none.
Nothing in the workspace posts anything: the agent emits the issue as JSON on a task's
stdout, and a person publishes it from their own machine with `gh` — so no token that can
write to this repository is ever reachable from the workspace holding 55.8M rows of real
personal data. Nothing on the platform can post.

**Every count in the *What is built* table below is derivable by a command; the commands
are collected in [Re-deriving these numbers](#re-deriving-these-numbers), and
[`tests/test_readme_counts.py`](tests/test_readme_counts.py) re-derives them on every CI
run and fails when the table drifts.** That rule is the point, not a flourish.
For seven phases this file stated that the Data Vault, the Kimball star and the
Unity Catalog governance were *"roadmap, not built"* while all three were merged and
public — a disclaimer that was believed precisely because a document volunteering its own
limits reads as honest. It was filed as an issue against this repository
([#25](https://github.com/Joorgem/open-payments-lakehouse/issues/25)) and then survived
three more merges. **A document that describes a project reports the expected value for
any project; the only defence is that a reader can re-run it.**

## What is built

| layer | what is in the repository | count |
|---|---|---|
| **Bronze** | registered tables, each with a staging/bronze/quarantine triple, an Auto Loader read, a batch-scoped DQ gate and a quarantine table | **7** |
| | distinct landing modes (`local`, `zips`, `generated`, `api`, `postgres`) | **5** |
| **Silver — Data Vault 2.0** | hubs · links · satellites · effectivity satellites · reference tables | **18** (3 · 3 · 4 · 2 · 6) |
| **Gold — Kimball** | `dim_company` (SCD2), `dim_date`, `dim_channel`, `dim_currency`, `fact_payment`, `pit_estabelecimento` | **6** |
| **DataOps** | derived views: reconciliation, per-file reconciliation, task telemetry, freshness | **4** |
| **Deployment** | Databricks Asset Bundle jobs / their tasks | **21 / 99** |
| **Decisions** | ADRs in [`docs/adr/`](docs/adr/) | **20** |
| **Evidence** | run-evidence and validation documents in [`docs/`](docs/) | **20** |
| **Tests** | selected by a default `uv run pytest`, of 3,330 collected (the rest need Docker) | **3,283** |

The data it has actually held: **337,712,651 rows** of CNPJ bronze across `empresas`,
`socios` and `estabelecimentos`, at **two monthly snapshots each** (2026-06 and 2026-07).
Those six figures are in [`docs/f1.4b-pr-b-run-evidence.md`](docs/f1.4b-pr-b-run-evidence.md)
§21.2, and are re-confirmed there from a second, independent query in §21.3.

## The four sources, and how each one arrives

Four kinds of source are ingested — files, event streams, APIs, databases — and **they do
not all arrive the same way.**

| source | what it is | how it lands | bronze table |
|---|---|---|---|
| **Files** | CNPJ monthly snapshots: `cp1252`, `;`-delimited, headerless zips — ten parts each for the three large tables — served over a Nextcloud/WebDAV share that returns transient HTTP 500s about half the time | extraction host downloads with PROPFIND listing, Range-resume and size checks → PUT into a Unity Catalog Volume over the control plane → `unzip_table.py` expands them **in-Volume** | `bronze_cnpj_empresas`, `_socios`, `_estabelecimentos`, `_lookup` |
| **Event streams** *(landed as files)* | a deterministic synthetic payment stream with defects injected on purpose | `generate_payments.py`, a serverless job task, writes JSON Lines into the Volume; Auto Loader reads the **directory**, not a broker — the managed Kafka read is a separate thing with its own table, below | `bronze_payments` |
| **APIs** | the BCB/Olinda PTAX USD/BRL series | `fetch_ptax.py`, **a serverless job task**, asks the endpoint directly — 60 HTTPS requests in 52 s ([`docs/f-api-run-evidence.md`](docs/f-api-run-evidence.md) §2.5) | `bronze_ptax` |
| **Databases** | a Postgres merchant registry in a container on the development machine | one `REPEATABLE READ READ ONLY` transaction on the extraction host — the snapshot plus the instant that read it → Volume | `bronze_merchant` |

**The Kafka read is a separate thing and must not be folded into the row above.**
`bronze_payments` is Auto Loader over generated *files*. A managed Kafka topic is also
read — `stream_managed_broker.py` completes a SASL_SSL/SCRAM handshake from a serverless job
against a hosted Redpanda cluster — but it writes its own table,
`streaming_payments_managed_broker`, deliberately kept out of the bronze registry so it
cannot leave a permanent row in a freshness view for a broker that is about to expire.
Saying "event streams" without that split is a claim this project has already had to
correct once.

## What is distinctive

Files into a bronze table is table stakes. These are not:

- **A disappearance-driven effectivity satellite.** The RFB's partner file carries no
  partner key and no delete signal — a partner leaves by *not being in next month's
  snapshot*. `sat_eff_company_partner` derives the departure from the absence rather than
  waiting for an event that never comes.
  [ADR 0011](docs/adr/0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md)
- **An as-of-known-time star.** `dim_company` is append-only SCD2 and
  `pit_estabelecimento` resolves a hub's satellites at a stated `as_of_date`, so a query
  can ask what the warehouse *knew* on a date instead of what is true now.
  [ADR 0015](docs/adr/0015-as-of-known-time-and-append-only-scd2.md)
- **FX resolved by publication instant, not by a holiday calendar.** PTAX stamps a quote
  with the instant it was *published*, which is not the date it was requested for — a
  quote asked for 1984-11-28 comes back stamped 1984-12-03. The fact carries the requested
  date and resolves the rate by the published one.
  [ADR 0016](docs/adr/0016-fx-resolved-by-publication-instant-not-a-holiday-calendar.md)
- **A live Unity Catalog column mask over real personal data.** The two name columns on
  `socios` are masked by `is_member('opl_pii_readers')`, and the table is created empty and
  masked *before* the first write — a table created by its first write holds names in the
  clear until the mask arrives.
  [ADR 0008](docs/adr/0008-pii-masking-socios.md)
- **Exactly-once tested by killing a process** between the data commit and the offset
  commit, with a negative control that was supposed to duplicate and did: the naive arm
  landed 39 rows for 29 deliveries — 10 duplicates, and they are exactly the killed
  batch's offsets, each landed twice — while the guarded arm landed 29 for 29. **That
  result is labelled *Reported*, not *Controller-verified*** — an implementer produced it
  and the independent reviewer reproduced it twice — and it carries that label because an
  earlier draft claimed the stronger one and a late reader caught it.
  [ADR 0019](docs/adr/0019-the-proof-runs-where-a-process-can-be-killed.md),
  [`docs/f5-run-evidence.md`](docs/f5-run-evidence.md) §2.2
- **A triage agent that is deterministic, with a language model as its control arm** — and
  the control's four sweeps, **20 trials and 170 verbatim responses**, are committed
  ([`docs/f6-llm-control-responses.json`](docs/f6-llm-control-responses.json)) **for where
  the two disagree, in both directions.** The model declined correctly on a fabricated
  incident 5/5 — falsifying this project's own published prediction that it would not — and
  reproduced the size ladder 30/30. Then it inverted the one word that decides the verdict:
  `does_not_reconcile` came back only on incidents that have **no** reconciliation row, and
  never once on `592660596679630`, the one incident that genuinely does not reconcile, where
  the shipped classifier answers `does_not_reconcile` → `hold_do_not_promote`. The agent's
  own drafted output on that incident is
  [issue #29](https://github.com/Joorgem/open-payments-lakehouse/issues/29).
  [ADR 0020](docs/adr/0020-the-triage-accelerator-is-deterministic-and-the-model-runs-as-the-control.md)
- **Twenty of the twenty-one bundle jobs open with `assert_deployed_revision`,** which
  refuses to run against a wheel nobody deployed — because an Auto Loader checkpoint moved
  by the wrong artefact marks those files seen for every later run.
  [ADR 0009](docs/adr/0009-deployed-revision-provenance.md)

## Honest limits

- **CI runs on pull requests and on pushes to `main`**, not on every push. A push to a
  branch with no PR open runs nothing — see `.github/workflows/ci.yml`.
- **Schedules are declared, and not one of them can fire.** Which jobs declare one:
  `git grep -l quartz_cron_expression -- databricks/resources/`. No committed bundle file
  writes `pause_status` — the target's `mode` writes it, and the target this repository
  deploys is `mode: development`, under which the CLI renders `PAUSED`. So every run this
  project has ever made was launched by hand. What a scheduled run would have to bind before
  it could fire is
  [ADR 0021](docs/adr/0021-the-deploy-binds-a-scheduled-runs-expected-revision.md).
- **The payment fact does not go through the vault.** `fact_payment` loads from
  `bronze_payments` and resolves its two company roles against `dim_company`, which *is*
  built from the vault's `sat_empresa_dados`. The vault covers the CNPJ and merchant
  domains; there is no payment hub.
- **The permissive branch of the mask has never been read.** `opl_pii_readers` has zero
  members, so every reader gets `***` and nobody has exercised the other arm. Doing so
  would mean granting a principal `SELECT` on tens of millions of rows of real personal
  data; it was proven on a purpose-built throwaway instead, and ADR 0008 says so rather
  than claiming both halves.
- **The managed Kafka cluster is a trial** whose credits expire around 2026-09-03. When it
  stops answering, `stream_managed_broker.py` will fail to connect — that is a dead trial
  account, not a regression here, and this repository will say so once it has been probed.
- **This runs on Databricks Free Edition**, serverless only, with its limits recorded
  rather than worked around.

## Topology, and the premise this README got wrong

Extraction for two of the four sources runs off Databricks. **Not because serverless is cut
off from the internet.** This document said that for four phases and called it *verified*.
A later phase measured a serverless task reaching the public internet: HTTP 200 and a
220-byte quote from BCB/Olinda, plus **192,973 bytes from a second, unrelated host** — and
the second host is what makes that general egress rather than one allowlisted domain
([`docs/f-api-run-evidence.md`](docs/f-api-run-evidence.md) §0.8). It then shipped an API
fetch as a production job task. **What survives is per-source and concrete:** the CNPJ
share needs Range-resume over multi-gigabyte downloads and unzipping before landing, and
the Postgres source is a container on a development machine behind a home NAT — egress
*out* of Databricks creates no route *in*, which is the opposite direction. That half is
labelled *argued*, not measured, because there is no address to point a probe at.
[ADR 0002](docs/adr/0002-two-layer-topology.md) carries the amendment and the measurement
that forced it.

```text
OFF-PLATFORM (the extraction host) -- because of the SOURCE, not because of egress
  CNPJ WebDAV share --> extractor (PROPFIND, Range-resume, retry, size check)
  Postgres :5433    --> one REPEATABLE READ READ ONLY txn (+ the instant that read it)
                                  |
                    PUT via the Databricks SDK (PAT, control plane)
                                  v
                        Unity Catalog Volume
                                  |
ON-PLATFORM (Databricks Free Edition: serverless, Unity Catalog, Jobs)
  unzip_table.py           --> expands the landed zips inside the Volume
  generate_payments.py     --> writes the synthetic payment stream into the Volume
  fetch_ptax.py            --> asks BCB/Olinda directly, from a serverless task
  stream_managed_broker.py --> reads a managed Kafka topic over SASL_SSL, from a job
                                  |
                                  v
  Auto Loader --> staging --> batch-scoped DQ gate --> bronze | quarantine
       bronze --> Data Vault 2.0 (hubs, links, satellites, effectivity, reference)
              --> Kimball star (SCD2 dim_company, conformed dims, fact_payment, PIT)
              --> DataOps views (reconciliation, freshness, task telemetry)
              --> deterministic RCA triage --> a DRAFTED issue, as JSON on stdout
                                  |
OFF-PLATFORM again -- the credential boundary: nothing on the platform can post
  scripts/open_triage_issue.py --post  (a person, with `gh`; not packaged into the wheel,
                                        so no workspace task can even import it)
```

## Run locally

Local Spark needs a JDK — Temurin 17, the version CI installs. On Windows it also needs
Hadoop native bits that `uv` does not manage: `winutils.exe` and `hadoop.dll` from
[`cdarlint/winutils`](https://github.com/cdarlint/winutils) in
`%HADOOP_HOME%\bin`. Unix-like systems need only the JDK. If your shell does not already
carry them:

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"  # your JDK
export HADOOP_HOME="/c/hadoop"                                              # Windows only
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
```

```bash
uv sync --all-groups --all-extras
uv run pytest                       # unit tests, incl. a Delta roundtrip (needs the JDK)
uv run ruff check .

docker compose up -d                # Postgres on host port 5433 + Redpanda
uv run pytest -m integration

# a real download + unzip + Unity Catalog Volume upload
uv run python scripts/extract_cnpj.py --month 2026-06 \
  --groups Cnaes,Motivos,Municipios,Naturezas,Paises,Qualificacoes
```

The extraction command downloads and unzips locally either way; uploading to the Volume
needs Databricks credentials in a git-ignored `.env` (profile `opl-free`). Pass
`--no-upload` to download and unzip only.

`--groups` is spelled out because each landed file goes to the landing directory of the
bronze table that reads it, and that directory comes from `opl.bronze.registry`. The
default group set is the wider dev *recorte* of
[ADR 0003](docs/adr/0003-cnpj-extraction-layer.md), which also includes `Simples` — a table
bronze does not register, so there is nowhere in the Volume for it to land and the run is
refused before anything is downloaded. `--no-upload` captures the whole recorte, `Simples`
included.

The Asset Bundle is checked with `databricks bundle validate -t free`, run from
`databricks/` against the `opl-free` CLI profile.

## Where the record is

- **[`docs/adr/`](docs/adr/)** — one architectural decision per file, twenty of them. Where
  a decision's premise was later measured false, the correction is written into the file
  **beside** the original rather than replacing it; ADR 0002 is the clearest case.
- **[`docs/`](docs/)** — twenty run-evidence and validation documents, each recording
  what a run actually printed. **Thirteen of them** carry the labelling convention this project
  runs on: *Controller-verified* (someone ran the command and read the output) against
  *Reported* (a task's stdout, an implementer or an agent said so). The other **seven** are
  the oldest in the directory and predate the convention.

Neither list is reproduced here. A list of every evidence document inside a README is a list
of things to go stale, and that is the defect this file is recovering from.

## Re-deriving these numbers

```bash
# bronze / vault / gold / dataops counts
uv run python - <<'PY'
from collections import Counter
from opl.bronze.registry import REGISTRY as BRONZE
from opl.vault.registry import build_registry
from opl.vault.domains import DOMAINS
from opl.gold.registry import REGISTRY as GOLD
from opl.dataops.views import DATAOPS_VIEWS

vault = build_registry(DOMAINS)
print("bronze tables", len(BRONZE))
print("landing modes", sorted({t.landing for t in BRONZE.values()}))
print("vault tables ", len(vault), Counter(type(v).__name__ for v in vault.values()))
print("gold tables  ", len(GOLD))
print("dataops views", len(DATAOPS_VIEWS))
PY

# bundle jobs, their tasks, and the provenance guard
uv run python - <<'PY'
import glob, yaml

jobs = {
    name: job
    for path in glob.glob("databricks/resources/*.yml")
    for name, job in ((yaml.safe_load(open(path, encoding="utf-8")) or {})
                      .get("resources", {}).get("jobs") or {}).items()
}
tasks = [t for job in jobs.values() for t in job.get("tasks", [])]
first = [job["tasks"][0]["task_key"] for job in jobs.values() if job.get("tasks")]
print("jobs", len(jobs), "| tasks", len(tasks))
print("assert_deployed_revision first:", first.count("assert_deployed_revision"), "of", len(jobs))
PY

git ls-files 'docs/adr/0*.md' | wc -l              # ADRs
git ls-files docs | grep -c '^docs/[^/]*\.md$'     # evidence documents
uv run pytest --collect-only -q | tail -1          # tests selected / collected
git grep -l quartz_cron_expression -- databricks/resources/   # the jobs that declare a schedule
git grep -n 'pause_status:' -- databricks/         # nothing: the target's mode writes it
grep -n -A2 '^on:' .github/workflows/ci.yml        # what actually triggers CI

# how many evidence documents carry the Controller-verified / Reported convention
git ls-files docs | grep '^docs/[^/]*\.md$' | xargs grep -l 'Controller-verified' | wc -l

# the 337,712,651 rows: six cells, summed
grep -n -A6 '21.2 Row counts' docs/f1.4b-pr-b-run-evidence.md

# and all of the above, re-derived and compared against this file
uv run pytest tests/test_readme_counts.py -q
```

## Engineering approach

Built with AI-assisted engineering under human review; every ADR and design call is one I
can walk through and defend. The working rule is that a claim is closed by the probe that
closes it — where something was later measured false, the measurement and the retraction
are committed beside the original rather than quietly replacing it.

Licensed under Apache 2.0 — see [`LICENSE`](LICENSE).
