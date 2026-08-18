# F4 — DataOps: what was measured before anything was built

**This document holds Task 0's measurements and, as the phase runs, its predictions and what the
runs said.** It lives in `docs/` and not in `.plans/` deliberately: `.plans/` is git-ignored and this
repository is public, and F3 shipped a section pointing a public reader at that directory, where they
reached nothing.

**Controller-verified** means the controller ran the command in this session and read the output.
**Reported** means an audit lens, a task's stdout or a subagent's report said it. Every claim below
carries one of the two labels. Revision 1 of this phase's plan said *"measured"* nineteen times and
never once said by whom, and four of its five worst errors were single-sourced §0 figures — this
split is the repair.

**Provenance is given as the re-runnable command, not as a statement id.** F-DB §0.1 measured that
`GET /api/2.0/sql/statements/<id>` returns `Not Found` for a statement **seconds** old, with a
control, so a published statement id is not a durable handle in this workspace. A job `run_id` is —
7 of 7 at ~9 days, and 51 of the 71 live run ids are cited by number in committed `docs/` files with
nothing aged out. **So run ids are cited; SQL is quoted; statement ids are given only where they were
recorded at the time.**

**Predictions are published BEFORE the run that tests them** (master protocol §4.5). A number first
written down after the run that produced it is not a prediction. §2 is where they live.

---

## 0. Task 0 — measured before a line was built

Four independent measurement lenses ran before the plan existed, and four independent audits then
read the plan they produced. **The audits returned five CRITICAL and five BLOCKING findings, and
three of them reversed a ruling rather than a figure.** What follows is what survived, labelled.

### 0.1 THREE RULINGS WERE REVERSED BY MEASUREMENT, NOT BY OPINION

**Controller-verified.** Each of these was a sentence in the phase plan's revision 1; each is false;
each was catchable by one command.

| revision 1 asserted | measured | consequence |
|---|---|---|
| a graded DQ gate would **activate** a delete path that had never executed — filed as the phase's *hidden cost* | the delete path executed on 2026-07-31 and removed **16,743,815,717 B** with `refused=0`, and ADR 0006 books its unreachability as *"the defect this decision must not leave standing"*, with **8.21 GB** parked and a **~48 GB/month** floor | **reaching reclaim is the product.** The phase's headline moved from grading the gate to wiring the reclaim |
| *"there is no un-compacted baseline left to optimise from"* | `link_empresa_estabelecimento` — **512 files, 7,201,236,749 B, one Delta version, never compacted** — is the largest table in the vault by bytes | the performance case study's refusal was withdrawn and the benchmark reinstated |
| schema and catalog grants belong in the Databricks Asset Bundle | the repo's only target is `mode: development`, which **renames** the schema to `dev_<prefix>_default`; a production target **collides** (`Schema 'default' already exists`); and declared grants are **authoritative**, so the first deploy would revoke the platform's own `CREATE TABLE` on the schema every pipeline writes into | governance is **all imperative**. The bundle half was deleted rather than fixed |

**A fourth reversal is the plan's, not an audit's**, and it removes an ordering dependency all four
audits inherited: the re-derivation the phase needs runs over **staging**, which this project
deliberately never masks (`masking.py:155-171`, pinned by
`tests/bronze/test_masking.py::test_the_control_covers_bronze_and_quarantine_and_never_staging`).
One audit called the UC mask a blocker on 64% of that corpus and re-ordered the phase's tasks to
repair it first. **The corpus was the wrong table.**

### 0.2 THE QUARANTINE CENSUS, AND THE SHADOWING THAT MEASURES ZERO

**Controller-verified**, 2026-08-18, statement `01f19b1b-88a3-1ce0-9afb-44022b43842e`:

| quarantine | `_dq_reject_reason` | rows | distinct `_source_file` |
|---|---|---|---|
| `bronze_cnpj_socios_quarantine` | `null_or_empty_nome_socio_razao_social` | 3,583 | **20** |
| `bronze_payments_quarantine` | `rescued_data_present` | 2,000 | 1 |
| `bronze_cnpj_estab_quarantine` | `encoding_replacement_char` | 4 | 2 |
| `bronze_cnpj_empresas_quarantine` | `null_or_empty_razao_social` | 2 | 2 |

**5,589 rows, four reasons, one per table.** Four of seven quarantines hold rows.

**The under-report the phase was commissioned to expose is a construction, not an event, and it was
already measured.** ADR 0006:325-328, on 2026-08-03, published `fffd_any / req_blank / both` over
both staging months of all three CNPJ tables: estabelecimentos 4/0/0 and 4/0/0, empresas 0/1/0 twice,
socios 0/1797/0 and 0/1786/0. **`both = 0` in all six cells**, pinned as a live property by
`tests/bronze/test_rules.py::test_a_blank_required_column_hides_the_lost_byte_behind_it`.

**What that leaves genuinely missing** — and it is ADR 0006's own condition 1 — is per-reason counts
derived from **all** matching rules rather than the first, over the four contracts that sweep never
covered: payments, ptax, merchant and lookup. That is this phase's Task 3.

**One number the census does not show:** `fail_on_dq` fired **eleven** times across **five** jobs, and
`bronze_cnpj_lookup_quarantine` — whose contract fired three of the eleven — holds **zero** rows,
because that table was recreated on 2026-07-31, a week after its firings (**reported**, provenance
lens). **Three quarantines are empty, not two**, and the joint-most-blocking contract in the
workspace contributes nothing to any retroactive classification.

### 0.3 THE TELEMETRY EXISTS; THE VIEW IS THE DELIVERABLE

**Controller-verified**, 2026-08-18:

```sql
SELECT COUNT(*), COUNT(DISTINCT run_id) FROM system.lakeflow.job_task_run_timeline;
-- 270 rows / 260 distinct task runs

SELECT COUNT(*), COUNT(DISTINCT t.run_id)
FROM system.lakeflow.job_task_run_timeline t
JOIN system.query.history q ON q.query_source.job_info.job_task_run_id = t.run_id;
-- 674 rows / 136 matched task runs
```

Three properties decide how the view is written, and revision 1 of the plan had none of them:

1. **The join key is `t.run_id`.** `t.job_run_id` yields zero rows *silently*; `t.task_run_id` does
   not exist. (**Reported**, buildability lens; the working key is controller-verified above.)
2. **Coverage is 136 of 260 — 52%.** The tasks that match nothing are the ones that issue no SQL:
   `assert_deployed_revision`, `check_bad_rows`, `fail_on_dq`, `unzip`, `smoke`. **`fail_on_dq`, the
   phase's headline DQ event, has zero rows in `system.query.history`.**
3. **The join fans out ~5×.** Any per-task metric read straight off it is multiplied by the statement
   count, so the `query.history` side is aggregated to task-run grain before anything reads it.

**Both sources are MANAGED tables** in `system.information_schema.tables` (**reported**, provenance
lens). The plan's revision 1 heading said *"it is a view, not a table"* of the **sources**; the view
is what F4 builds. Corrected before it shipped.

**Cost, corrected.** There is no 24–29 s per-task floor: that range is the interquartile spread of
**one** task (`assert_deployed_revision`, 15–33 s over 37 runs), and **88 of 268 task runs finished
under 24 s**, with `fail_on_dq` at 14–18 s (**reported**, provenance lens). The conclusion survives
in weaker form — a new task costs ~15–30 s, a large fraction of the smallest jobs — and it is why
this phase adds exactly one job task.

### 0.4 THE UN-COMPACTED BASELINE THAT WAS SAID NOT TO EXIST

**Controller-verified**, 2026-08-18, statement `01f19b1b-5e9b-149b-9c91-f9eabd790329`:

```
DESCRIBE DETAIL workspace.default.link_empresa_estabelecimento
  numFiles 512 | sizeInBytes 7,201,236,749 | partitionColumns [] | clusteringColumns []
  createdAt 2026-08-10T22:07:15Z | lastModified 2026-08-10T22:15:06Z
```

and, grouping `system.storage.predictive_optimization_operations_history` by table and operation,
**Predictive Optimization has run COMPACTION on nine tables and never on this one** — only an
`ANALYZE`, 2026-08-11T02:46:14Z. 512 files at ~14 MB each is exactly what
`docs/f2-wave-1-workspace-run-evidence.md:340` recorded at write time, eight days ago.

PO is **not** a past event: it ran `ANALYZE` as recently as 2026-08-18T06:55Z and `VACUUM` at
2026-08-18T01:50Z (controller-verified in the same grouping). Any baseline taken without disabling it
per table can be rewritten under the run.

**Why PO compacted nine tables and declined the largest is UNMEASURED**, and is worth exactly one
probe, not a guess.

### 0.5 THE LANDING RESIDUE, AND THE FACT THAT IT IS STILL REACHABLE

**Controller-verified**, 2026-08-18, `databricks fs ls --output json` summed per directory:

| Volume path | files | bytes |
|---|---|---|
| `cnpj/2026-06/empresas` | 10 | **5,359,720,597** |
| `cnpj/2026-06/socios` | 10 | **2,852,557,826** |
| `cnpj/2026-06/estabelecimentos` | 0 | 0 — reclaimed 2026-07-31 |
| `cnpj/2026-07/{empresas,socios,estabelecimentos}` | 0 | 0 |
| **total residue** | **20** | **8,212,278,423** |

matching ADR 0006's booked 8.21 GB **to the byte**. The `zips/` siblings exist for all three groups,
so `reclaim_landing`'s way-back-to-the-source argument holds.

**And the residue is reachable from bronze, which is the finding this phase is built on.**
Controller-verified:

```sql
SELECT _batch_id, _snapshot_month, COUNT(DISTINCT _source_file), MIN(_source_file)
FROM workspace.default.bronze_cnpj_empresas GROUP BY 1,2;
-- 321750543973966 | 2026-06 | 10 | /Volumes/.../cnpj/2026-06/empresas/K3241.K03200Y0.D60613.EMPRECSV
-- 371067950667703 | 2026-07 | 10 | /Volumes/.../cnpj/2026-07/empresas/K3241.K03200Y0.D60711.EMPRECSV
```

and socios batch `1121645114029617` = 2026-06 over 10 files, estabelecimentos batch
`118868417561350` = 2026-06 — which is verbatim the batch F1.4a's reclaim already emptied
(`docs/f1.4a-migration-evidence.md:472`).

`retention.files_of_batch` reads **bronze**, so for the two 2026-06 batches it returns those twenty
files **today**. **The standing 8.21 GB is not a backlog needing a bespoke operator delete; it is one
job run away**, and the audit finding that said otherwise reasoned from *future* batches, whose
checkpoints are indeed consumed, rather than from the original batch, which is still in bronze.

**Checked and found true, not a defect:** the empty 2026-07 directories are the result of **three
operator hand-deletes recorded as such** — `docs/f1.4b-pr-b-run-evidence.md` §19, *"The hand-delete
between groups — an operator action, not reclaim working"*. Nothing silently deletes landing files,
and the ~48 GB/month floor survives as a projection of what accumulates when nobody hand-deletes.

### 0.6 GOVERNANCE: THE MASK CANNOT BE OPENED, AND ONE CLAIM ABOUT IT WAS WITHDRAWN

**Controller-verified.** `masking.py:63` uses `is_account_group_member('opl_pii_readers')`; the
`information_schema.routines` definition is unchanged; the owner reads `***` through it right now.
**The permissive branch of this project's mask remains untested by construction.**

**A claim to the contrary was published on this branch on 2026-08-18 and withdrawn the same day,
before it merged** (`4478d07`). What had been measured is that **a** UC column mask discriminates
between two principals — on a scratch mask with a different predicate — not that **this** one does.
The withdrawn claim had no statement id, no SQL and no surviving object, and the service principal it
used has since been deleted. **It was caught by an audit reading the PR's own text.**

**Reported** (platform lens), and load-bearing for the repair:

- `is_member` behaves identically inside a serverless job session, as the user and as a `run_as`
  service principal, and **fails closed for a group that does not exist** — so substituting it trades
  an unopenable control for an openable one with the same floor.
- A workspace-local group works **in a mask predicate** and is **refused as a grant principal**
  (`PRINCIPAL_DOES_NOT_EXIST`); the only account group that resolves is `account users`, i.e.
  everyone. **So "RBAC here" necessarily means a workspace-local group in the predicate plus `SELECT`
  granted per service principal.** There is no group in this workspace that can be both.
- **Removing a principal from a group does not close the mask promptly** — a measured fail-open
  window of **> 3 m 50 s, ≤ ~17 min**, with a fresh OAuth token on each read, so it is server-side
  membership caching. Addition propagates immediately; removal does not, which is the unsafe
  direction.

**Unbacked and not to be cited without a re-run:** the two-principal transcript (principal deleted),
the ABAC demonstration (policy removed), and the "70 governed tag policies" (no endpoint named, and
the provenance lens could reach none of `/api/2.0/tag-policies`, `/api/2.1/tag-policies`,
`/api/2.0/account/tag-policies`).

### 0.7 THE CACHE FLAG HAS THE SAME DISEASE AS THE MANIFEST, BIASED AGAINST THE RUNS THAT MATTER

**Reported** (platform lens), and it is the fifth instance of this repository's signature error:
`/api/2.0/sql/history/queries?include_metrics=true` returns `status: FINISHED` with **4** metric keys
and `result_from_cache: null`, filling to **29** keys about 3 s later — **but a cached result is
complete on the first read**. Only the *uncached* runs transiently read `None`, which is the
identical shape to the structural absence `.plans/sql.sh`'s own header retracts.

**So: poll until the flag is non-null, discard a run whose flag never fills, and never read `null` as
`False`.**

**Controller-verified, and it is a finding about this phase's own tooling:** the helper the audit and
the session prompt both name as the thing to fix — `.plans/q.sh` — **does not exist**. Neither does
`.plans/sp_sql.py`, cited by a second audit. `find` over the repository, `.venv` excluded, returns
nothing for either. They were session artefacts that never landed on disk.

**An instruction pointing at a file that does not exist reads exactly like an instruction that was
carried out.** That is this project's second recurring species — a guard whose output cannot
distinguish *passed* from *never ran* — showing up in the tooling rather than the code. Every
`result_from_cache` figure taken through that helper is unverified.

### 0.8 SYSTEM-TABLE RETENTION: A MEASURED FLOOR, NOT AN UNKNOWN

**Reported** (platform lens): all seven system tables this phase reads have their earliest row within
hours of workspace creation and **nothing has been trimmed**. So the retention floor is **~25 days**
and the ceiling is unmeasurable, because the workspace is younger than any documented horizon.
Databricks documents 365 days; that is documentation, not a measurement. **The durability guard runs
while the phase runs**, because it is what would catch the first trim.

### 0.9 WHAT REMAINS UNMEASURED

Listed here so the phase's close can say which of these it changed:

- whether a `bundle deploy` actually fails against the existing `workspace.default` schema (assumed
  from the underlying API's `already exists`);
- whether bundle-declared grants actually revoke undeclared ones;
- whether the platform stops maintaining a table whose PO flag reads `DISABLE` (PO's cadence is hours
  to days, so it is not settleable in a session — the flag is the request, the ops-history table is
  the receipt);
- the exact revocation-propagation delay, bounded to 3 m 50 s – ~17 min;
- why PO compacted nine tables and declined the largest;
- the RFB's actual publication cadence for the 2026-07 lookup zips — `f1.4b-pr-b` §25.5 records only
  that they are not on disk, so "source freshness" for `bronze_cnpj_lookup` is not yet a defined
  quantity.

---

## 1. What each task must produce

*(Filled as the phase runs. The plan is `.plans/2026-08-18-f4-dataops.md` revision 2.)*

---

## 2. Predictions, published before the runs that test them

### 2.1 The reclaim decoupling — the phase's headline

**Published 2026-08-18, before any run.** The prediction is what a `reclaim_landing` invocation
scoped to each 2026-06 batch will report and what the Volume will hold afterwards:

| table | batch | predicted output | predicted bytes |
|---|---|---|---|
| empresas | `321750543973966` | `deleted=10 already_absent=0 failed=0 refused=0` | **5,359,720,597** |
| socios | `1121645114029617` | `deleted=10 already_absent=0 failed=0 refused=0` | **2,852,557,826** |

and afterwards `cnpj/2026-06/{empresas,socios}` list **zero files**, while `cnpj/2026-06/zips/` still
holds all three groups' zips, untouched.

**What would falsify it, and each is a real outcome rather than a hedge:** a non-zero `refused` means
`LandingScope`'s containment rejected a path that bronze names — which would be a defect in the
month derivation, not in the delete; a non-zero `already_absent` means something removed files this
document listed; and `deleted < 10` on either table means `files_of_batch` does not return what the
Volume holds, which would falsify §0.5's central claim.

### 2.2 The all-matching-rules sweep

**Published 2026-08-18, before any run.** Over all seven contracts and every staging batch, evaluating
every rule rather than the first: **the count of rows matching ≥2 rules is predicted to be zero
everywhere**, extending ADR 0006's six measured cells from three contracts to seven.

**A non-zero count anywhere falsifies ADR 0006's "the hole is latent, not open" for a contract it
never covered**, and is the more valuable outcome of the two.

### 2.3 The compaction benchmark

**Published before the run**, with its mechanism beside it so a confirmation is worth something:
bin-packing does not reduce the bytes a full aggregate must read, so the predicted shape on
`link_empresa_estabelecimento` is **`read_files_count` down sharply, `read_bytes` roughly unchanged,
and wall clock improved only if per-file task overhead dominates**. The point-lookup null result
already falsified the mirror-image hypothesis — 22–39× fewer bytes and no movement in the clock — so
this prediction is deliberately the one that measurement has most reason to refuse.

---

## 3. What is still unexercised

*(Filled at the phase's close, per protocol §9 condition 6.)*
