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
files **today**. *(Written at Task 0. **`files_of_batch` no longer exists** — Task 2 replaced it with
`file_accounts_of_batch`, whose proof is strictly stronger; the sentence is left as measured and
corrected here rather than rewritten, and §1.3 records that the two proofs admit the same twenty
files on these two batches.)* **The standing 8.21 GB is not a backlog needing a bespoke operator delete; it is one
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

**So the reader was written rather than fixed** (`.plans/cache_flag.sh` + `.plans/cache_flag.py`,
git-ignored like the rest of the operator tooling). It polls until the flag is non-null, exits
non-zero on a flag that never fills, and prints `DISCARD … This is NOT a False` rather than a number.
**It failed closed twice while being built** — two shell-quoting bugs turned the parser into a
`SyntaxError`, and both times the wrapper printed `DISCARD` instead of a reading, which is the
behaviour being bought. The parser moved out of the heredoc for that reason.

**Five readings taken with it, controller-verified 2026-08-18. Two of the audit's three claims
reproduce exactly; the third did not reproduce, and one number moved that was said not to.**

| run | statement | `result_from_cache` | metric keys | `read_bytes` |
|---|---|---|---|---|
| A | `amount_brl > 0.0000031`, read after settling | **False** | 30 | **386,515** |
| B | **byte-identical re-run of A**, read immediately | **True** | 26 | **0** |
| C | fresh varied literal, read immediately | **False** | 29 | **386,504** |
| D–F | three further varied literals | **False** ×3 | — | **386,504 ×3** |

- **The cache behaviour reproduces exactly.** An identical statement returns `True` with
  `read_bytes 0`, `read_files_count 0` and 375 ms against 917 ms — which is precisely how a "268 ms
  baseline" gets published. **Varying a literal defeats the cache**, five times out of five.
- **The transient-`null` race did NOT reproduce.** Run C was uncached and read immediately, and its
  metrics were already populated at 29 keys on the **first** poll. The audit measured the race and a
  race is a race, so one non-reproduction refutes nothing — but the poll's justification rests on a
  reported measurement this controller could not reproduce in one attempt, and that is recorded
  rather than glossed. **The poll costs nothing and the failure it guards against is silent**, which
  is why it stays.
- **`read_bytes` is not quite the invariant T5 wants to denominate in.** Four of five uncached runs
  read **386,504** exactly; run A read **386,515**, eleven bytes more. Eleven bytes on 386 KB is
  0.003% and irrelevant to a 7.2 GB before/after — but the claim T5 inherits is *"byte-identical
  within a batch"*, and the honest version is **"byte-identical in four of five readings, with one
  11-byte excursion on the session's first touch of the table"**. The mechanism for the excursion is
  **unmeasured**; a first-touch metadata read is a guess, not a finding.

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

## 1. The tasks, as they ran

### 1.1 Task 1 — the reconciliation, and it found the stranding without being told

**Built:** `src/opl/bronze/reconcile.py`, `databricks/src/create_dataops_views.py`,
`databricks/resources/dataops_views_job.yml`, `tests/bronze/test_reconcile.py`, and one entry in
`tests/test_job_yaml_launch_guards.py`'s guard list. Two views, no table, no schema change.

**Deployed and run. Controller-verified**, 2026-08-18:

| | |
|---|---|
| revision the run was launched for | `9d8ea78c431c271bbe7b1c10e89a3778db8d2d64` |
| revision stamped in the deployed wheel | `9d8ea78c431c271bbe7b1c10e89a3778db8d2d64` |
| deployed wheel sha256 | `2009eb63eb08f968016d114972dc4285c0bcf889d073b7a6783e211d2a8394e7` |
| local wheel sha256 | **identical** — the deployed artefact was downloaded and hashed, not trusted |
| `opl/bronze/reconcile.py` present inside the downloaded wheel | yes |
| job run | **`836110216544566`**, `opl-dataops-views`, **SUCCESS** |

**What the view says, read back through the view rather than through the query that built it:**

| verdict | (table, batch) pairs |
|---|---|
| `reconciled` | **14** |
| `stranded_gated` | **1** |

```
source   | batch_id        | staged | promoted | quarantined | unaccounted | verdict
payments | 592660596679630 |  10000 |        0 |        2000 |        8000 | stranded_gated
remedy: databricks bundle run repromote_triaged_batch -t free
        --params table=payments,batch_id=592660596679630,revision=$(git rev-parse HEAD)
```

**The acceptance was that it finds `592660596679630` without being told about it, and it does.**
The other fourteen pairs reconcile exactly, including the four CNPJ batches that reached bronze
through a repromote after the gate blocked them — which is the property that makes this a
reconciliation and not a test for "the gate fired".

**The file grain, which is Task 2's input. Controller-verified** through
`dataops_reconciliation_by_file`: all **20** files of the two 2026-06 CNPJ batches are
`reclaimable`, and the stranded payments file is **not** (8,000 unaccounted). Socios' two batches
carry 1,797 and 1,786 rejected rows spread over 20 files and every one of those files is
reclaimable — because a rejected row **is** an accounted-for row once it is in quarantine, which is
exactly the distinction `retention.files_of_batch`'s proof could not make. *(It no longer has to:
Task 2 deleted that function and put this predicate on the delete path — §1.3.)*

### 1.3 Task 2 — the reclaim reaches the triage path, and the proof is repaired in the same change

**Built, and reviewed by an agent that did not write it.** The wiring is a third task on
`repromote_triaged_batch`; the proof is not a wiring change at all. `files_of_batch` returned
`DISTINCT _source_file FROM <bronze>` — that is, **`promoted(f) > 0`** — and its docstring called it
*"the whole safety argument"*. That is airtight only under an all-or-nothing gate. It is **deleted**,
not deprecated, and replaced by

> `promoted(f) > 0 AND promoted(f) + quarantined(f) = staged(f)`

whose first conjunct **is** the old predicate, so `new ⇒ old` and the delete set is a **subset** of
what shipped before. A retention control is allowed to move in that direction and no other.

**Controller-verified on the live warehouse, independently of the implementer and of the reviewer**,
2026-08-18 — the two proofs compared over the batches the acceptance run will use:

| batch | files | repaired proof admits | old proof admits | staged | promoted | quarantined |
|---|---|---|---|---|---|---|
| empresas `321750543973966` | 10 | **10** | **10** | 68,629,148 | 68,629,147 | 1 |
| socios `1121645114029617` | 10 | **10** | **10** | 27,838,448 | 27,836,651 | 1,797 |

**The strengthening costs nothing on the batches it was written for** — which is the right result and
not a disappointing one: socios' 1,797 rejected rows are spread across those same ten files, and
every file still reconciles, because a rejected row *is* an accounted-for row once quarantine holds
it. The predicate refuses a different case, and the phase has one: the stranded payments file, where
8,000 rows reached neither table.

#### The run, and the prediction it tested

**Deployed and run 2026-08-18. Controller-verified**, deployed wheel downloaded and hashed:
sha256 `d2628a98fadc65c76db5cdfa99e46b4ca529ef3fd82b8f33b0cacd10776943a4` on both sides, stamped
revision `eb7c6fab1d948c110d7392a52c6f763559157ba6` equal to the revision each run was launched for.

**Before the delete, the recovery path was verified present** — because it is the whole safety
argument and a control nobody checks is not a control: `cnpj/2026-06/zips/` held **10 zips per
group**, 1,352,336,436 B (empresas), 680,600,148 B (socios), 5,259,919,847 B (estabelecimentos).

| run | job output, quoted verbatim |
|---|---|
| **`945043269742472`** SUCCESS | `promote_batch: batch 321750543973966 is ALREADY in workspace.default.bronze_cnpj_empresas with all 68629147 of its promotable rows -- append skipped` · `1 rejected row(s) … stay in quarantine` · **`reclaim_landing: batch=321750543973966 table=empresas deleted=10 already_absent=0 failed=0 refused=0 held_back=0`** |
| **`163880365790949`** SUCCESS | `promote_batch: batch 1121645114029617 is ALREADY in workspace.default.bronze_cnpj_socios with all 27836651 of its promotable rows -- append skipped` · `1797 rejected row(s) … stay in quarantine` · **`reclaim_landing: batch=1121645114029617 table=socios deleted=10 already_absent=0 failed=0 refused=0 held_back=0`** |

**Every one of §2.1's predictions is CONFIRMED**, including the two that were written to be falsifiable
and were not: `refused=0` and `held_back=0` on both. **The Volume, verified after — by listing it,
not by the success line:**

| path | before | after |
|---|---|---|
| `cnpj/2026-06/empresas` | 10 files / 5,359,720,597 B | **0 files** |
| `cnpj/2026-06/socios` | 10 files / 2,852,557,826 B | **0 files** |
| `cnpj/2026-06/zips/{empresas,socios,estabelecimentos}` | 10 / 10 / 10 zips | **10 / 10 / 10, byte-identical** |

**8,212,278,423 bytes freed, and the way back to the source untouched.** ADR 0006's *"the defect
this decision must not leave standing"* — booked on 2026-08-03 with an 8.21 GB standing residue and
a ~48 GB/month projected floor — **is discharged**, by the wiring ADR 0006 itself named and deferred
*"as its own change"*.

**A control, because a delete that also moved data would be a much worse outcome than a delete that
failed.** Read back through `dataops_reconciliation` after both runs: **14 reconciled, 1
`stranded_gated`**, 337,766,032 staged / 337,762,443 promoted / 3,589 quarantined — unchanged. The
reclaim touched the Volume and nothing else.

**What this did NOT do, stated because the audit that found the residue got this wrong in the other
direction:** it does not make future months self-clearing on the in-flow path. `reclaim_landing`
still hangs off `promote` there, and the gate still blocks empresas and socios every month. What
changed is that the **triage** path — the one an operator actually runs after a gated batch — now
reclaims, so the residue stops accumulating at the point a human already has to act.

#### How this task was reviewed, because the chain is the finding

Implementer → independent reviewer (**10 findings**) → correction → **review of the correction** →
correction 2. The second review is the one that earned its place, and what it found is what this
phase predicted about itself: **the bug count was low, the code was right, and the defects were all
in NEW CLAIMS the correction had written.** The worst was a safety argument on the headline wiring —
*"the derivation cannot outlive the proof"* — which is **false**, and which **a passing test already
in this repository falsifies**: `months_of_batch` returns empty when a staging table lacks
`_snapshot_month`, while the counting query grains on `(_batch_id, _source_file)` and never reads it,
so that table yields full `staged` counts and its files reconcile. Month gone, proof intact, and the
consequence inverts the paragraph's conclusion — a repromote there goes **red after a promote that
worked**.

**Controller-measured, which is what turned it from an argument into a scope:** all **seven** live
staging tables carry `_snapshot_month` and `_source_file` today, so it is **latent, not reachable**
in this workspace. The claim was false either way.

**The structural fact is worth more than the finding.** `months_of_batch`'s own pre-existing
docstring states the narrow version *correctly*; the correction pass escalated it into a false
universal and shipped it as a safety argument on the wiring. That is the third instance of this
project's correction-passes-overshoot pattern, and it was caught by the only mechanism that has ever
caught it: someone who did not write it reading it against the code.

**And the chain ran in both directions.** Correction 2 refused to write the replacement sentence this
controller proposed, because it was *also* over-general; and it corrected two of the reviewer's own
numbers — the claim that a code path previously did no Spark work at all (it did), and a 68M-row
table that is in fact 144,193,416 rows.

### 1.2 THE DECISION ON `592660596679630`: DO NOT PROMOTE, and the reason is measurable

**A stranding reported and left unowned is not closed**, so the decision is recorded here rather
than left implicit. It was taken by the controller on 2026-08-18 with Jorge's delegation, and the
argument is not the one that looked strongest at first.

**The weak argument, stated so it is not mistaken for the reason.** The 2,000
`rescued_data_present` rows are F1b's deliberately injected schema drift and the 8,000 clean rows
are that experiment's other half — so promoting them "spoils an experiment". That is a preference,
not a mechanism, and this project does not decide on preferences.

**The decisive argument is that a promote falsifies committed documents, and the arithmetic says by
how much. Controller-verified**, 2026-08-18:

| | rows |
|---|---|
| `bronze_payments` | **40,150** |
| `fact_payment` | **40,000** |
| `COUNT(DISTINCT transaction_id)` in `bronze_payments` | **40,000** |

The 150-row gap between bronze and the fact is documented at `gold_load_fact.py:125-143` as the
legitimate repeats removed by deduplication, and it is the same 150 F1b published as its injected
duplicate count — `40,150 − 40,000`, computed two independent ways and agreeing. **A promote makes
bronze 48,150 against a fact still at 40,000**, because the gold fact loader is append-only and
refuses a target it did not write in the same run. The documented 150 becomes an undocumented 8,150,
and the rule that explains it stops explaining it. Restoring agreement would mean a gold rebuild and
a correction pass across F1b's, F3's and F-API's published counts — for rows nobody needs.

**Second, and independently:** the observation ledger classifies keys as `rejected_by_our_gate`, and
a promote is the one act that rewrites such a classification retroactively, underneath the
effectivity satellites F-DB's headline rests on. Today's repromote path does **not** flip it — it
re-applies the rules and re-rejects the same rows — which is precisely why this is a decision about
the 8,000 clean rows and not about the 2,000 rejected ones.

**Third, and it is the one this phase should say out loud:** the stranding is F4's only live
acceptance case. A phase that builds a finder, points it at the one defect it has, and then deletes
the defect has tested nothing that survives the phase.

**What would reverse it:** a use for those 8,000 rows that a fifth generated stream cannot serve
more cheaply. There is none today — the generator is deterministic and another profile costs one
run. **The command stays printed by the view, and nothing automated will ever run it.**

**What Task 1 does NOT do, said plainly:** it writes no table, so nothing turns red when a batch
strands. It is a view an operator or a dashboard reads. Making a stranding fail a run is a
different decision — it would put a gate in front of a condition whose only current instance is a
deliberate experiment.

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

**Two preconditions the implementer named as open, closed by the controller BEFORE the run rather
than diagnosed after it. Controller-verified 2026-08-18:**

1. **The month must be derivable from the batch.** `reclaim_landing` now derives its delete boundary
   from `_snapshot_month` on the batch's staging rows, and §0.5 had measured that column in *bronze*,
   not in staging. Measured now:
   `bronze_cnpj_empresas_staging` for `321750543973966` → `_snapshot_month = 2026-06`, **one distinct
   value over all 68,629,148 rows**; `bronze_cnpj_socios_staging` for `1121645114029617` → `2026-06`,
   one value over 27,838,448. Neither is NULL and neither names two months, so `resolve_month`
   resolves rather than refusing.
2. **The rules must not have widened since the batches were promoted.** `plan_promotion` re-derives
   `staged_promotable` from **today's** rules and refuses if bronze no longer equals it — so a rule
   widened since 2026-08-03 would turn the acceptance run red before reclaim ever started. Checked
   two ways: the exact-order assertions pinning `empresas` and `socios` in `tests/bronze/test_rules.py`
   are **unchanged** across `bb9b7b3..HEAD`, and the one commit in that range that edited a shared
   rule module (`7332283`) changed the **PTAX** publication-instant rule, not
   `unprovable_snapshot_ref_date`. Today's sets read
   `[null_or_empty_*…, bad_cnpj_basico_length, encoding_replacement_char, unprovable_snapshot_ref_date]`
   for both.

**This is the check ADR 0006 demands of anyone reusing its numbers** — *"Any number derived from this
section must be re-derived after every change to `rules_for`, or discarded"* — applied to its own
2026-08-03 measurement before that measurement is leaned on again.

**What would falsify the prediction, and each is a real outcome rather than a hedge.** Restated
against the code that actually ships, because the first version of this criterion named
`files_of_batch`, which Task 2 deleted — **a falsification criterion phrased against a function that
no longer exists cannot falsify anything**, and it was caught by the independent reviewer rather than
by its author:

- a non-zero **`refused`** means `LandingScope`'s containment rejected a path bronze names — a defect
  in the month derivation, not in the delete;
- a non-zero **`held_back`** means a file whose rows do not reconcile, which is the outcome the old
  proof **could not produce at all** and is a finding rather than a failure — the controller measured
  `held_back = 0` for both batches before the run, so a non-zero one contradicts a published number;
- a non-zero **`already_absent`** means something removed files this document listed;
- **`deleted < 10`** on either table means `file_accounts_of_batch` does not return what the Volume
  holds, which would falsify §0.5's and §1.3's central claim;
- and a **red run before any delete** is the likeliest non-zero-risk outcome, not a wrong number:
  `spark.sql(..., args={...})` named-parameter binding is unexercised on serverless, and it is called
  before the month is resolved and before anything is unlinked.

### 1.4 Task 3 — every rule that matches, not the first, and the overlap is still zero

**ADR 0006 lists three conditions that would reverse its refusal of a per-reason DQ tolerance.**
Condition 1 is per-reason counts derived from **all** matching rules, and it *"must ship BEFORE any
per-reason tolerance, never with it"*. **This task ships condition 1 and nothing else** — no gate
change, no tolerance, no new column on any table. Condition 3 shipped as Task 2. **Condition 2 —
≥6 monthly observations per table at reject count ≥10 — is evidentiary and no code closes it**;
empresas' numerator is 1 and estabelecimentos' is 0, and more months of "1" and "0" will not move
them.

**Run in the workspace on the shipped entry point.** Job run **`80788495253423`**, task run
**`880229908911460`**, SUCCESS; wheel sha256 `5cb7c36b6b0a5665376994cc91a2001e7663b7f26fd6aa18f40f8dae36f54319`,
stamped revision equal to the revision the run was launched for, both verified by downloading the
artefact. **Controller-verified** by parsing the task's own output:

| | |
|---|---|
| (table, batch) pairs swept | **15** |
| staged rows read | **337,776,032** |
| distinct rule columns evaluated | **50**, across all seven contracts |
| `rules_matched_2_or_more` | printed **15 times, every one 0** |
| `rescued_and_at_least_one_rule` | printed **15 times, every one 0** |

**Every non-zero number in the entire sweep:**

| table | batch | reason | rows |
|---|---|---|---|
| estabelecimentos | `128878829411613` | `encoding_replacement_char` | **4** |
| estabelecimentos | `118868417561350` | `encoding_replacement_char` | **4** |
| empresas | `321750543973966` | `null_or_empty_razao_social` | 1 |
| empresas | `371067950667703` | `null_or_empty_razao_social` | 1 |
| socios | `409962018634322` | `null_or_empty_nome_socio_razao_social` | 1,786 |
| socios | `1121645114029617` | `null_or_empty_nome_socio_razao_social` | 1,797 |
| payments | `592660596679630` | `rescued_data_present` | **2,000**, and **zero rule matches** |

**§2.2's published prediction is CONFIRMED**, and it is now a stronger statement than the one it
extends: ADR 0006 measured the overlap on **2026-08-03** over **three** contracts in **six** cells
with three hand-written columns. This is **seven** contracts, **fifteen** pairs, **fifty** rule
columns, evaluated by **the deployed rule set itself** rather than by a query someone wrote beside
it. *"The hole is latent, not open"* now holds where it had never been asked.

**The corpus is staging, and that is the ruling that removed an ordering problem three of the four
audits had imposed.** They concluded this task must wait on a Unity Catalog mask repair, because
3,583 of the 5,589 quarantined rows turn on a column that reads `***`. `masking.py` covers bronze
and quarantine and **deliberately never staging** — and the implementer verified it rather than
taking it from the controller: `bronze_cnpj_socios_staging` holds **55,830,826 rows, none reading
`***`**, and 55,830,826 = 55,827,243 promoted + 3,583 quarantined. Staging is complete and unmasked.
A test pins it so a later edit cannot silently re-point the sweep at the masked table.

**A by-product that re-derives a question ADR 0006 left open.** The sweep implies **5,593** rejects
against **5,589** rows in quarantine. The difference is **4** — the four 2026-06 estabelecimentos
rows that the *narrower* gate of the day promoted un-flagged, and which today's rules reject. ADR
0006 records that as an open policy question (*"whether the system of record is re-gated when a rule
widens"*) and did not answer it. **Nothing here answers it either.** What is new is that the number
now falls out of a measurement instead of being remembered — and that both estabelecimentos batches
report **4**, which is ADR 0006's footnote re-derived from the deployed rules rather than from a
hand query.

**The independent reviewer derived that difference without the shipped code**, from the quarantine
grouped by table, reason and batch plus a hand-written U+FFFD count over estab staging, and located
it precisely: batch `118868417561350` is the 2026-06 cell, holds **4** U+FFFD rows and **0** in
quarantine, so those four are in bronze un-flagged; `128878829411613` is 2026-07 and its 4 *are*
quarantined. It is the only cell where "the gate of the day" and "the rules today" differ.

> **AND THE RECONCILIATION RESTS ON THE VERY COUNTERS THIS TASK SHIPPED, which is a better argument
> than the one first written here.** Summing per-rule counts and adding the rescued count is a count
> of **rows** only because both overlap counters are zero — otherwise any row carrying two reasons is
> counted twice and 5,593 is a count of *reason hits*, not of rows. So "5,593 versus 5,589, a
> difference of 4" is a claim that depends on `rules_matched_2_or_more = 0`, and stating it without
> that dependency would have been arithmetic resting on an unstated premise. Found by the reviewer;
> neither the commit message nor ADR 0006 said it.

**What makes the fifteen zeros worth anything is that the counter can count.** A counter that reports
zero fifteen times is otherwise indistinguishable from one that cannot report anything else — this
project's second recurring species, now found six times. The implementer's demanded test builds a row
that is **both** blank in a required column **and** carrying U+FFFD and asserts the counter reads
exactly 1; ten mutations of shipped code were each caught by named tests, including `when/otherwise`
→ `cast("int")`, the NULL-propagating spelling that would have made the overlap vanish for precisely
the rows carrying a NULL.

**What this task deliberately does not do:** adopt a threshold. Two of ADR 0006's three conditions
now hold and the third cannot be closed by code, so the refusal stands — and the ADR says which is
which rather than leaving a reader to infer it.

**Cost, measured rather than estimated:** the whole sweep — seven scans, 337,776,032 rows — ran in
**89 s**. The per-task floor this phase argued about is 15–30 s, so the measurement costs roughly
three task-starts and buys a number ADR 0006 has wanted since 2026-08-03.

> **THE SIXTH INSTANCE OF THIS PHASE'S SECOND SPECIES WAS IN THIS TASK'S OWN TESTS, and it threatened
> the sentence two paragraphs above.** `test_it_is_total_over_the_registry_rather_than_a_hand_written_list`
> asserted `"REGISTRY" in <the entry point's source>` — and the string `REGISTRY` appears in that
> file's **module docstring**, so the test held with the import and the loop both deleted. The
> reviewer proved it instead of arguing it: narrowing the sweep to three tables left **all four tests
> in the file green**.
>
> **And the one line that could have contradicted a narrowed sweep printed `len(REGISTRY)`** — the
> registry's size, not the number of tables actually measured. So a three-table sweep would print
> *"7 tables"*, every per-rule number would stay correct, and **"fifteen pairs, seven contracts" —
> published in ADR 0006 and in this document — would have become quietly false**. Both species at
> once: a guard that cannot fail, and a silent failure that preserves every other number.
>
> Closed in the correction pass (`ebeb721`) by asserting the **visited set** — ordered, so an extra
> table and a missing one both fail — and printing the count **actually measured**. Recorded here
> rather than only in a commit because the claim it threatened is one this document makes.
>
> **And the numbers above were re-taken afterwards**, because the correction changed the code that
> produced them. A measurement whose code moved under it is a measurement nobody has taken.
>
> **Re-run `321135201221285`**, task run `600061871178163`, SUCCESS, wheel sha256
> `cecf324ad491eca1b7d2ede77405a3c53a0a7020f4d45aec8228c4b788128293`, stamped revision `538a966`.
> The two runs' outputs were compared key by key by the controller: **208 keys each, and zero
> differences.** Same summary line, same fifteen pairs, same 337,776,032 rows, same seven non-zeros,
> same fifteen zeros. So the table above is the corrected code's output as well as the original's —
> which is what makes it citable, and which nobody would have known without re-running it.
>
> **One limit the correction pass stated against its own interest, and it is right:** with the sweep
> total, `len(measured)` and `len(REGISTRY)` are equal by construction, so no mutation can make the
> *table count* alone diverge. The visited-set assertion is what carries that half; the summary
> line's independent bite is on the **row** total, and that was proved by a mutation.

### 2.2 The all-matching-rules sweep

**Published 2026-08-18, before any run.** Over all seven contracts and every staging batch, evaluating
every rule rather than the first: **the count of rows matching ≥2 rules is predicted to be zero
everywhere**, extending ADR 0006's six measured cells from three contracts to seven.

**A non-zero count anywhere falsifies ADR 0006's "the hole is latent, not open" for a contract it
never covered**, and is the more valuable outcome of the two.

> **CONFIRMED, 2026-08-18** — run `80788495253423`, 15 pairs, 337,776,032 rows, 50 rule columns,
> **zero everywhere**, and the counter is shown able to report otherwise. The less valuable of the
> two outcomes, and it is recorded as the prediction that held rather than quietly folded into the
> narrative. §1.4.

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
