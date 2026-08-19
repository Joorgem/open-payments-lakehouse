# ADR 0018 — DataOps derives; it does not instrument, and it does not act

## Status

**Accepted**, F4, 2026-08-18/19. The phase is on no critical path — the MLV was reached at F3
and all four named sources closed at F-DB — which is exactly why every item in it had to earn
its place by measurement rather than by the spec's word. Three did not, and were deleted.

This ADR records the decisions no existing note owns. The DQ-tolerance question stays in
[ADR 0006](0006-bronze-dq-gate-policy.md), which F4 amended with *"Where the three stand"*; the
mask predicate and the grant model stay in [ADR 0008](0008-pii-masking-socios.md), amended
rather than superseded.

> **The numbers are in `docs/f4-run-evidence.md`**, with controller-verified separated from
> reported. This document carries the decisions; where a figure appears here it is quoted from
> there.

---

## Context

The master spec asks F4 for full checks, quarantine/replay, telemetry, a dashboard, UC
governance and a performance case study. **Four measurement lenses ran before this phase's plan
existed, and four independent audits then read the plan.** They returned five CRITICAL findings
and reversed three of its rulings. **Seven of the defects they found were the controller's
own**, and they are recorded where a reader of this repository can reach them —
`docs/f4-run-evidence.md` §0 and §1 — rather than only in the working plan, which is not
published.

The single fact that shaped everything below: **most of what the spec asks for either already
existed or could not be built honestly.** `system.lakeflow.job_task_run_timeline` and
`system.query.history` already hold the telemetry, retroactively, for every task ever run. The
gate's reject reasons already sit in four live quarantine tables. What was genuinely missing was
smaller and sharper than the spec's list, and it is what this phase built.

---

## Decision 1 — telemetry is a VIEW over system tables; the pipeline is not instrumented

`system.lakeflow.job_task_run_timeline` ⋈ `system.query.history` supplies duration, state,
`read_rows`, `written_rows` and `written_bytes` for every task run, with **zero instrumentation
and zero writes**. Instrumenting the eighteen entry points would cost a task start of ~15–30 s
against jobs whose smallest tasks are ~16 s, to re-derive what the platform already records.

**Three properties decide how the view is written, and all three are measured:**

- **The join key is `t.run_id`.** The obvious-looking `t.job_run_id` yields **zero rows,
  silently**; `t.task_run_id` does not exist.
- **It covers roughly half the task runs by construction** — the tasks that issue no SQL match
  nothing, including `fail_on_dq`, this project's headline DQ event. **A NULL there is rendered
  as "no metric", never as a zero.**
- **`execution_duration_seconds` is NOT additive across the hour-sliced periods — it repeats.**
  One task run carries `0 / 5633 / 5633` for a wall clock of 5,635 s, so `SUM` reports **11,266**
  — a silent 2× overstatement of the most expensive task in the workspace, on the column anyone
  reaches for first. `MAX` is correct, and `setup_`/`cleanup_` repeat the same way.

**And what the labels may be said to mean is narrower than it looks.** `no_sql_attributed` is
**not** proof that a task issued no SQL: the watermark it keys on proves the record holds a
*newer* statement, not every *older* one, and ingestion is not strictly ordered. A completeness
guarantee was looked for and does not exist — `DESCRIBE TABLE` returns 46 columns and not one
bounds ingestion, and `DESCRIBE DETAIL` on that table fails outright. **Every metric in the view
is a floor, not a total.**

### Rejected: the narrow written table for what the platform cannot know

The plan asked for a small Delta table holding reject counts on an idempotent re-run,
`collapsed_duplicates` and `already_present`. **It is not built**, and an empty one would have
been worse than none — a guard that cannot fail, with a schema.

- Two of the three are already recoverable: reject counts live in the quarantine and
  `dataops_reconciliation` reports them per (table, batch); `already_present` follows from Delta
  history.
- The third, `collapsed_duplicates`, is real (**4,329** of 2026-07's 27,990,592 partner-link
  rows) and is computed in five vault loader entry points. Persisting it means a Delta append
  **inside every loader's write path**, from the one task whose remit is that it writes nothing.
- **`max_retries: 0` does not prevent a retry** — 24 `(job_run_id, task_key)` pairs ran two
  attempts. A telemetry write is precisely the side effect that turns that into duplicate rows.

**What reverses it:** the moment anything **branches** on `collapsed_duplicates` rather than
printing it. The write then belongs in that change, keyed on the task run id, in the loaders
that produce the number.

---

## Decision 2 — freshness is two metrics, and neither may exist without a declared cadence

*Pipeline* freshness (`now − MAX(_ingested_at)`) is defined for all seven bronze tables;
*source* freshness (`now − MAX(_snapshot_ref_date)`) for five — `payments` and `ptax`
structurally lack the column and `rules.py` argues why stamping it would be a control omitted so
its own refused value could be written. A single collapsed number would print NULL for two of
seven and invite the wrong reading.

**But a metric without an expectation measures when an operator last typed a command.** There
are **zero** `schedule:` blocks in this repository and every ingest run was launched by hand, so
any threshold on a dashboard is invented rather than measured — the standard this phase applied
to Databricks' documented retention and to a billing claim applies to its own metric. **The
expected cadence therefore ships as data in the repo, beside the metric**, in four kinds:
`declared`, `paused`, `undeclared`, `no_source_axis`, each carrying its reason.

**The acceptance case is the one that would otherwise be muted in week one:** `bronze_cnpj_lookup`
sits two months behind its siblings, and the view labels it **`paused_by_decision`** carrying the
citation for the scope decision that put it there — not a fault. **A metric that cannot tell
"deliberately not ingested" from "ingest broken" has the defect it was built to avoid, moved one
level down.**

---

## Decision 3 — the reconciliation REPORTS, prints the remedy, and never acts

Three tables hold one ingest's rows and nothing had ever compared them. `dataops_reconciliation`
does, per (table, batch), with four verdicts; `dataops_reconciliation_by_file` does it one grain
down. **No table, no schema change** — every column it reads already exists on all 21
bronze-family tables.

**It prints the `repromote_triaged_batch` invocation for every stranded batch and runs none of
them.** That is the house standard rather than a nicety: `promote.require_batch_id`,
`reclaim_landing._report_nothing_proven` and `backfill_prewrite.refuse_non_empty_quarantine` all
print the query or DDL that resolves what they refused. **A view that promoted rows would be a
gate bypass wearing a dashboard.**

**The one live stranding is not promoted, and the reason is arithmetic rather than preference.**
`bronze_payments` is 40,150 against `fact_payment` 40,000 — a 150-row gap documented as
deduplication and equal to F1b's injected duplicate count, derived two independent ways. A
promote makes it 8,150 against a gold loader that is append-only and refuses a target it did not
write in the same run, so the rule that explains the gap stops explaining it, for rows nobody
needs. Recorded in the evidence with its owner.

---

## Decision 4 — the reclaim reaches the triage path, and the proof is repaired in the same change

ADR 0006 booked this as *"the defect this decision must not leave standing"* and carried it past
F1.4b *"as its own change"*. **F4 is that change**, and it freed **8,212,278,423 bytes**.

**Wiring it is not a one-line YAML edit, because the wiring is what breaks the proof.**
`retention.files_of_batch` returned the distinct `_source_file` values bronze holds rows of — that
is, `promoted(f) > 0` — and its docstring called it *"the whole safety argument"*. That is
airtight only under an all-or-nothing gate, where bronze holding *a* row of a file implies it
holds *every* row. A repromote breaks the equivalence by construction.

**So the proof is strengthened, not relaxed:**

> a file may go when `promoted(f) > 0 AND promoted(f) + quarantined(f) = staged(f)`

whose first conjunct **is** the old predicate, so `new ⇒ old` and the delete set is a **subset**
of what shipped before. A retention control is allowed to move in that direction and no other.
`files_of_batch` is **deleted** rather than deprecated — a weaker proof left in the module is a
weaker proof somebody calls.

**What it does not do:** make future months self-clearing on the in-flow path. The gate still
blocks empresas and socios every month. What changed is that the **triage** path — the one an
operator actually runs after a gated batch — now reclaims, so the residue stops accumulating at
the point a human already has to act.

---

## Decision 5 — no per-reason DQ tolerance, and the measurement that was missing ships instead

ADR 0006 rejects a per-reason threshold and names three conditions that would reverse that.
**Condition 3** (the reclaim decoupling) shipped as Decision 4. **Condition 1** — per-reason
counts derived from **all** matching rules rather than the first — ships here: 15 (table, batch)
pairs, 337,776,032 staged rows, 50 rule columns, evaluated by the deployed rule set rather than
by a query written beside it.

**Condition 2 is evidentiary and no code closes it.** It needs ≥6 monthly observations per table
at reject count ≥10; empresas' numerator is 1 and estabelecimentos' is 0. **So the refusal
stands, and the ADR says which condition is closed and which cannot be**, rather than leaving a
reader to infer that two of three is most of a case.

**The corpus is STAGING, and that ruling dissolved an ordering three audits had imposed.** They
concluded the sweep must wait on a UC mask repair, because 3,583 of the 5,589 quarantined rows
turn on a masked column. `masking.py` covers bronze and quarantine and **deliberately never
staging** — and staging holds every rejected row, unmasked, verified: socios staging is
55,830,826 rows, none reading `***`, and 55,827,243 + 3,583 = 55,830,826.

### Rejected: row-grain replay

Not for "no MERGE exists here", which is an argument about effort. On the measured ground:
**not one of the 5,589 quarantined rows has a repair that would let it pass a re-evaluation** —
4 whose bytes ADR 0006 rules irreparable by name, 3,585 that are *"a fact about the world that no
engineering removes"*, and 2,000 whose repair is a contract v2, i.e. an ingest. **A replay here
would have no replayable row**, which is the standard `rules.py` already applies when it refuses
a rule for a defect class nothing generates. **And batch-grain replay already ships** —
`repromote_triaged_batch`, which has run — so claiming the project has none undersells work that
exists.

---

## Decision 6 — governance is ALL imperative; nothing goes in the bundle

A Databricks Asset Bundle has **no `tables` resource and no `View`**. `grants` exists only on
Catalog, Schema, Volume, RegisteredModel, ExternalLocation and VectorSearchIndex. The plan first
concluded "split governance between the bundle and SQL"; **that is withdrawn** — on one
measured ground and two that are reasoned from measured parts, and the difference is stated
because `docs/f4-run-evidence.md` §0.9 still lists the second and third as open:

1. the repo's only target is `mode: development`, which rewrites `name: default` to
   `dev_<prefix>_default` — it would deploy green and govern **a new, empty schema**;
2. under a production target the name survives and then **collides**, against a schema owned by
   `_workspace_admins_workspace_<id>`;
3. `resources.<securable>.grants` is **authoritative**, so declaring it would revoke the
   platform's own `CREATE TABLE` on the schema every pipeline writes into.

**Only the first is measured end to end** — a scratch bundle was validated and the CLI
rewrote `name: default` to `dev_<prefix>_default`, so the deploy would govern an empty
schema. The second and third are **assumed on strong evidence and not proved**: the
underlying `POST /api/2.1/unity-catalog/schemas` does return `Schema 'default' already
exists` (measured), and `grants` is documented as authoritative — but **no bundle was
deployed to watch either happen**, because doing so is the act the decision exists to
avoid. **The decision stands on ground 1 alone**, which is sufficient: under the only
target this repository has, the bundle half governs a schema nobody reads.

`bundle deployment bind` is refused: it puts 55.8M rows of personal data inside
`bundle destroy`'s blast radius for a cosmetic gain.

**The grant lens is a denylist, not an allowlist, and that is the second time this module was
fixed.** The first repair widened an allowlist by one entry and left `MANAGE` — the privilege
whose purpose is acquiring privileges — dropped silently. Everything not explicitly harmless is
now revoked or **escalated and the run fails**. Measured: `REVOKE ALL PRIVILEGES ON TABLE`
removes ALL PRIVILEGES, MODIFY and APPLY TAG and **leaves MANAGE and READ METADATA standing**, so
the remediation the task prints echoes the observed action rather than a guess.

---

## Decision 7 — the performance case study leads with what did not move, and its own prediction is falsified

Three null results are the item's product: clustering a point lookup cut 47 files to 1 and **did
not move the clock**; the real gold query cannot be helped, because 1,027 keys spread uniformly
over 48 files put ~21 in every file; and clustering the PIT on `as_of_date` fails because the join
key is the satellite's hash. **They are REPORTED, not controller-verified, and they are not
re-derivable** — every live table now reports `clusteringColumns = []` and the PIT is at 43 files,
so the third ran against an object that no longer exists in that state. `docs/f4-run-evidence.md`
§0.4.1 records them with that label. **Their mechanism claims survive re-derivation from the key
distributions; their timings do not, and nothing here rests on a timing of theirs.**

**The plan's revision 1 then refused a baseline→optimised benchmark on the premise that no
un-compacted baseline remained. That premise was false** —
`link_empresa_estabelecimento`, 512 files and 7,201,236,749 B, never compacted — and the item was
re-decided rather than inherited.

**The published prediction is falsified in the half stated most confidently.** Files fell 512 →
128 as predicted; `read_bytes` fell **7.05%** rather than staying "roughly unchanged", because
compaction **recompresses**: 128 files at ~54 MB read fewer bytes than 512 at ~14 MB for the same
logical scan. **Compaction is not a pure re-packing — it changes how much there is to read.**

**Mandatory protocol, all measured:** disable Predictive Optimization **per table** before any
baseline and **restore `INHERIT` afterwards**, verified by reading the flag back both times;
denominate in `read_files_count` and `read_bytes`; quote wall clock as a range or not at all —
**the first uncached run of a session was 7.7× the ones after it**, which is enough to
manufacture an order of magnitude of improvement out of nothing. And **defeat the cache with a
literal that survives into the plan**: a varied tautology is constant-folded and the cache
answers, measured `True` with 0 bytes twice.

---

## Consequences

**What this phase leaves running.** Four views, one guarded job carrying every imperative
statement the bundle cannot express, a reclaim that reaches the triage path, a mask predicate
that can be opened by someone authorised, and a dashboard declared in the bundle rather than
clicked.

**What it leaves refused, each with a reversal condition:** the narrow telemetry table, row-grain
replay, the per-reason tolerance, and the bundle half of governance.

**What it cost to learn, and the reason this ADR is written the way it is.** The phase found
**ten** instances of one defect: a check whose output cannot distinguish *passed* from *never
ran*. They were in tests, in tooling, in a view's arm, in a controller's published claim
falsified four minutes later, in the safety check on a privacy deploy — where `***` was the
answer under all four possible outcomes — and, last, in a test asserting a function against its
own body, which left the access control's totality over 55.8M rows unlocked in the one direction
that matters. **Every one was found by somebody who did not write it**, and the tenth was found
by the review that closed the phase.

**So the standing instruction this phase adds:** when a check reports the expected value, ask
what else would produce that value. If the answer is "everything", it is not a check.

---

## References

- `docs/f4-run-evidence.md` — the measurements, controller-verified separated from reported
- [ADR 0006](0006-bronze-dq-gate-policy.md) — the DQ gate policy, and *"Where the three stand"*
- [ADR 0008](0008-pii-masking-socios.md) — the mask, the predicate repair, and what RBAC means here
- [ADR 0009](0009-deployed-revision-provenance.md) — why every job here carries the revision guard
- **The phase plan is NOT part of this repository.** It lives in a git-ignored working
  directory, so no link to it is given: F3 shipped a section pointing a public reader at
  that directory and they reached nothing, and `docs/f4-run-evidence.md` opens by refusing
  to repeat it. Everything a reader needs from the plan — the reversed rulings, the
  corrected figures and the defects the controller found in its own work — is in that
  document's §0 and §1.
