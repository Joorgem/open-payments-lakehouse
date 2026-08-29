# ADR 0020 — The triage accelerator is deterministic, and the model runs as the control

## Status

**Accepted**, F6, 2026-08-24/28. The master design spec frames this phase as an RCA
**accelerator**, *"not as an AI feature"*, and **Task 0 removed the only reason that framing might
have been a constraint rather than a choice**: the workspace already serves eleven foundation-model
endpoints on the credential this project has held since F0. So the refusal below is a decision, and
it needed a reason better than the spec's word. It has one, and the reason is a measurement.

This ADR records the decisions no existing note owns. *Derive, do not instrument* stays in
[ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) Decision 1, extended
one layer up without amendment; *report, never act* stays in that ADR's Decision 3 and is inherited
rather than rediscovered. The deployed-revision guard the workspace task carries stays in
[ADR 0009](0009-deployed-revision-provenance.md).

> **The numbers are in `docs/f6-run-evidence.md`**, with controller-verified separated from
> reported. This document carries the decisions; where a figure appears here it is quoted from
> there.

---

## Context

F4 left four facts about a failing DQ gate lying in four different places: which task run failed
(`system.lakeflow`, folded by `opl.dataops.telemetry`), which rows were rejected and why (the
table's quarantine), whether the batch reconciles (`opl.bronze.reconcile`), and what the same
table's recent runs looked like. **Nothing assembled them.** A triager assembled them by hand,
every time, starting from a run URL.

The phase opened on one unknown that decided its whole shape, and it had three readings: that the
accelerator needs no language model; that it needs one for the narrative half, which would be an
account and a token; or that opening an issue needs a second, independent token. **All three were
measured before a line of the plan was written, and two dissolved the gates they implied**
(evidence §0.1, §0.2).

And the corpus turned out to be smaller and stranger than the census suggested: **eleven incidents
wearing twenty-two rows**, because `max_retries: 0` does not prevent a retry; **five of the eleven
carry no quarantined rows at all**, which is not the same fact as "the gate rejected nothing"; and
**ten of the eleven have fewer than five prior gate executions to compare against** (§0.3, §0.5,
§0.10).

---

## Decision 1 — the classifier is deterministic, and the model is NOT it

Every grade this package emits is a `CASE` ladder over columns. No model participates in any
classification, ranking or recommendation on the shipped path.

**Three reasons, and the third decides it:**

1. **Every input F4 left is structured.** Typed telemetry columns, four quarantine tables with
   `_dq_reject_reason` populated, four reconciliation verdicts, a cadence declaration. There is no
   unstructured corpus here for a model to be better at.
2. **A deterministic classifier can be mutation-tested.** Change a threshold and a test goes red.
   Change a prompt and nothing goes red, because nothing was asserting anything.
3. **A model output has no falsifier, and this phase's whole point is that it needs one.** F5's
   exactly-once proof is worth something because the naive arm *had* to duplicate. A severity a
   model emits is worth nothing, because there is no output this repository would have called
   wrong.

**Rejected: an LLM classifier behind a "the model is the accelerator" framing.** It would ship the
exact artefact this phase exists to refuse — a thing that produces a plausible report for any input
— and it would ship it *as the deliverable*.

**What reverses it:** an input class that is genuinely unstructured — free-text operator notes,
vendor emails, a stack-trace corpus. The classifier would then have a job no `CASE` expression can
do, and the model would earn a place **behind** the deterministic verdict rather than instead of
it.

---

## Decision 2 — the model runs anyway, as the NEGATIVE CONTROL

This is the decision that makes Decision 1 rest on a measurement instead of an argument. The
cheapest demonstration that the shipped classifier is not a text generator is to **run a text
generator on the same corpus and measure where it disagrees** — **four sweeps, n = 5 each, 20
trials and 170 responses** in `docs/f6-llm-control-responses.json`: the real incidents, the same
incidents with the counts stripped, one fabricated incident that exists nowhere, and the stripped
sweep re-run with the decline option mid-menu.

**What it returned (§1.8), including the half that weakens Decision 1:**

- **It declined on the incident that does not exist**, 5/5, and 24/25 across four menu
  configurations. Prediction 4 is **falsified**, and is published as weakening Decision 1 because
  that is what the prediction said would happen.
- **It reproduced the size ladder perfectly** — `bulk_rejection` 15/15 for the three large
  incidents, `isolated_rejection` 15/15 for the three small.
- **And it inverted the one word that matters.** `does_not_reconcile` was returned only on
  incidents that have **no reconciliation row**, and **never once** on `592660596679630`, the one
  incident whose verdict genuinely does not reconcile. The causes it wrote there convert an absence
  of data into an asserted finding: *"staged rows missing from bronze and quarantine"*, where the
  input said there is no row at all.
- **Its confidence field is unusable in the direction anyone would use it.** Where it asserts a
  verdict, confidence is ≥ 0.9 in 78 of 81 — for the ones it gets right, for the one it grades on
  size against the ladder, and for the findings it invents. Ask what else produces a 0.9 and the
  answer is everything.

**The shipped classifier reads that same incident as `does_not_reconcile` → `hold_do_not_promote`
(§1.9), which is the correct answer and the least like *"it is big"*.**

**Rejected: skipping the arm because the spec says "not an AI feature".** A control is not a
feature. Skipping it would leave Decision 1 resting on an argument, which is the shape ADR 0018 and
ADR 0019 both refuse.

**Kept small on purpose:** one module, outside the shipped path, and **no shipped code depends on
any endpoint**.

**What reverses it:** nothing reverses running a control. What would change its *conclusion* is the
control outperforming the ladder on the same corpus — which would be a real result and would be
published as one.

---

## Decision 3 — the agent writes no table

It reads, ranks and drafts. It never promotes, re-runs, deletes, or writes to any table this
project owns. `repromote_triaged_batch` exists, takes a batch id, and is **launched by a human** —
`dataops_reconciliation` already prints that command beside a stranding and deliberately stops
there, because *"a view that promoted rows would be a gate bypass wearing a dashboard"*. The same
rule holds one layer up.

**This is ADR 0018 Decision 3 inherited, not rediscovered.** What F6 adds is that the rule also
removes an exposure rather than only a risk: **`max_retries: 0` does not prevent a retry**, and
this phase measured that on its own task. The deliberate failure-arm run shows
`triage_dq_incident` at attempt 0 FAILED and attempt 1 FAILED — one `job_run_id` wearing two
task-run rows, the exact shape of the corpus it was built to read. **Both attempts re-ran every
statement and left nothing behind, because there is nothing to leave.**

**Rejected: a `triage_findings` table.** It would need to be idempotent under a retry the platform
does not prevent, and it would be a second spelling of facts the system tables already hold.

**What reverses it:** a consumer that needs history *of triage* rather than history of incidents —
a trend over what was recommended and whether it was followed. That is a different product, and it
would need the retry question answered first.

---

## Decision 4 — the agent emits an issue; a separate publisher opens it, and the boundary is a DIRECTORY

`opl.triage_agent.issue` emits the issue as **data** — a record with named fields — and
`report.py` renders it. Neither posts anything. The publisher is `scripts/open_triage_issue.py`,
which is **outside the wheel**: `pyproject.toml` packages only `src/opl`, so **no task running in
the workspace can import it.**

**That is a credential boundary and not a filing convention.** `gh` on the operator's box already
carries `repo` scope. A Databricks task calling the GitHub API would need a PAT in a secret scope —
a new credential, a new human gate, and **a token with repository write sitting beside 55.8M rows
of personal data**. The directory is what makes that impossible rather than merely discouraged.

The publisher **prints by default and posts only under an explicit flag**, and takes exactly one
incident id.

**Rejected: opening the issue from CI with `GITHUB_TOKEN`.** The repository's
`default_workflow_permissions` is `read`, and whether a job-level `permissions: issues: write`
elevates above that **is not asserted anywhere in this repository** — it is prediction 3, and it
remains **untried**. This repository has already paid once for a `ci.yml` claim written from memory
and checked against a weak source.

**What reverses it:** a run that settles prediction 3, plus a reason to want the issue opened by
something other than a person.

---

## Decision 5 — blast radius is a DECLARED manifest, locked against the registries

The agent answers *which* tables are downstream of the gated one, and **never how much of them**.
It reads nothing and emits no SQL: the bundle declares which bronze table each vault loader is
handed, `opl.gold.registry` declares which vault table each gold table is built from, and this is a
declaration plus a derivation over registries the wheel already carries.

**A proportion was refused on a measured ground**, not on taste: it classifies socios near 100% in
this package's fixture and near 0% on the deploy, **with no test able to tell the two apart**.

**And the manifest is not a graph walk, because the graph is wrong.** **Two bronze tables reach
gold without a vault table in between**, and one of them is `payments` — the workspace's largest
incident. A manifest walked bronze → vault → gold answers *"nothing downstream"* for exactly the
incident that most needs the opposite. An import-time guard refuses a registered bronze table whose
downstream set is empty, **rather than printing the most reassuring wrong answer this package can
give.**

**Rejected: dynamic lineage** from the platform's own graph, and **rejected: a magnitude** of any
kind in that section.

**What reverses it:** Unity Catalog lineage becoming readable and complete on this edition, at
which point the declaration becomes the *lock* on a derived answer rather than the answer.

---

## Decision 6 — the comparison horizon is bounded, and the agent says when N was not available

The agent counts the prior gate executions an incident can be compared against, and **compares
nothing**. The word it publishes says whether a comparison is *possible*, never what one found.

**Because "compared against the last 5, nothing anomalous" is false for ten of this workspace's
eleven incidents**, and two have no prior execution at all. So the number actually **found** is on
every row beside the number asked for, and the ways of having less than N are **three words rather
than one**: fewer (`insufficient_history`), none (`no_prior_execution`), and *I could not look*
(`gate_run_absent`).

**The key is `check_bad_rows`, and choosing it is the whole decision.** The gate task was renamed
mid-project and **the telemetry serves runs under both names while marking neither as superseded**.
Keyed on `dq_gate_batch`, all three lookup incidents return **0** prior executions rather than 4, 3
and 1 — and `0` asserts *"this table was never gated before"*, the most reassuring lie available.
`check_bad_rows` is the only gate-adjacent task present in all seven jobs across the whole window,
and its count equals the sum of both gate spellings exactly.

**Rejected: `task_key = 'dq_gate_batch'`** (silently wrong by 5 at table grain, total at incident
grain); **rejected: a timestamp comparison** (counts the incident's own gate run as prior history,
wrong by one on all eleven, and reports `1` for the two incidents whose true history is `0`);
**rejected: `check_bad_rows.result_state`** as a signal, since the condition task is `SUCCEEDED` on
all 29 runs whether its answer was true or false — *the same number a workspace with zero incidents
would report*.

**What reverses it:** the telemetry marking a retired task key as superseded, which would make the
stable-key argument unnecessary rather than wrong.

---

## Consequences

- **The phase's headline artefact is a public GitHub issue** ([#29](https://github.com/Joorgem/open-payments-lakehouse/issues/29)),
  drafted by the shipped path from a real workspace run, recommending **not** to promote the
  largest incident in the workspace.
- **Every grade is mutation-testable and several were found blind**: a test counting CTE union legs
  instead of published columns, a guard test restating the guard body over valid data, a leak test
  green under a `SELECT *`. All closed, each pinned by a test whose failure arm was run.
- **The model arm cost nothing architecturally and bought the phase its only real falsifier.** It
  also falsified a prediction *against* the phase, which is the outcome that makes the rest
  credible.
- **Two claims that had never touched a markdown engine are now measured** (§1.10): the body
  renders as expected, **and so does the title** — GitHub renders code spans in issue titles, which
  this ADR asserted the opposite of for one commit before the closing review caught it. The
  title fence is load-bearing, not decoration.
- **`insufficient_history` is the majority state in this workspace, and the agent says so on every
  row.** An accelerator that reported a comparison it did not make would be this phase's species
  wearing a number.

## References

- `docs/f6-run-evidence.md` — the measurements, controller-verified separated from reported.
- [ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) — Decision 1
  (derive, do not instrument) and Decision 3 (report, never act), both inherited here.
- [ADR 0019](0019-the-proof-runs-where-a-process-can-be-killed.md) — the standing instruction that
  a prediction whose terms change meaning between two cells is two predictions, one unstated.
- [ADR 0009](0009-deployed-revision-provenance.md) — the deployed-revision guard the workspace task
  carries.
- [ADR 0006](0006-bronze-dq-gate-policy.md) — condition 2's `>= 10`, the only reject-count line
  this repository has ever argued for, and the one `severity.py` reuses rather than inventing a
  second.
