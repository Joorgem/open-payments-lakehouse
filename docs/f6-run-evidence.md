# F6 — RCA agent: what was measured, and what was predicted before it was

**Controller-verified** means the controller ran the command in this session and read the output.
**Reported** means an implementer, a reviewer or a task's stdout said it. Every claim in §0–§2
carries one of the two labels. **§3 is uniformly *Reported* unless a cell says otherwise** — it is
a ledger of what has NOT run, read off the code and the record rather than measured.

This is `docs/f5-run-evidence.md`'s shape, and F5's preamble explains why the split exists. It also
records that the convention was applied **unevenly** there — the phase's headline result carried
the wrong label until a late reader checked it against the preamble — and that *a convention
applied unevenly is weaker than no convention, because a reader cannot tell which sections were
careful.* This document is written against that.

**Predictions are published BEFORE the run that tests them** (master protocol §4.5). A number first
written down after the run that produced it is not a prediction. §2 is where they live.

> **THE STATEMENT IDS BELOW EXPIRE.** Measured in F5 at ~5 days: an expired id returns the API's
> own named refusal, `Error: The statement <id> was not found.` **That is expiry, not fabrication,
> and the two are distinguishable** — the API names the statement, while MSYS path-rewriting
> returns a bare `Error: Not Found` that names nothing. A statement id is provenance for work of
> the last day; **job `run_id`s are durable** and are what a later reader should follow.

> **The phase plan is NOT part of this repository.** It lives in a git-ignored working directory,
> so no link to it is given: F3 shipped a section pointing a public reader at that directory and
> they reached nothing. Everything a reader needs from it is here.

---

## 0. Task 0 — measured before this phase's plan existed

The phase opened on one unknown that decided its whole shape, and it had three readings: that the
triage accelerator needs no language model; that it needs one for the narrative half, which would
be an account and a token; or that opening an issue needs a second, independent token. **All three
were measured before a line of the plan was written, and two of them dissolved the gates they
implied.**

Everything in §0 is **Controller-verified**, 2026-08-24, on branch `feat/f6-rca-agent` cut from
`origin/main` at `5d769a3` over a clean tree.

### 0.1 THE MODEL GATE DOES NOT EXIST — the workspace already serves models on the credential in `.env`

`.env` sets exactly seven names, measured by diffing the process environment before and after
sourcing it rather than by reading the file: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
`KAFKA_BOOTSTRAP`, `KAFKA_PASSWORD`, `KAFKA_USER`, `RP_CLIENT_ID`, `RP_CLIENT_SECRET`.
**There is no model key.**

> **`GEMINI_API_KEY` is set on this box and is AMBIENT** — present in the environment *before*
> `.env` is sourced. It belongs to the operator's machine, not to this project: nothing in the
> repository's documented workflow reads it, and it exists on exactly one machine and in no CI.
> **It is recorded here so that nobody later "discovers" it and reads it as a project
> credential.** Nothing in this phase uses it.

**But the Databricks workspace this project has held a token for since F0 serves eleven
foundation-model endpoints**, `GET /api/2.0/serving-endpoints`, every one `state.ready = READY`,
every one `FOUNDATION_MODEL_API` / pay-per-token: `databricks-gpt-oss-120b`,
`databricks-gpt-oss-20b`, `databricks-qwen3-next-80b-a3b-instruct`, `databricks-qwen35-122b-a10b`,
`databricks-llama-4-maverick`, `databricks-gemma-3-12b`, `databricks-meta-llama-3-1-8b-instruct`,
`databricks-meta-llama-3-3-70b-instruct`, and three embedding endpoints
(`databricks-gte-large-en`, `databricks-bge-large-en`, `databricks-qwen3-embedding-0-6b`).

The SQL warehouse resolves fifteen `ai_*` functions — `ai_query`, `ai_classify`, `ai_gen`,
`ai_summarize`, `ai_extract`, `ai_forecast`, `ai_mask`, `ai_parse_document`, `ai_similarity`,
`ai_top_drivers`, `ai_translate`, `ai_analyze_sentiment`, `ai_complete`, `ai_fix_grammar`,
`ai_generate_text` — statement `01f19fce-ff7e-1ebc-9629-2cb736e3e5b1`.

#### AND NONE OF THAT IS A CAPABILITY — IT IS `read_kafka` AGAIN

F5 §0.2 established that a function resolving in a catalog of names says nothing about whether it
can do anything, and F5 §0.3 recorded a probe that returned **one string for four different
worlds**. So the capability was measured with two arms, and the second is what makes the first
mean anything:

| arm | statement | result |
|---|---|---|
| **positive** — `ai_query('databricks-gpt-oss-20b', 'Reply with exactly this token and nothing else, no punctuation: OPLF6-5d769a3-7d3a91')` | `01f19fcf-24d1-1084-a6d7-f176ee200ad9` | **returned `OPLF6-5d769a3-7d3a91`** |
| **negative control** — the identical call against `databricks-endpoint-that-does-not-exist-7d3a91` | `01f19fcf-2d56-1c89-a3c8-0fae5b00d685` | **FAILED** — `[REMOTE_FUNCTION_HTTP_FAILED_ERROR]` → HTTP 404 `RESOURCE_DOES_NOT_EXIST`, **naming the endpoint**, SQLSTATE `57012` |

**The nonce was coined seconds before the call and embeds this branch's base revision.** No canned
response, no result cache and no stub can contain it. And the negative arm establishes that the
probe **can report a refusal**, and that the refusal *names what was missing* — so a success is
not the only word the instrument knows.

**Conclusion, and it is the shape of the phase:** a language model is reachable **with the
credential this project already has**. No account, no token, no new secret. **The gate is
dissolved by measurement rather than waived by judgement.** What remains is spend, and master
protocol §5 lists spending Free Edition compute as explicitly **not** a gate.

#### ~~AND UNLIKE F5's TRIAL, THE SPEND IS MEASURABLE~~ — THE TABLE IS READABLE AND IT DOES NOT ITEMISE INFERENCE

`system.billing.usage` **is** readable on this workspace: **1,654 rows, 2026-07-23 → 2026-08-24**,
statement `01f19fd0-3f5e-1065-8587-cdf527a4d117`. F5 published **no** figure for its Redpanda trial
balance because four control-plane endpoints returned `NOT_FOUND` and Prometheus returned 401.

> ~~**F6 has no such excuse, so every model call this phase makes gets its cost published rather
> than estimated.**~~
>
> **WITHDRAWN WITHIN THE HOUR, BY THE CONTROLLER'S OWN NEXT PROBE, AND IT IS THE CONTROLLER'S OWN
> OVERCLAIM.** *Readable* was measured; *itemised* was assumed from it, and they are not the same
> property. Measured after the sentence above was written:
>
> - The two `ai_query` calls of §0.1 produced **no model-serving row** for 2026-08-24. That date
>   carries only `PREMIUM_SERVERLESS_SQL_COMPUTE`, `PREMIUM_JOBS_SERVERLESS_COMPUTE`,
>   `PREMIUM_DATABRICKS_STORAGE`, `PUBLIC_CONNECTIVITY_DATA_PROCESSED` and
>   `INTER_REGION_EGRESS` — statement `01f19fd4-30c7-1e18-8cd4-23fc5a5d3864`.
> - **No inference SKU exists in this workspace's entire billing history.** All eight distinct
>   `(sku_name, billing_origin_product)` pairs ever recorded are SQL, JOBS,
>   PREDICTIVE_OPTIMIZATION, DEFAULT_STORAGE, GENIE and three NETWORKING rows — statement
>   `01f19fd4-3c4f-1b48-8f60-25944e14f36d`. **There is no `MODEL_SERVING` and no
>   `FOUNDATION_MODEL`.**
>
> **So `ai_query` from the warehouse is not separable from the warehouse.** Its cost is either
> folded into `PREMIUM_SERVERLESS_SQL_COMPUTE` or not billed at all on this edition, and **this
> document cannot tell those two apart** — one figure covering two worlds is ADR 0018's species,
> and it would be arriving in the sentence that congratulated this phase for not being F5.
>
> **What F6 may publish instead, and it is narrower on purpose:** the `PREMIUM_SERVERLESS_SQL_
> COMPUTE` DBU delta across the sweep window, labelled as an **UPPER BOUND that includes every
> other statement issued in that window**. A bound that says what it contains is worth more than
> a figure that cannot say where it came from. **F5's refusal to substitute a ring-buffer cap is
> the precedent, and it is followed rather than cited.**
>
> A three-day lag was considered and does **not** rescue the original claim: the withdrawal rests
> on the *absence of the SKU across the whole history*, not on today's row being missing. If an
> inference SKU appears later, this bullet is what gets corrected, and the correction is a
> measurement rather than a hope.

### 0.2 THE GITHUB GATE DOES NOT EXIST EITHER — for the path this phase takes

- The repository has **Issues enabled**, is **public**, and carried 2 open issues at Task 0.
- `gh` on this box is authenticated with scopes **`gist, read:org, repo, user, workflow`**. `repo`
  covers issue creation. **No new PAT is needed to open an issue from this machine.**
- `.github/workflows/ci.yml` has **no `permissions:` block**, and the repository's
  `default_workflow_permissions` is **`read`**. So the built-in `GITHUB_TOKEN` cannot open an
  issue **as configured today**.

> **WHETHER A JOB-LEVEL `permissions: issues: write` ELEVATES ABOVE A REPOSITORY DEFAULT OF `read`
> IS NOT ASSERTED HERE.** It is a **prediction** (§2), closed by a run. This repository has already
> paid once for a `ci.yml` claim written from memory and "verified" against a weak source — that
> file's own comment carries the whole history — and a documented behaviour quoted from memory is
> the same move wearing a different hat.

- **A Databricks job is the one caller with no credential at all.** F5 §0.2 measured that
  serverless has outbound egress to `github.com:443`, so the platform is not the blocker; the
  absence of a token is. **Putting a GitHub PAT into a Databricks secret scope would be a real
  human gate**, and the phase's design (ADR 0020) is what avoids needing one.

**Conclusion: neither prompted gate is required.** The two were correctly identified as
**independent**, and treating them as a single package would have cost the phase both halves.

### 0.3 THE INCIDENT CORPUS — eleven incidents wearing twenty-two rows

`fail_on_dq` is this project's headline DQ event. F4 recorded it firing **11 times across 5 jobs**.
Measured today — statements `01f19fcf-6013-1c79-ac48-83d0d530ed40`,
`01f19fcf-69e3-10dd-a645-2026ba632139`, `01f19fcf-75d1-1aef-8b39-bc7f0a332d90`:

```
timeline_rows | distinct run_id | distinct job_run_id | jobs | oldest       | newest
      22      |       22        |         11          |   5  | 2026-07-24   | 2026-08-12
```

**Eleven `job_run_id`s, each carrying exactly two task runs, every one `FAILED`.** Uniform across
all eleven — not one job's accident. That is **`max_retries: 0` failing to prevent a retry**, a
fact this repository has measured twice before (24 `(job_run_id, task_key)` pairs in F4; again in
F5's T8), **now appearing inside the corpus the RCA agent is built to read.**

> **NEITHER NUMBER IS NEW, AND SAYING SO IS THE POINT.** `docs/f4-run-evidence.md` already carries
> **both**: *"`fail_on_dq` fired **eleven** times across **five** jobs"* (§0.2) and *"`fail_on_dq`,
> which genuinely issues none on all **22** of its runs"* (§ on the telemetry labels). **What was
> not established there is that they are the same population at two grains** — the two sentences sit
> in different sections, neither refers to the other, and nothing says which one answers *"how many
> incidents are there"*.
>
> **What this phase adds is the reconciliation and its consequence:** that 22 = 11 × 2 **uniformly**,
> that the second row is the retry rather than an hour slice (`timeline_periods = 1` on every row),
> and — §0.4 — what that second row does to a join. **A reader of F4 could take either number for
> the incident count and find support for it in that document.**

The full corpus, joined to the quarantine tables — statement
`01f19fcf-a821-1ff3-9488-b458aa0b8c86`. Job names are shown with the bundle's development prefix
**redacted**, because it carries an operator identifier this repository does not commit:

| job_run_id | job (prefix redacted) | quarantine | rows |
|---|---|---|---|
| `592660596679630` | `[dev <operator>] opl-bronze-payments` | payments | **2,000** |
| `1121645114029617` | `… opl-bronze-cnpj-socios` | socios | **1,797** |
| `409962018634322` | `… opl-bronze-cnpj-socios` | socios | **1,786** |
| `128878829411613` | `… opl-bronze-cnpj-estabelecimentos` | estab | **4** |
| `321750543973966` | `… opl-bronze-cnpj-empresas` | empresas | **1** |
| `371067950667703` | `… opl-bronze-cnpj-empresas` | empresas | **1** |
| `184706631093131` | `… opl-bronze-cnpj-lookup` | — | **0** |
| `241387611390862` | `… opl-bronze-cnpj-lookup` | — | **0** |
| `996871467498110` | `… opl-bronze-cnpj-lookup` | — | **0** |
| `187805471003061` | `… opl-bronze-cnpj-estabelecimentos` | — | **0** |
| `315230730740144` | `… opl-bronze-cnpj-estabelecimentos` | — | **0** |

2,000 + 1,797 + 1,786 + 4 + 1 + 1 = **5,589** — **F4's census exactly.** That is a confirmation
rather than a second measurement that merely agreed in shape: the per-reason totals F4 published
decompose here at a finer grain, and `null_or_empty_nome_socio_razao_social`'s **3,583** is
**two** incidents three weeks apart (1,797 and 1,786), not one.

**So the grain of an incident is `(table, batch)` — equivalently `job_run_id` — and not a reject
reason.** A severity computed per reason fuses two socios incidents into one.

**Five incidents carry no quarantined rows at all**, and that is not the same fact as "the gate
rejected nothing" — see §0.5.

### 0.4 THE JOIN KEY, AND TWO SILENT TRAPS MEASURED ON IT

A quarantine's `_batch_id` is the failing run's `job_run_id`. That join is what the whole phase
rests on, and it has two ways to be wrong without saying so — statement
`01f19fcf-b8e1-18de-8af6-71e4cc0d5b7f`:

| the join, written as | rows |
|---|---|
| `CAST(t.job_run_id AS STRING) = q._batch_id` | **4,000** |
| `t.run_id = CAST(q._batch_id AS BIGINT)` | **0**, and it raises nothing |
| ground truth — rows in `bronze_payments_quarantine` for that batch | **2,000** |

> ~~**`_batch_id` is `STRING` while `job_run_id` is numeric**, so the wrong form is a type trap.~~
>
> **FALSIFIED, AND IT IS THE CONTROLLER'S OWN, PUBLISHED IN THE DOCUMENT WHOSE PURPOSE IS
> SEPARATING MEASURED FROM ASSUMED.** `typeof(_batch_id)` **was** measured — `string`. The other
> half was not: **`run_id`, `job_run_id` and `job_id` are ALL `string`** in
> `system.lakeflow.job_task_run_timeline`, statement `01f19fd7-246a-152c-8850-9712543bdff6`, and
> all three are `STRING` in the view as well. **`CAST(job_run_id AS STRING)` converts nothing.**
>
> **So the trap is not a type trap at all — it is a WRONG-KEY trap, and the cast was a red herring
> the controller introduced in its own probe.** `run_id` is the **task**-run id; `job_run_id` is
> the **job**-run id; `_batch_id` equals the latter. The zero row count is two different
> identifiers being compared, not a coercion failure.
>
> **Found by T1's implementer, who was asked to say what in the brief was wrong and did.** The
> shipped code never carried the confusion — `incidents.py` keys on `job_run_id` and its docstring
> says the no-op cast is kept as a type contract, not as a repair.

**What survives, and it is the half that matters:**

- **The wrong key returns zero rows and raises nothing.** `run_id` and `job_run_id` are both
  plausible column names on a table about runs, both are `STRING`, and one of them silently
  answers "there are no quarantined rows for this incident". That is the same shape as F4's
  measured `t.job_run_id` trap on the telemetry join — F4's own words: *"the obvious-looking
  `t.job_run_id` yields zero rows, silently"* — arriving through the opposite door.
- **The right key returns 4,000 against a 2,000-row quarantine**, because the timeline holds
  **two** task-run rows per incident and every quarantined row fans out across both. **A severity
  driven by that count overstates every incident in this workspace by exactly 2×.** This one is
  independent of the key confusion above and is the more dangerous of the two, because 4,000 is
  not obviously wrong the way 0 is.

**This is the third silent 2× this project has measured**, after `execution_duration_seconds`
repeating across hour-sliced periods (F4: one task run carrying `0 / 5633 / 5633` for 5,635 s of
wall clock, `SUM` reporting 11,266). The cause here is different and the shape is identical: **the
timeline is not one row per incident, and the first thing anyone reaches for assumes it is.**

### 0.5 F4's VIEW ALREADY SOLVES IT, AND FIVE INCIDENTS HAVE A STATE THAT IS NOT "CLEAN"

`dataops_task_telemetry` returns all 22 `fail_on_dq` rows with **`attempt` ∈ {1, 2} as an explicit
column**, `timeline_periods = 1`, `result_state = FAILED`, and **`sql_telemetry =
'no_sql_attributed'`** — F4's honest rendering of *"this task issued no SQL, so there is no
metric"*, never a zero. Statement `01f19fcf-da27-14cf-9f23-89b297c14e7a`.

**So F6 reads its incident feed from F4's view and instruments nothing.** ADR 0018's Decision 1 —
*DataOps derives; it does not instrument* — extends one layer up without amendment.

All four F4 views still exist (`dataops_freshness`, `dataops_reconciliation`,
`dataops_reconciliation_by_file`, `dataops_task_telemetry`) — statement
`01f19fcf-3bb2-1293-afc5-cfb0739dd086`.

#### The five zero-row incidents, and why the derivation is read off the WIRING

`databricks/resources/bronze_payments_job.yml`: `check_bad_rows` is a `condition_task`,
`op: EQUAL_TO`, left `{{tasks.dq_gate_batch.values.bad_row_count}}`, right `"0"`. `promote` depends
on outcome **`true`**; **`fail_on_dq` depends on outcome `false`**. And `dq_gate_batch` appends the
rejected rows to quarantine **before** it publishes `bad_row_count`.

**So the chain closes at both ends: `fail_on_dq` ran ⇒ `dq_gate_batch` SUCCEEDED and published a
non-zero count ⇒ that many rows were in quarantine at that instant.** Zero rows there today means
**the evidence was removed after the fact** — a verdict that is neither `clean` nor low severity.

**Three of the five are accounted for by F4** and are quoted rather than re-derived:
`bronze_cnpj_lookup_quarantine` *"holds zero rows, because that table was recreated on 2026-07-31,
a week after its firings"* — F4's label on that is **Reported** (provenance lens), and it stays
Reported here.

**Two are NOT accounted for by anything in the record: `187805471003061` and `315230730740144`,
both estabelecimentos.** F4's sentence is about a *quarantine table* that is empty; the
estabelecimentos quarantine is **not** empty — it holds the 4 rows of `128878829411613`. So these
two incidents sit in a populated table and contributed nothing to it, which F4's explanation does
not cover. **They are listed as unexplained rather than folded into the lookup sentence**, and
§3 carries them.

#### And `dataops_reconciliation` covers only SIX of the eleven

Statement `01f19fd3-5528-16f8-be47-048443cf7419`: the view holds **15 rows**, and the five
zero-quarantine incidents appear in **none** of them — their staging rows are gone too, so the
view that would name their table cannot. **Only `592660596679630` is `stranded_gated`**
(10,000 staged / 0 promoted / 2,000 quarantined / **8,000 unaccounted**); every other batch,
including four that fired the gate and were later repromoted, reads `reconciled`. That is F4's
documented behaviour — *"a reconciliation is not a test for 'the gate fired'"* — and it means the
reconciliation verdict and the incident feed answer different questions.

### 0.6 HOW AN INCIDENT KNOWS ITS TABLE — declared, not inferred

Since §0.5's five incidents have no quarantine row and no reconciliation row, neither can name the
bronze table an incident is about. **`system.lakeflow.job_tasks` cannot either**: its columns are
`account_id, workspace_id, job_id, task_key, depends_on_keys, change_time, delete_time,
timeout_seconds, health_rules` — statement `01f19fd3-8a0f-170d-ae9b-47f2894d3552`. **Task
parameters are not in the system tables.**

**The repository declares it.** Every bronze job YAML gives its `fail_on_dq` task the table as a
literal parameter, and the seven declared job names map onto the seven `opl.bronze.registry` keys
exactly:

| declared job name | `fail_on_dq` parameter |
|---|---|
| `opl-bronze-cnpj-empresas` | `empresas` |
| `opl-bronze-cnpj-estabelecimentos` | `estabelecimentos` |
| `opl-bronze-cnpj-lookup` | `lookup` |
| `opl-bronze-merchant` | `merchant` |
| `opl-bronze-payments` | `payments` |
| `opl-bronze-ptax` | `ptax` |
| `opl-bronze-cnpj-socios` | `socios` |

**So the resolution is exact rather than heuristic**, and it is locked against the YAMLs rather
than retyped — `fail_on_dq`'s own docstring records that a hardcoded quarantine name once *"sent
two real Estabelecimentos runs to the lookup quarantine"*.

**The runtime `job_name` carries a bundle development prefix that embeds an operator identifier.**
It is stripped, never pinned, and never committed — CLAUDE.md forbids committing an OS username,
and F5 established the same rule for the winget packages directory.

### 0.7 A DEFECT IN THE RECORD F5 CLOSED FIVE DAYS AGO

`.plans/HANDOFF.md`'s **"TWO STANDING FACTS THAT MUST NOT BE LOST"** item 1, and
`docs/f5-run-evidence.md` §3's *"Carried out of the phase as follow-ups"*, both state that
`src/opl/streaming/watermarked_dedup.py` sits at **795** of the 800-line cap and
`tests/test_streaming_watermarked_dedup.py` at **793**, and instruct whoever touches either to
split it first.

**Measured on `origin/main` today with `wc -l`: 459 and 292.** Traced across the F5 branch:

```
f5d9abd 795   c19ea0b 795   2fa610c 795   2d077a8 459   5d769a3 459
```

**The split landed in `2d077a8` — the closing documentation commit itself**, which also created
`src/opl/streaming/lateness.py`. **The commit that published the standing fact is the commit whose
own diff falsified it.**

The tightest tracked files today are **`tests/vault/test_socios_vault.py` (799)** and
**`src/opl/gold/facts.py` (799)**, then `tests/test_payment_streaming.py` (783),
`tests/vault/test_cnpj_vault.py` (780) and `src/opl/generator/profiles.py` (773).

> **This is F5's own named species arriving on schedule.** That phase's closing lesson was that
> **the defect had moved out of the code and into the document that judges it** — *"the one place
> a review that only reads code will not look"*. The split was correct engineering; the sentence
> beside it was not updated. Both files are corrected in this phase (§9 condition 5), and a
> retraction closes by `grep -i`, not by fixing the paragraph one happened to open.

### 0.8 THE COMPARISON BASELINE HAS A RETIRED KEY IN IT, AND THE NAIVE QUERY LOSES FIVE RUNS SILENTLY

The master spec asks the agent to *compare against the last N executions*. **The task that
identifies "a gate ran" was renamed mid-project, and nothing in the telemetry says so.**

Measured — statements `01f19fd5-33c7-1293-845f-1a093602421e`,
`01f19fd5-4642-1a17-b465-85e990f4bb40`, `01f19fd5-5e38-178c-98e9-54a9cff66dbb`:

| task_key | job runs | jobs | oldest | newest |
|---|---|---|---|---|
| `ingest` | 29 | 7 | 2026-07-24 | 2026-08-18 |
| **`dq_gate`** | **5** | **1** | 2026-07-24 | **2026-07-24** |
| `check_bad_rows` | **29** | 7 | 2026-07-24 | 2026-08-18 |
| `promote` | 27 | 7 | 2026-07-24 | 2026-08-18 |
| `fail_on_dq` | 11 | 5 | 2026-07-24 | 2026-08-12 |
| **`dq_gate_batch`** | **24** | 7 | 2026-07-27 | 2026-08-18 |

`dq_gate` is the lookup's retired whole-table gate — `databricks/src/dq_gate_batch.py`'s own
docstring records the migration: *"The lookup's whole-table gate (dq_gate.py) is gone, so the
lookup inherits batch scoping."* **The telemetry keeps running under both names and marks
neither as superseded.**

**The identity that settles which key is safe, and it is arithmetic rather than argument:**

```
dq_gate (5) + dq_gate_batch (24) = 29 = check_bad_rows (29) = ingest (29)
```

**`check_bad_rows` is the only gate-adjacent task present in all seven jobs across the whole
window**, and its count equals the sum of both gate spellings exactly. It is the condition task
that consumes `bad_row_count`, so it exists on every run where a gate produced a verdict —
under either spelling.

#### What the naive key costs, measured on the worst table

A history query keyed on `task_key = 'dq_gate_batch'` returns, for
`opl-bronze-cnpj-lookup`, **one** gate run. Its real history is **six** — five under `dq_gate`
on 2026-07-24 and one under `dq_gate_batch` on 2026-07-31.

**It is wrong by 5, and `1` is a perfectly plausible answer.** Nothing raises, nothing is NULL,
and no column says a task name was retired. **This is ADR 0018's species landing inside the
baseline that severity is judged against** — and it lands hardest on the one table whose three
incidents already have no quarantine evidence (§0.5), so the naive query truncates the history of
precisely the incidents that most need it.

#### And the horizon is short for four of seven tables, whatever key is used

Distinct `dq_gate_batch` job runs per job: estabelecimentos **8**, payments **5**, socios **4**,
merchant **3**, empresas **2**, lookup **1**, ptax **1**.

**A comparison against "the last 5 executions" is impossible for four of the seven tables**, and
F4's measured retention floor (~25 days) will keep shortening it — the oldest row in this whole
corpus is 2026-07-24, 31 days back. **So `insufficient_history` is not an edge case in this
workspace; it is the majority state**, and an agent that reports "compared against the last N,
nothing anomalous" without saying how many it actually found would be reporting a comparison it
did not make for most tables in the project.

---

## 1. What has been built and run

### 1.1 T1 — the incident feed, and the two traps closed on live data

`src/opl/triage_agent/incidents.py` reads F4's `dataops_task_telemetry` and returns **one record
per incident**. It classifies nothing, samples nothing and writes nothing. The query is spelled
**once, in SQL**, with the relation parameterised so a test can point it at a fixture — the shape
`opl.bronze.reconcile` established and for its reason: an identical Python ladder beside the SQL
would be a second spelling.

**Controller-verified:** `17 passed in 34.14s`, read from the output file rather than from the
terminal status — the trap `CLAUDE.md` names and which F5's controller fell into three times.

**Reported** — measured by the independent reviewer, against the **live** view rather than the
fixture, and this is the part that matters:

| what | statement | result |
|---|---|---|
| the shipped SQL over the real `dataops_task_telemetry` | `01f19fda-f4bf-159f-a9ea-adf5f003d51f` | **11 rows**, every one `attempts = 2`, `result_states = ["FAILED","FAILED"]`, the dev prefix stripped from every `job_name`, and the bronze table resolved for **all 11** — matching §0.3 job-run-id for job-run-id |
| the fan-out, both spellings, one statement | `01f19fdb-041d-1884-bd88-71e714604bab` | feed ⋈ quarantine = **2,000** · raw timeline ⋈ same quarantine = **4,000** · quarantine total = **2,000** |

**So §0.4's 2× is closed by the feed on real data, not argued away in a docstring.** The same
statement carries the wrong answer beside the right one, which is what makes the right one
readable as a repair rather than as a number.

#### The mapping fork, and why the runtime read was refused

The seven job → table pairs are **declared as data and locked by a test**, on `cadence.py`'s
pattern. The alternative — reading `databricks/resources/*.yml` at runtime — was refused on a
measured ground rather than on taste: `pyproject.toml` packages only `src/opl`, so the bundle YAML
is **not in the wheel**. A module reading it works on this box and raises in the one place it
matters. A third option, deriving the table from the `opl-bronze-[cnpj-]<key>` naming convention,
was rejected in the docstring as *a correlate of the mapping rather than the mapping*.

The lock sweeps every `*.yml` in `resources/`, pulls each `fail_on_dq` parameter from the parsed
YAML and holds it equal to the declaration **in both directions**. **Reported:** the reviewer
fired it with four mutations of their own — renaming a job, adding an eighth gate job, deleting a
gate task, and handing a gate two parameters — rather than only with the implementer's.

#### A behaviour checked on BOTH computes, because this repository has paid for assuming one

The unknown-job arm returns NULL through `element_at(map(...), key)`. **Reported**, measured by the
reviewer: NULL under `spark.sql.ansi.enabled` **both `true` and `false`** on local pyspark 3.5.9,
**and on the deploy target** — statement `01f19fdb-585e-120f-a8ac-c19c41ea54eb`,
`missing_key_is_null=true`, `null_key_is_null=true`, `strip_of_null_is_null=true`.

That is F-DB's `to_date`/ANSI lesson applied before it cost anything: a NULL that depends on an
engine setting is a different claim on each compute.

#### The review chain, and the four refusals that improved the brief

Implementer → independent reviewer → correction → review of the correction.

**The implementer refused four instructions from the controller's dispatch, and two of them
corrected the controller:**

- Refused a `result_state = 'FAILED'` filter. `fail_on_dq` is reachable only through
  `check_bad_rows → false`, so its **presence** is the incident; filtering on terminal state would
  make membership depend on a structurally-FAILED column and silently drop an attempt the timeline
  ingested with a NULL state.
- **Falsified §0.4's type claim** — see the retraction there.
- **Named the controller's own test #4 as this project's species**, and replaced a standalone zero
  with a zero discriminated against a 12 over the same two tables.
- Flagged `table` as a reserved word duplicating `dataops_reconciliation`'s `source`.

**The independent reviewer returned no BLOCKING defect, two HIGH and four MEDIUM**, every one
demonstrated by a mutation they ran and reverted with a checksum proof rather than by reading:

- **HIGH-1** — the test naming the two import-time guards **could not fail**: it restated the
  guard bodies over valid data. Deleting both import-time calls still gave `17 passed`. The module
  cites `cadence.py` as its pattern; `cadence.py` pairs every import-time refusal with a
  `pytest.raises` sibling, and T1 took the pattern without the half that proves it works.
- **HIGH-2** — the test proving "an unknown job yields NULL" **passed against a lookup rewritten
  to return NULL for everything**. ADR 0018's species, inside the file written to hunt it.
- **MEDIUM-1** — the rename is right and **more** right than the controller stated: `dataops_
  freshness` also publishes the registry key as `source`, so `table` was the **third** spelling.
  And renaming the column to `source` while the function's `source` parameter means *the relation
  to read from* would reproduce the defect inside one signature — so the parameter moves too.
- **MEDIUM-2/3/4 and three LOW** — four assertions that dict-key on `job_run_id` and so absorb the
  very fold defect they read as checking; a `== []` with its positive control in a different test
  function; and §0.8's retired-task-key hazard absent from the header of the module that pins a
  task key.

> **§0.8's hazard was CLOSED rather than left open, and by the reviewer rather than by argument.**
> A full `task_key` census over the telemetry view shows **no retired predecessor for
> `fail_on_dq`**. The only retired pairs in this workspace are `dq_gate`(5)/`dq_gate_batch`(24)
> and `reclaim`(1)/`reclaim_landing`(4), **neither of which is a failure task** — so the pinned
> key is safe, and the module header now says how that was established rather than that it is
> true. **Reported.**

#### The correction, and the one defect it found in itself

All eight findings closed. **Controller-verified**, re-run independently and read from the output
file: `uv run pytest tests/triage_agent/test_incidents.py tests/test_size_caps.py
tests/dataops/test_cadence.py` → **`36 passed in 27.79s`**; `ruff` clean; `incidents.py` **338**
lines with its longest function at 43, `tests/triage_agent/test_incidents.py` **728**. T1's own
tests went **17 → 22**.

**Each fix is pinned by a named test that fails when the fix is reverted** — *Reported*, and the
correction quoted the failing summary for each rather than asserting it:

| finding | test that now fails on revert |
|---|---|
| HIGH-1 | `test_the_guards_run_at_import_so_deleting_the_call_is_a_failure_not_a_silent_loss` (re-executes the module body via `importlib.util`), plus two `pytest.raises` siblings on `cadence.py`'s template |
| HIGH-2 | the unknown-job test now asserts a **known** job resolves in the same view — the reviewer's `'-zz'` mutation fails it |
| MEDIUM-1 | three tests fail if the emitted name is reverted; parameter renamed `source=` → `view=` so the column and the parameter no longer share a word with two meanings |
| MEDIUM-2 | `len(records) == len(rows)` beside every keyed comprehension — **five sites, one of which the finding had not listed** |
| MEDIUM-3 | the right-key join moved **into** the test; with the quarantine mutated to match nothing it now fails on `assert 0 == 12` instead of passing on `[]` |
| MEDIUM-4 | `test_the_lock_catches_a_gate_task_renamed_in_the_bundle_and_not_here` |

**And the falsified docstring sentence was deleted rather than softened.** HIGH-1's claim —
*"a future edit removing the import-time call is a failure rather than a silent loss"* — was shown
false; it is now either true and tested, or gone.

> **THE CORRECTION CAUGHT THIS PHASE'S SPECIES INSIDE A TEST IT WAS WRITING TO CLOSE THE SPECIES,
> AND IT CAUGHT IT ITSELF.** *Reported.* Its first draft of the cross-module lock — the test
> asserting that `source` is the name both F4 views already use — searched `freshness_sql()` for
> the literal `"' AS source,"`. **It PASSED under the mutation it existed to catch**, because the
> cadence leg left that substring behind elsewhere in the generated SQL.
>
> **It was found by running the failure arm rather than by trusting the green**, and replaced with
> counts derived from `REGISTRY` — the lock now fires at `assert 7 == (2 * 7)` on one leg and
> `assert 0 == 7` on the other.
>
> This is F5's pattern exactly — *a correction shipping a new ceiling with no test of its failure
> arm* — arriving in the first correction pass of this phase, in the test written against the very
> defect it reproduced. **What made the difference is that the failure arm was run before the
> green was believed.**

**The correction separated its tested claims from its untested ones and named the untested ones
plainly**, which is the discipline F5's closing lesson asks for. Stated as **not** tested, and
carried here at the same label: the `task_key` census result (a fact about the live workspace that
nothing in the wheel can assert — quoted from the reviewer, **not re-measured**); *"runs recorded
under a retired spelling stay outside this feed"* (a statement of absence with no possible test);
and `COLLECT_LIST`'s documented non-determinism (from Spark's docs, and the corpus cannot
distinguish sorted order from attempt order because **every state in it is `FAILED`**, which the
header says outright rather than leaving the price to look paid).

**One LOW was skipped with a reason rather than silently**: `tests/test_task_wiring.py`'s literal
`"fail_on_dq"` names a **script module** under `databricks/src/`, not a task key — the two coincide
today, and importing a triage constant to name a script would manufacture a coincidental second
spelling in another task's file.

#### THE ONE TEST THAT TOOK THREE ATTEMPTS TO STOP BEING BLIND

The lock asserting that both F4 views publish `source` is the most instructive artefact of this
task, and each of its three versions looked correct to the person who wrote it.

| attempt | what it checked | how it was found blind |
|---|---|---|
| 1 — substring | `"' AS source,"` present in the generated SQL | **passed under the mutation it existed to catch** — another leg left the substring behind. Found by **its own author, running the failure arm** before believing the green |
| 2 — per-key counts | 14 and 7 occurrences across the two views | **passed under two mutations** — it counted the generated **CTE union legs** and never the column the views **publish**. Found by the **review of the correction** |
| 3 — projected columns | the column list each view actually publishes | audited against **twelve mutations** by a fourth agent; both renames turn it red |

**A count blind spot one hop further out than a substring blind spot is still a blind spot** — and
the second attempt was shipped *as the repair for the first*.

**Reported**, from the narrow review of attempt 3, which was dispatched precisely because nobody
but its author had read it: `_published_columns` returns the exact published projection for both
views, hand-checked; it handles nested parens, `CASE … END AS source`, literals containing commas
and `)`, window functions and reflowed whitespace; and it **fails loudly** — raising rather than
returning a wrong list — on almost every shape it cannot parse. Two shapes would be silently
wrong (a top-level `UNION`, and a trailing line comment ending in ` AS source`); **neither view
has either shape**, and both are listed in §3 as latent.

The review also established that the retained per-key COUNT arm is **not decoration**: renaming
`AS source` in one union leg only is caught by the count and **structurally cannot** be seen by
the published-column read. The two arms cover different directions.

> **AND THE NARROW REVIEW FOUND ONE REAL DEFECT IN THE PARSER, WHICH IS WHY IT WAS SENT.** The
> literal-marking arm left a string on the first `'` it met, so a **backslash-escaped apostrophe**
> ended the literal early and the rest of it was read as query structure — surfacing as a bare
> `IndexError` naming neither the note that was edited nor the column the test is about.
> `opl.dataops.freshness._quote` exists *precisely* so an operator's prose in `cadence.why` may
> carry an apostrophe, and its own docstring calls that *"a matter of time"*; `CLAUDE.md` records
> that `''` is not an escape on Databricks and the backslash is. **The shape the parser had to
> survive is the one the codebase invites.**
>
> **Controller-verified**, fixed and pinned by `test_a_backslash_escaped_apostrophe_does_not_end_a
> _literal`, with the failure arm run before the green was believed: removing the escape handling
> gives `1 failed, 9 passed` and the new test is the only failure; the file restores to an
> identical sha256.

#### The split, and a process lesson the reviewer could not work around

`tests/triage_agent/test_incidents.py` reached **845** of the 800-line cap during the fixes, so it
was split at a **measured** seam rather than a chosen one: the lower half referenced **zero** of
the upper half's fixture machinery, and `importlib`, `shutil`, `yaml` and `RESOURCES` were imported
at the top only for it. Result: **508** (13 tests) + **409** (9 tests, plus the escape test added
after), total unchanged, and the declaration file runs in **~1.4 s with no JVM** against the
25–33 s it used to wait for a Spark session it never asked for.

> **THE REVIEWER COULD NOT CERTIFY "NO ASSERTION CHANGED IN THE MOVE", AND SAID SO RATHER THAN
> IMPLYING IT.** The file was intent-to-add and absent from `HEAD`, there was no stash and no
> snapshot, so there was nothing to diff the moved bodies against. They verified names, counts,
> import surface and residue — all consistent with a clean move — and stated plainly that nothing
> in the current contents *contradicts* the claim, which is weaker than the claim.
>
> **The fix is procedural and is adopted for the rest of this phase: commit the task before
> splitting it.** A split is only reviewable as a split if there is a baseline to diff it against.
> This task is committed at `56773b6`, so every later split has one.

**Controller-verified at the close of T1:** `uv run pytest tests/triage_agent/ tests/test_size_caps.py
tests/dataops/test_cadence.py` → **`38 passed in 40.58s`**, read from the output file; `ruff` clean;
`incidents.py` **347**, `test_incidents.py` **508**, `test_incidents_declaration.py` **409**.

## 2. Predictions, published before the runs that test them

*(Written before the runs. See §2 of this document as tasks reach their runs.)*

## 3. What is still unexercised

**Protocol §9 condition 6.** A path that ran zero rows through it is not a path that works, and
this list is what stops the phase being read as more exercised than it is. Each entry says what
would exercise it. **Uniformly *Reported* unless a cell says otherwise** — these are read off the
code and the record rather than measured.

*(Filled as tasks land; the entries below are already owed.)*

### Properties this phase chose NOT to guard, and the choice is recorded

- **"The declaration half of T1's tests touches no Spark" is enforced by nothing.** The split
  bought a ~1.4 s no-JVM file; adding a Spark test to it would silently cost that, and no test
  would go red. `tests/test_size_caps.py` covers the line count and nothing covers the JVM.
  **The guard was considered and deliberately not built**, on the narrow reviewer's argument:
  every cheap spelling of it is this repository's hunted species one level down — a signature
  scan for a `spark`/`probe` parameter passes while a module-scope `SparkSession.builder`, an
  autouse fixture or a transitive `pyspark` import still starts a JVM, and a wall-clock assertion
  is flaky on this box. The honest spelling is ~15 lines in `test_size_caps.py`'s style **with a
  control asserting the same reader finds those tokens in the sibling file** — a one-file special
  case inside a repo-wide sweep, which is scope the phase spec says to resist. *What would
  exercise it: someone adding a Spark test to that file and nobody noticing.*
- **Four prose corrections in T1 are asserted by nothing** — the "three of seven" job-name count,
  the "two names across three sibling views" phrasing, the `view`-versus-`source` wording, and the
  `sorted` rationale in `table_of_job_sql`. They are true as of `56773b6` and would go stale
  silently. *What would exercise them: nothing. They are prose, and are listed so that a later
  reader knows they carry no test.*

### Latent in T1's projection reader, and neither shape exists today

- **A top-level `UNION` in either F4 view.** `_projection_of` takes the last top-level `SELECT`,
  while Spark names a union's output from the **first** leg — so a union whose legs disagree would
  return a wrong column list **silently**. Both views fold through CTEs today and neither is a
  top-level union. *What would exercise it: rewriting either view as a top-level union.*
- **A trailing line comment ending in ` AS source` before a comma.** Contrived, and nothing in
  either builder emits SQL comments. *What would exercise it: a hand-written comment inside a
  generated projection.*

### Carried in from the phase's own Task 0

- **Two `fail_on_dq` incidents on estabelecimentos — `187805471003061` and `315230730740144` —
  have no quarantined rows and nothing in the record explains them.** F4's account covers only the
  three lookup firings (a table recreated 2026-07-31). These two sit in a **populated** quarantine
  and contributed nothing to it. *What would exercise it: reading the two runs' own task output,
  if it is still retained, or a Delta history read on `bronze_cnpj_estab_quarantine`.*
- **Whether a job-level `permissions: issues: write` elevates above a repository default of
  `read`.** Asserted by nobody and quoted from no documentation (§0.2). *What would exercise it:
  the first workflow run that tries.*
