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

### 0.9 THE COLUMN MASKS, MEASURED — because a later module cites this section for them

**Controller-verified 2026-08-24**, statement `01f19ff8-d9b0-1928-b669-cdc750ea7926`, over
`workspace.information_schema.column_masks`:

| table | column | mask |
|---|---|---|
| `bronze_cnpj_socios` | `nome_do_representante` | `workspace.default.mask_personal_name` |
| `bronze_cnpj_socios` | `nome_socio_razao_social` | `workspace.default.mask_personal_name` |
| `bronze_cnpj_socios_quarantine` | `nome_do_representante` | `workspace.default.mask_personal_name` |
| `bronze_cnpj_socios_quarantine` | `nome_socio_razao_social` | `workspace.default.mask_personal_name` |

**Four masks, on two tables, one contract.** ADR 0008 records the same four at 2026-08-01/08-18
against `system.information_schema.column_masks`; this is a re-reading on the day F6's evidence
sampler was designed, and it agrees.

**Why this section exists at all, and it is a defect in the record rather than a measurement gap.**
`src/opl/triage_agent/evidence.py` cites *"`docs/f6-run-evidence.md` carries the measurement"* for
the claim that the workspace's four masks are the four its declaration profiles — the paragraph
whose whole purpose is separating what the module **asserts** from what it **quotes**. The
controller had measured it and never written it down, so **the citation resolved to nothing**, in
the one place a reader goes to check that a quoted fact was really measured. Found by the review of
T2's correction, which followed the pointer.

#### AND THIS IS WHAT MAKES THE TWO LARGEST INCIDENTS DANGEROUS TO SAMPLE

The two socios incidents are **3,583 rows** whose reject reason is
`null_or_empty_nome_socio_razao_social` — the rejection **is** that the name column is null or
empty — **and that same column is masked.** A triager sampling it reads `***` and cannot tell
*"masked from me"* from *"empty, which is why the row was rejected"*: one string covering the two
worlds, where the second is the very fact the reject reason asserts.

ADR 0018 already records this shape once, in a privacy deploy check where *"`***` was the answer
under all four possible outcomes"*. **T2's sampler is built so the question cannot arise** — it
emits the word `masked` from the declaration and **never reads the column** (§1.2).

### 0.10 THE COMPARISON BASELINE, MEASURED PER INCIDENT — and three silent ways to get it wrong

§0.8 established which task key is safe. **This is the same question one grain finer: for each of
the eleven incidents, how many prior gate executions actually exist to compare it against.** T4 is
the module that answers it, so the answer is measured here first and T4 reproduces it or one of
the two is wrong.

**Controller-verified 2026-08-25**, three statements against the live workspace.

#### The gate history, per job, on the stable key

`check_bad_rows` job runs grouped by `job_id` — statement `01f1a0ac-7ca0-1b71-abdc-cad9432050f6`:

```
8 · 6 · 5 · 4 · 3 · 2 · 1  =  29
```

**Seven `job_id`s, each with exactly one `job_name`, and NO run carries a NULL `job_name`.** Both
halves matter: the counts reconcile with §0.8's `check_bad_rows (29)` exactly, and the absence of
NULLs means the history could be keyed on either column *today*. It is keyed on `job_id` anyway —
`telemetry.py` keeps task runs whose job has aged out of `system.lakeflow.jobs`, and those rows
carry a NULL name and a live id. **That is a property of this corpus on this date, not a guarantee,
and it is written down as the former.**

**Also measured: `check_bad_rows` runs ONCE per job run** — 29 task runs over 29 job runs, on every
one of the seven jobs. The two-attempt fan-out §0.4 measures is `fail_on_dq`'s alone: the retry
re-ran the *failing* task, not the whole job. **So the 2× trap does not bite this key in this
corpus — which is exactly why a test over this corpus cannot see a missing fold.** The fold to
`job_run_id` belongs in the query regardless, and only a constructed doubled row can prove it is
there.

#### The prior-execution count per incident, and it is the phase's own numbers

Statement `01f1a0ac-9340-13d4-b55f-e756e11950a7`. Job names resolved through §0.3:

| job_run_id | table | prior runs, **stable key** | same, **naive timestamp** | same, **naive key** | prior incidents |
|---|---|---|---|---|---|
| `128878829411613` | estab | **7** | 8 | 7 | 2 |
| `184706631093131` | lookup | **4** | 5 | **0** | 2 |
| `187805471003061` | estab | **3** | 4 | 3 | 1 |
| `241387611390862` | lookup | **3** | 4 | **0** | 1 |
| `409962018634322` | socios | **3** | 4 | 3 | 1 |
| `315230730740144` | estab | **2** | 3 | 2 | 0 |
| `592660596679630` | payments | **2** | 3 | 2 | 0 |
| `371067950667703` | empresas | **1** | 2 | 1 | 1 |
| `996871467498110` | lookup | **1** | 2 | **0** | 0 |
| `1121645114029617` | socios | **0** | 1 | 0 | 0 |
| `321750543973966` | empresas | **0** | 1 | 0 | 0 |

**It reconciles in both directions, which is what makes it a measurement rather than a query that
ran.** The estabelecimentos job has 8 gate runs and its three incidents sit at prior 7, 3 and 2 —
runs #8, #4 and #3. The lookup job has 6, five of them on 2026-07-24 under `dq_gate` and one on
2026-07-31 under `dq_gate_batch`; its three incidents are runs #5, #4 and #2, **all five of the
`dq_gate` day**. Socios' and empresas' zero-history incidents are each that job's FIRST gate run
ever. Every column is derivable from the others and none was typed twice.

#### THE THREE WAYS TO GET IT WRONG, AND ALL THREE RETURN A PLAUSIBLE NUMBER

**1. The naive timestamp comparison inflates EVERY incident by exactly one — including the two
zeroes.** `check_bad_rows` starts *before* `fail_on_dq` inside the same job run, so "gate runs that
started before this incident started" counts **the incident's own gate run** as prior history. The
middle column above is that query. It is wrong by one on all eleven, nothing raises, and **`1` is
the answer it gives for the two incidents whose true history is `0`** — so the one state a triager
most needs to see, *this table has never been gated before*, is the state the defect deletes. This
is the fourth silent off-by-one or 2× this project has measured, after §0.4's fan-out, F4's
hour-sliced durations and F4's telemetry join.

> **AND THE MINIMAL REPAIR IS NOT THE TIMESTAMP, WHICH THE NAME ABOVE IMPLIES AND WHICH IS
> WORTH CORRECTING.** *Amended 2026-08-25, from T4's independent reviewer's measurement.*
> Adding an **identity** exclusion — `job_run_id <> :batch_id` — to the controller's hand query,
> leaving its `fail_on_dq` anchor and its `<` exactly where they were, returns all eleven correct
> counts. So the defect is *"no identity exclusion, on an anchor that postdates the row being
> excluded"*, and the two halves are separable: **move the anchor onto the gate run itself and the
> identity predicate becomes provably unreachable dead code** — which is what T4's implementer
> refused the controller over, and why the shipped query uses `<=` rather than `<`.

> **AND IT IS THE CONTROLLER'S OWN, CAUGHT IN THE CONTROLLER'S OWN PROBE.** Statement
> `01f1a0ac-5fe2-17d0-8896-c2b9ffa853ff` is that query, run first and read as the answer. It was
> caught not by review but by **arithmetic that refused to close**: it reported 8 prior runs for an
> incident on a job with 8 gate runs *in total*, which requires a ninth run that does not exist.
> **A number that cannot be true is a cheaper defect than a number that merely is not** — every
> other cell in that statement was off by one and looked fine.

**2. The naive KEY does not shorten the lookup's history; it ERASES it.** Keyed on
`dq_gate_batch`, all three lookup incidents return **0** prior executions rather than 4, 3 and 1.
§0.8 measured the retired-name hazard as *"wrong by 5"* at table grain; at incident grain it is
total, because the only `dq_gate_batch` run the lookup ever had is dated **after** all three of its
incidents. **The three incidents whose quarantine evidence is already gone (§0.5) are exactly the
three whose history the naive key also deletes.**

> **THE `0` IS A PROPERTY OF WHAT THE QUERY ANCHORS ON, AND SAYING SO MAKES THE FINDING
> SHARPER RATHER THAN WEAKER.** *Amended 2026-08-25, on T4's implementer's reading of the
> shipped module against this section.* The `0` above is the controller's hand query, which
> anchors each incident on its own **`fail_on_dq`** row — always present, so the subquery
> still returns a count, and the count is `0`. **T4's module anchors on the incident's own
> `check_bad_rows` run instead**, so under the retired spelling there is no anchor at all and
> the reading comes back `gate_run_absent` with **NULL** counts rather than `0`.
>
> **Both are the same wrong key and they fail differently, and the difference is the whole
> point of this section.** `0` asserts *"this table was never gated before"* — a lie, and the
> most reassuring one available. `gate_run_absent` asserts *"I could not look"*. The naive
> key reaches the first through the anchor that is always there; **an implementation is not
> safe from this hazard merely by not spelling `dq_gate_batch`, it is safe by refusing to
> publish a count it did not measure.**

**3. `check_bad_rows.result_state` cannot tell a fired gate from a clean one.** Statement
`01f1a0ac-d078-154b-b9bf-94c1e0b4b44a`: **`check_bad_rows` is `SUCCEEDED` on all 29 runs**,
`dq_gate` on all 5, `dq_gate_batch` on all 24. The condition task succeeds whether its answer is
true or false. So a history that counted "clean prior runs" off the terminal state would report
**29 SUCCEEDED — the same number a workspace with zero incidents would report.** ADR 0018's
standing instruction, arriving in the column that looks most like the answer. **The only signal
that a gate found rejected rows is the PRESENCE of a `fail_on_dq` task run**, which is what the
`prior incidents` column above is counted from, and it is `FAILED` on all 22 of its rows.

#### WHAT THIS DOES TO `insufficient_history`

Against the spec's *"last N executions"* at **N = 5**: **exactly one incident of the eleven has
five prior gate runs.** Ten do not, and two have none at all. §0.8 called
`insufficient_history` the majority state at table grain; at incident grain it is **ten of
eleven**, and *"compared against the last 5, nothing anomalous"* would be a sentence about a
comparison that did not happen for ten of the eleven incidents this phase exists to triage.

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
> `opl.dataops.freshness.sql_string_literal` (named `_quote` when this was written; T3 promoted it
> to a public name because it had grown a cross-package caller — ***Reported***, and on that narrow
> ground only: measured by AST over `src/` and `databricks/src/`, **49** private imports cross a
> module boundary and all 49 stay inside one subpackage, so `_quote`'s `opl.triage_agent` caller
> was the only one reaching across a **subpackage** boundary and after the rename there are none)
> exists *precisely* so an operator's prose in `cadence.why` may
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

### 1.2 T2 — the evidence for one incident, and the state that is not "clean"

Committed at `ac984e5`, split at `5a401ef`. `opl.triage_agent.evidence` assembles the quarantine
census by reject reason, a publishable row shape, and the reconciliation verdict. It classifies
nothing and writes nothing.

**The census cannot return an empty result**, by construction: an ungrouped `COUNT(*)` is exactly
one row on every input and a `LEFT JOIN … ON true` preserves it. **Reported**, and the reviewer
could not defeat it on an empty table, an other-batches-only table, or a NULL/empty `batch_id`.

**The removal is two words, not one** — `evidence_missing_quarantine_empty` (the table holds
nothing: the three lookup firings F4 accounts for) against `evidence_missing_batch_absent` (the
table holds other batches and not this one: `187805471003061` and `315230730740144`, which nothing
explains). **The implementer split it against the controller's brief and was right:** one word lets
the unexplained pair borrow the explained trio's account, which is the only account that exists.

**And `masked` is emitted WITHOUT READING THE COLUMN**, which is the second refusal and the better
one. The controller's brief said to report a masked column's *value* as masked; the implementer
refused the framing — a sampler that reads and then reports "masked" is **reader-dependent**, since
`is_member(...)` hands a group member the real name, so the same function would produce **a
different artefact per principal**. An artefact whose contents turn on the caller's group
membership is exactly what must not be published. The generated SQL carries a literal where every
other column carries a `CASE`.

> **THE PHASE'S FIRST BLOCKING DEFECT, AND IT IS A PII PATH IN A PUBLIC REPOSITORY.** Adding
> `nome_socio_razao_social AS leaked_name` — **unbackticked** — to `row_shapes_sql` left the whole
> suite green. Two blindnesses combined: the guard banned only the **backticked** spelling, while
> unbackticked is this module's own house spelling three lines away; and the taint sweep could not
> see that column at all, because the corpus pins it to `''` — **being empty is what got those rows
> rejected.** Both are closed, and each was measured to catch the leak with the other removed.
>
> **Nothing leaked.** The shipped code was clean; the defect was that no test could tell.

### 1.3 T3 — severity, and the hold that keeps the biggest incident from reading as the most urgent

Committed at `0503761`. A severity ladder and a **separate** action ladder, with every input
published beside the grade.

**The corpus hid two confounds and only the first was obvious.** *"Most rows"* and *"the only
stranding"* are the same incident, so a row-count grader and a verdict-aware one rank this
workspace identically — closed by a constructed disagreement. **The second was found by review:**
*"evidence removed"* and *"has no reconciliation row"* are the **same five incidents** (§0.5), so an
arm keyed on the verdict spelling ranks the corpus identically too — **and the first constructed
case could not see it**, because both its incidents are *in* the view. Two more constructed
relations close it; the substitution now reddens exactly them and nothing else.

**The hold, and why it is not a lookup table.** `592660596679630` is the largest incident and its
correct recommendation is **do not promote** — a decision recorded in `docs/f4-run-evidence.md`
§1.2, derivable from no column. It ships as a **declared hold carrying its citation**, on
`cadence.py`'s pattern, and the note carries the **decisive** argument rather than the weak one that
section rejects by name. **The falsifier is what stops it being decoration:** deleting the hold must
*flip* the recommendation, and respelling the ladder as `batch_id = '592660596679630'` — the lookup
spelling — fails that test while the citation test stays green.

#### THE FOURTH PUBLISHABLE STATEMENT JOINS T2's PAIRING RATHER THAN REPEATING HALF OF IT

`severity_sql`'s row is bound for a public GitHub issue. T3 first shipped only the name-count lock;
the review demonstrated it green under a `SELECT *` leak. **Controller-verified 2026-08-25**, with
the leak spliced in and its presence in the generated SQL confirmed before the result was read:

| arm | under a `SELECT *` over the socios quarantine projected as `l.*` |
|---|---|
| the name-count lock (T3's) | **22 passed** — blind by construction; the statement emits three `SELECT *` |
| the runtime taint sweep (T2's, now walking `severity_sql`) | **1 failed** — `severity on payments/592660596679630 emitted a value` |

**Neither arm is total and each one's only cover is the other's measured blind spot** — the name
count cannot see a leak that never spells the column; the sweep cannot see one that *transforms* the
value (`SUBSTR(nome_socio_razao_social, 1, 3)` carries no sentinel). Both are written down.

#### Eight refusals, and the two worth the record

The implementer refused eight of the controller's instructions and **all eight were right**.

- **A threshold between 1 and 4 rejected rows was refused rather than invented.** The only
  reject-count line this repository has ever argued for is ADR 0006 condition 2's `>= 10`, and
  inventing a second so a test could show three values is what `cadence.py` exists to refuse. The
  ordering is asserted including socios instead, so the size arm is load-bearing on live counts with
  nothing fabricated.
- **`unaccounted > 0` was refused as a stranding signal because `over_promoted` makes it NEGATIVE**,
  so an arithmetic grader would rank the batch whose counts contradict themselves as the mildest
  thing the module emits. **The fixture already contains one** — a live demonstration rather than an
  argument.

**`freshness._quote` is promoted to `sql_string_literal`.** The hold's note is English prose reaching
a SQL literal and `''` is not an escape on Spark, so the one correct spelling had to be reused — and
its own docstring said *"THIS FUNCTION IS NOT REUSED"*, which reuse makes false. The paragraph is
replaced rather than softened, and the two claims it fused are separated: it has a caller now, and
the latent hazard is still open.

> **A PATTERN THIS PHASE HAS NOW MEASURED FOUR TIMES.** In T1, twice in T2 and again in T3, the
> **review of a correction** found its defects in **new claims the correction had written**, not in
> bugs it fixed. T3's went further: a header still asserting the premise that produced the blocking
> defect, and a docstring claiming the count lock sees a leak *"in any spelling"* when it is
> structurally blind to `*`. **The correction is the most dangerous commit in a task** — it is
> trusted, it writes fresh prose, and nobody has read it.

### 1.4 The split of `test_severity.py`, and the first defect this phase created by writing two files at once

Committed at `66f3e96`, **free of behaviour**, against the baseline `0503761` that exists precisely
so this one could be checked. `test_severity.py` **798 → 575** (12 `probe` tests), new
`test_severity_declaration.py` **328** (10 tests, no JVM), `_HELD_BATCH` relocated to `conftest.py`,
two docstrings repointed, one phantom citation fixed in `severity.py`.

**Controller-verified:** `88 passed in 279.14s` across `tests/triage_agent/` and
`tests/test_size_caps.py`, read from the output file. **66 at T2's close + T3's 22 = 88**, and
12 + 10 = 22, so the arithmetic closes in both directions and no test was lost or duplicated —
which is the check this phase added after a killed subagent resurrected a deleted file and a suite
went green with 150 instead of 135. `ruff` clean; every touched file LF, zero CR bytes.

**Reported**, and by two agents using different methods, which is what makes it a proof rather than
a second opinion: an AST comparison over all 30 top-level definitions, and a raw-source block
comparison that also sees comments, string spelling and blank lines. The 22 test names after equal
the 22 before **in both directions**; every free name in every moved body resolves to the **same
object** in its new module; no module-level name was gained or lost. The no-JVM property was
established twice, once by reading `SparkContext._gateway` at session finish with a control that
printed a live gateway, and once by **deleting Java from `PATH` entirely** and still getting
`10 passed in 0.11s`, with a control that errored inside `launch_gateway`.

#### THE PROOF SCRIPT WAS BLIND TO NINETEEN TWENTIETHS OF WHAT THE CHANGE ADDED

The implementer's script compared **30 named functions and 8 named constants**. Of the ~95 lines the
change added, roughly **90 were module-docstring prose** — which that script cannot see at all,
along with comments, imports, module-level statements outside those eight names, and free-name
rebinding.

**Reported**, and demonstrated rather than argued: the reviewer rewrote both module headers into
flat falsehoods — *"EVERY TEST HERE BUILDS A JVM"*, *"section 3 DOES NOT EXIST AND NEVER DID"* — and
got **`14 passed`** and a clean `ruff`.

> **The verification artefact was itself the species it was built to refuse.** Its author's own
> red-arm control — changing a literal inside a function body — probed the one bucket it covers
> well. ADR 0018's instruction is *ask what else would produce that value*; what else produces
> `compared 30 functions; 0 differ` is **a change that rewrote every header into a lie**.

#### THREE CITATIONS POINTED AT NOTHING AND ONLY ONE WAS THE SPLIT'S FAULT

- `test_evidence_sample.py` named a test that had moved files — the known cost of a split, and the
  reason `5a401ef` is called *"repoint what then pointed at nothing"*.
- **`severity.py` cited a test name that has never existed in this repository.**
  `...flips_the_recommendation_on_the_one_batch_that_carries_one`; the real name ends
  `_on_that_batch`. Introduced **already broken** by `0503761` and invisible because the name is
  wrapped mid-token across two lines, so no `grep` for it could match. `git log --all -S` on the
  phantom fragment returns only the commit that wrote it.
- **A date naming a day on which the thing it described did not exist.** The same docstring dated
  its `SELECT *` leak measurement `2026-08-24`; `severity_sql` and `test_severity.py` are both
  created by `0503761`, dated **2026-08-25**, which is also the date §1.3 gives that measurement.
  A sweep over **all 18** ISO dates in `src/opl/triage_agent/` and `tests/triage_agent/` — each
  checked against the earliest commit whose blob contains the exact line, by two agents using
  different methods — found **that one and no other**.

#### THE ONE DEFECT NEITHER AGENT COULD HAVE PREVENTED, AND IT IS THE CONTROLLER'S PROCESS

The implementer wrote, in the new file's header, that `docs/f6-run-evidence.md` §3 *"does not yet
name this one, so as of this commit the property here is unguarded AND unrecorded."* **That was true
when it was written.** The controller was widening §3 **in the same working tree at the same time**,
and both changes were bound for the same commit — so the sentence was false at the moment it would
have shipped.

> **NEITHER PARTY WAS WRONG AND THE RESULT WAS WRONG ANYWAY.** This is a hazard with no precedent in
> the phase's rules: the standing instruction is that the code cites the record, and the record is
> the controller's, so **a controller who edits the record while an agent writes code that cites it
> can falsify a correct sentence without either of them making a mistake.** The reviewer found it,
> and found it by opening §3 rather than by reading the sentence.
>
> **The rule this phase adopts for the rest of its tasks:** while an agent holds a file that cites
> the record, the record is frozen — or the agent is told which section is moving under it. The
> cheaper half is that the controller stopped editing `docs/` for the whole of the correction pass
> once this was understood, which is why the second and third passes produced no repeat.

#### AND THE CONTROLLER'S OWN FIX SHIPPED THE PHASE'S PATTERN FOR THE FIFTH TIME

Recorded in full in §3's second bullet, and named here so §1.4 is not the section that omits it: the
header's *"five of the six recommended actions"* had **understated** since the day it was written —
wrong in the one direction that costs a reader nothing and trips nothing, which is why T3's review
and the split's review both passed over it. The controller corrected the count and, in the same
edit, wrote *"AND NOTHING ASSERTS EITHER COUNT"* — **false, eleven lines above the assertion that
refutes it.** The review of the correction refuted it with two mutations.

**The fifth time in this phase that a correction's defect landed in the NEW claim rather than in the
bug it fixed, and the first time the new claim was the controller's own.** The retraction ships
struck through in the header rather than deleted, and §3 now carries the narrower true gap: **a
seventh recommended action, reached by nothing, would leave every test in this repository green.**

### 1.5 T4 — the last-N comparison, and the character that decided whether a defence existed

Committed at `1803652`. `opl.triage_agent.history` returns, for one incident, `prior_executions`,
`prior_incidents` and a **reading**, at `N_EXECUTIONS = 5`.

**Controller-verified:** `134 passed in 98.31s` across `tests/triage_agent/` and
`tests/test_size_caps.py`, read from the output file — 88 before the task plus 46. `ruff` clean;
zero CR bytes by raw byte count; longest function 44 against the 50 cap.

**Prediction 6 CONFIRMED, on fixtures, by three parties independently** — the implementer, the
independent reviewer on a fixture it built without reading the implementer's, and the correction
reviewer. The eleven prior counts are §0.10's exactly and no quantity was adjusted to match.
**Prediction 7 is falsified in letter and confirmed in substance; §2 carries why, and the cause was
the controller's own later dispatch.** Neither has met the workspace: the live arm is T8's.

#### THE SHARPEST ARTEFACT OF THE TASK IS ONE CHARACTER

The controller instructed: exclude the incident's own run **by identity, not by a timestamp
comparison**. **The implementer refused it as under-specified and was right.** With the natural
`started_at < own_gate_start`, the identity predicate `job_run_id <> :batch_id` is **provably
unreachable** — the anchor is `MIN(started_at)` over the incident's own rows, so every own-run row
satisfies `started_at >= anchor`. **The controller's own defence would have shipped as dead code**,
and dead code that looks like a guard is this repository's most-hunted species. The shipped query
uses `<=`, which makes the identity load-bearing.

**And then the review found that nothing held the decision.** *Reported*, measured:

| mutation | result |
|---|---|
| `<=` → `<` | **36 passed** — silent |
| then delete the now-dead identity predicate too | **36 passed** — still silent |
| control: delete the identity predicate, keep `<=` | **10 failed, 26 passed** |

**One character turned a load-bearing predicate into decoration with no test going red, and the
correct decision was protected only by the fact that somebody had thought about it.** It is pinned
behaviourally now, by a constructed **tie** — two gate runs of one job sharing an instant, which
`<` drops and `<=` keeps (7 against 6).

> **THE TIE LIVES INSIDE THE MEASURED FIXTURE AND THAT WAS CHALLENGED, THEN CLEARED BY
> MEASUREMENT.** T3 put its constructed cases in separate labelled relations so that *"they were
> read from different views"* could never explain a result, and the controller asked whether
> injecting a tie into the measured corpus broke that discipline. **Disabling the retiming fails
> exactly one test — the tie test, on its own self-check** (`assert 2 == 1`, *"the fixture lost its
> tie"*) — so it moves no measured quantity, and it is labelled INVENTED in three places. The
> reviewer's argument for injecting rather than separating is the better one and is adopted: the
> tie's whole content **is a relationship between two runs of the measured schedule**, so a
> separate relation would have had to restate the schedule to hold it, which is exactly how *"they
> were read from different views"* becomes available again.

#### A FOURTH SILENT DEFECT THAT NOBODY PREDICTED, FOUND BY AN IMPLEMENTER REFUSING AN INSTRUCTION

§0.10 named three. The implementer named a fourth and broke a controller instruction to close it:
**a batch whose `check_bad_rows` row has aged out of the telemetry has no anchor, so every naive
spelling returns `0` prior executions** — indistinguishable from *never gated before*. It needs no
drift at all: F4's ~25-day retention floor ages the telemetry out while a quarantine keeps its
`_batch_id` forever.

The controller had instructed that the number found be on every row. **The module emits
`gate_run_absent` with NULL counts instead** — *a number that was not measured is not published as
`0`* — and the refusal is right for T2's reason one level up. **Three absence words, not one:**
`gate_run_absent` is *"I could not look"*, `no_prior_execution` is *"nothing has ever been gated"*,
`insufficient_history` is *"fewer than N exist"*. The eleven read **8 / 2 / 1**.

#### TWO CORRECTIONS, AND THE SIXTH INSTANCE OF THIS PHASE'S PATTERN

The first correction closed the HIGH and four MEDIUM. **Its review found that it had replaced a
false header claim with a different false header claim, in the same paragraph, while fixing it** —
*"EACH LINK MOVES ONE THING FROM THE ONE BEFORE IT"*, where the four constants in chain order move
**one, then three, then two**, and the bullets underneath silently changed baseline mid-list. **The
sixth time on this phase that a correction's defect landed in a new claim rather than in the bug it
fixed.** The second correction **deleted the umbrella rather than writing a third**; each bullet now
names the query it is measured against.

The same review upheld a suspicion the controller had raised and the first correction had answered
honestly: **three assertions in the bundle sweep that no test fired**, inside a file whose own
header promises *"EVERY LOCK HAS A MUTATION BESIDE IT"* — the shape T1's review already condemned in
this package. **The second correction refused the controller's count**: the sweep carries **four**
asserts, not three, plus a fifth check elsewhere and a sixth lock with no mutation at all. It fired
all six, so the header's sentence is now true of every lock that reads a file.

> **AND THE CONTROLLER BROKE ITS OWN PUBLISHED RULE IN THE ENTRY ABOUT ENTRIES GOING STALE.** §3's
> declaration-file bullet was corrected from "TWO files" to "THREE" and the argument **two
> paragraphs below it** was left saying *"There are two files now."* The rule this document
> publishes — *a retraction closes by `grep -i`, not by fixing the paragraph you happened to open*
> — was written by the controller and then not followed by the controller, inside the section about
> exactly that failure. **Third occurrence of the species, and §3 carries it in place.**

#### AND THE HISTORY OF THIS FILE'S CAP IS NOW A STANDING FACT

`tests/triage_agent/test_history.py` closes the task at **788 of 800**. Two files in this package
have already been split after reaching **845** and **798**, each costing a review pass.
**The next addition to that file splits first**, and T4 built its declaration half as a separate
file from the start rather than splitting into one later, which is the lesson applied rather than
recorded.

---

## 2. Predictions, published before the runs that test them

**WHERE THESE WERE FIRST WRITTEN, AND WHY IT HAS TO BE SAID.** Predictions 1–5 were published in
the phase plan on **2026-08-24**, before T1 ran and before any of them was tested. That plan lives
in a git-ignored working directory (see the preamble), **so a reader of this repository has no way
to check that claim** — it is reproduced here for the reason the preamble gives, and the honest
label is that the *provenance* of the date is Reported while each prediction's *outcome* below
carries its own label. Predictions **6–8 are new, are T4's, and are written here BEFORE the module
that tests them exists.**

Each names what falsifies it, and each falsifier is a real outcome rather than a hedge.

| # | prediction | status |
|---|---|---|
| 1 | The feed returns **11** incidents over **22** task runs, and the naive spelling returns 22 | **CONFIRMED** |
| 2 | **Five** incidents classify `evidence_missing` and **six** carry rows | **OPEN** — closed by T8 |
| 3 | A `permissions: issues: write` block opens an issue despite `default_workflow_permissions: read` | **OPEN** — T6 |
| 4 | The LLM control returns a confident, fluent root cause for a `job_run_id` that exists nowhere | **OPEN** — T7 |
| 5 | The LLM control assigns the **same** severity band to the 2,000-row and the 1-row incident when the counts are stripped | **OPEN** — T7 |
| 6 | The shipped history module reproduces §0.10's eleven prior-execution counts **exactly** | **OPEN** — T4 |
| 7 | ~~**Ten of eleven** incidents report `insufficient_history` at N = 5~~, and **two** report zero prior executions | **FALSIFIED IN LETTER, CONFIRMED IN SUBSTANCE** — and by the controller's own later instruction; live arm still T8's |
| 8 | The lookup's three incidents still return **4 / 3 / 1** on the stable key against the LIVE view at T8 | **OPEN** — T4/T8 |

**1 — CONFIRMED, and on live data rather than on the fixture.** T1's independent reviewer ran the
shipped SQL against the real `dataops_task_telemetry` — statement `01f19fda-f4bf-159f-a9ea-adf5f
003d51f`, **11 rows, every one `attempts = 2`** — and the fan-out statement `01f19fdb-041d-1884-bd
88-71e714604bab` carries the naive answer beside the right one (4,000 against 2,000 for a 2,000-row
quarantine). *Reported*, from the reviewer. §0.3's 22-over-11 is Controller-verified separately.
*Falsified by:* either number moving, which would have meant §0.3 measured a smaller population
than it claimed. Neither moved.

**2 — OPEN, and it is listed as open on purpose.** T2 and T3 built the classification and their
tests exercise it, but **on a fixture**. §0.3 measured the live corpus's five-and-six split, which
is the same *fact about the workspace* — it is **not** the same claim as *"the shipped module
classifies them that way against the live tables"*, and only T8's workspace run closes that.
*Falsified by:* any of the six coming back empty (the quarantine recreated since §0.3) or any of
the five acquiring rows (a repromote nobody recorded).

**3** — *Falsified by:* a 403 from the API, in which case the local `gh` path stands and the CI
path is reported as **refused**, not as untried. Asserted by nobody and quoted from no
documentation (§0.2).

**4** — *Falsified by:* the model declining, which would be a genuinely interesting result, would
weaken plan decision §1.1, and would be published as weakening it.

**5** — *Falsified by:* it separating them anyway, which would mean it inferred from the table name
rather than from the numbers. **That is a third outcome, neither pass nor fail**, and this
prediction is written so it can be seen.

> **PREDICTIONS 4 AND 5 ARE ABOUT A STOCHASTIC INSTRUMENT AND A SINGLE SAMPLE IS NOT A RESULT.**
> Each sweep runs the same prompt **n ≥ 5 times** and reports the spread, and any clause that
> cannot survive being restated as a rate over n trials is rewritten **before** it is published.

**6** — the eleven counts are **7 · 4 · 3 · 3 · 3 · 2 · 2 · 1 · 1 · 0 · 0** (§0.10, per incident).
*Falsified by:* any count differing. That is not a hedge — the controller's hand query and the
shipped module are two spellings of one question, **and §0.10 records that the controller's first
spelling of it was wrong by one on all eleven.** If they disagree, this document says which was
corrected and how it was decided, rather than adopting the module's answer because it is newer.

**7 — FALSIFIED IN LETTER, AND THE CONTROLLER FALSIFIED IT ITSELF, AFTER PUBLISHING IT.** The
shipped module reads **8** `insufficient_history`, **2** `no_prior_execution`, **1**
`history_complete`. Ten of eleven are still short of N and two still have none — *the substance
holds* — but only eight carry the word the prediction named.

**What changed it was T4's dispatch, written by the controller three hours after the prediction.**
That dispatch required *"fewer than N exist"* to be a distinct state with its own word and cited
T2's two-word absence split approvingly, including that T2's implementer had been right to refuse
the controller's single word. The implementer applied it and then split once more, for a reason
the prediction had no way to contain: **`gate_run_absent` is a third absence** — the batch whose
gate run has aged out of the telemetry — and it is NOT *"fewer than N"* at all, it is *"I could
not look"*. So the eleven distribute across three words where the prediction assumed one.

> **A PREDICTION IS NOT FALSIFIED BY BEING OUT-DESIGNED, AND THIS ONE IS RECORDED AS FALSIFIED
> ANYWAY.** The alternative — restating it in the vocabulary the design later adopted — is how a
> prediction stops being able to be wrong. The number that was checkable was `10 × insufficient_
> history`; it came out `8`. **What this costs the controller is the right to say the prediction
> was confirmed; what it buys the reader is that §2 still contains a claim that could fail.**

**The two zeroes remain the load-bearing half**: they are the only incidents for which *"no prior
execution exists"* and *"the query counted its own run"* give different answers, and §0.10 measures
that the naive spelling reports `1` for both. **Both are now `no_prior_execution`, not `0` on an
`insufficient_history` row** — a stronger separation than the prediction asked for.

**Confirmed ON FIXTURES ONLY, by two parties independently** — T4's implementer and T4's reviewer,
the latter on a fixture it built itself without reading the former's. *Reported.* **Nothing has run
against the workspace**; the live arm is T8's, and prediction 8 is what it turns on.

**8 — the one prediction here that the controller genuinely does not know the answer to.** The
lookup's four, three and one prior runs are all `dq_gate` rows dated **2026-07-24**. F4 measured a
**~25-day retention floor** — on `system.query.history`, not on `system.lakeflow.job_task_run_
timeline`, and the two are different tables with no established common floor. Those rows are
**32 days old on 2026-08-25 and still present**, which is already past that floor. *Falsified by:*
them having aged out by the workspace run — **which would not be a failure but the phase's first
measurement of the timeline's own retention**, and would be published as that.

## 3. What is still unexercised

**Protocol §9 condition 6.** A path that ran zero rows through it is not a path that works, and
this list is what stops the phase being read as more exercised than it is. Each entry says what
would exercise it. **Uniformly *Reported* unless a cell says otherwise** — these are read off the
code and the record rather than measured.

*(Filled as tasks land; the entries below are already owed.)*

### Properties this phase chose NOT to guard, and the choice is recorded

- **"The declaration half is free of Spark" is enforced by nothing, and it is now THREE files.**
  T1's split bought a ~1.4 s no-JVM file, T3's split bought a second — `tests/triage_agent/test_
  severity_declaration.py`, **~1.09 s wall with no JVM gateway** — and **T4 built its third from
  the start rather than splitting into it**: `tests/triage_agent/test_history_declaration.py`,
  **23 tests in 1.69 s**. Adding a Spark test to any of the three would silently cost the property
  and no test would go red. `tests/test_size_caps.py` covers the line count and nothing covers the
  JVM.

  **The third file's no-JVM property is the best-established of the three and is *Reported*:**
  T4's reviewer ran it under a plugin replacing `pyspark.java_gateway.launch_gateway`,
  `pyspark.context.launch_gateway` **and** `subprocess.Popen` (tripping on any argv naming java or
  spark-submit), got `tripped=[]`, **and fired the positive control** — the same plugin on
  `test_history.py` raises `A JVM GATEWAY WAS LAUNCHED`. That is a guard proven able to fire,
  which is the half the first two files' measurements did not have.

  > **AND THIS ENTRY WENT STALE IN PLACE FOR THE SECOND TIME, THE SAME WAY, ONE TASK APART.** It
  > said "TWO files" while T4's declaration file cited it as the third — the citing file was
  > right and the cited entry was wrong, which is the exact inversion of the T3 occurrence, where
  > the record was right and the citing file wrong. **The rule adopted in §1.4 — freeze the record
  > while an agent holds a file that cites it — prevented the first failure mode and has no
  > purchase on this one**, because here the agent correctly anticipated an update the controller
  > then did not make. A frozen record is not a current one. *Found by T4's independent reviewer,
  > following the citation rather than reading the sentence.*

  **The guard was considered and deliberately not built**, on the narrow reviewer's argument:
  every cheap spelling of it is this repository's hunted species one level down — a signature scan
  for a `spark`/`probe` parameter passes while a module-scope `SparkSession.builder`, an autouse
  fixture or a transitive `pyspark` import still starts a JVM, and a wall-clock assertion is flaky
  on this box. The honest spelling is ~15 lines in `test_size_caps.py`'s style **with a control
  asserting the same reader finds those tokens in the sibling file**.

  **THAT ARGUMENT IS RE-READ HERE RATHER THAN RE-CITED, BECAUSE ONE OF ITS TWO LEGS NO LONGER
  HOLDS.** It was refused as a *one-file special case inside a repo-wide sweep* — scope the phase
  spec says to resist. There are **three** files now, so that leg is gone; the other leg, that
  every cheap spelling of the guard is blind, is untouched and is the one that still decides it.

  > **AND "two" STOOD IN THIS SENTENCE AFTER THE ENTRY ABOVE IT WAS CORRECTED TO "three" — THE
  > THIRD OCCURRENCE, INSIDE THE PARAGRAPH THAT DIAGNOSES THE SPECIES.** The controller fixed the
  > bullet it had opened and not the argument two paragraphs below, which is **exactly** the
  > failure the phase's own rule names: *a retraction closes by `grep -i`, not by fixing the
  > paragraph you happened to open.* The rule was written down, published in this document, and
  > then not followed by the person who published it — in the entry about entries going stale.
  > *Found by T4's correction reviewer, reading the section rather than the bullet.* **The**
  entry stood literally true while its subject doubled**, because a bullet that names its subject
  by name cannot notice a sibling — this phase's second species, the defect moving out of the code
  and into the document that judges it, arriving in the ledger of what is *not* guarded. It stays
  unbuilt for this phase, recorded as a decision whose stated reason is now half of what it was.
  *What would exercise it: someone adding a Spark test to any of the three and nobody noticing.*
- **A SEVENTH recommended action, reached by nothing, would leave every test green.** The
  severity ladder is closed: `test_the_rank_and_the_word_are_one_ladder_and_cannot_disagree`
  holds `tuple(_EXPECTED_RANKS) == SEVERITIES` and then `{reached} == set(_EXPECTED_RANKS)`,
  so a fifth severity fails it. The action ladder has no counterpart — that test checks only
  `recommended_action in RECOMMENDED_ACTIONS`, which is membership. All six ARE each pinned
  by an equality on a `_graded(...)` row, so an action that stops being reachable reddens a
  named test (*Reported*, demonstrated by mutation); **nothing compares the reached set
  against the tuple**, so the gap is one-directional. *What would exercise it: adding a
  seventh recommended action.*

  > **THIS ENTRY IS THE SECOND VERSION OF ITSELF AND THE FIRST WAS THE CONTROLLER'S, WRONG,
  > AND WRONG IN THE SENTENCE JUSTIFYING WHY NO GUARD WAS BUILT.** T3's header claimed *"all
  > four severities and five of the six recommended actions are reached"*; a correction pass
  > asked to CHECK rather than carry it found **all six** — the claim UNDERSTATED, which is
  > the one direction nobody audits, so it had survived T3's own review and the split's. The
  > controller then corrected the count and added *"AND NOTHING ASSERTS EITHER COUNT … what
  > would exercise them: nothing"*. **That was false for both halves**, and the review of the
  > correction refuted it with two mutations: the severity coverage is asserted **eleven lines
  > below the paragraph denying it**, and every action is individually pinned. **The fifth
  > time this phase has watched a correction ship its defect in the NEW claim rather than in
  > the bug it fixed, and the first time the new claim was the controller's own.**
- **Four prose corrections in T1 are asserted by nothing** — the "three of seven" job-name count,
  the "two names across three sibling views" phrasing, the `view`-versus-`source` wording, and the
  `sorted` rationale in `table_of_job_sql`. They are true as of `56773b6` and would go stale
  silently. *What would exercise them: nothing. They are prose, and are listed so that a later
  reader knows they carry no test.*

### Carried out of T4, and every entry names what would exercise it

- **`history.py` has never run against the DEPLOYED view over real system tables.** It now runs
  against the shipped `task_telemetry_sql` **over empty system tables**, which proves the four
  column names resolve against the view definition this project deploys — and **nothing more**: no
  count is checked by it, and `result_state` is not among the four. *What would exercise it: T8's
  workspace run.* **This is the difference between prediction 6 confirmed on a fixture, which it
  is, and prediction 6 confirmed, which it is not.**
- **Nothing permutes the reading ladder's arms.** The order of the `no_prior_execution` /
  `insufficient_history` pair decides the answer and is read off SQL's first-match rule rather than
  executed; the absent arm's *position* is vacuous while its *presence* is fired (removing it turns
  *"I could not look"* into `history_complete`). *What would exercise it: a test that reorders
  `_READING_LADDER` and asserts the answer moves.* It was considered and not built.
- **`own_gate` returning two rows is unexercised.** The module groups by `job_id` rather than
  `MAX()`-ing it, on `incidents.py`'s reasoning — the two spellings agree on every input and differ
  in how they FAIL, and grouping was chosen because it breaks the one-row-per-incident property a
  reader can check instead of silently labelling a row with the larger of two values. **That "loud
  wrong answer" has never been made to happen.** *What would exercise it: one `job_run_id`
  appearing under two `job_id`s in the telemetry.*
- **The gate-spelling lock cannot recover history recorded under a name nobody declared.** It
  catches a future rename, in the commit that makes it — six checks, each now fired on the drift it
  refuses. It does not and cannot see runs already in the telemetry under a retired spelling, which
  is §0.8's unclosable half arriving one task later. *What would exercise it: nothing in the wheel.
  A person would have to widen the declaration by hand.*
- **Nothing asks the module about a batch whose gate ran and found nothing — except in one file.**
  Closed during T4's second correction, and named here because the gap is what let a mutation
  (anchor and bound moved together) pass all fourteen tests in `test_history.py` while being
  refused one file away. *What would exercise it: it now is exercised; the entry records that the
  coverage lives in `test_history_absence.py` and not beside the tests it protects.*

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
