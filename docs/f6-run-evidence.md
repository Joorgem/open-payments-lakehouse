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

#### ~~AND UNLIKE F5's TRIAL, THE SPEND IS MEASURABLE~~ — THE TABLE IS READABLE AND ~~IT DOES NOT ITEMISE INFERENCE~~ **IT DOES, MEASURED 2026-08-28 — SEE THE AMENDMENT AT THE END OF THIS SECTION**

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

> #### AND THE WITHDRAWAL IS ITSELF FALSIFIED — THE SKU EXISTS, AND THE BULLET ABOVE IS THE ONE THAT SAID SO
>
> **Controller-verified 2026-08-28**, statement `01f1a2fa-6af6-18a8-a443-c66d484ba75a`, and first
> found by T7's implementer at `01f1a2f7-4477-1b70-b473-309076a5b0c0`. The sentence above says
> *"if an inference SKU appears later, this bullet is what gets corrected, and the correction is
> a measurement rather than a hope."* **This is that measurement.**
>
> `system.billing.usage` now carries **ten** distinct `(sku_name, billing_origin_product)` pairs,
> not eight, and two of them are inference:
>
> | pair | rows | DBU | window |
> |---|---|---|---|
> | `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO` / `MODEL_SERVING` | 1 | **0.446427500** | 2026-08-24T15:10 -> 15:20Z |
> | `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO` / `AI_GATEWAY` | 1 | 0.000001429 | same |
>
> **That window is §0.1's OWN probe** — the two `ai_query` calls of the positive and negative
> arms. `usage_metadata.endpoint_name` reads `databricks-gpt-oss-20b`. **So inference is itemised
> on this workspace, is attributable per endpoint, and the cost of a model call is separable from
> the warehouse after all.**
>
> **AND THE REASON THE WITHDRAWAL WAS WRONG IS NOT THE ONE IT RULED OUT, WHICH IS WHAT MAKES IT
> WORTH THIS MUCH SPACE.** It considered a lag and rejected it on a ground that was sound as
> stated: the claim rested on *"the absence of the SKU across the whole history"* rather than on
> today's row being missing. **What neither half noticed is that at that instant the whole history
> contained ZERO INFERENCE EVENTS.** §0.1's own two `ai_query` calls, minutes earlier, were the
> first inference this workspace had ever run. So the census was taken over a history that
> contained none of the thing being censused, and **"this platform does not bill inference" and
> "inference has never happened here" produced one reading.**
>
> **That is ADR 0018's species, inside the paragraph written to avoid ADR 0018's species, in the
> document whose purpose is separating measured from assumed — and it is the controller's own,
> for the second time in this section.** The first retraction was right to be made; its
> replacement was wrong for a reason the replacement could not see.
>
> ~~**What this does NOT rescue, and the distinction is measured rather than argued:** T7 still
> publishes **no** cost delta for its own sweep window, because the rows for 2026-08-28 **have not
> landed** — `PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_OHIO` stops at 2026-08-25.~~ **THEY LANDED
> THE SAME DAY, AND THE FIGURES ARE PUBLISHED HERE.** *Controller-verified 2026-08-28T22:09Z,
> statement `01f1a32d-1be2-11e3-af0e-2ca3db4d8774`* —
> `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO`, endpoint `databricks-gpt-oss-20b`:
>
> | hour | product | DBU | rows |
> |---|---|---|---|
> | 2026-08-24T15:00Z | MODEL_SERVING | **0.446427** | 1 |
> | 2026-08-24T15:00Z | AI_GATEWAY | 0.000001 | 1 |
> | 2026-08-28T15:00Z | MODEL_SERVING | **7.812481** | 20 |
> | 2026-08-28T15:00Z | AI_GATEWAY | 0.001022 | 2 |
> | 2026-08-28T16:00Z | MODEL_SERVING | **42.857040** | 78 |
> | 2026-08-28T16:00Z | AI_GATEWAY | 0.004729 | 4 |
>
> The 08-24 row is this section's own two probe calls. `PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_
> OHIO` now reaches 2026-08-28T19:00Z too (statement `01f1a32d-b9bc-1c61-a789-3c4d3ab426c5`), so
> the horizon that grounded the refusal has moved past the sweep. **The observed lag is at most
> ~5 hours** — the 16:00Z rows were readable at 22:09Z the same day. The struck paragraph bounded
> that lag only as *more than zero and at most four days*, which was **loose but not wrong**.
>
> **AND WHOEVER READS 2026-08-28's INFERENCE ROWS MUST NOT READ THEM AS THE SWEEP.**
> **08-28's `MODEL_SERVING` total is 50.669521 DBU over 98 rows, and that is NOT what the sweeps
> cost.** The published corpus is **twenty trials over four sweeps**; the review permuted the menu
> across seventy more, and the pilots are in there as well — so that figure covers roughly ninety
> `ai_query` trials, not twenty. A figure quoted from that day as *"what the sweep cost"* would be
> wrong by roughly four to five times, and it would be wrong in the direction that flatters
> nobody.
> The corpus file is the authority on which trials were published.
>
> **The consequence for the phase:** the labelled-upper-bound framing that plan §1.2 and T7
> mandate was correct when it was written and is **obsolete going forward**. T7's own inference
> cost becomes separately attributable once 08-28 lands. Three sentences in the plan still carry
> the superseded instruction; they are historical and are left standing as such, and **this
> paragraph is what a reader should reach first** — the retraction closes here, by `grep -i`, and
> the four other hits for the word in this repository are logical inference in three ADRs and in
> `.plans/HANDOFF.md`, a different word that must not be swept up with it.

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

### 1.6 T5 — the blast radius, the leg the brief did not have, and a claim about a claim

Committed at `2bf510c`. `opl.triage_agent.blast_radius` answers **which** tables are downstream of
an incident's bronze table, never how much. **Controller-verified:** `174 passed in 120.43s` across
`tests/triage_agent/` and `tests/test_size_caps.py`, read from the output file — 166 before the
task plus 8. `ruff` clean; zero CR bytes; longest function 49 against the 50 cap.

**No proportion, and the reason is measured.** The shared fixture chose counts so each batch
reconciles — socios reads 1,800 staged against 55,830,826 live rows, and only payments' counts are
real — so any proportion classifies socios near 100% here and near 0% in the deploy. T3 refused it
for this, and the phase plan predicted T5 would meet the same wall. Nothing the module emits is a
magnitude, checked by dumping every f-string it can produce.

#### THE CONTROLLER'S BRIEF HAD THREE LEGS AND THE GRAPH HAS FOUR

bronze→vault is declared in the bundle and keyed on the loader task's **parameter**; vault→gold is
derived from the gold registry's own spec fields plus `parent_hub`; bronze→gold direct is read out
of the entry points the bundle names. **The fourth is the gold→gold closure, and the implementer
added it against the brief.** `fact_payment` names no vault table, so without the closure it is
unreachable from `empresas` and `estabelecimentos` — **the one table the whole layer exists to
produce, missing from two of seven answers.**

> **THE CONTROLLER THEN REPEATED THE ERROR IN THE OPPOSITE DIRECTION.** The review dispatch said
> the closure was needed for `empresas`, `estabelecimentos` **and `ptax`**. `ptax` reaches
> `fact_payment` through the direct leg with no closure at all. **Reported by the reviewer against
> the dispatch**, and it is the same defect twice: a controller reasoning about a graph it had
> derived incompletely, once by omission and once by over-extension.

**An independent reviewer re-derived all four legs from its own code** — parsing every job YAML
itself, walking the gold registry itself, reading the four entry points by hand — **and reproduced
the declaration table for table**, all seven bronze tables. *Reported.*

#### THE EMPTY ANSWER IS REFUSED AT IMPORT, AND IT IS THE PHASE'S SPECIES

`payments` and `ptax` reach gold without touching the vault, so a bronze→vault→gold walk returns
**nothing** for them — and `payments` is `592660596679630`'s table, the largest incident in the
corpus and the one whose recommendation is *do not promote*. **"Nothing downstream" raises nothing,
is plausible for any bronze table, and is the most reassuring answer available.**
`_assert_no_bronze_table_reaches_nothing` fails the **import**: deleting `payments` from the direct
declaration stops both test files at **collection, exit 2**, rather than reporting a comfortable
green.

#### THE GUARDS WERE DECORATION UNTIL A REVIEW MEASURED THEM

**Five of seven import-time guard calls could be deleted with the suite green** — and one of the
five, the guard for the only leg no sweep attests, had **no `pytest.raises` sibling, no firing
test, and was not imported by either test file at all**. Its own docstring said a typo in
`"fact_payments"` *"would drop the star's only fact out of the workspace's largest incident"*, and
nothing had ever watched it say so.

**The file's own header had named the hazard verbatim** — *"Deleting the calls at the foot of
`incidents.py` left this suite green once, which is how that was learned"* — so it promised what it
did not deliver, and the helper that closes it already existed in the file and was already used
twice. **Controller-verified**, one call at a time: each deletion reddens **exactly one** test,
seven distinct names, `1 failed, 39 passed` each, the file restored to hash after every one.

#### THE SEVENTH INSTANCE, AND THIS TIME EVERY FINDING WAS IN THE CORRECTION'S OWN PROSE

A narrow review of the correction — sent at nothing but what the correction had *written* —
returned **three HIGH, one MEDIUM and two LOW, all prose, none touching a mechanism.**

- **A collision argument false in half.** It claimed the leg-3 rule cannot collide with a bronze
  task's first parameter because a gold guard refuses names bronze holds. That guard holds bronze's
  **Delta names** (`bronze_payments`); a bronze task's first parameter is the **registry key**
  (`payments`), which the guard never sees. Demonstrated: register a gold table named `payments`
  and **five bronze tasks classify as gold entry points.** Nothing breaks today; the *reason given*
  was wrong.
- **A claim contradicting one the same pass wrote two files away** — *"`ptax`, whose only read in
  the whole repository"* against its own DataOps bullet asserting three views read `bronze_ptax`.
- **A boundary sentence that re-scoped between its halves**, naming "a script that reads a bronze
  table" and then citing a totality that is over gold **tables** — which such a script does not
  build.

> **AND THE FIX THAT MATTERS IS A SHAPE, NOT A SENTENCE.** The correction had claimed **exactly
> two** blind spellings in its `ast` sweep, derived by reading two `isinstance` guards. The review
> constructed **seven**, one of them `databricks/src`'s own live idiom. **A cardinality over an
> unbounded complement cannot be true.** The second correction deleted the count and restated the
> docstring as the **accept set** — a transcription of the five conditions the loop applies, ending
> *"anything else contributes nothing"*. **Total by construction rather than by enumeration**, and
> no new spelling can falsify it. *Controller-verified against the loop's five conditions.*

**A judgement the correction made by DELETING, and the review confirmed it for a stronger reason
than the correction had.** Asked whether to name two adjacent `test_conformed.py` tests beside
*"no test in this repository holds it"*, the correction declined — naming them would cost a second
claim, phrased as a negative over two other files. The reviewer then checked what the correction
had declined to check: **those tests would not discriminate**, because the fixture fact reaches
every member of both enumerated dimensions, so a `SELECT DISTINCT`-from-fact implementation passes
them identically. **Declining to check was right, and checking established why.**

#### THE POTENTIALLY BLOCKING CALL, SETTLED BY READING RATHER THAN ASSUMING

`payments` reaches `dim_date` but **not** `dim_channel`/`dim_currency`. Settled in
`load_conformed_dimension`: `_refuse_a_mismatched_source` **raises** if an enumerated dimension is
handed a date source, so `span` is provably `None`; the rows are written from the declared contract
domain **before** `fact_side_cardinality` is evaluated; and the only pre-write refusal reads the
spec, not the data. **The exclusion is correct under the definition the header states — tables
whose CONTENT is affected.** Had it been wrong it would have been a blocking under-report on the
headline incident's table.

#### AN INTERRUPTED REVIEWER, AND THE CHECK THAT MADE ITS OUTPUT USABLE

T5's independent review was **killed mid-run while it held mutations**. The tree was verified
before anything else — `git status` at the four expected entries, every line count unchanged, HEAD
unchanged, **and `174 passed` with all thirteen per-file counts matched** — which is the check this
phase adopted after a killed subagent once resurrected a deleted file and a suite went green with
150 instead of 135. Every mutation had been reverted. **The review was then resumed from its
transcript rather than re-run**, and returned complete. The only residue was one stale `.pyc` from
a scratch module whose source it had already removed.

**And a pre-existing hole was reported rather than fixed**: a loader task added to an existing,
already-classified non-`vault_*` job file is invisible to `tests/test_vault_job_wiring.py`'s
totality lock while still passing it. A brand-new YAML file **is** caught. T5's own sweep is
strictly stronger — keyed on the parameter, over every `*.yml` — and the hole is left for whoever
owns that file.

#### AND ONE TOOLING TRAP DESTROYED A FINISHED MODULE, BECAUSE TWO CORRECT RULES COMBINE

`tests/test_size_caps.py` reads `git ls-files` and is **blind to untracked files** — it has produced
a false green four times in this project — so every new file is `git add -N`'d before the cap test
is believed. **An intent-to-add file holds the EMPTY blob in the index.** So `git checkout -- <that
file>` does not restore it; it **truncates it to nothing**.

**Both rules are right and the combination deletes work.** It took `src/opl/triage_agent/history.py`
out mid-task during T4, after a mutation, at the moment the agent reached for the ordinary way to
undo one. Every T4 and T5 agent since has reverted by **inverse substitution from its own copy with
a hash proof**, which is now the phase's standing method and is why the killed T5 reviewer cost
nothing.

**It is recorded here as well as in `CLAUDE.md` because `CLAUDE.md` is git-ignored in this
repository** — a reader of the committed history would otherwise find the practice everywhere in
these commit messages and the reason for it nowhere.

### 1.7 T6 — the issue as data, and an escape that guarded a third of itself

Committed at `0a74b48`. `opl.triage_agent.issue` assembles one incident into a payload, `report`
renders it, `scripts/open_triage_issue.py` posts it. **Controller-verified:** `280 passed in
124.60s` across `tests/triage_agent/`, `tests/test_size_caps.py` and `tests/bronze/test_reconcile.py`
— the last because T6 adds `VERDICTS` to `reconcile.py`. `ruff` clean; zero CR bytes; no function
at or over 50; no file at or over 800.

**NO ISSUE WAS OPENED.** The publisher prints unless told twice, and the phase's one real issue
**waits for T8** so its provenance names a run that happened rather than the placeholder the
fixture carries. That is a controller decision and it costs nothing.

#### THE CREDENTIAL BOUNDARY IS A DIRECTORY, AND THAT IS THE WHOLE DESIGN

The agent emits the issue as **data**; a thin publisher posts it. `scripts/` is outside the wheel
— `pyproject.toml` packages only `src/opl` — and `grep -rn "open_triage_issue" databricks/` returns
nothing, so **no Databricks task can reach the publisher.** That is what keeps a GitHub PAT out of
a Databricks secret scope, where obtaining one would be a human gate. *Reported*, verified from
both sides by T6's reviewer.

#### A PII PATH NEITHER PRIVACY ARM HAD

`job_name` carries the bundle's `[dev <operator>]` prefix — **a real Windows username** — into what
becomes a public issue. T1 strips it in SQL; nothing stopped a caller reading the raw timeline and
handing the payload an unstripped name. Refused now at **both** doors, each proven by its own
deletion, plus a test that reads `databricks.yml` and refuses a target prefixing job names any
other way. `produced_by` — documented as *"the command a human typed"*, which on this box is
`uv run python C:\Users\jorge\…` — was found by the reviewer asking what the field's documented
intent was, and is constrained, escaped and labelled.

#### THE PROVENANCE SECTION PROMISED MEASUREMENT AND CHECKED NOTHING

`_provenance` claimed *"a number in this body is either something a statement returned in a run
that can be named, or something a human typed into this repository and a test holds against the
bundle."* **The first half was held by nothing.** Hardcoding a wrong relation left **52 passed**
with every body naming the payments quarantine and no test objecting.

**Closed by DERIVING rather than validating**, which is the general lesson: `read_from` is
**deleted**; the quarantine and the reconciliation view are recomputed at render from `source` and
the config; the telemetry view is a named field labelled the caller's word; `statements` are
constrained to the four facts and print `not recorded` where a run recorded none. **The body now
labels every line `DERIVED by this wheel` / `THE CALLER'S WORD, checked by nothing` / `DECLARED,
not measured`** — and the closing claim that all four declared things are locked by a test was
**deleted**, because it is true of the map and the manifest and false of the window.

#### AND THE ESCAPE GUARDED A THIRD OF ITSELF — THE EIGHTH INSTANCE, AND THE FIRST WITH A PUBLIC CONSEQUENCE

`_code` fences a value by CommonMark's rule. **The fence itself was the only part of it a test
pinned**; each arm below was deleted on its own and the suite stayed green.
*Controller-verified*, one mutation at a time, each reverted to the exact baseline hash:

| arm removed | before the fix | now reddens |
|---|---|---|
| space padding | **green** | `test_a_value_that_begins_with_a_backtick_is_padded_or_no_code_span_opens_at_all` |
| `\n` fold | **green** | `test_a_blank_line_in_a_row_value_cannot_end_the_paragraph_the_span_sits_in` |
| `\r` fold | **green** | same test |
| empty fallback | **green** | `test_an_empty_value_renders_a_code_span_and_not_two_literal_backticks` |

**Two of the four are live breakouts, not cosmetics.** Without the pad, a value beginning with a
backtick opens a three-run with no three-closer, CommonMark emits **no code span at all**, and the
rest of the value is live markdown. Without the fold, a blank line ends the paragraph and the span
never closes. **The reachable input for both is `reject_reason` — a quarantine row value — which is
the exact input `_code`'s own docstring says it exists for.**

**Four more fields bypassed the fence entirely and three more reached the body with no formatter at
all**, two of them inside `**`. Routed through the fence, constrained at the file door against
vocabularies read from the modules that declare them, and **`bool` refused beside `str`** because
`True == 1` would pass the rank arm and print `True` where a count goes.

> **ONE THING IS RULED RATHER THAN GUARDED, AND SAYS SO IN THE SOURCE.** `_reconciliation`'s absent
> arm is entered only when the value **equals** a constant this package declares, so no crafted
> value reaches it, **no mutation reddens it, and none should.** A guard there would be decoration;
> the fence stays for uniformity and the docstring says it is not counted as a defence.

#### A CLAIM THAT REACHED PRODUCTION SOURCE THROUGH THREE AGENTS WITHOUT ANYONE MEASURING IT

`render_title` fences its batch id because **GitHub renders code spans in issue titles**. That
reached the docstring as *fact* through a chain — a GitHub community discussion, quoted by T6's
reviewer, repeated by the controller's dispatch, written into the code by the correction. **Nobody
in that chain rendered anything.** §0.2 records what this repository already paid for a documented
behaviour quoted from memory and checked against a weak source.

**The docstring now calls it what it is:** a cheap precaution on an unverified claim, not a defence
resting on a measurement the way the body's does. **The phase's one real issue carries exactly that
construction in its title, so opening it settles the question by observation.**

A smaller instance of the same shape, and it is the controller's: a reviewer wrote that the
quarantine expression is printed *"eleven lines above"*; the controller repeated it in a dispatch;
the correction wrote it into a production docstring. **It is 147 lines.** The correction flagged it
as the one claim of its own it doubted, and was right.

---

### 1.8 T7 — the text generator run on the same corpus, and the column that means nothing

Committed at `2679cb1`. Plan §1.2. The shipped classifier is deterministic **by choice** — §0.1
measured that a model is reachable on the credential this project already holds — so the cheapest
demonstration that it is not a text generator is to run a text generator on the same eleven
incidents and measure where it disagrees. `databricks/src/triage_llm_control.py` is outside the
shipped path: nothing under `src/opl/` imports it and it is in no bundle YAML.

#### THE RESULT CACHE WAS THE THREAT TO THE WHOLE MEASUREMENT, AND IT WAS MEASURED RATHER THAN ASSUMED

n = 5 trials of a textually identical statement is exactly the shape the DBSQL result cache
answers. Then *"five of five agreed"* measures the cache, and **"the model is deterministic" and
"the cache answered" produce one string** — ADR 0018's species inside the experiment built to hunt
it. `.plans/sql.sh`'s header already records, measured, that a **comment nonce does not defeat this
cache**, so no mechanism could be assumed.

**Two arms, and the first is what makes the second mean anything. Reported**, from T7's implementer:

| arm | statements | result |
|---|---|---|
| **positive control** — the same `SELECT COUNT(*)` twice | `01f1a2f6-b696-1362-8235-6185d0940f3c`, `01f1a2f6-de01-1548-865c-f91d0a46518e` | `False` (**71,874,352 bytes read**) then **`True`** (**0 bytes**) |
| **the arm that matters** — the same `ai_query` twice | `01f1a2f6-e72c-1f09-b28f-8b6a176bd616`, `01f1a2f6-ec7e-1656-8e0f-57cdd61cd8b3` | **`False` both**, both reading real bytes |

**The result cache does not serve `ai_query`.** So no prompt alteration was needed: the prompts are
byte-identical across the trials of an arm, and that is measured rather than claimed —
**Controller-verified**, exactly **one `statement_sha256` per arm** in the results file. The flag
was still read on every trial: **20/20 `result_from_cache: False`, 0 discarded.** The independent
reviewer carried the same reading across **85** trials. *Reported*, on that reviewer's own corpus
which **is not committed** (§3): that every prompt there returned five distinct response strings.
**That does NOT hold of the published corpus and is not claimed of it** — 3 of its 34 prompt-groups
repeat a string, `592660596679630`'s facts prompt worst at 2 distinct strings over 5 trials. The
cache conclusion does not rest on it: **20/20 `result_from_cache: False` with real bytes read is
the reading, and it is Controller-verified.**

**The dispersion below is therefore the model's, and the two worlds are separated.**

#### THE SWEEPS, AS RATES OVER n = 5

**Controller-verified**, recomputed from `docs/f6-llm-control-responses.json` independently of the
implementer's report; every figure reproduced.

| sweep | responses | declines | `clean` | unparseable | agreement with the shipped ladder |
|---|---|---|---|---|---|
| the eleven with their facts | 55 | **0/55** | **0/25** on the five evidence-missing | 0/55 | **36/55** |
| the same eleven, counts stripped | 55 | 29/55 (29/30 rows-present, **0/25** zero-row) | 0/25 | 0/55 | 9/55 |
| the fabricated incident | 5 | **5/5** | — | 0/5 | — |
| stripped again, decline mid-menu | 55 | 25/30 rows-present | 0/25 | 0/55 | — |

The six incidents that carry rows were graded **purely on size, 30/30**: `bulk_rejection` for
2,000 / 1,797 / 1,786 and `isolated_rejection` for 4 / 1 / 1, unanimously, reproducing
`_POPULATION_SCALE_ROWS`'s `>= 10` line exactly. **That is the half the control gets right**, and it
is why the 36/55 is not a low number badly explained.

#### AND THE WORD IT GETS WRONG IS WRONG IN A SHAPE NOBODY PREDICTED

`does_not_reconcile` was returned **14 times in the facts sweep, every one of them on an incident
that has NO reconciliation row** — the five of §0.5, whose fact is `no_reconciliation_row`. It was
returned **0 times in the 15 responses the published corpus holds for it** on `592660596679630`,
the one incident in this workspace whose verdict genuinely is a non-reconciling verdict
(`stranded_gated`).

**The model used the reconciliation word exclusively where reconciliation could not be evaluated,
and never once where it actually failed.** *Reported*, extended by the independent reviewer across
85 trials and ten prompt configurations: **142** on the five, **0 of 50** on `592660596679630`.

And the causes it wrote there do not merely pick a wrong word — they **convert an absence of data
into an asserted finding**:

- *"Reconciliation shows staged rows missing from bronze and quarantine."*
- *"Staged rows are missing from both bronze and quarantine, leading to a no_reconciliation_row
  verdict."* — `no_reconciliation_row` was the **input**; it is reported as the **conclusion**.

**Plan §4's falsifier 2 named the failure as the word `clean`. It came back 0/25 — and 0/25 again
with the pipeline premise removed, 0 in all 170 responses.** So the control passed the test as
written and failed a harsher one nobody had written down. *Reported*, the no-premise arm is the
reviewer's: strip the premise and the five zero-row incidents go `does_not_reconcile` **25/25**,
`evidence_removed` **0/25** — so the 11/25 shipped-correct answers exist **only because the prompt
hands the model the pipeline chain**.

#### THE CONFIDENCE COLUMN IS ADR 0018's SPECIES, IN THE FIELD A TRIAGER WOULD FILTER ON

**Controller-verified**, over the 115 responses of the first three sweeps:

| | n | confidence >= 0.9 |
|---|---|---|
| the model **asserted a verdict** | 81 | **78 (96%)** — 75 of them exactly `0.9` |
| the model **declined** | 34 | 9 (26%), spread across 0.0 – 0.9 |

`0.9` is what it returns for the six it grades correctly, for `592660596679630` graded on size
against the ladder's `does_not_reconcile` (**0.9, five of five**), and for the invented
reconciliation findings above (**0.9 and 0.95**). **Ask what else would produce a 0.9 and the answer
is everything.**

**And the instrument is not simply emitting a constant, which is what makes this sharp rather than
cheap:** across the **34** declines of the first three sweeps the same field spreads over **eight**
distinct values — 0.0, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9. (Seven is true of `numbers_stripped`
alone.) **Confidence discriminates only once the model has already decided it cannot answer** —
precisely backwards from what triage needs, and unusable as a filter in the direction anyone would reach for it.

#### THE COST, MEASURED — AND WHY THE DAY'S TOTAL IS NOT THE SWEEP'S

**There are TWO sweep windows and a later reader needs both**: the first three sweeps ran
**2026-08-28T15:56:24Z → 15:59:32Z**, and the correction's fourth arm ran
**16:57:03Z → 16:58:02Z**, which is the corpus file's own `ended_at`. A cost attributed to the
first window alone misses a quarter of the published trials.

~~**No cost delta is published for T7's window**, because the rows have not arrived~~ — **they
arrived the same day.** `MODEL_SERVING` on `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_OHIO`
totals **50.669521 DBU over 98 rows** for 2026-08-28; §0.1's amendment carries the hourly split
and the statement id, *Controller-verified*. **That total is NOT this sweep's cost** — the same
day carries T7's pilots, the review's ~70 permutation trials and the correction's fourth arm, so
roughly ninety trials sit behind it and the published corpus's twenty are only part of them.

**And §0.1's withdrawal was falsified in the process** — the inference SKU exists and is
attributable per endpoint.

#### FIVE PASSES, AND THE FIFTH FOUND TWO HIGH DEFECTS IN THE FOURTH'S OWN NEW PROSE

**The ninth instance of this phase's pattern, and it is nine for nine.**

- The correction justified deleting pagination from the cache-flag reader by claiming `page_token`
  **may not** be sent with `include_metrics` — *"measured"*. The review sent it: HTTP 200, the
  token honoured, `metrics` on all 100 rows, and it then built the paged reader the docstring
  called impossible and read a real `False` off it. **What had been measured was the correction's
  own omission of the flag on the follow-up request, published as a server-side prohibition** —
  the **third** `from_cache` claim in this file's history printed in the shape of a measurement,
  which is the species the module's own header names. The sentence is deleted; the 1,000-row window
  stands as a **choice** rather than a forced move.
- *"The five zero-row incidents lose nothing in these four fields"* — **false in 10 of 20 cells.**
  The premise is true (`0` maps to `none`, losslessly) and the inference does not follow, because
  three of the four stripped fields on a zero-row incident are **not** zero. It appeared in three
  places, and the test that claimed to pin it checked only which presence word each cell maps to.
  Deleted in all three.

**What the fifth pass also established, and it is the better half of the audit:** the correction's
prompt-render lock closes the guard gap **more completely than it claimed** — the reviewer mutated
**every one of the 66 declared cells** one at a time and none escaped. The lock is a **drift** lock
and its docstring says so: a cell mistyped *before* the sweeps ran leaves file and corpus agreeing,
and that residue is in §3.

---

### 1.9 T8 — the workspace run, and the four predictions that did not move a number

Committed at `9293313` and `8df888d`. `databricks/src/triage_dq_incident.py` runs the shipped
`opl.triage_agent` path on serverless against the live corpus and emits the facts payload as one
fenced JSON array. **It writes nothing** — no table, no view, no volume file, no task value.

**THE OUTPUT CHANNEL IS THE RUN'S OWN STDOUT, AND THAT WAS MEASURED BEFORE IT WAS DESIGNED.**
*Reported:* `databricks jobs get-run-output` on a 26-day-old serverless `spark_python_task`
returned this project's own stdout with `logs_truncated: false`, and both T8 runs came back the
same way. **A UC volume file was rejected because it is a WRITE** — workspace state on a path a
second attempt overwrites, inside the one package whose header says it never writes. A notebook
task with `dbutils.notebook.exit` was rejected because it moves the entry point out of
`databricks/src/*.py`, the set the serverless-capability guard and the git-at-runtime ban sweep:
**escaping the guards to make output easier to fetch is the wrong trade.** *Reverses it:*
`logs_truncated: true`.

**Deploy verified BY ARTEFACT, never by the success line** (plan §7 rule 11). *Reported:* the
wheel's sha256 is identical on both sides (`2bc8c9b7…`, 766,765 B) and `opl/_revision.py` **inside
the downloaded wheel** reads `8df888de90d1b8b0bbd7f0bc1cc166d78cd12f47` = HEAD. The implementer
also verified the **synced entry point** (`b5c546f0…`), which `assert_deployed_revision`'s own
docstring says it does not re-read — **that half was uncovered, and it is the half this task lives
in.**

*Controller-verified, read back from the runs themselves:*

| run | job `run_id` | result |
|---|---|---|
| first, rev `9293313` | `883121603089733` | SUCCESS, 11 incidents |
| **deliberate failure arm** | `940537760125301` | FAILED, as designed |
| final, rev `8df888d` | **`1119885373986326`** | SUCCESS, 11 incidents |

The two successful runs, 35 minutes and one revision apart, produced **byte-identical payloads
apart from `produced_by`**. ***Reported*, and it cannot be raised**: the first run's payload was
not retained, so nobody can check it.

#### THE FOUR PREDICTIONS, LIVE — Controller-verified, derived from the payload rather than read off the report

| # | prediction | live |
|---|---|---|
| 2 | five `evidence_missing`, six carrying rows | **3** `quarantine_empty` + **2** `batch_absent` = 5; **6** `rows_present`; rejected rows sum **5,589** = F4's census; `attempts = 2` and `["FAILED","FAILED"]` on all eleven; verdicts **5** `no_reconciliation_row` / **5** `reconciled` / **1** `stranded_gated` |
| 6 | §0.10's eleven counts exactly | **`7 · 4 · 3 · 3 · 3 · 2 · 2 · 1 · 1 · 0 · 0`** — and `prior_incidents` matches §0.10's last column too, a column the prediction did not name |
| 7 live | 8 / 2 / 1 | **8** `insufficient_history`, **2** `no_prior_execution`, **1** `history_complete`, **0** `gate_run_absent` |
| 8 | the lookup still returns 4 / 3 / 1 | **4 · 3 · 1.** The rows have **not** aged out |

**No spelling needed correcting.** The shipped module agrees incident for incident with
**§0.10's hand query of 2026-08-25** — no hand query was re-run at T8 — which is what §2's
prediction 6 asked and is the outcome it could not assume.

**The five zero-row incidents publish `quarantined: null`, not `0`** — the module refuses to print a
count it did not measure, which is §0.10's own amended lesson arriving in the payload.

#### THE RETENTION MEASUREMENT, PUBLISHED BECAUSE IT IS NEW AND NOT BECAUSE IT WAS ASKED FOR

Prediction 8's falsifier was the rows ageing out, *"which would not be a failure but the phase's
first measurement of the timeline's own retention"*. They did not, so the measurement is a **lower
bound rather than a floor**. *Controller-verified:* `system.lakeflow.job_task_run_timeline` on
2026-08-28 still serves `check_bad_rows` 29/29, `dq_gate` 5/5, `dq_gate_batch` 24/24 and `fail_on_dq` 22 rows over 11 job
runs, **oldest row 2026-07-24 — 35 days** (statement `01f1a30b-2869-127a-bb98-9e22540354d8`).
F4's ~25-day floor was measured on `system.query.history`; **the timeline's is at least 35 days,
and nothing here says where it cuts.**

> **THE BRIEF CALLED THOSE ROWS `dq_gate` ROWS AND THE IMPLEMENTER REFUSED IT, CORRECTLY.**
> `history.py` anchors and counts on **`check_bad_rows`** — §0.10's own 2026-08-25 amendment says
> so, and it is the whole reason the retired-spelling hazard does not bite. Both spellings survived
> here so the conclusion is unchanged, **but the retention claim is about `check_bad_rows` task
> runs of 2026-07-24 and is published that way.**

#### AND `max_retries: 0` FAILED TO PREVENT A RETRY ON THIS PHASE'S OWN TASK, LIVE

*Controller-verified* on the run itself: the failure arm `940537760125301` shows
`triage_dq_incident` at **attempt 0 FAILED (`117486604282256`) and attempt 1 FAILED
(`499322268897276`)** — one `job_run_id` wearing two task-run rows, **§0.3's exact shape,
produced by the task built to read that corpus.** Both attempts re-ran every statement and left nothing behind. *That is
plan §1.3's justification arriving as evidence rather than as an argument*, and it is the fourth
time this repository has measured the same platform behaviour.

**The agent also cannot contaminate the corpus it triages**, which is a property worth stating
because nothing enforces it: its task keys are disjoint from the three roles `history.py` declares,
and after three triage runs `check_bad_rows` is still 29/29 and `fail_on_dq` still 22 over 11 —
*Controller-verified*, the same statement `01f1a30b-2869-127a-bb98-9e22540354d8` as the retention
reading above.

### 1.10 T8b — the one real issue, and two allegations settled by a markdown engine

**Issue #29 is open**, on `592660596679630`, the incident whose recommendation is *not to promote*.
It is the phase's public, permanent artefact and it was rendered, read in full and scanned for a
host, an organisation id, a token and an operator username before it was posted.

**The live grade is the phase's thesis in one row.** The shipped classifier reads
`592660596679630` as **`does_not_reconcile` → `hold_do_not_promote`**, because the reconciliation
arm outranks the size arm and a declared hold outranks every derivation. **§1.8's text generator
read the same incident as `bulk_rejection`, confidence 0.9, five times out of five, and never
mentioned the stranding once.** The largest incident in the workspace is the one whose correct
answer is the least like *"it is big"*.

**THE TWO THINGS §1.7 AND §3 RECORDED AS UNVERIFIED ARE NOW MEASURED, AND THEY SPLIT.**
*Controller-verified 2026-08-28 on issue #29:*

| claim | reading |
|---|---|
| the body renders as expected | **TRUE** — the API's `body_html` carries `<code class="notranslate">` around every fenced value, plus `<strong>` and real lists. **The first markdown engine anything in this phase has touched.** |
| GitHub renders code spans in issue titles | **TRUE.** The page's `data-testid="issue-title"` element contains `[triage] payments batch <code>592660596679630</code>: …`, and GitHub's own GraphQL payload embedded in the page names the field **`titleHTML`**. The fence works. |

**So the claim that reached production source through a chain of three agents was RIGHT**, and this
document said the opposite for one commit. The correction is below and it is the controller's.

> #### ~~THE TITLE IS NOT RENDERED~~ — RETRACTED WITHIN A DAY, AND IT IS THE THIRD RETRACTION-OF-A-RETRACTION IN THIS DOCUMENT
>
> **What this section said in `21da2f5`:** that GitHub does **not** render code spans in issue
> titles, on two readings — that the REST v3 issue endpoint serves `body_html` and **no
> `title_html`**, and that the rendered page's `<title>`, `og:title` and `twitter:title` carry the
> backticks **literally**. Both readings are true. **Neither can bear on the question, and the
> conclusion drawn from them was false.**
>
> - **An HTML `<title>` element cannot contain markup**, and OpenGraph/Twitter `content`
>   attributes are plain-text slots. **Literal backticks appear there whether or not the H1 renders
>   a span** — so that reading returns the same string in both worlds.
> - **`title_html`'s absence is a fact about REST v3**, not about GitHub. The GraphQL API serves
>   `titleHTML`, and the page embeds it.
>
> **Found by the divided closing review's documentation half, which refused the controller's chosen
> evidence and went to the artefact.** The rendered H1 was in the very page the controller had
> already downloaded; the controller grepped `<title>` and the meta tags and never looked at the
> issue-title element.
>
> **THIS IS THE PHASE'S OWN HUNTED SPECIES, IN THE MEASUREMENT WRITTEN TO END A CHAIN OF UNVERIFIED
> CLAIMS.** ADR 0018's instruction is *ask what else would produce that value*. A literal backtick
> in `<title>` is produced by **every** world, including the one where the title renders. The
> controller published a retraction of a true claim on a reading that could not distinguish them —
> and did it in the section whose whole subject is a claim nobody had ever checked.
>
> **It reached five places and one is permanent:** this table, §3, `report.py`'s header,
> `render_title`'s docstring, and ADR 0020's Consequences. All four editable sites are corrected in
> the same commit as this paragraph. **`21da2f5`'s commit message is not editable and states the
> false claim outright** — a reader following `git log` will meet it, which is why this paragraph
> names the commit.

**The fencing is KEPT, and now for the reason it was written:** the title's `batch_id` is a value
the **timeline returned**, not a word this repository chose, and GitHub renders it. **Whether `@`
and `#` LINKIFY in a title is a different question and is still unmeasured** — #29's title contains
neither, so opening it did not settle that half.

### 1.11 T9 — the divided closing review, and the blocking defect was the controller's

The closing pass reads the branch as **two disjoint packages — code and record — by different
agents**, neither seeing the other's findings. On F5 that returned four blockers in the code and
five in the record that **seven per-task reviews had already missed**. It did it again.

#### THE CODE HALF — 62 mutations, 49 red, and THIRTEEN GREEN

*Reported.* Every mutation applied, run and reverted by inverse substitution with sha256 identity;
no `git checkout` on any file.

**The HIGH is a seam and no module read alone could show it.** `evidence`, `severity` and `history`
each **hand-typed the four reconcile verdicts** inside the very guard that refuses a cross-module
word collision. **This phase introduced `reconcile.VERDICTS`** — derived from `_VERDICT_LADDER`
precisely so a fifth arm reaches every consumer in the commit that adds it — **and wired it into
exactly one of four consumers.**

So the guards caught a collision with the four verdicts that exist and were **structurally blind to
a collision with a fifth, which is the only way the collision can arrive.** The reviewer added a
fifth verdict named `bulk_rejection` — simultaneously `severity.BULK_REJECTION`, the exact *one
string answering two questions on one incident* all three docstrings say they refuse — and got
**`270 passed`**, with all three guards passing over it.

> **AND THE TESTS THAT PINNED THOSE GUARDS WERE BLIND IN THE SAME WAY, WHICH IS THE SHARPER HALF.**
> They mutated **a rename of one of the four** — and a rename onto a word these files declare is
> **unreachable**, so the old mutation could never have been the failing input. A guard whose test
> fires only on an impossible input is a guard with no test. Both now mutate a fifth verdict, and
> **Controller-verified**: the fix turns that mutation into `ValueError: ['bulk_rejection'] are
> graded words here AND verdicts published on the same row`.

**Why a per-task review structurally could not see it:** `VERDICTS` was added under T6, for
`issue.py`. The three guards were written under T2, T3 and T4, **before it existed**. Each file,
read alone, shows a guard with a correct-looking list.

The other two seam findings have the same shape — something derived on one side and frozen on the
other:

- **A privacy asymmetry in the provenance.** `issue.py` shape-checks `produced_by` against seven
  path forms; `Provenance` carries **two more caller strings** — `telemetry_view` and every
  statement id — that reach the public body verbatim, checked by nothing. The reviewer drove a
  Windows operator path through the **file door**, the publisher's own entry, whose docstring says
  *"A FILE IS NOT A TRUSTED CALLER"*, and read the username out of the rendered body. Not reachable
  from the shipped task, and closed anyway: one renamed helper, one shape list, all three fields.
- **`report.py` claimed the absence arm was THE ONLY unguarded `_code` fence in the file.** It is
  one of **five**.

#### THE RECORD HALF — and the BLOCKING defect is the controller's own

**One BLOCKING, four HIGH, seven MEDIUM, five LOW.** The blocking one is the sharpest thing in this
phase and it is worth stating without softening:

> **THE CONTROLLER RETRACTED A TRUE CLAIM, ON EVIDENCE THAT COULD NOT BEAR ON IT, IN THE SECTION
> WHOSE WHOLE SUBJECT WAS A CLAIM NOBODY HAD CHECKED.** §1.10 carries the full account. The
> reviewer **refused the controller's chosen evidence and went to the artefact** — which no
> task-level review had any reason to do, because the controller had presented a measurement.

The other four HIGH were all in the controller's own prose too: §2 contradicting itself on two of
eight predictions, an unrestricted universal about the corpus that its own file falsifies, a
prediction-5 rescue that read a band answer off the sweep the same section calls incapable of
asking a band question, and a §0.1 amendment that misdiagnosed its own error as something subtler
than the billing lag it actually was.

**What that adds up to, and it is the phase's closing lesson rather than a tally.** F5's lesson was
that *the defect had moved out of the code and into the document that judges it*. F6 tested that by
splitting the pass, and the split returned **one blocking defect in the record and none in the
code**. The record half's findings were almost entirely **cross-section** — a claim in §1.9 that
only contradicts something in §2, a §3 entry only falsified by §1.10 on the same page — and every
one of them was written by the same author on the same day. **Nobody but its author had read most
of this document**, and that is exactly the condition the divided pass exists to end.

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
| 2 | **Five** incidents classify `evidence_missing` and **six** carry rows | **CONFIRMED** — live at T8, 3 + 2 and 6, summing to F4's 5,589 |
| 3 | A `permissions: issues: write` block opens an issue despite `default_workflow_permissions: read` | **OPEN** — **untried**; only a workflow run that attempts it closes this (§3) |
| 4 | ~~The LLM control returns a confident, fluent root cause for a `job_run_id` that exists nowhere~~ | **FALSIFIED** — it declined, 5/5, and 24/25 across four menu orders |
| 5 | ~~The LLM control assigns the **same** severity band to the 2,000-row and the 1-row incident when the counts are stripped~~ | **FALSIFIED IN LETTER** — it assigned **no** band to either; the substance holds by another route |
| 6 | The shipped history module reproduces §0.10's eleven prior-execution counts **exactly** | **CONFIRMED** — live at T8, and `prior_incidents` too |
| 7 | ~~**Ten of eleven** incidents report `insufficient_history` at N = 5~~, and **two** report zero prior executions | **FALSIFIED IN LETTER, CONFIRMED IN SUBSTANCE** — and by the controller's own later instruction; **live arm CONFIRMED at T8**, 8 / 2 / 1 / 0 |
| 8 | The lookup's three incidents still return **4 / 3 / 1** on the stable key against the LIVE view at T8 | **CONFIRMED** — 4 · 3 · 1 live; the rows did not age out |

**1 — CONFIRMED, and on live data rather than on the fixture.** T1's independent reviewer ran the
shipped SQL against the real `dataops_task_telemetry` — statement `01f19fda-f4bf-159f-a9ea-adf5f
003d51f`, **11 rows, every one `attempts = 2`** — and the fan-out statement `01f19fdb-041d-1884-bd
88-71e714604bab` carries the naive answer beside the right one (4,000 against 2,000 for a 2,000-row
quarantine). *Reported*, from the reviewer. §0.3's 22-over-11 is Controller-verified separately.
*Falsified by:* either number moving, which would have meant §0.3 measured a smaller population
than it claimed. Neither moved.

**2 — CONFIRMED at T8, and it was listed as open until the workspace run closed it.** T2 and T3
built the classification and their tests exercised it **on a fixture**; §0.3's five-and-six split is
the same *fact about the workspace* but **not** the same claim as *"the shipped module classifies
them that way against the live tables"*. **T8's run `1119885373986326` closed it** — 3
`quarantine_empty` + 2 `batch_absent` and 6 `rows_present`, summing to F4's 5,589 (§1.9).
*Falsified by:* any of the six coming back empty (the quarantine recreated since §0.3) or any of
the five acquiring rows (a repromote nobody recorded).

**3** — *Falsified by:* a 403 from the API, in which case the local `gh` path stands and the CI
path is reported as **refused**, not as untried. Asserted by nobody and quoted from no
documentation (§0.2).

**4 — FALSIFIED, AND PUBLISHED AS WEAKENING §1.1 BECAUSE THAT IS WHAT THE PREDICTION SAID WOULD
HAPPEN.** *Falsified by:* the model declining. It declined — `insufficient_information` **5/5** on
a `job_run_id` proved to exist in neither id column of the timeline nor any of the seven
registered quarantine tables. §1.8 carries the run.

**And the falsification was itself confounded until somebody measured it.** The decline option sat
**last** in a six-word menu, so the rate was inseparable from the option's position — a fact about
the prompt wearing the shape of a fact about the model. The independent reviewer permuted the
order rather than arguing about it: **24 of 25 declines across four menu configurations.** The
result is robust. *Reported.*

> **WHAT THE PERMUTATION FOUND INSTEAD IS WORTH MORE THAN WHAT IT WAS SENT FOR.** A first arm moved
> the decline while leaving its own gloss reading *"the facts given do not support any of the five
> verdicts **above**"* — which moving it makes **false**. There the decline fell to **2/5**. So the
> rate tracks the **gloss's semantic validity**, not its position, and an import guard the module
> defended on argument alone is now known to be load-bearing by measurement.

**5 — FALSIFIED IN LETTER.** *Falsified by:* it separating them anyway, which would mean it
inferred from the table name — a **third** outcome, neither pass nor fail, and the prediction was
written so it could be seen. **A fourth outcome arrived instead: it assigned no band at all.** Both
incidents came back `insufficient_information` 5/5. A sentence asserting that it assigns *the same
band* is false when it assigns none.

**§2's own precedent forbids the rescue.** Prediction 7 above was falsified in letter and kept
struck, with the reason stated: *"restating it in the vocabulary the design later adopted is how a
prediction stops being able to be wrong."* Rewriting 5 as *"returns the same answer"* is that move.

> **AND THE DIFFERENCE FROM PREDICTION 7 HAS TO BE SAID, OR §2 GAINS A FALSE SYMMETRY.** Prediction
> 7 was falsified in letter and **confirmed in substance** — the shortfall was really there, in a
> finer vocabulary. **Prediction 5 gets no such comfort from its own run.** A decline is not a
> weaker form of *"same band"*: a model that declines **because scale is missing** has signalled
> that scale matters, which cuts **against** the fusion §4 hunts rather than for it. Booking that
> as support for the thesis would be this phase's species inside its own scorecard.

**The shipped sweep 2 tested neither the prediction nor its falsifier, and that is an instrument
defect rather than a result** — `present, count withheld` induces a decline on **29 of 30**
rows-present responses in the shipped sweep and on **25 of 30** with the option mid-menu, so the
sweep cannot ask a band question at all.
Found by the independent reviewer, against a test docstring announcing itself as *"PREDICTION 5's
INSTRUMENT"*; the framing was deleted rather than reworded.

**What can be said about the substance, stated as rates because §5's rule requires it.
Controller-verified:** with the counts present the separation is perfect and unanimous — the three
large incidents (2,000 / 1,797 / 1,786) read `bulk_rejection` **15/15** and the three small
(4 / 1 / 1) read `isolated_rejection` **15/15**. **With the counts stripped, `bulk_rejection` is
emitted 0 times in 110 responses.** So the threshold behaviour is carried entirely by the digits,
and that much is a rate over n.

> **AND THE BAND COMPARISON IS NOT, WHICH THE FIRST DRAFT OF THIS PARAGRAPH BORROWED A DENOMINATOR
> TO HIDE.** *Corrected from the divided closing review.* Of the **60** stripped responses on the
> six incidents that carry rows, only **6** assigned a band at all: n = 4 across the three large
> and n = 2 across the three small, five of the six from a single arm, and
> `128878829411613` **never received one**. The **110** in the sentence above belongs to the
> `bulk_rejection` absence and lent its size to a claim measured on six observations.
>
> **This section calls sweep 2 an instrument that cannot ask a band question, and then read a band
> answer off it.** The rescue is deleted rather than restated: the counts-present arm carries
> *"the threshold behaviour is carried entirely by the digits"* on its own, at 15/15 and 15/15,
> **without borrowing from a sweep this document calls defective.**

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

**Confirmed on fixtures by two parties independently** — T4's implementer and T4's reviewer, the
latter on a fixture it built itself without reading the former's. *Reported.* ~~**Nothing has run
against the workspace**~~ — **T8 ran it: 8 `insufficient_history`, 2 `no_prior_execution`, 1
`history_complete`, 0 `gate_run_absent` (§1.9), Controller-verified from the payload.**

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

- **"The declaration half is free of Spark" is enforced by nothing, and THE COUNT IN THIS ENTRY
  HAS NOW GONE STALE THREE TIMES.** It said two files, then three, and the T6 implementer read it
  as six. **Measured 2026-08-26, by deleting `java` from `PATH` and running each file of
  `tests/triage_agent/` alone — the only reading that is about a JVM rather than about a word:**

  ```
  no JVM   blast_radius · blast_radius_lock · evidence_contract · history_declaration
           incidents_declaration · issue_payload · issue_publisher · issue_report
           severity_declaration
  JVM      evidence_census · evidence_sample · history · history_absence · incidents · issue
           severity
  ```

  **THE PER-FILE COUNTS ARE DROPPED AND THE LIST IS PUBLISHED AS INCOMPLETE**, which is the same
  lesson as the box below arriving one step later. Two files' counts have since moved, and
  **`tests/triage_agent/test_issue_markdown.py` — created by T6 in `0a74b48` and on disk before
  `9bbd343` published this entry — is in neither list and has never been put through the
  experiment.** The directory holds seventeen test files today; sixteen were classified.

  **`test_evidence_contract.py` has been JVM-free since T2 and was never counted by anybody.**
  Adding a Spark test to any of the no-JVM files would silently cost the property and no test would
  go red: `tests/test_size_caps.py` covers line counts and nothing covers the JVM.

  > **THE CONTROLLER ALMOST UPDATED THIS ENTRY FROM A `grep` AND THAT WOULD HAVE BEEN WRONG TOO.**
  > A token search for `spark`/`probe` over the files classifies all three `_declaration` files as
  > Spark, because the word appears in their prose. **That is this repository's substring blindness,
  > arriving in the controller's own attempt to fix a stale count** — the fourth time this entry has
  > been mis-stated, and the first time by the instrument rather than by the neglect.
  >
  > **So the number is removed from the claim and the MEASUREMENT is put in its place.** A count in
  > prose goes stale on the next task; `PATH` with no `java` in it does not. Whoever next needs this
  > figure runs the command rather than trusting the paragraph.

  **The guard was considered and deliberately not built**, on the narrow reviewer's argument: every
  cheap spelling of it is this repository's hunted species one level down — a signature scan for a
  `spark`/`probe` parameter passes while a module-scope `SparkSession.builder`, an autouse fixture
  or a transitive `pyspark` import still starts a JVM, and a wall-clock assertion is flaky on this
  box. **The one leg of that argument which has since collapsed** is that it would be a *one-file
  special case inside a repo-wide sweep*: it is most of the directory. The other leg — that every
  cheap spelling is blind — is untouched and is the one that still decides it. The honest spelling
  is the `PATH` experiment above, which is not a unit test.

  *What would exercise it: someone adding a Spark test to any of the no-JVM files and nobody
  noticing.*
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

### ~~THE FULL SUITE HAS NEVER RUN ON THIS BRANCH, AND CI DID NOT FIRE WHEN ASKED~~
### BOTH HALVES FALSIFIED 2026-08-28 — CI RUNS HERE, AND THE SUITE HAS PASSED ON THIS BRANCH TWICE

**Controller-verified 2026-08-26.** Everything this phase has verified is
`tests/triage_agent/` plus `tests/test_size_caps.py` — **174 tests of the repository's 2,683**, on
one Windows box. That nothing outside `opl.triage_agent` broke across five tasks is an **argument
an implementer made** (nothing outside the package imports the new modules) and **has never been
run.** `CLAUDE.md` says to quote CI for "the suite passes"; **this phase has never had a CI run to
quote.**

The branch was pushed and PR **#28** opened against `main` to close that gap before T6–T9 stack
four more tasks on top. **The `pull_request` trigger did not fire.** Measured, in this order:

| step | result |
|---|---|
| `git push -u origin feat/f6-rca-agent` | branch created; `ci.yml` triggers only on `push: [main]` and `pull_request`, so a feature-branch push runs nothing **by design** |
| PR #28 opened as a **draft** | **no run** |
| `gh pr ready 28` | **no run** — `ready_for_review` is not in `pull_request`'s default types (`opened`, `synchronize`, `reopened`) |
| `gh pr close 28 && gh pr reopen 28` | **no run**, though `reopened` **is** a default type |
| `.../actions/permissions` | `enabled: true`, `allowed_actions: all` |
| the `CI` workflow | registered, `state: active`, `.github/workflows/ci.yml` present on the branch |
| check-runs for head `6ce1a56` | **0** |
| newest run in the whole repository | `2026-08-24T13:35:43Z` — the F5 merge, two days before |

**No cause is asserted here.** The workflow is active, Actions are enabled, the PR is open,
non-draft and `MERGEABLE`, and a `reopened` event that should be a default trigger produced
nothing. **What is measured is the absence, not the reason** — and this repository has already paid
once for a `ci.yml` claim written from memory and "verified" against a weak source, so a guess
about GitHub's trigger semantics would be that mistake wearing a different hat.

> **THIS IS A THREAT TO THE PHASE'S OWN CLOSING CONDITION.** Protocol §9.3 is *CI green on the
> merged PR*. A phase that cannot make CI run cannot satisfy it, and discovering that at T9 costs
> more than discovering it now. **The commit carrying this section is pushed as the next probe** —
> it fires `synchronize`, which is a default type, and the result is recorded here rather than
> assumed either way.

*What would exercise the suite: a CI run on this branch. Until one exists, no claim in this
document about tests outside `tests/triage_agent/` and `tests/test_size_caps.py` has been made.*

#### THE PROBE CAME BACK, AND IT IS THE FIFTH EVENT TO PRODUCE NOTHING

**Controller-verified.** The commit above was pushed to the open PR's head branch, which fires
`synchronize` — a default `pull_request` type. **No run.** So the absence spans **five** distinct
events: `opened` (as a draft), `ready_for_review`, `reopened`, `synchronize`, and the
feature-branch `push` that `ci.yml` correctly ignores by design.

Further state, all measured and none of it explaining the absence: the repository is **public**,
not a fork, **not archived**, `disabled: false`; Actions are `enabled: true` with
`allowed_actions: all`; `default_workflow_permissions` is `read` (§0.2's figure, unchanged); the
`CI` workflow is registered and `active`; `ci.yml` is present on the branch; and PR #28 is open,
non-draft and `MERGEABLE` against `main`. The account-level Actions billing endpoint now returns
**HTTP 410 "This endpoint has been moved"**, so that reading was not taken — and a public
repository's minutes are not metered, so it is unlikely to be the cause.

> **STILL NO CAUSE IS ASSERTED, AND THAT IS THE POINT RATHER THAN A GAP IN THE WORK.** Five events,
> one absence, and every setting that could plausibly explain it measured and found innocent. The
> honest output of this probe is *"CI does not run on this branch and the reason is not in anything
> this session can read"* — which is a **result**, and is what gets carried to whoever can look at
> the account's own Actions settings.

**THE TWO THINGS THIS BLOCKS, NAMED SO NEITHER IS DISCOVERED LATE:**

1. **Protocol §9.3** — *CI green on the merged PR* — cannot be satisfied while no run occurs.
2. **Prediction 3**, which is closed by *a workflow run that tries to open an issue* with a
   job-level `permissions: issues: write` against the repository default of `read`. **A prediction
   that needs a run cannot be settled on a branch where runs do not happen.** T6's local `gh` path
   is unaffected — the phase's one real issue does not depend on CI — but the CI half of §0.2's
   open question stays open, and is reported as **refused by circumstance rather than untried**.


#### AND THE WHOLE SECTION ABOVE IS FALSIFIED — THE RUNS EXIST, AND THEY APPEARED AFTER THE READING

**Controller-verified 2026-08-28.** `gh run list` returns **three** completed runs on this branch,
**two of them green**, and the newest push queued a fourth within seconds:

| run | head | created | conclusion |
|---|---|---|---|
| `32988424065` | **`6ce1a56`** | 2026-08-26T16:26:46Z | **success** — `test`, `postgres`, `secret-scan`, `redpanda` all green |
| `32988241383` | `32c671c` | 2026-08-26T16:24:46Z | failure — `test` only |
| `33068096648` | **`9bbd343`** | 2026-08-27T11:36:07Z | **success** — all four jobs green, 2h14m47s |

**`6ce1a56` IS THE EXACT HEAD THE TABLE ABOVE RECORDS AS HAVING ZERO CHECK-RUNS.** The reading was
correct when it was taken and the run appeared afterwards — **18–20 minutes after it, for at least
two of the five events.** So the five events did not produce one absence; they produced one absence
**at the moment they were read**.

**WHY THE RUNS WERE NOT CREATED AT EVENT TIME IS NOT ESTABLISHED HERE, AND NOTHING BELOW SHOULD BE
READ AS SAYING IT IS.** What is measured is this: **two runs against three run-eligible events**
(`opened`, `reopened`, `synchronize`); **the run for the OLDER head `6ce1a56` was created two
minutes AFTER the run for the newer `32c671c`**; and **the newest push's run record appeared
within seconds**, so a gap of this size is not this repository's ordinary behaviour. None of that
names a cause, and none is offered.

**The one failure is not a defect in this branch:** `32c671c` died on
`ConnectionRefusedError: [Errno 111] Connection refused` across the vault suite — `47 failed,
2828 passed, 1 skipped, 172 errors in 4686.48s` — which is Spark failing to come up on the runner,
not an assertion. The commits on either side of it are green.

**THE TWO THINGS THE SECTION ABOVE SAID WERE BLOCKED, RESOLVED:**

1. **The full suite HAS run on this branch and passed.** `CLAUDE.md`'s instruction to *quote CI*
   for "the suite passes" is satisfiable for the first time in this phase, and the figure to quote
   is `9bbd343`, all four jobs, 2026-08-27. **The self-imposed restriction in this section — that
   no claim had been made about tests outside `tests/triage_agent/` and `tests/test_size_caps.py`
   — is lifted.** Note the shape of the job list: **four** jobs, not the two a partial read of
   `ci.yml` shows; `postgres`, `secret-scan` and `redpanda` are the others.
2. **Protocol §9.3 is achievable.** It was never blocked; it was believed to be.

**Prediction 3 is NOT resolved by this.** It is closed by *a workflow run that tries to open an
issue* with a job-level `permissions: issues: write` against the repository default of `read`. No
run has attempted that, so the CI half of §0.2's question stays open — now as **untried** rather
than as refused by circumstance, which is the opposite of what the paragraph above concluded.

> **AND THIS IS THE PHASE'S OWN SPECIES, IN THE PROBE WRITTEN TO BE CAREFUL ABOUT IT.** Every
> individual measurement above was right. Actions enabled, `allowed_actions: all`, workflow active,
> `ci.yml` present, PR `MERGEABLE`, check-runs 0 — all true when read. The discipline was right
> too: *"no cause is asserted"* is exactly what should have been written. **What went wrong is the
> one inference nobody flagged as an inference:** *"CI does not run on this branch"* and *"CI has
> not run on this branch YET"* are two worlds, and **check-runs = 0 is the same reading in both.**
> Ask what else would produce a zero there, and the answer is *a run that does not exist yet* —
> which says nothing about **why** it did not.
>
> **The fix was never a better setting to check. It was a later read** — and no amount of care
> inside a single session could have supplied one, which is why this is recorded as a lesson about
> when a measurement is taken rather than about how carefully.

### §9.3 IS CLOSED, AND IT TOOK TWO ATTEMPTS FOR A REASON THAT IS NOT THIS BRANCH

**Controller-verified 2026-08-29.** PR #28 merged at `ef123ac`; CI on the merge commit is
**green on all four jobs** — `test`, `postgres`, `secret-scan`, `redpanda` — run
`33226013099`, **attempt 2**.

**Attempt 1 failed, and it was not the code.** It died at **2:37:43** with
`ConnectionRefusedError: [Errno 111] Connection refused` across the vault suite —
**45 failed, 2,994 passed, 177 errors, and not one failed assertion.** That is Spark's driver
failing to come up on the runner.

**The discrimination was derived before it was re-run, and here it is shorter than F5's
version of the same argument:** `ef123ac` is a merge commit whose **tree hash is identical**
to `038f7c7`'s (`4a86232e…`), and `038f7c7` had passed `test` on the branch ~3 h earlier.
Where T8 had to argue *"the diff contains no executable line"*, here **there is no diff at
all** — a tree that already passed cannot have regressed. `gh run rerun --failed` was the
action; no code was touched.

> **THIS IS THE SECOND MEASURED OCCURRENCE, AND THE ENTRY EXISTS BECAUSE THE FAILURE LOOKS
> CATASTROPHIC AND THE WRONG REACTION IS EXPENSIVE.**
>
> | run | head | result | duration |
> |---|---|---|---|
> | `32988241383` | `32c671c` (branch) | 47 failed, 2,828 passed, 172 errors | 1:18:06 |
> | `33226013099` attempt 1 | `ef123ac` (main) | 45 failed, 2,994 passed, 177 errors | **2:37:43** |
>
> **Three ways to tell it from a real failure:** a mass `ConnectionRefusedError` with **no
> `assert`** in the failure lines means no test decided anything; **compare the tree, not the
> diff** (`git rev-parse <a>^{tree} <b>^{tree} | uniq -c`); and **duration is the tell** —
> healthy `test` on this suite is ~1 h 32 m, and F6 also saw a 2 h 55 m run that was
> cancelled.
>
> **Do NOT "fix" anything on a mass `ConnectionRefusedError`.** There is nothing there to
> fix, and a code change made under that misreading is a real defect introduced to chase a
> phantom — which is this phase's own hunted species arriving in the CI log.

*What remains unexercised: nothing measures where this flake comes from, or how often. Two
occurrences in one week is a rate over an unknown denominator. What would exercise it: a
run-history sweep counting `ConnectionRefusedError` failures against total `test` runs.*

### Carried out of T8 and T8b, and every entry names what would exercise it

- **`gate_run_absent` has never occurred in the workspace** — 0 of 11 live, reached only on
  fixtures. It needs an incident whose `check_bad_rows` row has aged out while its quarantine keeps
  its `_batch_id`, and the timeline's 35-day-and-counting retention is why it has not happened.
- **The `UnknownTable` arm of the live task.** All eleven resolved, so *"one bad incident fails the
  whole run"* is exercised only against a fake session. *What would exercise it: renaming a bronze
  job in the bundle and firing its gate.*
- **`logs_truncated: true` has never been observed** — and it is the output fork's own stated
  reversal condition. 21,213 characters against a cap nothing here measures. *What would exercise
  it: a deliberate oversized probe.*
- **`stranded_unexplained` and `over_promoted`** — two of severity's three unreconciled verdicts
  are still unreachable from this corpus.
- **The history query's `job_run_id` fold** — `check_bad_rows` still runs once per job run (29/29),
  so **only a constructed doubled row can prove the fold is there.** §0.10 says this outright and
  the live run does not change it.
- **`emit`'s fence refusal** — no live reject reason contains the marker.
- ~~**The title fence is now known to protect nothing**~~ — **that entry was itself false and is
  retracted in §1.10.** GitHub **does** render code spans in issue titles; the fence is
  load-bearing and stays. **Whether `@` and `#` LINKIFY in an issue title is still unmeasured** —
  #29's title contains neither, so opening it did not settle that half. *What would exercise it: a
  title carrying either character, which no incident id can produce.*
- **`assert_deployed_revision` does not re-read the SYNCED ENTRY POINT** — its own docstring says
  so, and that is the half `databricks/src/triage_dq_incident.py` actually lives in. The wheel's
  sha256 and `opl/_revision.py` inside it were checked; the entry point was read **once, by hand,
  by T8's implementer** (`b5c546f0…`), and nothing in the job does it. A stale `databricks/src`
  sync against a fresh wheel passes the guard. *What would exercise it: deploying a wheel while
  the sync of `databricks/src` fails or is skipped.*
- **The facts payload is git-ignored** (`.plans/`), so a public reader reaches the run's numbers
  only through issue #29 and through this section. *What would change it: committing a redacted
  payload, which no condition of this phase asks for.*

### Carried out of T7, and every entry names what would exercise it

- **THE INDEPENDENT REVIEWER'S 85-TRIAL, TEN-CONFIGURATION CORPUS IS NOT COMMITTED**, so
  *"142 on the five"*, *"0 of 50 on `592660596679630`"*, *"24/25 declines"* and every
  per-menu-position rate quoted from it rest on a *Reported* label with **nothing a reader can
  recompute**. `docs/f6-llm-control-responses.json` holds four sweeps, 20 trials and 170
  responses, and none of those figures is derivable from it. **This is §9 condition 6 applied to
  a measurement rather than to a code path**, and the conclusions those figures carry — that the
  decline is robust to menu order, and that the reconciliation inversion holds at scale — are the
  two the phase leans on hardest. *What would change it: committing that corpus, redacted the way
  the published one is.*
- **`Warehouse` is untested in its entirety** — `run`, `_get`, `cache_flag`, `_rows_of` and `main`.
  The reviewer deleted a row-truncating mutation and a `None`-to-`False` mutation together and the
  suite stayed green at 344. *What would exercise it: a fake-transport test over `requests.Session`.*
  One half is now closed rather than listed: `run_trial` asserts `set(rows) == keys`, so a
  truncated statement no longer publishes as an empty response.
- **`is_publishable`'s discard path has never fired at runtime** — 0 of 90 trials. The predicate is
  pinned by unit test in all four cells; the code that produces its input has never produced
  anything but `False`. *What would exercise it: a trial whose flag reads `True` or never fills.*
- **`manifest.total_row_count` and `manifest.truncated` are never read.** *What would exercise it:
  a statement the warehouse truncates.*
- **The 1,000-row cache-flag window has no test** and is exercised only live (1001 returns HTTP
  400). Past 1,000 statements, *"outside the window"* and *"metrics unfilled"* both read `None`;
  `.plans/cache_flag.sh` separates them with exit 3 and exit 2 because an operator acts differently
  on each, and the module accepts the conflation because **both discard the trial**. *What would
  exercise it: reading a flag for a statement more than 1,000 statements old.*
- **The declared corpus is QUOTED from §0.3/§0.5/§0.10, not queried**, and its lock is a **drift**
  lock: every one of the 66 cells is held equal to the published prompts, so nothing can move now,
  **but a cell mistyped before the sweeps ran would leave file and corpus agreeing.** *What would
  exercise it: T8's live run disagreeing with §0.5 or §0.10 — and if it does, `CORPUS` is what gets
  corrected.*
- **`--append`'s clobber path**, now refused by an assertion rather than by intent. *What would
  exercise it: appending an arm whose name is already published.*
- **One endpoint, one prompt design, no temperature or seed control.** Every rate in §1.8 is
  conditional on `databricks-gpt-oss-20b` at endpoint defaults and on one prompt, which is why the
  prompts are published verbatim in the corpus. Ten other READY endpoints are untried and the
  module takes no `--endpoint`. *What would exercise it: the flag, and a re-run.*
- **The corpus file's header names the SHIPPED menu order only.** A quarter of the trials ran the
  decline mid-menu, and that arm differs from the shipped one in **two** lines plus one thing that
  changes with no byte changing — the instruction's *"including the last"* points at the decline in
  the shipped arm and at `clean` in the fourth. **So the fourth arm is not a one-variable
  experiment on menu position**, each arm's real menu is recoverable only from its own published
  prompt, and this sentence is where a reader of the corpus meets that.
- **The model was handed the un-prefixed job name** (`opl-bronze-payments`), which is not what the
  workspace uses — the runtime name carries the bundle's development prefix, and §0.6 records why
  that must not be committed. Uniform across all eleven, so it cannot separate them.
- **The fabricated-incident prompt discloses that every lookup came back empty.** So what
  prediction 4 falsified is *"it produces an RCA when the facts say nothing was found"*, **not**
  *"it cannot be induced to"*. *What would exercise the harder question: an arm handing only the
  id, with no search results at all.*

### Carried out of T6, and every entry names what would exercise it

**THE FIRST FOUR ENTRIES OF THIS BLOCK WERE CLOSED BY THE PHASE THAT WROTE THEM — T8 and T8b did
exactly what each of them named, and §1.10 carries the readings.** They are struck rather than
deleted because §3 carried the second of them as *UNVERIFIED* while the T8b block above already
carried its retraction, and a reader meeting one before the other should see both.

- ~~**NOTHING IN THIS PHASE HAS BEEN RENDERED BY A MARKDOWN ENGINE.**~~ Every escaping claim was
  reasoned from the CommonMark spec and asserted against the **emitted string**, never against
  rendered HTML — **until issue #29 was opened, rendered and read in full (§1.10).**
- ~~**That GitHub renders code spans in issue titles is UNVERIFIED**~~ — **MEASURED TRUE at T8b
  (§1.10), and the fence stays.** **Whether `@` and `#` LINKIFY in a title is still unmeasured**;
  #29's title carries neither.
- ~~**`gh issue create` has never been invoked.**~~ **It was, once, for issue #29.** The publisher
  tests still stub `subprocess.run`, so only the success path has met the real CLI. *What would
  exercise the rest: a `gh` invocation that fails.*
- ~~**The Spark arm does not exercise the file door.**~~ **T8 did what this entry named** — the
  workspace run's emitted JSON was read back through `from_mapping` and became #29's body.
- **`hold_note` is re-derived at the file door and NOT refused when absent.** A held batch carrying
  no note is a state the report tests build on purpose, so the check is one-directional. *What
  would exercise it: a payload claiming a hold the repository does not declare — refused — against
  one dropping a note it does — accepted.*

### Carried out of T5, and every entry names what would exercise it

- **A bronze registry key becoming a gold table name is guarded by NOTHING.** The leg-3 rule
  classifies a task as a gold loader iff its first parameter names a registered gold table.
  `opl.gold.registry_guards._assert_no_gold_name_is_owned_by_another_layer` refuses a gold name the
  **vault** holds, and refuses one that collides with a bronze **Delta** name (`bronze_payments`) —
  **but a bronze task's first parameter is the registry KEY** (`payments`), and all seven keys are
  accepted as gold table names. Measured: with a gold table named `payments` registered, **five
  bronze tasks classify as gold entry points**. No key is a gold name today. *What would exercise
  it: registering a gold table named `payments`, `socios`, `lookup`, `merchant`, `ptax`, `empresas`
  or `estabelecimentos`.*
- **A script that reads a bronze table while the bundle hands its task no gold table name is
  SILENT to the sweep, and nothing narrows it.** Measured: a new `gold_prepare_fx.py` reading
  `merchant`, wired into the fact job with first parameter `"ptax"`, is invisible while all three
  of the sweep's assertions stay green — the totality it was once claimed to be narrowed by is
  total over gold **tables**, and such a script builds none. The other two arms of the rule's
  failure profile are loud and are written down beside this one. *What would exercise it: a gold
  job task that reads bronze without being handed a gold table name.*
- **The `ast` reader recognises exactly one spelling and anything else contributes nothing.** That
  is stated as an accept set rather than as a count of misses, so it cannot be falsified by a new
  spelling — but it also means **a new, undeclared bronze read in any other spelling is invisible**,
  and one of those spellings (`table_spec(args[0] if args else "")`) is `databricks/src`'s own live
  idiom for the bronze reader. An indirection applied to a read that IS declared is caught, because
  the sweep then disagrees with the declaration. *What would exercise it: a new gold entry point
  reading bronze through anything but a bare-name call on a `from opl.contracts import <module>`
  alias.*
- **`tests/test_vault_job_wiring.py`'s totality lock cannot see a loader task added to an existing,
  already-classified non-`vault_*` job file.** Pre-existing, confirmed by mutation — injecting a
  `vault_load_satellite.py` task into `smoke_job.yml` leaves the job-wiring files green and reddens
  only T5's lock. A brand-new YAML file **is** caught, by
  `test_every_yaml_under_resources_is_classified`. **Reported, not fixed:** that lock is not this
  task's, and T5's own sweep — keyed on the parameter, over every `*.yml` — is strictly stronger.
  *What would exercise it: whoever next owns that file.*
- **T5's own split cannot be certified as behaviour-free.** `test_blast_radius.py` reached 807
  lines before `tests/test_size_caps.py` caught it, and was split at a measured seam — but the
  pre-split file exists in **no commit and nowhere on disk**, so there is nothing to diff the moved
  bodies against. This is T1's position exactly, and the procedural fix adopted then (*commit the
  task before splitting it*) does not reach a file that hit the cap **inside** its own task. *What
  would exercise it: nothing. It is recorded so a later reader knows the claim rests on the
  implementer's word.*

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
