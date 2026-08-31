# The unexercised ledger

**Controller-verified where a row says so; every classification is *Reported*.** Consolidated
2026-08-31 (F7 T4) from the ten unexercised sections nine run-evidence documents carry, plus
three findings this task's own run produced.

## 0. How to read this, and what a bucket means

### 0.1 Why this file exists

Nine documents each published a *what is still unexercised* list, and **nobody ever read them
as one list**. The cost is measurable: **24 entries had been closed by later work and were never
struck** (§4.1), **10 of them by the same document that still carries them**, and **two closed
before they were written**. A finding that is recorded and not consumed is indistinguishable
from a finding nobody made.

This file replaces those sections' **authority**, not their existence. They stay where they are:
they are point-in-time records, and `docs/f-db-run-evidence.md` §3 already argued why erasing
one is wrong. When this file and a source section disagree, **this file is the one that is
maintained** and the source section is history.

**And this file is not a tenth hand-maintained list, because it has an obliged consumer.**
`tests/test_unexercised_ledger.py` reads every row, resolves every anchor against the source
line it names, re-counts every total published below, and refuses an entry that sits in two
buckets or names no exerciser. A tenth list with no test would reproduce the exact defect this
file exists to fix.

### 0.2 The five buckets

An entry's bucket is decided by **its own claim, read today**, and by what could make that claim
false:

| bucket | the claim is | what could change it |
|---|---|---|
| **STANDING LIMITS** (§1) | true, and it is kept that way on purpose | only an edit to this repository: a declaration, an import, a deploy, a source rewrite. **No run can close these and none should be asked to.** They are limits with a reason, not debt |
| **PUBLISHED CAVEATS** (§2) | true, and its own *what would exercise it* is **nothing** | nothing, while a shipped design decision holds. By the ledger's own rule these leave the ledger and are published as caveats |
| **STILL UNEXERCISED** (§3) | true today | a run, a corpus, a deploy, or work someone could do. **This is the carry-forward** |
| **CLOSED** (§4) | **false today**, because a run or a commit made it false | nothing; it is done. Recorded so the next reader can see what was closed and by whom |
| **NO LONGER MEANINGFUL** (§5) | not about an unexercised path at all, or mis-stated about the code, or superseded in its own file | nothing; the question stopped applying |

**The line between a STANDING LIMIT and debt.** Many follow-ups have an edit-shaped exerciser
("add the counter", "write the test"). Those are debt and live in §3. A standing limit is a
**guard or a blind spot the repository has decided to keep**, and `fapi:1553`'s own document
states the family in its own words: *"the guard protects an edit, not a data source."*

### 0.3 Which facts are mechanical and which are hand-assigned

The same distinction `docs/adr/README.md` draws between parsed and declared facts.

| column | mechanical or hand-assigned |
|---|---|
| **id** (`key:line`) | **mechanical.** `key` resolves through §0.4's table; `line` is a line number in that file, and the test reads it |
| **claim** | **mechanical.** Quoted verbatim from the source line the id names. The test asserts the quote is still there |
| **bucket** | **HAND-ASSIGNED, and no parser reads it.** Deciding that `stream._require_currencies` fires on an edit while `unhashable_case_divergence` fires on data is a judgement about what a guard *protects*. Nothing derives it and nothing checks it |
| **what would exercise it** | **HAND-WRITTEN PROSE, asserted by nothing but its non-emptiness.** `f6:1948` already books four such lines as carrying no test; this file inherits that and does not pretend otherwise |
| **closed by / evidence** | **half mechanical.** A commit and a `file:line` anchor are resolved by the test. **A run id and a statement id are not checkable offline and the test does not pretend to check them** |
| **the totals in §0.5** | **mechanical.** Re-derived from the tables below and compared |
| **the two counts §0.1 states in a sentence** | **mechanical.** Re-derived from §4.1's rows and from the source file each of them names |

**"Is this still true today?" is not derivable either.** No test can run a Databricks job, so
**closing an entry is always a human act** — which is exactly why §4 must exist. The only defence
against another silent closure is that closing one is now a visible edit to a single file
rather than a strike buried five hundred lines into a 2,400-line phase document.

### 0.4 The sources, keyed

Every id is `key:line`. These ten sections are the whole input. The test asserts each heading
still appears exactly once in the file beside it, so a rename or a deletion goes red — and it
re-derives the whole list with the sweep §8 publishes, so an eleventh ledger section appearing
anywhere under `docs/` and not declared here goes red too.

| key | file | ledger heading |
|---|---|---|
| `f1b` | `docs/f1b-run-evidence.md` | `## 4. What is still unexercised` |
| `f2w1` | `docs/f2-wave-1-run-evidence.md` | `## 18. Three paths this phase did not exercise` |
| `f2ws` | `docs/f2-wave-1-workspace-run-evidence.md` | `### 1.5 Two modelled paths are still unexercised, and this run did not change that` |
| `f2ws` | `docs/f2-wave-1-workspace-run-evidence.md` | `### 5.3 What is still unexercised, after everything ran` |
| `f3ws` | `docs/f3-workspace-run-evidence.md` | `### 9.4 What is still unexercised, after everything ran` |
| `f4` | `docs/f4-run-evidence.md` | `## 3. What is still unexercised` |
| `f5` | `docs/f5-run-evidence.md` | `## 3. What is still unexercised` |
| `f6` | `docs/f6-run-evidence.md` | `## 3. What is still unexercised` |
| `fapi` | `docs/f-api-run-evidence.md` | `## 3. What ships UNEXERCISED` |
| `fdb` | `docs/f-db-run-evidence.md` | `## 3. What ships UNEXERCISED` |

**Nine documents, ten sections.** `f2ws` carries its ledger in two places, and its §1.5 is
superseded by its own §5.3 five hundred lines below — this repository's signature defect, inside
its own ledger.

Three entries added by F7 T4 are anchored in code rather than in a ledger, because that is where
the claim is verifiable:

| key | file |
|---|---|
| `seed` | `scripts/seed_merchant_db.py` |
| `pgsrc` | `src/opl/extraction/postgres_source.py` |

### 0.5 The totals

| bucket | entries |
|---|---|
| STANDING LIMITS | 16 |
| PUBLISHED CAVEATS | 10 |
| STILL UNEXERCISED | 114 |
| CLOSED | 34 |
| NO LONGER MEANINGFUL | 8 |
| **TOTAL** | **182** |

**Do not re-type these.** They are re-derived from the tables below by
`tests/test_unexercised_ledger.py`, which fails naming the bucket that moved.

**Why this is more ids than there are bullets in the nine ledgers.** One bullet, table row or
titled block in a ledger section is one source *site*. **Three sites carry more than one claim**,
and where those claims land in different buckets the site is split: `f4:1360`/`f4:1386`,
`fdb:1521`/`fdb:1522`/`fdb:1523` and `fdb:1525`/`fdb:1526`. **Two bullets are not entries at
all**: `fdb` §3's `:1387` and `:1391` are the retirement notices for `fdb:1343` and `fdb:1361`,
so they are evidence rather than debt. And §1.3's rows are new in F7 T4 and are in no ledger.
**The site count itself is *Reported*** — counted by hand, asserted by nothing, which is why
it is not published here as a number.

Publish the derivation, not the number:

```bash
# how many entries this file carries, counted off the rows rather than read off the table
grep -cE '^\| `[a-z0-9]+:[0-9]+` \|' docs/unexercised-ledger.md
# and the whole comparison, buckets included
uv run pytest tests/test_unexercised_ledger.py -q
```

---

## 1. STANDING LIMITS — they fire on a change to this repository, not on a row

**No run can ever close these.** What they refuse is a source change, an import, a declaration, a
deploy or a config edit. Published as one named family because the ledger lines a reader would
otherwise take as owed work collapse into one paragraph and three tables.

`fapi:1553` says it in its own words: *"the guard protects an edit, not a data source"*, and
`fapi:1598` says it again: *"the guard protects a diff, not a data source."*

### 1.1 Refusals — something is actively refused

| id | claim | why no run can close it |
|---|---|---|
| `fapi:1542` | The `before` placement. | fires on a newly declared `StreamProfile`, which is a diff |
| `fapi:1549` | The straddling window. | fires on a new declaration; F3 measured a straddle needs 48x the shared `event_interval_ms` |
| `fapi:1553` | `stream._require_currencies`' four refusals. | the input is a declared profile; a live declaration reaching any of them would fail the build |
| `fapi:1589` | `FactRole`'s reader-versus-source refusal. | import-time, on the registry |
| `fapi:1598` | `_refuse_a_derived_role_this_loader_cannot_produce`. | import-time, on the registry, before a session starts |
| `f4:1286` | `assert_mask_predicate`'s failure arm | the exerciser is a deploy that skips `ensure_masked_table`, or a wheel from another revision |
| `fdb:1497` | `_grain_derivation_mismatch` AND `_refuse_a_prefixed_hub_grain` HAVE REFUSED NOTHING BUT A | fires when someone declares a third grain; the two that exist both derive both halves from the link spec |
| `f6:2352` | A top-level `UNION` in either F4 view. | the exerciser is a SQL rewrite of a view |
| `f6:2356` | A trailing line comment ending in ` AS source` before a comma. | the exerciser is a hand-written comment inside a generated projection |

### 1.2 Blind spots — nothing refuses the edit; the edit goes unseen

The answer is the same and the reason a reader must not infer owed work is the same: **no volume
of data will ever change them.**

| id | claim | why no run can close it |
|---|---|---|
| `f6:2281` | A bronze registry key becoming a gold table name is guarded by NOTHING. | the exerciser is registering a gold table under one of the seven bronze registry keys |
| `f6:2290` | A script that reads a bronze table while the bundle hands its task no gold table name is | the exerciser is a new gold job task |
| `f6:2297` | The `ast` reader recognises exactly one spelling and anything else contributes nothing. | the exerciser is new source text in another spelling |
| `f6:2305` | `tests/test_vault_job_wiring.py`'s totality lock cannot see a loader task added to an existing, | the exerciser is a YAML edit; kept because T5's own sweep is strictly stronger |

### 1.3 Added by F7 T4 — three merchant rules the registry cannot produce

**New, and no ledger had noticed it.** Derived from the registry DDL against the merchant rule
set before anything was run, and recorded in `.plans/2026-08-31-f7-t4-merchant-quarantine-run.md`
§1. `rules_for("merchant")` is `_required_rules(MERCHANT_CONTRACT)` plus **seven** named rules
(`sed -n '424,438p' src/opl/bronze/rules.py`). Three of the seven cannot be reached by a row the
Postgres registry can hold, so they defend against corruption in transit, a schema change, or a
different source arriving on the same contract — **standing limits, not debt.**

| id | claim | why no run can close it |
|---|---|---|
| `seed:119` | credit_limit       numeric(14,2) NOT NULL, | the column is `numeric`, so `unparseable_credit_limit` cannot occur in a row the registry can hold |
| `seed:120` | onboarded_on       date          NOT NULL, | the column is `date`, so `bad_onboarded_on_shape` cannot occur in a row the registry can hold |
| `pgsrc:552` | SNAPSHOT_AT_COLUMN: observation.instant, | `_snapshot_at` is stamped by the extractor from `clock_timestamp()` inside the reading transaction, not read from the registry, so `bad_snapshot_at_shape` is not a source property at all |

**The fourth of this kind is a choice rather than a type**, and it stays in §3 for that reason:
`fdb:1471`'s `unhashable_case_divergence` is unreachable because T10 refuses seeding one of the
forty divergent characters to "prove" it — a manual `psql` or a different mutation script would
reach it.

---

## 2. PUBLISHED CAVEATS — the ledger's own rule says these leave the ledger

An entry whose own *what would exercise it* is **nothing** is not debt; it is a caveat, and it is
published as one. **A "nothing" is a claim like any other**, so every row names the structural
argument that makes it one. `f6:1927`'s own entry records what happened the last time a
"nothing" was asserted without one: *"That was false for both halves, and the review of the
correction refuted it with two mutations."*

| id | claim | the argument that makes the exerciser not exist |
|---|---|---|
| `f1b:229` | The three defect classes in one promoted stream. | the gate is all-or-nothing, so a promoted stream cannot carry a rejected row |
| `f2w1:1147` | no absence for the observation ledger to report | the reference loader is insert-only by design: the anti-join drops a candidate whose `codigo` is present, so no `hash_diff` comparison and no `applied_date` sequence can ever exist for these six tables |
| `f2ws:639` | Single-month by construction. | the same argument, restated in `f2ws` (see §6) |
| `f3ws:590` | The three conformed ghosts, all of them. | all three conformed keys are **derived** from the fact's own columns, so there is no lookup to coalesce onto a ghost |
| `f4:1393` | Whether the platform stops maintaining a table whose PO flag reads `DISABLE`. | the flag is a request and the ops-history table is the receipt; PO's cadence is hours to days, so a session cannot settle it |
| `f6:1948` | Four prose corrections in T1 are asserted by nothing | they are prose; the source says *"What would exercise them: nothing"* |
| `f6:2312` | T5's own split cannot be certified as behaviour-free. | the pre-split file exists in no commit and nowhere on disk, so there is nothing to diff the moved bodies against |
| `f6:2339` | The gate-spelling lock cannot recover history recorded under a name nobody declared. | *"nothing in the wheel"* — a person would have to widen the declaration by hand |
| `fdb:1434` | `opl.config.landing_postgres_tmp` IS DECLARED AND NOTHING WILL EVER WRITE TO IT | this mode's producer runs on the host and PUTs a verified local file; the constant exists only because `registry_landing._landing_and_tmp` resolves both directories in one dispatch |
| `fdb:1525` | `closed_by` HAS EXACTLY ONE VALUE. | `src/opl/vault/effectivity.py` pins `CLOSING_STATE` to `absent_after_observation` and says `rejected_by_our_gate` closes no window, so `closed_by` has one value **by construction** (`grep -n CLOSING_STATE src/opl/vault/effectivity.py`) |

---

## 3. STILL UNEXERCISED — the carry-forward

**Ordered by subsystem, not by phase.** Nine phase-ordered lists is what exists now and is
precisely why nobody read them as one list; ordering by subsystem puts F2's satellite tie-break
next to F-DB's and F4's `stranded_unexplained` next to F6's, which is where the duplication
becomes visible. Every duplicate pair is named once in §6.

**The third column is prose and is asserted by nothing but its non-emptiness.** `nothing` is not
a legal value here: an entry whose exerciser is nothing belongs in §2, and the test enforces it.

### 3.1 Bronze ingest and the DQ gate

| id | claim | what would exercise it |
|---|---|---|
| `f1b:223` | Task 2 built schema drift deliberately and deferred value drift | a value-drift `DefectSpec`; `grep drift_from_index src/opl/generator/` finds only the schema-drift field |
| `f1b:225` | `reclaim_landing` for a generated source. | a reclaim task on that job; a generated table's way back is the seed |
| `f4:1287` | **`held_back`** in `reclaim_landing` | a batch whose file does not reconcile |
| `f4:1289` | **`skip_notice`** in the rule sweep | a staging table predating a rule's column |
| `f4:1308` | The stranded payments batch | promoting batch `592660596679630`, deliberately not done |
| `f4:1312` | The in-flow reclaim still never fires. | a month in which the gate does not block empresas and socios |
| `f4:1315` | `bronze_cnpj_lookup` is still two months behind. | an ingest of the 2026-07 and 2026-08 lookup zips |
| `f4:1334` | The month cross-check can be unwired from all three ingestion jobs with the suite green. | removing the parameter from all three; removing it from one is caught by the byte-diff paste lock |
| `f4:1343` | `reclaim_landing` resolves the landing dir by a second spelling | closing the bypass, which turns the test that pins it red |
| `fapi:1518` | The refuse-a-different-file-under-one-name branch, for PTAX. | a second `opl-bronze-ptax` run over the same window |
| `fapi:1520` | `encoding_replacement_char` is SHADOWED on four of the five columns it is folded | a U+FFFD landing in `currency`, the one column no earlier content rule inspects |
| `fapi:1531` | Every PTAX DQ rule except `bad_quote_date_shape` and `unparseable_data_hora_cotacao` | a landed PTAX file this repository did not build from its own validated response |
| `fapi:1619` | THE DQ GATE'S FAILING ROUTE, IN THIS PHASE. | a `drifting` profile run against PTAX; `bronze_ptax_quarantine` holds zero rows |
| `fapi:1631` | `reclaim_landing`, for PTAX. | a file in the landing directory written by another wheel, hand-repaired or copied in, judged by the gate alone |
| `fapi:1636` | The refuse-a-different-file-under-one-name branch, for PTAX | the same second run (see §6) |
| `fdb:1449` | THE ANSI-MODE PATH IS TESTED AND HAS NEVER RUN ON DATABRICKS. | a Databricks run of `ref_date_from_instant` after the CodeRabbit fix; the run of record predates it |
| `fdb:1471` | `unhashable_case_divergence` HAS REJECTED NOTHING OUTSIDE A FIXTURE. | a manual `psql`, a re-seed, or a mutation script that is not this one |
| `fdb:1523` | `_rescued_data` was never populated | a merchant row carrying an undeclared key that Auto Loader rescues; it cannot be the same row that proves a divergence, because `rescued_data_present` sits above every per-table rule |

### 3.2 Vault loaders

| id | claim | what would exercise it |
|---|---|---|
| `f2w1:1118` | All 68,629,147 keys of 2026-06 are present in 2026-07 | a real departure in an RFB snapshot; the RFB retains baixadas |
| `f2w1:1129` | Zero duplicate `(cnpj_basico, _snapshot_month)` rows in either month | a duplicate key-month pair; still 0 in F-DB |
| `f2ws:636` | 0 collapsed duplicates on all four satellites | the same duplicate pair, in the workspace (see §6) |
| `f3ws:605` | `_refuse_a_target_the_source_has_outgrown`. | a third RFB snapshot |
| `f3ws:610` | The PIT's out-of-order-backfill refusal. | a snapshot loaded between two the table already holds |
| `fdb:1369` | `effectivity._statements`' carry-forward window still orders by `APPLIED_DATE`, which is a | two observations on one calendar day, which T8's scheduling decision currently prevents |
| `fdb:1486` | THE DEFECT `ObservationGrain.key_prefixes` FIXES IS UNREACHABLE BY THIS REPOSITORY'S OWN | a mutation that changes `cnpj` while keeping its eight-character root; `mutated()` never touches `cnpj` |
| `fdb:1504` | NO NON-EMPTY `key_prefixes` REACHES ANY GRAIN BUT ONE. | a second link with a declared derivation on an identifying end; the field's second consumer arrives with wave 2 or not at all |
| `fdb:1526` | the other half of ADR 0010's whole subject | `opl_vault_merchant` re-run after `repromote_triaged_batch`; see §3.9's hazard before doing it |

### 3.3 Gold

| id | claim | what would exercise it |
|---|---|---|
| `f3ws:581` | The ghost row on BOTH role keys of `fact_payment`. | a payment whose counterparty resolves to no hub key; 0 of 80,000 references reach it today |
| `fapi:1452` | The duplicate-quote disagreement branch. | a duplicate pair whose rates disagree; one pair in 3.6 years and it agrees |
| `fapi:1466` | THE `orderBy` TIE-BREAK, WHICH HAS NO MEASURED WITNESS AT ALL. | two distinct quote dates sharing one publication instant; one repeated stamp in 42 years and it is within a single quote date |
| `fapi:1495` | The holiday crossing, on fact rows. | a fact row inside a holiday's carry-forward window |
| `fapi:1497` | The below-the-series refusal. | a payment below the series' first quote date |
| `fapi:1565` | The below-the-series refusal, on fact rows. | the same, in the workspace (see §6) |
| `fapi:1569` | The disagreeing-duplicate refusal, now at the GOLD side too. | a disagreeing duplicate in the landed `bronze_ptax`, which appends |
| `fapi:1575` | The unreadable-rate refusal in `rate_intervals`. | a bronze row that did not come through the gate one layer up |
| `fapi:1586` | The empty-series refusal | a `bronze_ptax` that reduces to no quotes behind a successful ingest |
| `fapi:1604` | The one-rate branch of `gold_load_fact.py`'s FX note. | a star where every row converted at 1.00000; the run's `fx_rates_used` came back 3 |
| `fapi:1613` | CONFIRMED UNEXERCISED BY THE RUNS, not merely predicted so. | a corpus that reaches any of the four: the ghost on both role keys, the four orphan counters, `fx_beyond_series`, the one-rate branch. A zero is not coverage |
| `fapi:1642` | THE GOLD-SIDE REDUCE RAN OVER A POPULATION THAT NEEDED NO REDUCING. | a re-run, which is what makes the agreeing-duplicate path an ordinary event |
| `fapi:1648` | The empty-series refusal and the below-the-series refusal | the same two populations, after the runs (see §6) |
| `fapi:1651` | The holiday crossing, on fact rows | a fact row nearer than the 15 days that separate 2026-06-04 from 2026-06-19 |
| `fapi:1656` | THE TWO-RATE PROPERTY RESTS ON ONE STREAM AND ONE DAY. | a second stream or a second converting day; 4,905 of 40,000 fact rows convert and all fall on 2026-06-22 |

### 3.4 Streaming

| id | claim | what would exercise it |
|---|---|---|
| `f5:893` | **`_progress_of`'s truncation refusal** (`len(progresses) >= cap`) | a run with as many progress updates as the cap; over `promotable` that floor is 104 records a trigger |
| `f5:894` | **`RingBufferReading`'s fourth state** | a serverless run resumed against a checkpoint that had already committed a batch and still consumed something |
| `f5:895` | **`_progress_of`'s trailing-progress arithmetic on the STATELESS path** | a stateless `availableNow` run near the cap with the progress list read back |
| `f5:896` | the drift column through the Kafka transport | publishing a `drifting` profile to a topic and rebuilding the landed `kafka_value` bytes |
| `f5:897` | a fault across MORE THAN ONE partition | the same two arms over a multi-partition topic |
| `f5:898` | **`read_kafka` from the SQL warehouse** | a statement reading the managed broker through it |
| `f5:902` | The `<=` in the late-data model, and it is UNDECIDED rather than assumed. | a corpus that puts a delivered record's margin exactly on an arm's delay |
| `f5:914` | The ring-buffer cap on serverless. | a Databricks compute that permits the read |
| `f5:920` | The exactly-once proof on the deploy target. | a way to terminate a task's process at a chosen instant on that compute; refused with a reason in ADR 0019 |
| `f5:925` | No latency, no throughput, anywhere in this phase. | a warmed session and repeated trials under F4 Task 6's protocol |
| `f5:932` | The two SCRAM login-module spellings are a measured pair with two untried corners. | the OSS name on serverless, and the shaded name anywhere else |
| `f5:937` | `foreachBatch` on serverless. | a serverless `foreachBatch` run; both exactly-once arms have only ever run locally |
| `f5:940` | `dbutils.secrets.get` from a `spark_python_task` | a missing scope or a revoked key, on either compute |
| `f5:945` | `src/opl/streaming/__init__.py` describes the CI job that was not built. | whoever next touches that module |
| `f5:963` | `describe_reader_options` does not cover the logical plan | any code that calls `explain()` on the reader frame before the query starts |
| `f5:968` | The orphan topic left by a fixture that dies during SETUP. | a session that raises during setup and never reaches the finaliser |
| `f5:980` | What the broker does after ~2026-09-03. | F7 T7's dated probe; this document must say the broker is gone once it is |

### 3.5 Governance

| id | claim | what would exercise it |
|---|---|---|
| `f4:1283` | `apply_pii_governance`'s GRANT branch | a principal added to the roster and the job re-run |
| `f4:1284` | its REVOKE branch | a real out-of-band grant, or a roster that shrinks |
| `f4:1285` | `UngovernedRead` | `MANAGE`, `ALL PRIVILEGES` or an ancestor grant issued out of band |
| `f4:1293` | `is_member` inside a serverless job session | the floor re-derived inside a job session; it is reported, not controller-verified |
| `f4:1295` | The membership-lag figures are warehouse-side. | a trial inside a job session |
| `f4:1298` | Its *failure* mode | an unsupported named-parameter binding on serverless, raising before any delete |
| `f4:1303` | The permissive branch of `mask_personal_name`. | granting a principal `SELECT` on 55.8M rows of real personal data; refused by choice, ADR 0008 |
| `f4:1317` | A second masked contract. | a second entry in `MASKED_COLUMNS` |
| `f4:1347` | `apply_pii_governance` prints no counter for what it observed. | adding the counter both its sibling tasks already have |

### 3.6 DataOps and triage

| id | claim | what would exercise it |
|---|---|---|
| `f4:1288` | **`stranded_unexplained`** and **`over_promoted`** in the reconciliation | a mid-stream ingest failure, or a double promote |
| `f4:1328` | view column contract is unlocked in one direction | a test comparing the dashboard's 23 column names to what the views return; the machinery is in the same file |
| `f4:1339` | Two remedy commands for the same job disagree | reconciling `promote.require_batch_id` with `reconcile.py` |
| `f4:1354` | The running "guards found" tally is spelled five ways in code | one spelling, derived; left standing on purpose because fixing it by hand is the act that put it there |
| `f4:1397` | Whether Lakeview renders the committed `.lvdash.json`. | a human opening the dashboard; nobody has |
| `f6:1927` | A SEVENTH recommended action, reached by nothing, would leave every test green. | a test comparing the reached set against the tuple, or a seventh action |
| `f6:2175` | `gate_run_absent` has never occurred in the workspace | an incident whose `check_bad_rows` row has aged out while its quarantine keeps its `_batch_id` |
| `f6:2178` | The `UnknownTable` arm of the live task. | renaming a bronze job in the bundle and firing its gate |
| `f6:2181` | `logs_truncated: true` has never been observed | a deliberate oversized probe |
| `f6:2184` | `stranded_unexplained` and `over_promoted` | the same two verdicts, from the triage side (see §6) |
| `f6:2186` | The history query's `job_run_id` fold | a constructed doubled row; `check_bad_rows` still runs once per job run |
| `f6:2189` | `emit`'s fence refusal | a live reject reason containing the marker |
| `f6:2190` | Whether `@` and `#` LINKIFY in an issue title is still unmeasured | a title carrying either character, which no incident id can produce, so a hand-written title |
| `f6:2201` | The facts payload is git-ignored | committing a redacted payload |
| `f6:2207` | THE INDEPENDENT REVIEWER'S 85-TRIAL, TEN-CONFIGURATION CORPUS IS NOT COMMITTED | committing that corpus, redacted the way the published one is |
| `f6:2216` | `Warehouse` is untested in its entirety | a fake-transport test over `requests.Session` |
| `f6:2221` | `is_publishable`'s discard path has never fired at runtime | a trial whose cache flag reads `True` or never fills |
| `f6:2224` | `manifest.total_row_count` and `manifest.truncated` are never read. | a statement the warehouse truncates |
| `f6:2226` | The 1,000-row cache-flag window has no test | reading a flag for a statement more than 1,000 statements old |
| `f6:2231` | The declared corpus is QUOTED from | a live run disagreeing with the sections it is quoted from |
| `f6:2236` | `--append`'s clobber path | appending an arm whose name is already published |
| `f6:2238` | One endpoint, one prompt design, no temperature or seed control. | an `--endpoint` flag and a re-run against one of the ten other READY endpoints |
| `f6:2242` | The corpus file's header names the SHIPPED menu order only. | a one-variable experiment on menu position, which the fourth arm is not |
| `f6:2248` | The model was handed the un-prefixed job name | a trial handing the runtime name, which carries the bundle's development prefix |
| `f6:2251` | The fabricated-incident prompt discloses that every lookup came back empty. | an arm handing only the id, with no search results at all |
| `f6:2274` | `hold_note` is re-derived at the file door and NOT refused when absent. | a payload claiming a hold the repository does not declare, against one dropping a note it does |
| `f6:2328` | Nothing permutes the reading ladder's arms. | a test that reorders `_READING_LADDER` and asserts the answer moves; considered and not built |
| `f6:2333` | `own_gate` returning two rows is unexercised. | one `job_run_id` appearing under two `job_id`s in the telemetry |
| `f6:2362` | Two `fail_on_dq` incidents on estabelecimentos | reading the two runs' own task output, or a Delta history read on `bronze_cnpj_estab_quarantine` |

### 3.7 Extraction and sources

| id | claim | what would exercise it |
|---|---|---|
| `fapi:1514` | The fetch's non-200 branch. | a live BCB response that is not 200 |
| `fapi:1516` | The refusal of a fetch window that yields no quotes. | a window in which BCB publishes nothing at all |
| `fdb:1444` | `_refuse_a_watermark_before_t2` HAS NEVER FIRED AGAINST A REAL MUTATION. | `mutate --ready-on` and the extractor driven in one session |
| `fdb:1457` | THE INCREMENTAL QUERY'S BOUNDARY WAS EXERCISED AND ITS COMPLEMENT WAS NOT. | the 48-row miss the boundary demonstration says nothing about |
| `fdb:1476` | `_refuse_a_since_before_t2` HAS NEVER REFUSED A REAL RACE. | two snapshots driven on two calendar days with the race actually occurring |
| `fdb:1515` | THREE REFUSALS PASSED RATHER THAN FIRED, and passing is not evidence about them. | a real race reaching `_refuse_a_since_before_t2`, `_refuse_a_watermark_before_t2` or `_refuse_an_incremental_run_without_the_hand_off` |
| `fdb:1533` | `mutate --release-after`, the extractor's `--no-upload`, and `report_diagnostics=false` | a run that passes any of the three |
| `fdb:1550` | A PARTIAL SNAPSHOT READS AS DEPARTURES, AND ONLY THE EXTRACTOR'S OWN PIPELINE GUARDS IT. | a short file in the Volume. **Widened by F7 T4:** the entry says only a manual write bypasses the three extractor guards; a fully-gated batch produces the same shape with all three PASSING, because the gate removes the rows downstream of them |

### 3.8 CI and platform

| id | claim | what would exercise it |
|---|---|---|
| `f4:1350` | Three residual tautologies and one floorless sweep | adding a non-empty floor to `test_cadence.py`'s glob |
| `f4:1360` | A CI failure during the close, and what it is honest to say about it | the experiment the entry names: the failing test in isolation against the same runner class, or the suite with F4's Spark modules deselected. F7's sweep answered a different question |
| `f4:1390` | System-table retention's ceiling. | time; the workspace is younger than any documented horizon |
| `f5:975` | How much of the trial credit is left. | an endpoint that answers; four return `NOT_FOUND` and Prometheus returns 401 |
| `f6:1883` | "The declaration half is free of Spark" is enforced by nothing | someone adding a Spark test to any of the no-JVM files and nobody noticing. Do not re-count the files: run the measurement, a `PATH` with no `java` in it |
| `f6:2166` | nothing measures where this flake comes from | a measurement of the cause; F7's sweep bounded it at four occurrences but `gh run list` counts RUNS and this flake is a property of ATTEMPTS |
| `f6:2195` | `assert_deployed_revision` does not re-read the SYNCED ENTRY POINT | deploying a wheel while the sync of `databricks/src` fails or is skipped |
| `f6:2367` | Whether a job-level `permissions: issues: write` elevates above a repository default of | the first workflow run that tries |
| `fapi:1498` | The provenance guard's `+dirty` REFUSAL, in the workspace | a deploy built from a dirty tree, which means publishing an artefact known to be built from uncommitted work. **This is the only correct statement of this in the corpus** (see §6) |

### 3.9 One hazard to read before closing `fdb:1526`

**Recorded by F7 T4 before the run, and it is why the vault job was not run.** The observation
ledger's presence universe is bronze union quarantine with `months=None`, so re-running
`opl_vault_merchant` over a poisoned snapshot **without an intervening `repromote_triaged_batch`**
puts every previously observed key in neither table at that instant. That state is
`absent_after_observation`, which **is** `CLOSING_STATE`, so `sat_eff_merchant_empresa` closes
every one of those windows and appends the closes to an append-only table — on today's
population, roughly 1,103 false closes against 16 real ones, indistinguishable afterwards.

The safe order is: land, run `bronze_merchant` and let it fail at `fail_on_dq`, run
`repromote_triaged_batch`, and **only then** run `opl_vault_merchant`.

---

## 4. CLOSED — what a later phase actually did

**This section is the finding, and it is published rather than folded away.** A ledger that lists
only open items gives a reader no way to notice that entries were closed and nobody said so.

**The pattern is legible and it is this file's whole justification:** a ledger closes its own
entries reliably **within** a phase and **almost never** across one. Every row in §4.1 crosses a
phase boundary, or a section boundary inside a very long file, or both.

Evidence kinds: `commit:` and `anchor:` are resolved by the test. **`run:` and `stmt:` are not
checkable offline and the test does not pretend to check them** — it asserts their shape and
nothing more.

### 4.1 Closed and never struck

| id | claim | what closed it | evidence |
|---|---|---|---|
| `f2w1:1159` | Nothing in wave 1 has run against the workspace at all. | the F2 workspace run, three days later. Self-declared *not a gap* when written, and never revisited | anchor:docs/f2-wave-1-workspace-run-evidence.md:129 |
| `f2ws:634` | End-dating on a descriptive satellite. | F-DB's vault run, 2026-08-18, whose own section title is *"the first end-dating this lakehouse has written"*. The entry is also mis-stated: the descriptive satellite has no end-date column at all | anchor:docs/f-db-run-evidence.md:1220 anchor:src/opl/vault/satellites.py:24 |
| `f2ws:640` | The provenance guard's refusal | ADR 0009's Status, **2026-08-03 — eight days BEFORE this line was written** | anchor:docs/adr/0009-deployed-revision-provenance.md:4 anchor:docs/f1.4b-pr-b-run-evidence.md:978 |
| `f2ws:641` | `reclaim_landing`'s wired path | F4's two live runs, 2026-08-18, which deleted files through it | anchor:docs/f4-run-evidence.md:542 |
| `f3ws:597` | No orphan count was captured into this phase's run facts | F-API published the number: the four orphan counters are all 0. The counters themselves stay unexercised and are carried as `fapi:1613` | anchor:docs/f-api-run-evidence.md:1615 |
| `f3ws:600` | `dim_currency` at fact-side cardinality 1, and `dim_date` at 2. | F-API measured 2 and 2, and `fx_rate_date_key` taking 4 of the 51 values `dim_date` holds. Both published numbers are wrong | anchor:docs/f-api-run-evidence.md:1659 |
| `f3ws:614` | The provenance guard's REFUSAL half, in the workspace. | the same ADR 0009 Status, **ten days before**. The entry is careful, is emphatic about not over-claiming, and is wrong | anchor:docs/adr/0009-deployed-revision-provenance.md:4 anchor:docs/f1.4b-pr-b-run-evidence.md:978 |
| `f3ws:618` | Everything F2 left unexercised is still unexercised | false in two of its four limbs: F4 ran `reclaim_landing`'s wired path, and the descriptive satellite has no end-date path to exercise | anchor:docs/f4-run-evidence.md:542 anchor:src/opl/vault/satellites.py:24 |
| `f4:1386` | should now read **~20 min at ~2,683** | F7, 2026-08-31: the same step ran 31 m 23 s and 4 h 53 m 08 s for ~3,200 tests, so no single figure is publishable. `CLAUDE.md` still carries the stale one | anchor:docs/f6-run-evidence.md:2133 |
| `f5:871` | The CI `redpanda` job. | F6's PR run, 2026-08-26 — `test`, `postgres`, `secret-scan`, `redpanda` all green. The PR was opened, the job ran, and the entry still says never | anchor:docs/f6-run-evidence.md:2031 run:32988424065 |
| `f5:877` | The Windows session-start race, on `ubuntu-latest`. | F6 and F7's sweep: four runs with 637 to 654 `ConnectionRefusedError` each, on the runner. The answer is not the one the entry expects | anchor:docs/f6-run-evidence.md:2103 |
| `f5:883` | The job's own time budget on Linux. | F7, 2026-08-31: the estimate is replaced by a spread, which is stronger than the entry asked for | anchor:docs/f6-run-evidence.md:2133 |
| `f6:2322` | `history.py` has never run against the DEPLOYED view over real system tables. | T8's workspace run, **in the same document, three sections above it** | anchor:docs/f6-run-evidence.md:2175 |
| `fdb:1347` | THE `axis=` PARAMETER HAS NEVER BEEN PASSED A NON-DEFAULT VALUE OUTSIDE THE LEDGER. | the merchant vault run: `INSTANT_SNAPSHOT` through exactly those functions | anchor:docs/f-db-run-evidence.md:1225 |
| `fdb:1356` | The four entry points fixed in `76c61e5` pass `axis=source.snapshot_axis`, and that | the same run: merchant is the seventh source and it is not the default | anchor:docs/f-db-run-evidence.md:1225 |
| `fdb:1401` | NOTHING HAS BEEN INGESTED. `bronze_merchant` does not exist as a table. | the run of record: `bronze_merchant` rows 2,192 | anchor:docs/f-db-run-evidence.md:1222 |
| `fdb:1406` | THE `postgres/` LANDING ROOT HAS NEVER RECEIVED A BYTE. | two snapshots landed to the Volume and ingested | anchor:docs/f-db-run-evidence.md:1154 |
| `fdb:1441` | THE TWO SNAPSHOTS ON TWO DIFFERENT CALENDAR DAYS (T8) HAVE NOT BEEN TAKEN. | verbatim contradicted **earlier in the same file** | anchor:docs/f-db-run-evidence.md:1154 |
| `fdb:1455` | `ref_date_from_instant` HAS RUN ON LOCAL SPARK ONLY | the run of record: 2 distinct `_snapshot_ref_date`, derived by that function over 2,192 real rows on serverless | anchor:docs/f-db-run-evidence.md:1224 |
| `fdb:1464` | THE THREE LIVE POSTGRES TESTS RUN ON ONE WINDOWS BOX AND NOWHERE ELSE. | the `postgres` CI job, which runs `uv run pytest -m postgres` on `ubuntu-latest` and which `fdb:1535` records as green **later in the same section**. Found by F7 T4; the source analysis flagged it as a check and left it open | anchor:docs/f-db-run-evidence.md:1541 anchor:.github/workflows/ci.yml:38 |
| `fdb:1480` | `src/opl/unicode_case.py` AND `src/opl/bronze/rule_predicates.py` HAVE RUN ZERO ROWS ON | 2,192 merchant rows through `rules_for("merchant")`, every rule of which is built in `rule_predicates.py`, which imports `DIVERGENT_CHARACTER_CLASS` from `opl.unicode_case` | anchor:src/opl/bronze/rule_predicates.py:37 anchor:docs/f-db-run-evidence.md:1222 |
| `fdb:1494` | `ObservationGrain.key_prefixes` AND `key_expression` HAVE RUN ZERO ROWS ON DATABRICKS | the run of record loaded `link_merchant_empresa`, the only link with a declared derivation on an identifying end | anchor:docs/f-db-run-evidence.md:1225 |
| `fdb:1521` | `fail_on_dq` AND THE `check_bad_rows` FALSE BRANCH NEVER RAN. | **F7 T4's run, 2026-08-31.** One bad row in 1,089 took the condition's false arm; `promote` was excluded and `fail_on_dq` failed, which is the success condition | run:529699767706804 |
| `fdb:1522` | `bronze_merchant_quarantine` has | **F7 T4's run, 2026-08-31.** Quarantine went 0 to 1; the row carries `_dq_reject_reason` `bad_cnpj_shape` and nothing else, and bronze did not move | run:529699767706804 stmt:01f1a57b-e745-14e0-9d1d-dfef2158916e |

### 4.2 Closed and struck, or updated in place — the behaviour the rows above should have had

| id | claim | what closed it | evidence |
|---|---|---|---|
| `f5:952` | Two files sit within single digits of the 800-line cap | the commit carrying the bullet itself, which split both files | commit:2d077a8 |
| `f6:2263` | NOTHING IN THIS PHASE HAS BEEN RENDERED BY A MARKDOWN ENGINE. | T8b: issue #29 opened, rendered and read in full | anchor:docs/f6-run-evidence.md:2265 |
| `f6:2266` | That GitHub renders code spans in issue titles is UNVERIFIED | T8b measured it true; the fence is load-bearing and stays | anchor:docs/f6-run-evidence.md:2267 |
| `f6:2269` | `gh issue create` has never been invoked. | it was, once, for issue #29; only the success path has met the real CLI | anchor:docs/f6-run-evidence.md:2270 |
| `f6:2272` | The Spark arm does not exercise the file door. | T8's workspace run read the emitted JSON back through `from_mapping` | anchor:docs/f6-run-evidence.md:2273 |
| `f6:2344` | Nothing asks the module about a batch whose gate ran and found nothing | T4's second correction, in the same phase; the entry records where the coverage lives | anchor:docs/f6-run-evidence.md:2347 |
| `fapi:1626` | NO LONGER UNEXERCISED: the no-quote-for-this-day branch. | F-API Task 5's own runs: 18 of 60 requests answered 200 with an empty value list | anchor:docs/f-api-run-evidence.md:1627 |
| `fdb:1343` | `INSTANT_SNAPSHOT` has no production reference at all. | F-DB Task 4, retired in the ledger's own voice because *"a ledger that only grows stops being read"* | anchor:docs/f-db-run-evidence.md:1387 |
| `fdb:1361` | `effectivity`'s axis-aware path cannot run on a non-monthly source today, for a reason | F-DB Task 4: the third audit path exists, so unreachable became merely unexercised | anchor:docs/f-db-run-evidence.md:1391 |
| `fdb:1535` | THE NEW CI JOB HAS NOT RUN ON GITHUB, and this is the honest version of a claim that would | the push that opened PR #21, the same night: the `postgres` job ran on GitHub and passed | anchor:docs/f-db-run-evidence.md:1541 |

Two more were updated in place without a strike and are **not** counted here, because a live
unexercised claim survives in each and keeps them in §3: `f4:1298` (`spark.sql(..., args={...})`
*"now has run on serverless"*, its failure mode has not) and `f5:940` (`dbutils.secrets.get`
*"now has run"*, its failure mode has not).

---

## 5. NO LONGER MEANINGFUL

The thing described was removed, superseded, or was never an unexercised path.

| id | claim | what happened |
|---|---|---|
| `f1b:231` | `max_retries: 0` still does not prevent a retry. | observed twice, on two sources, in the run that records it. A published platform caveat, not an unexercised path |
| `f2ws:139` | End-dating: not exercised. | superseded verbatim by §5.3 of the same file. Two spellings of one fact in one document (see §6) |
| `f2ws:142` | The satellite dedup tie-break: not exercised. | superseded verbatim by §5.3 of the same file (see §6) |
| `f6:1954` | THE FULL SUITE HAS NEVER RUN ON THIS BRANCH, AND CI DID NOT FIRE WHEN ASKED | falsified twice in place by its own phase and again by F7. What survives is a lesson about **when** a measurement is taken, and it belongs in the postmortem, not in a debt list |
| `fapi:1557` | The disagreement between `dim_currency`'s member count and its fact-side cardinality | the entry says so itself: *"That is not an unexercised path - it is the opposite"* |
| `fapi:1594` | The high-end coverage report is not a refusal and must not be read as one. | a reading instruction, not a path |
| `fapi:1661` | The `assert_deployed_revision` guard cost 21.5% of this phase's task time | a cost measurement carrying its own retraction. Not a path |
| `fdb:1529` | THE HUB-GRAIN VERSUS LINK-GRAIN DIVERGENCE IS NOT REACHABLE BY THIS DATA. | reachability here is a property of the seed script, which this project authors. A statement about a fixture, not about a shipped path |

---

## 6. The duplicates, named once

Naming each duplicated claim once, with all its sites, is what makes the carry-forward count
defensible. The **owner** is the row §3, §4 or §5 treats as authoritative; the other sites carry
the same claim and are counted at their own ids.

| the claim | sites | note |
|---|---|---|
| The satellite dedup tie-break | `f2w1:1129` · `f2ws:142` · `f2ws:636` · `f3ws:618` | four sites in three documents; still 0 duplicates in F-DB |
| The provenance guard's refusal | `f2ws:640` · `f3ws:614` · `fapi:1498` | **two of the three are wrong.** `fapi:1498` is the only correct statement in the corpus: it narrows to the `+dirty` shape and explicitly restores a strike that had over-closed |
| `reclaim_landing` | `f1b:225` · `f2ws:641` · `f3ws:618` · `f4:1312` · `f4:1343` · `fapi:1631` | six sites in five documents; the wired path closed in F4, the generated-source and PTAX cases remain open for different reasons |
| End-dating on a descriptive satellite | `f2ws:139` · `f2ws:634` · `f3ws:618` | mis-stated at every site: the descriptive satellite has no end-date column |
| Reference-table history | `f2w1:1147` · `f2ws:639` · `f3ws:618` | a caveat, not debt: the loader is insert-only by design |
| `stranded_unexplained` / `over_promoted` | `f4:1288` · `f6:2184` | one claim, two phases, neither aware of the other |
| The below-the-series refusal | `fapi:1497` · `fapi:1565` · `fapi:1648` | three sites in one document |
| The empty-series refusal | `fapi:1586` · `fapi:1648` | |
| The duplicate-quote disagreement refusal | `fapi:1452` · `fapi:1569` · `fapi:1642` | `:1569` is a genuinely second refusal at the gold side, not the same one counted twice; bronze appends, so the two see different populations |
| The refuse-a-different-file-under-one-name branch | `fapi:1518` · `fapi:1636` | |
| The holiday crossing on fact rows | `fapi:1495` · `fapi:1651` | `:1651` closes it as unexercised **by measurement** rather than by argument |
| The conformed ghosts and orphan counters | `f3ws:581` · `f3ws:590` · `f3ws:597` · `fapi:1613` | the record gap closed in F-API; the counters are still zero |
| Whether `@` and `#` linkify in an issue title | `f6:2190` · `f6:2266` | one claim carried by both a live entry and a struck one |

---

## 7. What this document does NOT cover

- **The ADRs' own unexercised statements.** Four ADRs carry them and one delegates:
  `docs/adr/0016-...md` says outright that `docs/f-api-run-evidence.md` §3 is the full ledger;
  `0008` has a *"unexercised here by choice"* with a *what would exercise it* of its own; `0015`
  names a structurally unexercised ghost; `0017` ships `key_prefixes` unexercised; `0019` lists
  the exactly-once proof as unexercised rather than settled. They are not a tenth ledger.
- **The reversal-conditions table in `docs/adr/README.md`.** It is functionally a ledger in a
  different vocabulary (`NOT READ` is close to unexercised), it is **generated** by
  `scripts/generate_adr_index.py`, and its readings are hand-declared in `scripts/adr_index.py`.
  It has its own consumer in `tests/test_adr_index.py`. Do not copy rows between the two files;
  link to them.
- **The `does-not-establish` sections.** Other documents under `docs/` carry a near relative of
  a ledger (*"what this does not prove"*, *"what this PR does not settle"*). None names *what
  would exercise it*, which is the property that makes a ledger a ledger, so none is
  consolidated here.
- **Whether an entry is still true.** No test can run a Databricks job. See §0.3.

---

## 8. How this stays true

**The process rule, and it is the whole mechanism.** Every phase's closing review adds one step:
*which rows of §3 did this phase close?* — and the answer goes into §4 in the merge commit.
Protocol §9 condition 6 only requires a phase to publish **its own** unexercised paths; that is
why every row of §4.1 went unstruck, because no phase was ever obliged to look at anyone else's
list. **The condition needs a second half: publish what you closed of someone else's.**

**Do not re-type a number from this file into another one.** Publish the command:

```bash
# does every anchor still point at its claim, and do the totals still add up?
uv run pytest tests/test_unexercised_ledger.py -q

# the ten source sections, swept rather than listed. The test runs this same pattern and
# asserts the result IS the table in 0.4, so an eleventh ledger cannot appear unnoticed.
grep -inE '^#+ .*(unexercised|did not exercise)' docs/*-evidence.md
```
