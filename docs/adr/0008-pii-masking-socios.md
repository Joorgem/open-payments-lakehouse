# ADR 0008 — mask the partner NAMES in `socios`, before the first row lands

## Status
Accepted. **Live on bronze and on quarantine as of 2026-08-01** — the mask is
applied to `workspace.default.bronze_cnpj_socios` and to
`workspace.default.bronze_cnpj_socios_quarantine`, and verified by reading through
both, not only by the DDL having run
([What the live run proved](#what-the-live-run-proved-and-what-it-only-confirmed)).
`system.information_schema.column_masks` returns **four rows**: two tables, both
name columns each. One incompatibility with Unity Catalog was found while
implementing this (a masked table may not carry a `CHECK` constraint); it was
probed against the live workspace and resolved by dropping socios' `CHECK`, with
the trade recorded below.

**The quarantine mask was applied to a table that already held rows** — the one
statement no probe had covered. It succeeded: quarantine kept its **1,797** rows
and both name columns now return exactly one distinct value, `***`. It also hid
the null that put those rows there, which is recorded as a cost in
[The boundary](#the-boundary-quarantine-is-masked-too-staging-cannot-be) rather
than passed over.

**Staging is deliberately not masked**, for a mechanism rather than for want of
effort (same section), and it keeps its rows by decision — see
[Staging retention](#staging-retention-the-rows-stay-and-what-guards-them).

## Context

`socios` is the only CNPJ table in this lakehouse that names natural persons.
Its 11 columns carry two of them:

| column | what it holds |
|---|---|
| `nome_socio_razao_social` | the partner — a company's `razão social` when `identificador_socio` = 1, a **person's civil name** when it is 2 or 3 |
| `nome_do_representante` | the civil name of a legal representative — **always a natural person** |

**The identifier is not the exposure; the name is.** The Receita Federal already
masks CPF at source: `cpf_cnpj_socio` arrives as `***DDDDDD**`, six middle digits
only, irreversible. There is nothing left to protect in that column and nothing
this project could add to it. What arrives complete, unmasked, and directly
identifying is the name. A control aimed at "the CPF column" would therefore be
theatre — it would mask a value the source already masked and leave the
identifying one in the clear.

**Two columns, not the one the spec named.** The F1.4 design says "UC column mask
on `nome_socio_razao_social`". `nome_do_representante` is the same category of
data by the same argument, and arguably a stronger case: `nome_socio_razao_social`
is a person's name only for some rows, while `nome_do_representante` is a person's
name for every row that has one. Masking the first and not the second would show a
control applied by *column name* rather than by looking at what the columns hold,
which is exactly the finding a governance review is built to produce. Both are
masked. This is a deliberate widening of the spec, recorded here so it reads as a
decision rather than as scope drift.

**Why now, in F1.4b, and not in F4.** The full governance work — grants, tags,
column-level lineage, the Purview/Collibra mapping — is F4. This one control is
pulled forward because ingesting personal data and adding the protection three
phases later is precisely the anti-pattern a governance reviewer looks for. "The
control was applied when the data landed" is a different sentence from "I went
back and masked it afterwards", and only one of them can be made true by acting
now. The scope is one mask plus this ADR; nothing else from F4 moves.

**Why the table is created explicitly, which nothing else in this repo does.**
No bronze table is created deliberately anywhere in this project. Each is created
*implicitly* by `saveAsTable(bronze_table)` in append mode inside
`opl.bronze.promote.promote_batch`, with the constraint DDL following it in
`databricks/src/promote_batch.py::_assert_constraints`. Under that ordering a mask
added the ordinary way lands **after** the first append: the table would exist,
holding names in the clear, and the control would follow the data — the second
sentence, not the first. So `socios` is the one contract whose tables this repo
builds by hand: `ensure_masked_table` issues `CREATE TABLE IF NOT EXISTS` for the
bronze and quarantine tables, then the mask function, then `ALTER … SET MASK` for
both columns of both tables, and only then does the job go on to unzip and ingest.
Each writer finds a table that is already masked.

The cost of that choice is that these two socios schemas are now *declared* rather
than produced, so they can be wrong in a way no other bronze table can. Bronze is
declared from the contract (`cnpj_schemas.TABLES`) plus a measured list of the
seven metadata columns, and two of those are **not** strings — `_ingested_at` is
`TIMESTAMP` and `_snapshot_ref_date` is `DATE`. An earlier draft declared all of
them `STRING`, which builds a table the first append cannot write to. Quarantine is
that same shape plus the gate's `_dq_reject_reason`, a column the contract does not
contain and whose name lives in `opl.bronze.dq` — a module `masking` may not import,
because `registry` imports `masking` and the extraction scripts import `registry`
where pyspark is not installed. Both are pinned in `tests/bronze/test_masking.py`:
as literals, by reading the reject column's name back out of its owner, and by
appending the DataFrames the real ingest and the real gate produce to real local
Delta tables created from these very DDLs — with a committed mutation probe proving
Delta refuses a mistyped column rather than casting it.

## Decision

1. **A SQL UDF as the mask**, in the same catalog and schema as the table:

   ```sql
   CREATE OR REPLACE FUNCTION workspace.default.mask_personal_name(name STRING)
   RETURNS STRING
   RETURN CASE WHEN is_account_group_member('opl_pii_readers') THEN name ELSE '***' END
   ```

2. **Applied to both name columns of `socios`** by
   `ALTER TABLE … ALTER COLUMN … SET MASK`, from `MASKED_COLUMNS`, which is keyed
   by *contract* so the mask follows the data rather than a table name.

3. **On the contract's BRONZE and QUARANTINE tables, and deliberately not on
   staging.** `masked_table_ddls` owns that list and carries the reason; the short
   version is that `promote_batch` reads staging and writes what it read into
   bronze, so a mask there would put `***` into the system of record. See
   [The boundary](#the-boundary-quarantine-is-masked-too-staging-cannot-be).

4. **Applied before ingest**, by a job task (`ensure_masked_table`) that runs
   ahead of `unzip`, and that is a no-op for any table declaring no masked column
   so the same task can sit in any job's YAML without a per-table branch.

5. **socios carries no `CHECK` constraint**, and is the only registered table
   without one — see the next section. Refused at import for any masked contract by
   `registry._assert_no_masked_contract_declares_a_check_constraint`.

### Fail-closed, and why that is the right direction

> **MEASURED 2026-08-18, during F4's measurement pass: THE PERMISSIVE HALF CANNOT BE OPENED AS
> WRITTEN, and this is stronger than "untested".** `is_account_group_member` resolves **account**
> groups only. Measured on this workspace: a workspace-local group was created, the user was
> added to it via SCIM, and `is_account_group_member('<that group>')` returned **false** while
> `is_member('<that group>')` returned **true**. Of the account-level groups, exactly one
> resolves — `account users`, i.e. everyone — and `databricks account groups list` returns
> `Error: Not Found`, because the workspace host is not an account host and account SCIM is not
> reachable with this token.
>
> **So `opl_pii_readers` cannot be created in a form that satisfies this predicate from this
> box.** `src/opl/bronze/masking.py:173`'s *"it becomes correct the moment `opl_pii_readers`
> exists"* names a moment that cannot arrive here.
>
> **THE FAIL-CLOSED ARGUMENT BELOW IS UNAFFECTED AND IS WHY THIS IS A CORRECTION AND NOT A
> RETRACTION** — the mask hides, always, and has hidden 55,827,243 rows. What is wrong is the
> claim that the control is merely dormant. **The repair is F4's decision** — switch the
> predicate to `is_member`, which was measured working, or state that the control is
> permanently closed here — and it is deliberately not taken in this note, because measuring a
> fact and choosing a fix are different acts and this project separates them.
>
> **AND THE REVEALING HALF IS NOW MEASURED ANYWAY**, by a route this ADR did not contemplate: a
> **service principal** with an OAuth secret, which Free Edition does allow. Two principals read
> the same column of the same table at the same moment — one got cleartext, the other `***`.
> The sentence below about the permissive branch being untested by construction was true when
> written and is no longer.

`is_account_group_member` returns **false** for a group that does not exist. So in
a workspace where `opl_pii_readers` was never created — which is the current state
of this one — *every* reader sees `***`, including the table owner's own queries.
The control degrades toward hiding, not toward revealing. The alternative
formulations all fail open: a mask keyed on an allow-list table that has not been
created yet, or one that checks `current_user()` against a list, reveal the name
when their dependency is missing. Creating the group is what grants access; doing
nothing denies it.

## Consequences

- **Rows are stored in the clear; the mask is a read-time control.** UC
  column masks rewrite the query, they do not transform the stored value. Anyone
  with direct access to the underlying storage path bypasses them. This is the
  right trade for bronze, whose whole job is to record what the source asserted
  verbatim, but it means the mask is a governance control, not encryption — and it
  means applying a mask to a table that already holds rows protects every read from
  then on without undoing the reads that already happened.
- **Time travel is unavailable on a masked table**
  (`COLUMN_MASKS_FEATURE_NOT_SUPPORTED.TIME_TRAVEL`). Nothing in the pipeline
  reads bronze history today; a future one cannot.
- **`_batch_id` idempotence is unaffected** — no metadata column is masked.
- **Every statement `ensure_masked_table` issues is idempotent**, which matters
  because `max_retries: 0` does not prevent a retry on `INTERNAL_ERROR`.
  `CREATE TABLE IF NOT EXISTS` is a no-op over a populated table (and is
  deliberately not `CREATE OR REPLACE TABLE`, which would drop its rows — verified
  against local Delta); `CREATE OR REPLACE FUNCTION` is the documented way to
  modify a function a live mask references; and re-applying `SET MASK` to an
  already-masked column **succeeds**, measured in the probe under
  [What masking costs](#what-masking-costs-socios-gives-up-one-declarative-constraint)
  — which is **below** this bullet, not above it, as this sentence said until the
  F1.4b run went looking for it. Applying `SET MASK` for the *first* time to a
  table that already holds rows is a different statement, and it has now been
  measured too: it ran against the 1,797-row quarantine table on 2026-08-01,
  succeeded, and did not truncate it. A `DROP MASK`
  before the `SET` would also be idempotent and is deliberately *not* used: it
  takes the mask off a populated table for the width of two statements on every
  monthly re-run, and a control that is briefly absent every month is worse than
  one that is never absent at all.
- **socios keeps no `CHECK` constraint** and loses one layer of defence in depth.
  See below. Only *bronze* is affected: `promote_batch._assert_constraints` issues
  that DDL against the bronze table alone, which is why quarantine can be masked at
  no cost.
- **Quarantine is masked; staging is not, and cannot safely be.** See
  [The boundary](#the-boundary-quarantine-is-masked-too-staging-cannot-be).

## What masking costs: socios gives up one declarative constraint

Unity Catalog refuses a column mask and a `CHECK` constraint **on the same table**,
in both directions:

- `COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED` — "Creating `CHECK` constraint on
  table `<tableName>` with column mask policies is not supported."
- `COLUMN_MASKS_FEATURE_NOT_SUPPORTED.CHECK_CONSTRAINT` — "Setting column mask
  policies for tables with `CHECK` constraints".

Both are **table**-scoped, so it does not help that the masks are on the name
columns and the CHECK would be on `cnpj_basico`.

**Probed against the live workspace rather than settled on the reference**, with a
throwaway table and function, both dropped afterwards:

| statement | result |
|---|---|
| `CREATE TABLE _probe_mask_check (a STRING, nome STRING)` | SUCCEEDED |
| `CREATE FUNCTION _probe_mask(…)` | SUCCEEDED |
| `ALTER TABLE … ALTER COLUMN nome SET MASK _probe_mask` | SUCCEEDED |
| `ALTER TABLE … ALTER COLUMN a SET NOT NULL` | **SUCCEEDED — not blocked** |
| `ALTER TABLE … ADD CONSTRAINT a_len8 CHECK (…)` | **FAILED**, `COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED`, SQLSTATE 0A000 |
| `ALTER TABLE … ALTER COLUMN nome SET MASK _probe_mask` (again) | SUCCEEDED — re-apply is fine |

That measurement is what makes the cost precise instead of a guess. **Only the
`CHECK` is refused; `SET NOT NULL` is not.** So socios loses exactly the
`cnpj_basico_len8` pair — the `ADD CONSTRAINT`, and the `DROP CONSTRAINT IF EXISTS`
that existed only to make the `ADD` re-runnable — and keeps both of its `NOT NULL`
statements. `empresas`, `estabelecimentos` and `lookup` are unmasked and keep
theirs.

**Something was given up, and it is worth saying so plainly.** Bronze no longer
re-asserts, declaratively and in Delta, that every socios `cnpj_basico` is eight
characters. What makes that an acceptable trade rather than a hole is *where the
same rule already runs*: `bad_cnpj_basico_length` (`opl.bronze.rules`) is evaluated
by the DQ gate **before** the promote, and `promote_batch` appends only rows that
passed it. No row that would violate the CHECK can reach bronze to be checked by
it. The Delta constraint was defence in depth — a second assertion of a rule the
gate already enforces — and defence in depth is exactly the kind of thing it is
legitimate to spend, once, for a control that protects personal data. It is not
free: the CHECK would also have caught a row inserted by some future path that
bypassed the gate entirely, and after this change nothing would.

**Why the DDL had to move, not just be tolerated.** `promote_batch._assert_
constraints` re-issues every one of `spec.constraints` after **every** append. Left
in place, the first socios promote would have appended its rows and then failed on
that statement — and the repair run, which correctly skips the already-committed
append, would have failed again on the same one. An unrepairable task, on the one
table holding personal names.

**Made unreachable rather than merely fixed.** The obvious future edit is someone
making the registry entries uniform again and pasting the pair back.
`registry._assert_no_masked_contract_declares_a_check_constraint` refuses that **at
import**, so it breaks every module that reads the registry rather than one job run
— which is the right severity, because a CI test protects a merge and not the
ad-hoc run of a branch. The guard lives in the registry and not in
`opl.bronze.masking` deliberately: **`promote_batch` — the task that actually
issues the statement — imports the registry and does not import `masking`**, so a
guard living in `masking` would never run in the task whose failure it exists to
prevent. The guard matches on statements that *create* a check and not on
`DROP CONSTRAINT`, because dropping a CHECK is legal on a masked table and is the
first step of masking a table that already carries one.

**The import direction is the claim, not the importer count.** `registry.py` itself
imports `masking`, for `MASKED_COLUMNS`, and has since `f02ff97` — the same commit
that added the guard. That is not in tension with the paragraph above, which turns
on `promote_batch` importing the registry and *not* `masking`. The reverse
direction is load-bearing too, and also holds: `masking` imports
`opl.contracts.cnpj_schemas` and nothing else — no pyspark, no registry — which is
what lets `registry.py` import it while staying importable on the extraction
machines, where pyspark is an optional extra usually not installed.

## What the live run proved, and what it only confirmed

Everything above was, until 2026-08-01, an argument plus a throwaway-table probe.
The F1.4b PR A run applied this to the real `bronze_cnpj_socios`. Full numbers in
[`docs/f1.4b-pr-a-run-evidence.md` §7](../f1.4b-pr-a-run-evidence.md#7-the-mask-verified-by-behaviour--not-by-ddl);
what belongs here is which of this ADR's claims stopped being claims.

**The ordering — the whole reason `socios` is built by hand.** While the batch sat
gated between the ingest and the human decision to repromote,
`workspace.default.bronze_cnpj_socios` existed **with 0 rows**, with both masks
already attached. The table was created and the control applied before a single
personal name was written to it. That is the observation that makes "the control
was applied when the data landed" a description rather than an intention, and it
is only available because the workspace had no socios table beforehand — the
pre-run state was checked, for exactly this reason, before anything was uploaded.

**Behaviour, not attachment.** `system.information_schema.column_masks` confirms
both columns bound to `workspace.default.mask_personal_name`, which is what the
DDL having run would predict. The check that does not follow from the DDL is
reading the table: `GROUP BY nome_socio_razao_social` over the **whole** table
returns exactly one distinct value, `***`, at the full promoted row count. Not a
`LIMIT 5` — every row. Unmasked columns on the same rows return real data, so the
query is not simply broken. Both name columns are masked, which is this ADR's
deliberate widening of the spec, now applied in the live catalog.

**Fail-closed, in practice.** `opl_pii_readers` does not exist in this workspace,
and the value returned to the table owner's own query is the masked one. The
direction argued above is the direction observed.

**And the cost, paid without incident.** `promote_batch._assert_constraints` ran
after the real append **without failing** — socios' two `SET NOT NULL` statements
against a masked, populated table. The probe in the previous section established
that on a throwaway table; this is the same result at 27.8M rows on the table
that matters.

**And one place where that ordering was *not* achieved.** Bronze got its mask
before its first row; quarantine did not. `bronze_cnpj_socios_quarantine` was
created by the gate's own write and held **1,797** rows for the width of a review
cycle before `ensure_masked_table` was extended to cover it. For quarantine, "I
went back and masked it afterwards" is the true sentence — which is worth stating
in the ADR whose entire argument is that the two sentences differ. What the late
application did establish is the statement no probe had covered: first-time
`SET MASK` against a populated table succeeds and does not truncate it. Every
future masked contract gets the bronze ordering, because `ensure_masked_table`
issues the quarantine DDL ahead of the gate; socios' quarantine is the one that
missed it.

What the run did **not** establish: nobody has ever seen this mask reveal a name,
because there is no `opl_pii_readers` group and no member of it to read through.
The permissive half of the control is untested by construction.

## The boundary: quarantine is masked too, staging cannot be

This section previously recorded "bronze is masked, and the two tables either side
of it are not" as an accepted limit, and argued that extending
`ensure_masked_table` to all three was "a small change" and "the same decision
again". Two independent reviews — CodeRabbit on PR #6 and this branch's own
whole-branch review — said the same thing about that: *"we wrote it down" is a
weaker answer than "we masked all three"*, and a governance reviewer would read a
stated limit as a solvable obstacle that was not solved. They were right about
half of it, and measuring the other half is what produced the split below.

**Quarantine is covered, and the mask is on it in the workspace.**
`ensure_masked_table` issues `CREATE TABLE IF NOT EXISTS` and both `SET MASK`s for
the quarantine table on every run, ahead of the gate's write, exactly as it does
for bronze — so any future masked contract gets a quarantine table that is masked
before it holds anything. This one did not: `bronze_cnpj_socios_quarantine` already
held **1,797** rows when the mask first reached it, on 2026-08-01. It applied
cleanly. `system.information_schema.column_masks` now returns four rows — two
tables, both name columns each — the 1,797 rows are still there, and
`count(DISTINCT)` over each name column returns exactly one value, `***`.

The CHECK incompatibility is not an obstacle here: only *bronze* receives
constraint DDL — `promote_batch._assert_constraints` re-issues `spec.constraints`
against the bronze table and nothing else — and no quarantine table in this repo
has ever carried a constraint. The quarantine schema is the bronze shape plus
`_dq_reject_reason`, declared in `opl.bronze.masking.create_quarantine_ddl` and
pinned against the gate's own `split(...)` output on a real local Delta table, the
same way bronze's is. This is also the table the argument most obviously applies
to: the quarantine is specifically a table a human is *expected* to open and read
during triage.

**Which is exactly where masking it costs something.** `mask_personal_name` returns
`'***'` unconditionally to any reader the function does not admit; it does not pass
NULL through. All 1,797 of these rows are in quarantine *because*
`nome_socio_razao_social` is null, and that null now reads as `'***'` —
indistinguishable from a name that was present and hidden. The verification itself
is the proof: `count(DISTINCT)` skips nulls, so a null that stayed null would have
returned **0** distinct values, and it returned **1**. The masked value is non-null.
**So the mask hides the very emptiness that caused the rejection.** Triage still
works, because `_dq_reject_reason` is not masked and names the rule — that is how
the 1,797 were counted — but a triager can no longer *see* the blank in the column
and must take the reason string's word for it. That is a real cost of masking a
triage table. It is worth paying, because the other columns of those rows are a
personal record and the alternative is leaving them in the clear on the one table a
human is told to open. It is still a cost, and this ADR would be overselling the
extension if it recorded only the benefit.

**Staging is not covered, and this is a refusal with a mechanism behind it rather
than the third of the job nobody got to.** A UC column mask is applied as each row
is fetched from the data source, to every principal the mask function does not
admit — which in this workspace is *everyone*, the table owner included, as the run
observed directly. Staging is not a leaf table:

- **`promote_batch` reads staging and writes what it read into bronze.**
  `opl.bronze.promote.promote_batch` builds its frame from
  `spark.read.table(staging_table)` and appends it with
  `saveAsTable(bronze_table)`. With `opl_pii_readers` absent, a masked staging makes
  the next promote read `***` and append `***` — into the system of record,
  permanently, for every row. Bronze's own mask would then be hiding a value that
  is no longer there.
- **It would silently disable the DQ rule that guards the same column.** The gate
  evaluates `null_or_empty_nome_socio_razao_social` against staging. `***` is
  neither null nor empty, so the rule would stop rejecting. The **1,797** rows it
  caught in the live run would have passed into bronze — a privacy control that
  turns off a data-quality control, and reports nothing while doing it.
- **Databricks additionally documents that tables with column masks do not support
  streaming workloads on dedicated compute**, and staging is written by
  `bronze_ingest`'s `writeStream(...).toTable(...)`.

So masking staging is not a smaller version of masking bronze. It is a change that
corrupts the data it is meant to protect. `masked_table_ddls` carries the argument
next to the DDL, and two tests refuse the "make the three uniform" edit by name —
one over the DDL, one over the SQL the task actually issues.

**What would make it correct**, stated so this is a precondition and not a
permanent no: the mask on staging becomes both safe and effective the moment
`opl_pii_readers` exists **and** the job's run-as principal is a member of it. Then
the pipeline reads real names and everyone else reads `***`. Creating that group and
its grants is F4's work, and it is the same prerequisite the fail-closed argument
above is waiting on. Until then, what protects staging is that nobody but its owner
can read it — which this ADR asserted for a round without checking, and which is
now measured in
[Staging retention](#staging-retention-the-rows-stay-and-what-guards-them).

**What the extension does and does not undo.** Masking quarantine protects every
future read of those rows. It does not change the fact that the **1,797** rows
landed unmasked on 2026-08-01 and were readable in the clear for the hours between
the gate's write and the `SET MASK`, nor that **27,838,448** rows sit in staging in
the clear now. A read-time control is not retroactive; it changes what happens
next.

**And one claim in the old text was simply false.** It said staging "is transient
(the promote drains it)". Nothing drains staging — `promote_batch` appends to
bronze and deletes nothing, `opl.bronze.retention` reclaims *landed files* and
documents that it deliberately does not touch staging, and the 27,838,448 rows are
still there after a successful promote. The old text also reasoned from a 4-row
estabelecimentos quarantine baseline (ADR 0006); socios produced 1,797, three orders
of magnitude more, so "the handful a rule rejected" was never the right picture
either. Those 1,797 have a null `nome_socio_razao_social` — that is why they were
rejected — but they carry the rest of a personal record, and their
`nome_do_representante` was not examined either way. The triage that actually
happened selected neither name column, which is a discipline and not a control.

## Staging retention: the rows stay, and what guards them

`bronze_cnpj_socios_staging` holds **27,838,448** rows with the partner names in
the clear, and it will keep holding them. That is a decision, recorded here rather
than left as a silence for a reader to discover.

> **⚠️ UPDATE, 2026-08-03 (F1.4b PR B): the exposure has roughly doubled, exactly
> as this section predicted it would.** F1.4b PR B ingested 2026-07, appending a
> **27,992,378**-row socios batch to the same staging table, and nothing drains
> it — which is this section's own thesis, now observed over a second month
> rather than argued. **Every `27,838,448` below is the one-month figure, and the
> table reports two MEASURED months — it is not a forecast.** The two batches
> already differ by 153,930 rows (27,838,448 → 27,992,378, +0.55%), so
> `27,992,378` is the size of one month and not a constant: **every future month
> needs a fresh count before it is added to a total.** If a number is needed for
> retention planning before that count exists, use an explicit upper bound of
> **28,500,000 rows/month** — that is a bound chosen to sit above the observed
> trend with room, *not* a measurement, and any figure derived from it must say
> so. Derived totals as of
> 2026-08-03:
>
> | | 2026-06 | 2026-07 | total |
> |---|---|---|---|
> | `bronze_cnpj_socios_staging` (names in the clear) | 27,838,448 | 27,992,378 | **55,830,826** |
> | `bronze_cnpj_socios` (masked) | 27,836,651 | 27,990,592 | **55,827,243** |
> | `bronze_cnpj_socios_quarantine` (masked) | 1,797 | 1,786 | **3,583** |
>
> **The three totals are arithmetic, not printed by any run.** The per-month
> figures are measured (`docs/f1.4b-pr-b-run-evidence.md` §18, §21.2, §23.1); the
> sums are this correction's own addition and should be re-queried before anyone
> relies on them. They reconcile — 55,827,243 + 3,583 = 55,830,826 — which is a
> consistency check, not an independent measurement.
>
> One property does **not** carry over with the row count. The "every row, not a
> `LIMIT 5`" verification below was a whole-table `count(DISTINCT)` over
> 27,836,651 rows. PR B's post-ingest re-check was a `SELECT` returning `*** ***`,
> not a whole-table aggregate, so **the exhaustive read-through property is still
> established only over the 2026-06 half.**

**The decision.** Staging is not drained after a successful promote, and this ADR
introduces no policy that drains it. `promote_batch` appends to bronze and deletes
nothing; `opl.bronze.retention` reclaims *landed files* and documents that it
deliberately does not touch staging. The 27,838,448 rows were still there after the
promote succeeded, which is how the number above was obtained.

**The reason is the rebuild path.** The documented repair procedure — rebuild a
batch and repromote it — reads the staging table. A rule that emptied staging on a
successful promote would delete that procedure's input at exactly the moment the
batch looks finished, which is the moment before anyone discovers it was not. So a
retention rule for staging is a decision with its own failure mode rather than a
hygiene chore, and taking it was not in this phase's scope.

**What guards the rows, measured rather than asserted.** An earlier version of this
section said staging's protection is "the workspace's ordinary table ACLs", which
was a claim nobody had run a query against. The queries, so this one is re-runnable:

```sql
SHOW GRANTS ON CATALOG workspace;
SHOW GRANTS ON SCHEMA  workspace.default;

SELECT * FROM workspace.information_schema.table_privileges
WHERE table_name LIKE 'bronze_cnpj_socios%';
```

| query | result |
|---|---|
| grants on the catalog | `USE CATALOG` to `_workspace_users_*` |
| grants on the schema | `USE SCHEMA` and `CREATE *` to `_workspace_users_*` |
| `table_privileges` for `bronze_cnpj_socios%` | **no rows** |

`USE CATALOG` and `USE SCHEMA` are **traversal, not read**: they let a principal
name an object, not select from it. No `SELECT` is granted on staging, on bronze or
on quarantine, to any principal. Only the owner can read staging. The old claim
turns out to have been true — and true by accident, since it was written without
this evidence, which is precisely the species of claim this ADR was caught making.

> **⚠️ CORRECTION, 2026-08-03 (F1.4b PR B).** **"Only the owner can read staging"
> is false, and the way it fails is worse than a missed GRANT.** Databricks
> **Predictive Optimization**, running as the metastore-inherited service
> principal `6dfb9574-f409-433e-9e93-5acc4a190ffe`, read and rewrote
> `bronze_cnpj_estab_staging` with **no grant of any kind**: on 2026-07-30 it ran
> `DATA_SKIPPING_COLUMN_SELECTION` against that table (0.187 estimated DBU) and
> set `delta.workloadBasedColumns.deltaFileStatistics`, and it **rewrote that
> table's files twice**: `COMPACTION` on 2026-07-27T23:10:07Z (18 files → 8,
> 0.2749 DBU) and again on 2026-07-28T03:39:42Z (**42 files → 21**, 0.2057 DBU).
> Both rows are quoted in full at
> `docs/f1.4b-pr-b-run-evidence.md` §24.1.
>
> *Corrected 2026-08-03 (final review), in two ways.* This bullet previously said
> "compacted **that staging table** from 42 files into 21", naming one rewrite and
> citing §24 — which at the time recorded no `COMPACTION` row at all, so the claim
> had no evidence anywhere in this repository. It now does, and it turns out there
> were **two** rewrites rather than one. Both are on
> **`bronze_cnpj_estab_staging`**; the metastore's history holds exactly two
> `COMPACTION` operations in total, so **no rewrite of socios staging has been
> observed** and this ADR does not claim one.
>
> The `table_privileges` measurement above is still **correct** — it returned no
> rows, and still would. What fails is the inference: **a platform service's
> access does not flow through `table_privileges`, so measuring grants does not
> bound who reads a table.**
>
> **Why a different table still falsifies the claim, stated rather than assumed.**
> The sentence being falsified is "Only the owner can read staging", and it was
> derived from a *method* — enumerate `table_privileges`, find no `SELECT`,
> conclude nobody else reads. That method is metastore-wide, and one principal
> reading one table in this schema with no grant of any kind refutes it wherever
> that read happened. The refutation is of the reasoning, not of a per-table fact,
> so it does not need the instance to be socios.
>
> **What it does not establish, equally plainly.** It is *not* evidence that
> Predictive Optimization has read `bronze_cnpj_socios_staging`. The observed
> trigger is a workload of `_batch_id`-filtered scans against a wide table —
> estabelecimentos is 30 columns and the only registered table over the
> `delta.dataSkippingNumIndexedCols` default of 32 once audit columns are counted;
> empresas and socios are narrower and carry no stats property. So the honest
> statement is a demonstrated *class* exposure: a staging table of the same layer,
> in the same schema, under the same owner, read and rewritten by a principal no
> grant mentions. Whether socios staging has been read is **unmeasured**, and
> "unmeasured" is the word — the query that would settle it is the same
> `system.storage.predictive_optimization_operations_history` filter, and it
> returns nothing for that table today.

**Where this reasoning is weak, said plainly rather than dressed up.**

- **The guard is an absence, not a control.** What protects staging is that no
  `GRANT SELECT` was ever issued. One `GRANT` reverses it, nothing in this
  repository would notice, and no test asserts the empty result above. It is a
  measurement of a moment, not an invariant. **And it is narrower than that:**
  the second reader that actually arrived needed no `GRANT` — see the correction
  above.
- **One workspace, one user.** A Free Edition workspace with a single account is
  the easiest possible case for this argument. The same queries against a shared
  workspace would very likely return rows, and the conclusion would have to be
  re-derived rather than cited. **This premise is also false as stated:** there
  are demonstrably at least two principals touching these tables, and the second
  is not an account anyone created.
- **The rebuild reason justifies keeping *a* batch, not every batch.** It covers
  retaining the most recent promoted batch. Staging accumulates monthly and nothing
  bounds it, so the reason given is narrower than the policy adopted. Bounded
  retention — keep the last batch, drop the rest — would serve the rebuild path at
  a fraction of the exposure. It is not designed here because the phase's scope was
  one mask; that is a reason it is absent, not an argument that it would be wrong.
- **The exposure is present and not retroactively fixable.** Those 27,838,448 rows
  are in the clear now. Masking staging is refused for the mechanism in
  [The boundary](#the-boundary-quarantine-is-masked-too-staging-cannot-be), so the
  only two real remedies are the `opl_pii_readers` precondition and a bounded
  retention rule. Both belong to F4 and neither is scheduled.

## What this does not settle

- **The `opl_pii_readers` group does not exist in this workspace.** Nothing in
  this change creates it. That is why the fail-closed direction matters, and
  creating the group plus the grants that go with it is F4's work.
- ~~**The mask has not been exercised against real socios data.**~~ **Discharged
  by the F1.4b PR A run** — see [What the live run proved](#what-the-live-run-proved-and-what-it-only-confirmed).
  The mask is attached to both columns of the live `bronze_cnpj_socios`, it was
  attached while the table held 0 rows, and reading through it over all 27,836,651
  rows returns one distinct value. Everything else in this ADR remains pinned as a
  string by unit tests, by one local Delta round-trip, and by an import-time guard.
- **The revealing half of the mask is untested, by construction.** Nobody has seen
  this function return a name, because there is no `opl_pii_readers` group and no
  member of it. What is verified is that it hides; that it correctly *shows* to an
  authorised reader is F4's to demonstrate, along with creating the group.
- ~~**The quarantine mask has not run against the workspace.**~~ **Applied on
  2026-08-01.** It ran against `bronze_cnpj_socios_quarantine` while the table
  already held its 1,797 rows — first-time `SET MASK` on a populated table, the
  statement no probe had covered. It succeeded, the rows are intact, and both name
  columns return one distinct value. The gap is discharged; what the application
  introduced instead is a triage cost, recorded in
  [The boundary](#the-boundary-quarantine-is-masked-too-staging-cannot-be).
- **Staging holds 27,838,448 socios rows with the names in the clear, and by
  decision will keep holding them.** The decision, the rebuild path it turns on,
  the owner-only read that guards it and four places that reasoning is weak are in
  [Staging retention](#staging-retention-the-rows-stay-and-what-guards-them).
  Masking staging remains refused for the mechanism in
  [The boundary](#the-boundary-quarantine-is-masked-too-staging-cannot-be).
