# ADR 0008 — mask the partner NAMES in `socios`, before the first row lands

## Status
Accepted, and **live as of 2026-08-01** — the mask is applied to
`workspace.default.bronze_cnpj_socios` and verified by reading through it, not
only by the DDL having run
([What the live run proved](#what-the-live-run-proved-and-what-it-only-confirmed)).
One incompatibility with Unity Catalog was found while implementing this
(a masked table may not carry a `CHECK` constraint); it was probed against the live
workspace and resolved by dropping socios' `CHECK`, with the trade recorded below.

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
sentence, not the first. So `socios` is the one bronze table this repo builds by
hand: `ensure_masked_table` issues `CREATE TABLE IF NOT EXISTS`, then the mask
function, then `ALTER … SET MASK` for both columns, and only then does the job go
on to unzip and ingest. The append finds a table that is already masked.

The cost of that choice is that the socios bronze schema is now *declared* rather
than produced, so it can be wrong in a way no other bronze table can. It is
declared from the contract (`cnpj_schemas.TABLES`) plus a measured list of the
seven metadata columns, and two of those are **not** strings — `_ingested_at` is
`TIMESTAMP` and `_snapshot_ref_date` is `DATE`. An earlier draft declared all of
them `STRING`, which builds a table the first append cannot write to. That is
pinned two ways in `tests/bronze/test_masking.py`: as literals, and by appending
the DataFrame the real ingest code produces to a real local Delta table created
from this very DDL, with a committed mutation probe proving Delta refuses the
mistyped column rather than casting it.

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

3. **Applied before ingest**, by a job task (`ensure_masked_table`) that runs
   ahead of `unzip`, and that is a no-op for any table declaring no masked column
   so the same task can sit in any job's YAML without a per-table branch.

4. **socios carries no `CHECK` constraint**, and is the only registered table
   without one — see the next section. Refused at import for any masked contract by
   `registry._assert_no_masked_contract_declares_a_check_constraint`.

### Fail-closed, and why that is the right direction

`is_account_group_member` returns **false** for a group that does not exist. So in
a workspace where `opl_pii_readers` was never created — which is the current state
of this one — *every* reader sees `***`, including the table owner's own queries.
The control degrades toward hiding, not toward revealing. The alternative
formulations all fail open: a mask keyed on an allow-list table that has not been
created yet, or one that checks `current_user()` against a list, reveal the name
when their dependency is missing. Creating the group is what grants access; doing
nothing denies it.

## Consequences

- **Bronze rows are stored in the clear; the mask is a read-time control.** UC
  column masks rewrite the query, they do not transform the stored value. Anyone
  with direct access to the underlying storage path bypasses them. This is the
  right trade for bronze, whose whole job is to record what the source asserted
  verbatim, but it means the mask is a governance control, not encryption.
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
  F1.4b run went looking for it. A `DROP MASK`
  before the `SET` would also be idempotent and is deliberately *not* used: it
  takes the mask off a populated table for the width of two statements on every
  monthly re-run, and a control that is briefly absent every month is worse than
  one that is never absent at all.
- **socios keeps no `CHECK` constraint** and loses one layer of defence in depth.
  See below.
- **The staging and quarantine tables are NOT masked.** See below.

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
[`docs/f1.4b-pr-a-run-evidence.md` §7](../f1.4b-pr-a-run-evidence.md#7-the-mask-verified-by-behaviour-not-by-ddl);
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

What the run did **not** establish: nobody has ever seen this mask reveal a name,
because there is no `opl_pii_readers` group and no member of it to read through.
The permissive half of the control is untested by construction.

## The limit of this control: staging and quarantine are not masked

`bronze_cnpj_socios_staging` and `bronze_cnpj_socios_quarantine` hold the partner
names **in the clear**. Both are created by their own writers — the ingest's
`toTable` and the gate's `saveAsTable` — neither passes through
`ensure_masked_table`, and no mask is applied to either. The quarantine is
specifically a table a human is *expected* to open and read during triage.

This is stated as a property of the control, not buried as an exception. What is
true today is: **bronze is masked, and the two tables either side of it are not.**

It narrows the control without making it decorative. Bronze is what everything
downstream reads and the table that will hold the full socios population for as
long as this lakehouse exists; staging is transient (the promote drains it) and
quarantine holds the handful of rows a rule rejected — 4 in 42,780,919 for
estabelecimentos, per ADR 0006. But the gap is real and a reviewer would find it in
a minute, so it is written here rather than discovered there. Extending
`ensure_masked_table` to all three is a small change; note that the CHECK
incompatibility above applies to staging too, so it is the same decision again.

**The live run put numbers on both halves of this.** Staging took all **27,838,448**
ingested socios rows, names in the clear, and it is the frame the gate and the
promote both read. Quarantine took **1,797** rows — three orders of magnitude above
the 4-row estabelecimentos baseline this section reasoned from, so "the handful a
rule rejected" is no longer the right picture of it. Those particular 1,797 have a
null `nome_socio_razao_social`, since that is the reason they were rejected; what
they carry in the clear is the rest of a personal record, and their
`nome_do_representante` was not examined either way. The triage that actually
happened was performed without selecting either name column, which is a discipline
and not a control. The decision to leave both tables unmasked stands; it is now
taken with the sizes in view rather than with an estimate.

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
