# ADR 0008 — mask the partner NAMES in `socios`, before the first row lands

## Status
Accepted, with one unresolved incompatibility recorded below (the CHECK
constraint) that must be settled before the socios job runs.

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
- **The staging and quarantine tables are NOT masked.** See below.

## Unresolved: the mask and the CHECK constraint cannot coexist

Unity Catalog refuses a column mask and a `CHECK` constraint **on the same
table**, in both orders:

- `COLUMN_MASKS_CHECK_CONSTRAINT_UNSUPPORTED` — "Creating `CHECK` constraint on
  table `<tableName>` with column mask policies is not supported."
- `COLUMN_MASKS_FEATURE_NOT_SUPPORTED.CHECK_CONSTRAINT` — "Setting column mask
  policies for tables with `CHECK` constraints".

Both are **table**-scoped, so it does not help that the mask is on the name
columns and the CHECK is on `cnpj_basico`. The socios registry entry declares

```
ALTER TABLE {table} ADD CONSTRAINT cnpj_basico_len8 CHECK (length(trim(cnpj_basico)) = 8)
```

and `promote_batch._assert_constraints` re-issues every one of `spec.constraints`
after **every** append. So as things stand the first socios promote appends its
rows and then fails on that DDL — and the repair run, which correctly skips the
already-committed append, fails on it again, which makes it unrepairable.

This is recorded rather than resolved because the resolution is a trade-off that
belongs to the phase owner, not to the task that found it. The options:

1. **Drop the CHECK for masked contracts.** The rule it re-asserts,
   `bad_cnpj_basico_length`, already runs in the DQ gate *before* the promote, so
   no row reaching bronze can violate it — the Delta constraint is a second line
   of defence, not the only one. `SET NOT NULL` is a nullability property, not a
   CHECK, and stays either way. Cheapest, and loses the least.
2. **Move the mask to a view** over an unmasked bronze table. Keeps both controls,
   but the base table then holds names in the clear with nothing on it — which is
   the ordering this ADR exists to avoid.
3. **Drop the mask.** Not considered further.

Option 1 is the recommendation. The collision is asserted as currently-live by
`test_the_socios_check_constraint_still_collides_with_its_mask`, so whoever
resolves it is sent back to this section.

## What this does not settle

- **`bronze_cnpj_socios_staging` and `bronze_cnpj_socios_quarantine` hold the
  names unmasked.** Both are created by their own writers (the ingest's `toTable`
  and the gate's `saveAsTable`), neither goes through `ensure_masked_table`, and
  the quarantine in particular is a table a human is *expected* to open and read
  during triage. Staging holds every ingested row until the promote; quarantine
  holds the rejects indefinitely. So the honest statement of the control's reach
  is: **bronze is masked, and the two tables either side of it are not.** That
  weakens the control but does not make it decorative — bronze is the table
  anything downstream reads, and it is the one that will hold ~26 M rows for as
  long as the lakehouse exists, while staging is transient and quarantine holds
  the handful of rows a rule rejected. Extending `ensure_masked_table` to all
  three is a small change and belongs in the same phase as resolving the CHECK
  collision above, since the same incompatibility applies to staging.
- **`SET MASK` on an already-masked column is the one statement whose second run
  is unverified.** The Databricks reference documents neither that it replaces the
  existing mask nor that it errors. The safe alternative — `DROP MASK` then
  `SET MASK`, which the "if any" in `DROP MASK`'s definition makes provably
  idempotent — was rejected deliberately: it takes the mask **off** a populated
  table for the width of two statements on every monthly re-run, and a privacy
  control that is briefly absent every month is worse than one whose
  re-application may fail loudly with the mask already correctly in place. If the
  live run shows `SET MASK` erroring on re-application, the fix is a pre-check
  against `information_schema.column_masks`, not a `DROP`.
- **The `opl_pii_readers` group does not exist in this workspace.** Nothing in
  this change creates it. That is why the fail-closed direction matters, and
  creating the group plus the grants that go with it is F4's work.
- **Nothing here has been executed against Databricks.** Every statement in this
  ADR is pinned as a string by unit tests and by one local Delta round-trip; the
  live application is a later task in this phase, and it is the validation.
