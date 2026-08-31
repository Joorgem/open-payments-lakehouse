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

> **⚠️ AMENDED, 2026-08-18 (F4 Task 5). This ADR is amended, not superseded.** Its
> fail-closed argument and its 55,827,243-row measurement both stand. What changed is
> that **the control as shipped could not be opened by anyone, ever**, and that is a
> defect this ADR asserted the opposite of.
>
> `is_account_group_member` reads **account** groups. Measured on this workspace: it
> returns **false** for a workspace-local group the reader demonstrably belongs to
> while `is_member` returns **true**; exactly one account group resolves at all
> (`account users`, i.e. everyone, which cannot function as a control); and
> `databricks account groups list` returns `Not Found`, because a workspace host does
> not serve account SCIM. So
> [What would make it correct](#the-boundary-quarantine-is-masked-too-staging-cannot-be)'s
> *"the moment `opl_pii_readers` exists"* named **a moment that could not arrive from
> this workspace**. The mask hid, always, from every reader including the owner.
>
> **The repair, and it is a one-word substitution with the floor intact.** The
> predicate in this repository is now `is_member('opl_pii_readers')`, over the
> **workspace-local** group, which can be created and now **has been** — empty,
> deliberately. `is_member` was measured inside a serverless job session, as the user
> *and* as a `run_as` service principal, to return **true** for a group the principal is
> in and **false for a group that does not exist**. So the
> [Fail-closed](#fail-closed-and-why-that-is-the-right-direction) section below survives
> the substitution verbatim; only the function name changes.
>
> ~~**SHIPPED IS NOT DEPLOYED, and the two halves of that sentence are in different
> states.**~~ **BOTH HALVES ARE NOW DONE — this paragraph described 2026-08-18 and was
> overtaken on 2026-08-19.** `opl_pii_readers` exists, empty; and the predicate went live
> on run `761461564584636`, after which `workspace.information_schema.routines` returns
> `CASE WHEN is_member('opl_pii_readers') THEN name ELSE '***' END` with `last_altered`
> **`2026-08-19T16:25:23.068Z`**, against `2026-08-03T21:31:27.142Z` before it.
>
> ~~measured 2026-08-18, `workspace.information_schema.routines` still returns
> `CASE WHEN is_account_group_member(...)`, `last_altered 2026-08-03T21:31:27Z`. The
> repaired predicate is in the repository and goes live the next time
> `ensure_masked_table` runs; nothing in this branch deployed it.~~ **There is no exposure in the meantime** and that is why the
> deploy was not rushed: both predicates return **false** for every principal that can
> reach these tables (the group is empty, and the account-group spelling resolves no
> workspace-local group at all), so the live mask hides exactly as the repaired one
> will. What is wrong until the deploy is the REASON it hides, not the result.
>
> **AND "THE NEXT TIME `ensure_masked_table` RUNS" NAMED A RUN THAT COULD NOT BE
> AFFORDED, which is a second version of the same defect and was found the same way —
> by trying to run it.** That task sat in exactly one job, `bronze_socios_job.yml`,
> where it is the first task of the socios INGESTION flow: its `unzip` re-extracts
> `cnpj/2026-06/zips/socios` into the landing directory, **re-landing the
> 2,852,557,826 B F4 Task 2 had just reclaimed** in the phase whose headline artefact is
> that 8,212,278,423 B were freed — and, because the Auto Loader checkpoint has already
> consumed those files, the ingest that follows stages nothing and the gate and promote
> run over an empty batch. **F4 Task 5b gives the repair a path that is not an ingest**:
> the same task, unchanged, now also runs in `dataops_views_job.yml` behind that job's
> existing revision guard and ahead of `apply_pii_governance`. Over the populated socios
> tables its `CREATE TABLE IF NOT EXISTS` statements are inert, the
> `CREATE OR REPLACE FUNCTION` is the repair — all four masks already reference
> `workspace.default.mask_personal_name`, so replacing the body repairs every one of
> them — and the four `SET MASK`s re-apply masks that are already attached. The
> fail-closed result does not change: measured on the opl-free warehouse 2026-08-18,
> `is_member('opl_pii_readers')` returns **false** for the owner, so the read stays
> `***` across the repair.
>
> **Read [What RBAC means here](#what-rbac-means-here-and-why-it-is-not-one-grant)
> before reading the grants**, because the shape is not what the phrase usually means
> and the per-principal grants will otherwise read as an unfinished job.
>
> Three things this amendment adds, each measured:
> [the two-part access model and why it cannot be one object](#what-rbac-means-here-and-why-it-is-not-one-grant);
> [group membership lags in BOTH directions while GRANT does not](#the-lag-group-membership-is-not-a-switch);
> and [what the permissive branch has and has not been shown to do](#the-permissive-branch-proven-on-a-throwaway-and-unexercised-here-by-choice).

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
   RETURN CASE WHEN is_member('opl_pii_readers') THEN name ELSE '***' END
   ```

   **`is_member`, not `is_account_group_member`, since F4 Task 5** — see the amendment
   note at the top. The earlier spelling could not be made to return true in this
   workspace for any group creatable from it.

   ~~**This is the DDL this repository holds, and it is not yet the DDL the workspace
   holds.** `information_schema.routines` still shows the `is_account_group_member`
   body, `last_altered 2026-08-03` (measured 2026-08-18).~~ **IT IS NOW BOTH.** Deployed
   on run `761461564584636`, 2026-08-19: `routine_definition` carries `is_member(` and
   `last_altered` reads `2026-08-19T16:25:23.068Z`. The sentence above described the day
   before. It changed when `ensure_masked_table` ran — which since F4 Task 5b is a run of
   `dataops_views_job.yml`, not of the socios ingestion flow (see the amendment) —
   and until then both spellings hide from everyone, so the difference is in the
   argument and not in what any reader sees.

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
   so the same task can sit in any job's YAML without a per-table branch. The same
   task also runs in the governance job, where there is no ingest for it to precede
   and its job is to re-issue the function and the masks (F4 Task 5b); every
   statement it issues is idempotent, which is what lets one task serve both.

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
> box.** `src/opl/bronze/masking.py`'s (line 173 before F4 Task 5 grew that file; the sentence now survives only inside `mask_function_ddl`'s docstring, quoted there to be refuted) *"it becomes correct the moment `opl_pii_readers`
> exists"* names a moment that cannot arrive here.
>
> **THE FAIL-CLOSED ARGUMENT BELOW IS UNAFFECTED AND IS WHY THIS IS A CORRECTION AND NOT A
> RETRACTION** — the mask hides, always, and has hidden 55,827,243 rows. What is wrong is the
> claim that the control is merely dormant. **The repair is F4's decision** — switch the
> predicate to `is_member`, which was measured working, or state that the control is
> permanently closed here — and it is deliberately not taken in this note, because measuring a
> fact and choosing a fix are different acts and this project separates them.
>
> **A CLAIM PUBLISHED HERE ON 2026-08-18 WAS TOO STRONG AND IS WITHDRAWN THE SAME DAY, BEFORE
> IT MERGED.** It read: *"the revealing half is now measured anyway … two principals read the
> same column of the same table at the same moment — one got cleartext, the other `***` … the
> sentence below about the permissive branch being untested by construction was true when
> written and is no longer."*
>
> **What was actually measured is that A UC column mask can reveal — not that THIS one can.**
> The experiment ran against a **scratch** mask whose predicate was `is_member(<a scratch
> group>)`. **The mask installed on `bronze_cnpj_socios` today is still this ADR's**, with
> `is_account_group_member('opl_pii_readers')` — `information_schema.routines` returns it
> verbatim, and the owner reads `***` through it right now.
>
> **So the withdrawn sentence contradicted the paragraph immediately above it.** That paragraph
> proves this predicate *cannot* return true here; the sentence claimed its branch had been
> exercised. Both cannot hold, and the one that holds is the paragraph.
>
> **The permissive branch of THIS mask remains untested by construction**, exactly as the
> section below says. What is new and survives: a service principal with an OAuth secret is
> available on Free Edition, so a two-principal proof **is buildable** once the predicate is
> repaired — which is F4's T4, and which must produce a **committed, re-runnable artefact**
> rather than a session transcript. **The withdrawn claim had none**: no statement id, no
> timestamp, no SQL, no surviving object. Caught by the F4 plan's provenance audit before this
> ADR merged.

> **AND F4 TOOK THE REPAIR THIS NOTE DEFERRED.** The predicate is `is_member` on and after
> run `761461564584636` (2026-08-19): `information_schema.routines` returns
> `CASE WHEN is_member('opl_pii_readers') THEN name ELSE '***' END` with `last_altered`
> `2026-08-19T16:25:23.068Z`, against `2026-08-03T21:31:27.142Z` before it. **The floor did not
> move** — `opl_pii_readers` has zero members, so every reader still sees `***`. See
> *The permissive branch* below for what is and is not exercised, and note that the sentence
> immediately following now describes `is_member`, which is the predicate that ships.

`is_member` returns **false** for a group that does not exist, and **false** for a group
that exists and holds nobody. ~~So in a workspace where `opl_pii_readers` was never created
— which is the current state of this one —~~ **F4 created `opl_pii_readers` on 2026-08-18 and
left it EMPTY, so the current state is the second case rather than the first**: *every* reader
sees `***`, including the table owner's own queries. Measured the same day —
`is_member('opl_pii_readers')` → `false` for the owner, and the masked column reads `***`.
The control degrades toward hiding, not toward revealing. The alternative
formulations all fail open: a mask keyed on an allow-list table that has not been
created yet, or one that checks `current_user()` against a list, reveal the name
when their dependency is missing. Creating the group is what grants access; doing
nothing denies it.

## What RBAC means here, and why it is not one GRANT

**Say this before showing the grants, because the shape is not what the phrase usually
means.** A reviewer looking for `GRANT SELECT ON TABLE ... TO opl_pii_readers` is
looking for a statement this platform refuses. Measured, 2026-08-18:

| statement | result |
|---|---|
| `GRANT SELECT ON TABLE ... TO opl_pii_readers` | **FAILED** — `PRINCIPAL_DOES_NOT_EXIST`, "Could not find principal with name opl_pii_readers" |
| the same, to a second workspace-local group | **FAILED**, identically |
| the same, to `account users` | SUCCEEDED — and `account users` is **everyone** |
| the same, to a service principal's `applicationId` | SUCCEEDED |
| the same group used in a **mask predicate**, `is_member(...)` | works — see below |

So a workspace-local group **works in a mask predicate and is refused as a grant
principal**, and the only account group that resolves is one that cannot function as a
control. **There is no principal in this workspace that can be both halves.** The
shipped model is therefore necessarily two objects:

1. the workspace-local group `opl_pii_readers` in the mask predicate, which decides
   what a reader **sees**; and
2. `SELECT` granted **per principal** — a service principal's `applicationId`, a user's
   email — which decides whether a reader can open the table **at all**.

`opl.bronze.pii_governance` holds the roster and the SQL;
`databricks/src/apply_pii_governance.py` issues it, in the `dataops_views` job behind
the same deployed-revision guard as everything else there.

**Both halves must hold, and either drift fails closed.** In the group without
`SELECT` → cannot open the table. With `SELECT` and not in the group → reads `***`.
That is defence in depth by accident of the platform rather than by design, and it is
worth keeping for the accident.

**The roster is empty today, by decision.** No principal holds `SELECT` on any socios
table — `information_schema.table_privileges` returns **0 rows** for
`bronze_cnpj_socios%`, re-checked 2026-08-18 — and `opl_pii_readers` was created with
**no members**. See
[the permissive branch](#the-permissive-branch-proven-on-a-throwaway-and-unexercised-here-by-choice).

**And this is what closes the weakest paragraph this ADR ever wrote.**
[Staging retention](#staging-retention-the-rows-stay-and-what-guards-them) says the
guard on these tables *"is an absence, not a control. One `GRANT` reverses it, nothing
in this repository would notice, and no test asserts the empty result."* The grants
task computes its plan from `SHOW GRANTS`, so a grant issued out of band is **seen** on
the next run instead of passing unremarked. That is a control rather than an absence.

**What it does with what it sees is asymmetric, and the earlier wording here — "revokes
every principal the reviewed roster does not name" — was false.** It was a control over
principals holding a direct, table-level, literally-spelled `SELECT`. Measured
2026-08-18 on a throwaway table (`workspace._probe_f4t5c.t`, created, granted, revoked,
dropped, and its absence verified in `information_schema`):

| state | `SHOW GRANTS ON TABLE` reports | `REVOKE SELECT ON TABLE` against it |
|---|---|---|
| `GRANT SELECT ON TABLE t` | `p \| SELECT \| TABLE \| …t` | **SUCCEEDS and the row is gone** |
| `GRANT ALL PRIVILEGES ON TABLE t` | `p \| ALL PRIVILEGES \| TABLE \| …t` | **SUCCEEDS and the row is still there** |
| `GRANT SELECT ON SCHEMA s` | `p \| SELECT \| SCHEMA \| …s` | **SUCCEEDS and the row is still there** |

So *"just give them everything"* — the likeliest shape of an out-of-band grant — was
invisible to a lens matching the string `SELECT`, and the obvious remedy for it does
nothing while reporting success. `information_schema.table_privileges` agrees and
spells it differently again: `ALL_PRIVILEGES` as a row of its own, never expanded into
a `SELECT`, and the inherited grant as a row on the TABLE with `inherited_from = SCHEMA`.

**The repair is asymmetric on purpose: the LOOK is wide and the REVOKE stays narrow.**
`opl.bronze.pii_governance` now reads all four `SHOW GRANTS` columns and treats
`SELECT` **and** `ALL PRIVILEGES` as conferring a read; it revokes exactly the one shape
that a `REVOKE SELECT ON TABLE` was measured to remove; and everything else that
confers a read is **printed one line each and then raised** (`UngovernedRead`) after
every statement the run could safely issue — naming the statement that does close it
(`REVOKE ALL PRIVILEGES ON TABLE …` and `REVOKE SELECT ON SCHEMA …`, both measured to
work). A governance task cannot revoke what the platform will not let it revoke; what
it must not do is report SUCCESS over it, or print `REVOKED` after a statement that
removed nothing. **The current state is clean under the wide lens too**: re-measured
2026-08-18, `SHOW GRANTS ON TABLE` returns **zero rows** on all three of
`bronze_cnpj_socios`, `bronze_cnpj_socios_quarantine` and `bronze_cnpj_socios_staging`.

It does **not** close the other half of that paragraph: Predictive Optimization read a
sibling staging table with no grant of any kind, and no `REVOKE` reaches a platform
service.

**Nothing of this went into the bundle, and that is a measured refusal.** A Databricks
Asset Bundle has no `tables` resource, and `grants` is a field on Catalog, Schema,
Volume, RegisteredModel, ExternalLocation and VectorSearchIndex — never on a table.
Governing the **schema** instead fails three separate ways here: this repo's only
target is `mode: development`, which rewrites `name: default` to `dev_<prefix>_default`
and would deploy green while governing a new, empty schema (the prefix cannot be
disabled); a `production` target keeps the name and then **collides** with the existing
`default`, owned by `_workspace_admins_workspace_<id>`; and
`resources.<securable>.grants` is **authoritative**, so declaring it would revoke
`_workspace_users_workspace_<id>`'s `USE SCHEMA / CREATE TABLE / CREATE FUNCTION /
CREATE VOLUME / CREATE MODEL / CREATE MATERIALIZED VIEW` on the schema every pipeline
here writes into. `bundle deployment bind` would adopt the schema and put 55.8M rows of
personal data inside `bundle destroy`'s blast radius. **Rejected**; revisit only with
`lifecycle: {prevent_destroy: true}` as a precondition.

## The lag: group membership is not a switch

**This ADR must not claim the control closes when membership is removed, because it
does not.** Measured 2026-08-18 on the `opl-free` SQL warehouse, reading
`is_member('<group>')` as a service principal with a **fresh OAuth token on every
read** and a varied literal to defeat the result cache:

| change | last read showing the OLD value | first read showing the NEW value | reads |
|---|---|---|---|
| principal **removed** from the group | +211 s | **+234 s** (~3 m 54 s) | 11 |
| principal **added** to the group | +294 s | **+318 s** (~5 m 18 s) | 14 |

**Eleven and fourteen consecutive reads** — corrected from "twelve and fourteen", and
settled rather than dropped. Reconstructed from `system.query.history` on 2026-08-18:
the removal series is a contiguous run of nonces 100–110 (**11** statements,
23:00:36.8 → 23:04:27.9) and the addition series is nonces 200–213 (**14** statements,
23:04:32.8 → 23:09:48.5). The two published deltas land on the last two reads of each
series to within a second, and the removal series spans only 231 s, so it cannot be the
one holding a +318 s reading: the rows are not swapped, the count is. The stray
"twelve" belongs to a THIRD series this table never described — the 12 reads
(22:54:41 → 22:59:19) spent waiting for the addition that preceded the two-principal
proof below. Query history stores the statements and not their results, so it settles
how many reads there were and not what they returned; the deltas remain the
transcript's.

**Both directions lag**, and on this compute the *addition* lagged longer than the
removal. That corrects a reported measurement — taken in a serverless job session —
that addition propagates immediately and only removal lags; whichever compute a reader
is on, **neither direction may be assumed instant**.

**WHAT TAKES MINUTES IS EXPIRING A CACHED NEGATIVE, NOT PROPAGATING A MEMBERSHIP.**
That is the mechanism, and it reconciles this table with two independent readings that
otherwise contradict it. An independent trial on the same warehouse, by a different
reader, measured a **removal** at **+284 s old / +304 s new (~5 m 04 s)** — about 30%
longer than the +234 s above — and an **addition visible in 4 seconds** when the group
was *created with* the member, i.e. when no principal had ever resolved that group to
`false`. Every slow reading in this ADR was taken by a principal that had already
resolved the group to `false` — the +318 s addition follows the removal series
immediately above it, so its reader had read `false` seconds earlier. Consequently:

- **`+318 s` is not a general property of addition** and must not be quoted as one. Its
  precondition is part of the measurement: the reader had already read this group as
  `false`. Without that precondition the same direction was observed to close in
  seconds.
- **`+234 s` is a LOWER BOUND from a SINGLE trial, not a bound.** An independent trial
  of the same direction took **304 s**. An incident responder reading "~3 m 54 s" as
  "closed within four minutes" would be reading something no measurement supports.
- **The conservative wording is what survives**: neither direction may be assumed
  instant, and the unsafe direction is removal. That was already this section's
  conclusion and none of the above weakens it.

**The removal direction is the one that is unsafe**, and it is the reason this
paragraph exists: for the width of that window a principal that has just been removed
from `opl_pii_readers` **still reads names in the clear**. A control that closes
eventually is still a control. It is not a switch, and an incident response that
depends on it closing *now* is depending on something that was measured not to happen.

**`GRANT`/`REVOKE` are the half that does close now**, measured in the same session:
after `REVOKE SELECT`, the principal's **very next statement** failed
`INSUFFICIENT_PERMISSIONS ... does not have SELECT on Table`, SQLSTATE **42501** —
while it was **still a member of the group**. So:

- to close access **in a hurry**, revoke the grant, not the membership;
- and any two-principal proof must use `GRANT`/`REVOKE` and **never** a membership
  flip, because a flip re-read inside one session records a false result. The
  transcript below obeys that.

## The permissive branch: proven on a throwaway, and unexercised here by choice

**What was proven.** On 2026-08-18 a purpose-built throwaway table
`workspace.default._probe_pii_mask`, holding **invented** names, carried the same
function shape — `CASE WHEN is_member('<a throwaway group>') THEN name ELSE '***' END`
— on the same two column names. Two real principals read it, one statement each, with
no membership flip between them:

| statement id | principal | `is_member` | `nome_socio_razao_social` |
|---|---|---|---|
| `01f19b58-83ea-1a68-a9cf-fd9f7cc6896d` | a service principal, in the group, with `SELECT` | `true` | **`ANA INVENTADA DA SILVA`** |
| `01f19b58-85aa-163d-8a8a-c811af970c2a` | the workspace owner, not in the group | `false` | **`***`** |

So a UC column mask with **this predicate** discriminates between two principals, and
the permissive branch of an `is_member`-keyed mask returns the name. A **throwaway
group** was used rather than `opl_pii_readers` so that the real group is never non-empty
for even a moment.

**What those two statement ids evidence, stated exactly, because the table above reads
as though they evidenced the values.** Both ids verify in `system.query.history`
(re-checked 2026-08-18): two different `executed_by` principals — the probe service
principal and the workspace owner — **3 seconds apart**, and the statement text of each
names the **throwaway** group `_probe_pii_readers` and never `opl_pii_readers`. So the
ids are real evidence, and what they are evidence OF is **who ran what, against which
group**: two distinct principals, one probe table, the real group never used. They are
**not** evidence of the last column. `system.query.history` stores no result data — its
43 columns carry `produced_rows` and not one cell of any row — and the table, the
function, the group and the service principal were all destroyed afterwards. `ANA
INVENTADA DA SILVA` / `***` is therefore **a transcript record that nobody, including
its author, can re-derive from this workspace**. What can be rebuilt is the apparatus:
`scripts/rebuild_pii_reader_sp.py` exists so the next reviewer re-runs the proof rather
than believing it. Every object named above was destroyed afterwards and the destruction
verified: the table and the function are gone from `information_schema`, the probe group
is gone, and SCIM `ServicePrincipals` reports `totalResults: 0`.

**What was NOT proven, stated plainly because this branch has already withdrawn one
overclaim about exactly this.** Nobody has read a name through
`workspace.default.mask_personal_name`. The real socios mask has **not** been exercised
in its permissive direction, and that is **a choice, not a gap in the work**:
exercising it means granting a principal `SELECT` on a table holding **55,827,243 rows
of real personal names in the clear**, and — given the lag measured above — leaving it
able to read them for minutes after the membership is withdrawn. The platform audit
refused exactly this trade earlier in the phase and proved the mechanism on a throwaway
instead. This ADR takes the same decision and records it as a decision.

**What would exercise it**, so this is a precondition and not a permanent no: add one
principal to `PII_READERS` in `opl.bronze.pii_governance` **and** to the
`opl_pii_readers` group, run the governance job, and read one row.
`scripts/rebuild_pii_reader_sp.py` builds the principal from nothing — the previous
one was deleted, which is what made an earlier transcript unre-readable. The unit test
`test_the_roster_is_empty_and_that_is_a_decision` goes red when the roster half is
done, which is how that decision reaches a reviewer rather than passing in a diff.

## The tags: the account's vocabulary, and the call that establishes it

An earlier measurement pass reported *"70 governed tag policies already provisioned,
including `class.name`, `class.br_cpf`, `class.br_cnpj`"* and **named no endpoint**; an
independent audit could reach none of `/api/2.0/tag-policies`, `/api/2.1/tag-policies`
or `/api/2.0/account/tag-policies`. **The claim is true and the call is
`GET /api/2.1/tag-policies`** — recorded here so it is never again a number without a
source:

```
databricks api get "/api/2.1/tag-policies?page_size=200" --profile opl-free
  -> top-level keys: ['tag_policies']   <- the WHOLE body; there is no other key
     tag_policies: 70 entries
     class.name    "A person's name, (e.g., first, middle, last names, titles, ...)"
     class.br_cpf  "A Brazilian CPF ... individual taxpayer identification number."
     class.br_cnpj "A Brazilian CNPJ ... business/company tax identification number."
```

**`next_page_token: null` was published here and has been withdrawn**: re-measured
2026-08-18, the response body's only top-level key is `tag_policies`, so that field
does not exist and the `null` was a structural absence printed in the shape of a
reading — the same defect `.plans/sql.sh`'s own header retracts for `from_cache: None`.
**The completeness conclusion survives and its evidence is the count**: 70 entries
returned against `page_size=200`, so nothing was left on a second page. It never rested
on a token anybody was served.

`/api/2.0/tag-policies` answers that the private-preview API is deprecated and names
the 2.1 path; `/api/2.0/account/tag-policies` is `Not Found`; `SHOW TAG POLICIES` is a
parse error. The audit's reach was correct for the paths it tried.

**Their allowed-value list is empty, which does not mean unusable.** Measured:
`SET TAGS ('class.name' = 'personal_name')` is refused with
`INVALID_PARAMETER_VALUE ... "not an allowed value for tag policy key class.name.
Allowed values: []"`, and `SET TAGS ('class.name' = '')` **succeeds** and reads back
from `information_schema.column_tags`. So the empty string is the only assignable
value, and that is what the code emits.

**A bespoke namespace was not available even as an alternative.**
`SET TAGS ('opl.pii' = ...)` is refused: *"Tag key contains reserved characters (., =,
>, <, %, &, ?, \)"*. The governed keys are the only dotted keys that exist; the choice
was between the account's vocabulary and an undotted invention of this project's, and
using somebody else's standard classification is the better of those two.

**What is classified**, by `opl.bronze.pii_governance.CLASSIFIED_COLUMNS`, on all three
socios tables:

| column | tag | why |
|---|---|---|
| `nome_socio_razao_social` | `class.name` | the masked column |
| `nome_do_representante` | `class.name` | the masked column this ADR widened the spec to include |
| `cpf_cnpj_socio` | `class.br_cpf` **and** `class.br_cnpj` | `identificador_socio` decides which one a row holds, so one key alone would be false for the other kind of partner |

`cpf_cnpj_socio` is classified and deliberately **not** masked — which is this ADR's
[Context](#context) argument (the Receita masks it at source, six middle digits,
irreversible) made visible to someone reading the catalog instead of this file.
`cnpj_basico` is deliberately absent: it is a CNPJ and tagging it would be true, but it
identifies the **company**, and a classification that drifts into every identifier
anyone could name stops meaning anything.

**The tags reach staging, which the mask cannot.** So does the grant discipline. A
`SET TAGS` and a `GRANT` change who may open a table and change no value any reader
gets — unlike a mask, which `promote_batch` would read and append into bronze. The
table this ADR refuses to mask is therefore not ungoverned.

**And there is a better reason than "it costs nothing".** A `class.name` tag on an
**unmasked** staging column is the catalog *stating that the exposure exists*. That is
strictly more useful to a governance reviewer than the same tag on a column already
hidden behind a mask: on bronze it labels data nobody can read anyway, while on staging
it is the one machine-readable statement in this workspace that 27.8M personal names sit
in the clear in a table nothing drains. The tag is not consolation for the mask that
cannot go there — it is the record of what the mask's absence means.

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

**Fail-closed, in practice.** ~~`opl_pii_readers` does not exist in this workspace,~~
**no principal is a member of `opl_pii_readers`** (the group exists — created empty by F4
on 2026-08-18; struck here by F7 on 2026-08-31) and the value returned to the
table owner's own query is the masked one. The
direction argued above is the direction observed, and the conclusion does not move with the
premise: `is_member` is false for the owner either way.

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
because there was no `opl_pii_readers` group and no member of it to read through.
The permissive half of the control is untested by construction.

> **UPDATE, 2026-08-18 (F4 Task 5).** Two of that sentence's three clauses have moved
> and the conclusion has not. The group now **exists** — and the predicate that named
> it could not have read it either way, which is the amendment at the top of this file.
> The group is **empty by decision**, so there is still no member to read through, and
> the permissive half of **this** mask is still untested. What *is* now measured, on a
> throwaway carrying invented names, is that a mask with this predicate reveals to one
> principal and hides from another — see
> [the permissive branch](#the-permissive-branch-proven-on-a-throwaway-and-unexercised-here-by-choice).

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
  `saveAsTable(bronze_table)`. ~~With `opl_pii_readers` absent,~~ **With the promote's
  principal outside `opl_pii_readers`** (struck by F7, 2026-08-31: the group exists, and
  membership rather than existence is what this argument turns on), a masked staging makes
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
permanent no: the mask on staging becomes both safe and effective once the job's
run-as principal is a member of `opl_pii_readers`. The group now **exists** and is
**empty**, so the precondition is unmet and staging stays unmasked; and until F4 the
predicate named an account group this workspace could not create, so the condition was
not merely unmet but unreachable — see the amendment at the top. Then
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

- ~~**The `opl_pii_readers` group does not exist in this workspace.**~~ **Created
  2026-08-18 (F4 Task 5), as a WORKSPACE-LOCAL group, with no members** — and the
  predicate was repaired in the same change, because until then it named an ACCOUNT
  group this workspace cannot create, so the group's existence alone would have
  changed nothing. The grants that go with it are
  `opl.bronze.pii_governance` + `databricks/src/apply_pii_governance.py`; the roster
  is empty, which is
  [a decision](#the-permissive-branch-proven-on-a-throwaway-and-unexercised-here-by-choice)
  rather than an omission.
- ~~**The mask has not been exercised against real socios data.**~~ **Discharged
  by the F1.4b PR A run** — see [What the live run proved](#what-the-live-run-proved-and-what-it-only-confirmed).
  The mask is attached to both columns of the live `bronze_cnpj_socios`, it was
  attached while the table held 0 rows, and reading through it over all 27,836,651
  rows returns one distinct value. Everything else in this ADR remains pinned as a
  string by unit tests, by one local Delta round-trip, and by an import-time guard.
- **The revealing half of THIS mask is still untested, and now by choice rather than
  by construction.** Nobody has seen `workspace.default.mask_personal_name` return a
  name. A mask with the same predicate and the same shape was measured revealing to
  one principal and hiding from another on a throwaway carrying invented names
  ([transcript](#the-permissive-branch-proven-on-a-throwaway-and-unexercised-here-by-choice));
  doing it on the real table means a principal reading 55.8M real names in the clear,
  and that trade was refused.
- **Group membership does not close promptly, in either direction** — a removal took
  **at least** ~3 m 54 s in one trial and ~5 m 04 s in an independent one, and an
  addition took ~5 m 18 s *for a reader that had already read the group as `false`*
  (the same direction closed in ~4 s for a group created with its member). Neither
  figure is a bound; what lags is the expiry of a cached negative. A `REVOKE` closes on
  the next statement. See [the lag](#the-lag-group-membership-is-not-a-switch).
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
