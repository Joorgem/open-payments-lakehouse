# F-DB — Postgres snapshot-diff, and the measurements taken before anything was built

**This document holds Task 0's measurements and, as the phase runs, its predictions and what
the runs said.** It lives in `docs/` and not in `.plans/` deliberately: `.plans/` is
git-ignored and this repository is public, and F3 shipped a section pointing a public reader
at that directory, where they reached nothing.

**Controller-verified** means the controller ran the command and read the output.
**Reported** means a task's own stdout or a subagent's report said it. Every claim below
carries one of the two labels.

**Predictions are published BEFORE the run that tests them** (master protocol §4.5). A number
first written down after the run that produced it is not a prediction.

---

## 0. Task 0 — measured before a table existed

The plan's Task 0 asked for eight measurements. **Five of them are ones F0 should have taken
and did not:** `docs/f0-validation-report.md` carries five premises of its own — the egress claim, UC Volume upload, `bundle validate`, CNPJ snapshot
availability, PTAX reachability — and **not one is about a database** (`grep -i postgres` → 0
hits), while master spec §9 lists F0 as standing up "Docker Redpanda/**Postgres**/Spark".

### 0.1 THE PROVENANCE GUARD THIS PROJECT RELIES ON HAS AN EXPIRY DATE, AND NOBODY HAD MEASURED IT

**Controller-verified.** F-API §3.1 established a rule this project has leaned on since:

> *"Checking that a published id resolves costs one API call and is the cheapest guard this
> project has."*

It is a good rule and **it decays**. `GET /api/2.0/sql/statements/<id>` against the `opl-free`
workspace, four ids of three vintages, run 2026-08-15:

| statement id | published by | ran | result |
|---|---|---|---|
| `01f19061-4f47-1b47-ab3a-1880491dda04` | F2 wave 1 | 2026-08-09 | **`was not found`** |
| `01f1974c-5f64-1fdb-a8cc-377cdddeddc3` | F3 | 2026-08-13 | **`was not found`** |
| `01f19831-a0bf-17d9-a6ce-815a9b45ce74` | F-API (ADR 0016) | 2026-08-14 | `state: CLOSED` |
| `01f19844-283f-1218-b6e5-73a3b1a3f342` | F-API §3.1 | 2026-08-14 | `state: CLOSED` |

> **THE BOUND THIS SECTION FIRST PUBLISHED WAS TIGHTER THAN THE MEASUREMENT AND IS
> RETRACTED.** It read *"retention expiring somewhere between one and two days"* and
> *"the boundary is age, not the id"*. Neither is supported by four ids.
>
> **What the four actually bound.** The checks ran ≈05:42 UTC on 2026-08-15. The newest
> failing id was published by F3, which ran **2026-08-13**; the oldest resolving id was
> published by F-API, which ran **2026-08-14**. So the boundary lies somewhere between
> roughly **8 and 35 hours** — a band that includes 24 h but is not bounded by it, and whose
> lower half is the dangerous one. **No published id exists inside that band**, so no number
> of extra checks against this repository's ~78 published ids can close it; only a statement
> run deliberately and re-checked at intervals could.
>
> **And "age, not the id" claims a mechanism the data cannot isolate.** Every resolving id
> comes from a single cluster of runs on one warehouse session. Age is confounded with
> warehouse restart, with a fixed-size history buffer, and with per-day partitioning of the
> statement store. **Retention is the most likely explanation and it is not the only one
> consistent with the evidence**, which is a weaker sentence than the one first published
> here and is the one the four ids earn.

**What is not in doubt** is the direction and its consequence: two ids from two different
phases and two different authors, both older, both gone; two newer, both present. This is not
the "resolves to nothing" species this repository has struck twice — those ids never named a
real execution, and these demonstrably did.

**What follows, and it is a fact about every evidence document in this repository:** the
statement ids published in `docs/f2-wave-1-*.md` and `docs/f3-*.md` — the ids that are the
stated provenance for the vault's and the star's headline numbers — **can no longer be
fetched by anyone.** F-API §3.1 already recorded that a `CLOSED` statement's rows cannot be
read; what is new here is that past the boundary above the statement cannot be found at all.

**This does not retract a single one of those numbers.** It retires the *mechanism* that was
supposed to let a reader check them, and it means the guard has to be exercised **while a
phase is running**, not at its close and never afterwards. §0.2 is what to do instead.

### 0.2 The claim this phase's headline rests on, RE-MEASURED rather than inherited

**Controller-verified.** The plan's Task 0 says *"Do not inherit a first"* — F-API published a
"first" that was false with the disproof eleven days old in the same folder. F-DB's headline is
that a hard DELETE produces **the first end-dating in this lakehouse**, and it rests on
`.plans/HANDOFF.md`'s *"zero departures — all 68,629,147 keys of 2026-06 are in 2026-07; the
RFB retains baixadas"*, whose statement id is the first row of §0.1 and no longer resolves.

So it was re-measured, today, over `bronze_cnpj_empresas`. Statement
**`01f1986b-af6c-1eb4-8585-f5edb3e11638`**, `from_cache: None`.

**The query is quoted in full, and §0.1 is why.** A statement id is the only handle a reader
has on a published measurement, the API does not return statement text, and §0.1 has just
established that the handle dies somewhere inside a day and a half. A number published against an
id alone is therefore unverifiable almost immediately — so the method goes in the document:

```sql
SELECT COUNT(*)                                                     AS keys_total,
       SUM(CASE WHEN in_jun=1 THEN 1 ELSE 0 END)                    AS jun_keys,
       SUM(CASE WHEN in_jul=1 THEN 1 ELSE 0 END)                    AS jul_keys,
       SUM(CASE WHEN in_jun=1 AND in_jul=0 THEN 1 ELSE 0 END)       AS departed,
       SUM(CASE WHEN in_jun=0 AND in_jul=1 THEN 1 ELSE 0 END)       AS arrived,
       SUM(CASE WHEN k IS NULL THEN 1 ELSE 0 END)                   AS null_keys
FROM (SELECT cnpj_basico AS k,
             MAX(CASE WHEN _snapshot_month='2026-06' THEN 1 ELSE 0 END) AS in_jun,
             MAX(CASE WHEN _snapshot_month='2026-07' THEN 1 ELSE 0 END) AS in_jul
      FROM workspace.default.bronze_cnpj_empresas
      WHERE _snapshot_month IN ('2026-06','2026-07')
      GROUP BY cnpj_basico)
```

`GROUP BY cnpj_basico` rather than `COUNT(DISTINCT …)` is standing decision §4.8 — a
`COUNT(DISTINCT a,b,c)` drops NULL-bearing rows and cost this repository 8,761 once. `GROUP BY`
keeps NULL as its own group, which is what makes `null_keys` a real check rather than a
tautology.

| | value |
|---|---|
| distinct `cnpj_basico` over both months | **69,062,849** |
| present in 2026-06 | **68,629,147** |
| present in 2026-07 | **69,062,849** |
| **departed** (in June, absent in July) | **0** |
| arrived (absent in June, present in July) | **433,702** |
| NULL keys | **0** |

**The claim holds.** 68,629,147 is reproduced exactly, and the departure count is still zero —
so empresas end-dating is still unexercised, and this phase's "first" is not inherited from a
citation but re-derived from the table.

**One cross-check that is real, and two this section first published that were not:**

- **EACH MONTH IS UNIQUE ON `cnpj_basico`, and that is the new fact.**
  `docs/f1.4b-pr-b-run-evidence.md` §21.2 is headed *"Row counts"* and gives empresas as
  68,629,147 and 69,062,849. The query above counts **distinct keys** and returns the same two
  numbers. Distinct keys equal rows in both months, so neither month carries a duplicate
  `cnpj_basico` — which is the fact that makes a row-count table and a key-count query
  comparable at all, and which nothing in this repository had stated.

> **TWO CROSS-CHECKS PUBLISHED HERE FIRST WERE CIRCULAR, AND THEY ARE RETRACTED RATHER THAN
> QUIETLY REPLACED** — the section was about not inheriting an unverified claim, and it
> shipped two of its own.
>
> - *"69,062,849 is exactly the published `hub_empresa` count, so the bronze-side distinct key
>   count and the vault-side hub count agree **without either being derived from the other**."*
>   **False.** `hub_empresa` is loaded **from** `bronze_cnpj_empresas`
>   (`src/opl/vault/domains/cnpj.py` records estabelecimentos as the *second* feed, so empresas
>   is the first). It is one source through two paths. That is still a genuine check — it says
>   the hub loader neither dropped nor invented a key over 69 million of them — but it is not
>   two independent sources, and the sentence claimed it was.
> - *"68,629,147 + 433,702 = 69,062,849 closes exactly."* **An identity, not a check.** The
>   same statement reports `departed = 0`, under which `jul = jun + arrived` cannot fail unless
>   the query contradicts itself. And `f1.4b`'s `delta` column *is*
>   69,062,849 − 68,629,147, so this was the same subtraction of the same two figures — not,
>   as claimed, "a different decomposition".
>
> **433,702 is still not a new number**, and that half stands: `f1.4b` §21.2 published it. It
> was very nearly written up here as a discovery, and the grep that caught it ran before
> writing rather than after.

**Method note, because it cost a statement.** The first attempt expressed the anti-join as a
correlated `NOT EXISTS` with a null-safe `<=>`, chosen to avoid the phantom-departure defect
`.plans/HANDOFF.md` records (*"A `LEFT ANTI JOIN … USING` on the partner key invents
departures"* — 8,757 phantom rows). It **failed**: statement
`01f1986b-9b8f-1d30-a71b-806c79d2aec8`, `[INTERNAL_ERROR] The Spark SQL phase optimization
failed with an internal error`, SQLSTATE `XX000`. The single-pass `GROUP BY` above is both the
repair and what standing decision §4.8 prescribes anyway, and it yields arrivals for free.

### 0.3 The counterparty pool, pinned — and the 142 keys that decide a column type

**Controller-verified.** The pool is not an artefact anywhere in this repository:
`POOL_SIZE = 1024` and `POOL_SEED = 20260812` are module constants
(`src/opl/generator/profiles.py:118,124`) and the draw runs **inside the Databricks job**,
because `src/opl/generator/cnpj_pool.py:20-21` deliberately imports no pyspark and no SDK.

Extracted once with the generator's own query, statement
**`01f1986b-c653-17e7-8c1d-807a684b8f45`**, `from_cache: None`:

```
SELECT cnpj_basico FROM workspace.default.hub_empresa
ORDER BY sha2(concat(cnpj_basico, '20260812'), 256) LIMIT 1024
```

| | value |
|---|---|
| rows returned | 1,024 |
| distinct | 1,024 |
| accepted by `cnpj_pool.validated_pool` | 1,024, and sorted |
| sha256 of the sorted key body, `grep -v '^#' <file> \| sha256sum` | `82e6a447c28befd565eaedf0556bba1752da7b3ba7bdc8b87474cf2eba8aff18` |
| min / max | `00057343` / `98418478` |
| **keys carrying a LEADING ZERO** | **142 of 1,024** |

Committed as `scripts/merchant_cnpj_pool.txt` (`98a5bc9`), with its provenance in the file's
own header. `scripts/` is outside the bundle sync root, so the pin never ships to Databricks.

**The 142 is a design input, not a curiosity.** Any column, cast or parse that treats a CNPJ as
a number destroys 13.9% of this pool silently — which is precisely the failure F1b's "100%
counterparty CNPJ resolution" check exists to catch (*"a cast to numeric eating leading zeros
is the failure it actually catches"*). It is why the Postgres `merchant.cnpj` column is `text`,
and the plan's §4 now says so with this number behind it.

### 0.4 The Postgres side — the artefact, and what it refused

**Reported** by the Task 0 implementer unless marked otherwise. The artefact is
**`scripts/probe_postgres.py`** plus three siblings (`_session`, `_container`, `_rendering`),
re-runnable as one command. *(Two of the three also run standalone; `_session` is a helper
module with no `main()`, and this document first said all three did.)* **PostgreSQL 16.14, psycopg 3.2.3.**

The persistence matrix sits behind `--persistence` because it runs `down -v`: an unguarded
default would delete Task 3's seed. The probe also **broke this repository's own 800-line
ceiling at 1,032 lines** — the largest **Python** file in the tree, though not the largest file: three evidence
documents and `scripts/merchant_cnpj_pool.txt`, committed three commits earlier on this same
branch at 1,046 lines, were already bigger — in the artefact whose entire argument
is measuring rather than assuming, and was split on seams already visible in its own output.

#### The container does NOT forget, and revision 1 of the plan said it did

| operation | data |
|---|---|
| fresh volume | `opl` holds **0 user tables** |
| `docker compose restart postgres` | **survives**, same volume id |
| `stop` + `start` | **survives**, same volume id |
| `down` (no `-v`) + `up -d` | **DESTROYED**, new volume id, dangling volumes 5 → 6 |

`postgres:16` declares an anonymous volume at `/var/lib/postgresql/data`. **So the seeder meets
a populated database on the common path and must be idempotent** (T6) — the opposite of the
risk revision 1 stated. **Exactly one orphaned PGDATA per `down`**, so `down -v`.

#### The isolation rulings, with the controls that make them mean something

- **One `SELECT` under `READ COMMITTED` is atomic.** 4.0 s scan, writer committing
  `UPDATE`+`DELETE`+`INSERT` at t=2.0 s: all 10 rows `old`, the deleted row still returned, the
  inserted row absent. **So the single-statement test revision 1 demanded cannot fail**, and
  T3's justification was re-derived from "the extraction is more than one statement".
- **Batched keyset read under `READ COMMITTED` smears**: `[(1,old),(2,old),(3,old),(4,new)…]`
  — **9 rows read from a 10-row table**, one deleted mid-read and one inserted behind the
  cursor. No instant contains that answer.
- **`REPEATABLE READ` holds** across two statements *and* across the same batched read.
  `READ ONLY` refuses a write outright — which matters, because **`opl` is a SUPERUSER**
  (`usesuper = True`), so nothing else stops the extractor writing to the database it observes.
- **The stamp gap is real and the fix is ordering.** `BEGIN` then 2.5 s of work:
  `transaction_timestamp()` **06:07:32.765107**, snapshot actually acquired at
  **06:07:35.268244** — **gap 2.503 s** — and a row committed at 06:07:33.994255, *after* the
  stamp, **is in the snapshot**. With the stamp as the transaction's **first statement**:
  **gap 0.001 s, 0 rows in it.**

#### The out-of-order commit — reproduced, and it is the phase's headline mechanism

t1 = 06:07:38.093608 (slow transaction, held open); t2 = 06:07:38.601781 (fast, committed);
extract records `watermark = t2`; the slow transaction then commits. `WHERE updated_at > t2`
returns **`[]`**, and **`[]` again a second later**. The snapshot diff sees the row.

**It was measured with a `BEFORE UPDATE` trigger in place** — i.e. with the *correct* fix for
the other two watermark classes already applied. That is what makes it the one number in this
phase's headline that the mutation script does not author: no amount of care on the source side
removes it, because `updated_at` orders by transaction **start** and visibility orders by
transaction **commit**.

The other two classes confirmed, and **they are not equally well measured**:

- **`DEFAULT now()` does not fire on `UPDATE`** — `…673055` before and after. That is a
  direct before/after comparison of the value itself, so it is a real control: it would have
  shown a different number had the default fired.
- **A hard DELETE takes 5 rows to 4 and the watermark returns `[]`.** The row count proves the
  DELETE ran. **The `[]` proves less than it looks.** The watermark is `max(updated_at)` over
  five rows written by one `INSERT … generate_series`, so every row carries *exactly* that
  value and `WHERE updated_at > watermark` is empty at every point in the function — before
  the DELETE as well as after. **The `[]` would print identically if the DELETE were removed.**
  Standing decision §4.6 in its symmetric form: a zero that a path could not have made
  non-zero is not evidence about that path. The class is real — a deleted row leaves nothing
  to carry a timestamp, which needs no experiment — but this particular `[]` does not
  demonstrate it, and §6's out-of-order measurement does it properly by printing
  `SNAPSHOT DIFF catches [1]` beside `WATERMARK catches []`.

#### The GUC matrix, and the silently wrong date

Baseline `TimeZone=Etc/UTC, DateStyle=ISO, MDY, extra_float_digits=1` →
`2026-08-03 17:23:01.123456+00` / `1234.50` / `0.30000000000000004` / `true`. Then, **with no
code change at all**:

| environment | `timestamptz::text` |
|---|---|
| `PGTZ=America/Sao_Paulo` | `2026-08-03 14:23:01.123456-03` |
| `PGDATESTYLE='SQL, DMY'` | `03/08/2026 17:23:01.123456 UTC` |
| `PGOPTIONS='-c extra_float_digits=0'` | baseline timestamp; `float8` renders `0.3` |
| all three together (`PGOPTIONS='-c extra_float_digits=-3'`) | `03/08/2026 14:23:01.123456 -03`, and `0.3` |

> **THE FOURTH ROW FIRST PUBLISHED HERE NAMED AN ENVIRONMENT THE PROBE NEVER RAN.** It read
> *"both, plus `PGOPTIONS='-c extra_float_digits=0'`"* — but `probe_postgres_rendering.py`'s
> `HOSTILE_ENV` sets **`-3`**, and the `0` row sets `PGOPTIONS` **alone**. The published row
> was two real runs spliced and attributed to a third. **It is the species this document's own
> §0.1 is about**, committed one section later: a number correct about one population, printed
> as the answer about another. Both real rows are now given separately, and neither claim
> weakens — `0` and `-3` both fail the float8 round-trip, which is the point.

**The misparse, end to end:** a writer at `SQL, DMY` renders `03/08/2026`; a reader at
`ISO, MDY` parses it as **2026-03-08**; the stored value was **2026-08-03**. Nothing raised.

**The pin defeats it** — every pinned GUC reads back correct inside that hostile environment,
and the rendering is byte-identical to the clean baseline.

**Stated precisely, because "all seven" overstates the test:** the hostile environment attacks
**three** of the seven — `TimeZone`, `DateStyle`, `extra_float_digits`. `bytea_output` and
`client_encoding` were already correct at startup, so their `ok` results would be identical if
the pin were a no-op and **they are not evidence that it works**; `IntervalStyle` differs from
the server default but was not attacked. And the probe pins `search_path` to
`pg_catalog, probe_f_db`, **not** T4's `pg_catalog, public`, because it works in its own
schema — the probe prints that deviation and this document did not carry it.

`col::text` is confirmed **not** the type's output function: `boolean` `'true'` vs `'t'`,
`char(5)` `'ab'` vs `'ab   '` — the cast strips padding, which is why `char(n)` is excluded
from the schema.

#### What Task 0 REFUSED, and it was right three times

1. **A plan row was FALSIFIED and is deleted.** T3 claimed a VOLATILE function "escapes the
   statement snapshot **even under `REPEATABLE READ`**". Volatility escapes the *statement*
   snapshot; it never escapes the *transaction* snapshot. **Controller-verified independently**:
   `SELECT peek(), pg_sleep(3), peek()` with a writer committing during the sleep gives
   **`READ COMMITTED` 3 → 13 (escapes)** and **`REPEATABLE READ READ ONLY` 3 → 3 (frozen)**.
   *(Those two figures come from an ad-hoc `SELECT peek(), pg_sleep(3), peek()` against a
   3-row table and are not reproducible from anything committed. The committed probe measures
   the same claim over a 10-row table and prints **`0 → 10`** and **`0 → 0`** — same verdicts,
   different fixture. A reader running the probe to check this line should expect the latter.)*
   The `READ COMMITTED` control is what makes the second reading evidence rather than a probe
   that missed. **The claim came into the plan from its own audit and would have shipped into
   ADR 0017.**
2. **"rows silently duplicated or dropped" was over-claimed.** `WHERE id > :last` is strictly
   increasing, so against an immutable primary key duplication is impossible *by construction*
   — a guard against it would guard a state the mechanism cannot reach. Dropping is real. The
   duplication belongs to **OFFSET** paging (`[1,2,3,3,4,…]`), now measured separately.
3. **`extra_float_digits=3` buys nothing over the server default of 1** — both round-trip since
   PG12 — and everything over an environment that sets 0. The pin stands; the reason was wrong.

**And a fourth, which arrived from a bug.** The probe deadlocked itself, and the cause is a
finding: a `READ ONLY` transaction that has merely *read* a table **blocks every `ALTER TABLE`
on it** until it commits. So an extractor holding its snapshot across a multi-minute upload
stops the operational database from being migrated — a sharper argument for committing before
the upload than the xmin horizon this plan had.

**Two hygiene notes the implementer recorded against itself:** the first persistence run used
an unscoped `docker compose up -d`, which started Redpanda and moved the dangling-volume count
while measuring it (re-run scoped; verdicts unchanged, box restored to the state it was found
in). And its first GUC table **measured nothing about `float8`**, because `0.1 + 0.2` in SQL is
*numeric* arithmetic and lands on exactly `0.3` under every setting — the test needs an
explicit `::float8`, and any implementer writing T4's version needs the same cast.

#### What Task 0 did not touch

**No claim is made about `psycopg[binary]` on serverless** (plan §7 keeps it an open
measurement) and **none about Databricks egress toward a laptop behind NAT** — both remain
**argued**, and nothing in this probe can be read as testing either.

---

## 3. What ships UNEXERCISED

**Standing decision §4.6: a path that ran zero rows through it is not a path that works.**
Accumulated as the phase runs rather than reconstructed at its end.

### Added by Task 2, as the snapshot axis landed

- **`INSTANT_SNAPSHOT` has no production reference at all.**
  `grep -rn INSTANT_SNAPSHOT src/ databricks/` returns only its own definition in
  `src/opl/bronze/snapshot_axis.py`. Every registered source is monthly, so the axis this
  phase exists to enable is declared and unused until Task 4 registers `bronze_merchant`.
- **THE `axis=` PARAMETER HAS NEVER BEEN PASSED A NON-DEFAULT VALUE OUTSIDE THE LEDGER.**
  `read_snapshot_window`, `earliest_record_source`, `hub_candidates`, `load_hub`,
  `link_candidates`, `load_link`, `satellite_candidates`, `_collapsed_duplicates`,
  `_diagnostics`, `effectivity._observed` and `required_months` all now *accept* an axis and
  have all run **zero rows** on a non-monthly one. The only non-default execution anywhere is
  `observation_ledger`, in `tests/vault/test_observation_axis.py`.
  **Commit `917f6ae`'s title says "the ledger, the loaders and the job window read the
  source's axis"; one of the three is exercised.** The other two are wired and untested by
  data — which is exactly what this ledger is for, and is not a reason to withhold the wiring.
- **The four entry points fixed in `76c61e5` pass `axis=source.snapshot_axis`, and that
  expression evaluates to the DEFAULT for all six registered sources.** So the fix is
  verified by a sweep over the scripts' text and by no run. The sweep was checked to
  discriminate — run against the four pre-fix scripts it fails on all four — but discriminating
  on source text is a weaker claim than a row passing through, and it is the claim being made.
- **`effectivity`'s axis-aware path cannot run on a non-monthly source today, for a reason
  outside itself.** `_observed` and `_reference_dates` both require `_snapshot_ref_date`, and
  T8 records that `bronze_merchant` cannot produce it from the RFB mainframe filename regex
  that derives it. Until Task 4's third audit-column path exists, the effectivity half of the
  axis work is unreachable rather than merely unexercised.

### A constraint Task 2's generalisation stops short of, recorded before it bites

**`effectivity._statements`' carry-forward window still orders by `APPLIED_DATE`, which is a
DATE.** `_observed` now groups by `(link.hash_key, axis.column, APPLIED_DATE)`, so the grouping
generalised and the ordering did not. Two observations on one calendar day therefore **tie**,
and `F.last(...)` over a tie is non-deterministic.

**Nothing is wrong today and the reason is T8, not `effectivity.py`.** T8 rules that the two
Postgres snapshots are taken on two genuinely different calendar days — a scheduling decision
made for the satellite's `groupBy(hash_key, applied_date)` fold — and that same decision is
what keeps this window unambiguous. **It is recorded here because the two are now coupled and
nothing says so in either file:** a future phase that relaxes T8 to two same-day snapshots
would silently reintroduce a non-deterministic tie in the effectivity close, one layer away
from where it changed the rule.
