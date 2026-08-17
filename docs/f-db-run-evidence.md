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

## 1. Predictions — published before the run that tests them

**Written before `scripts/seed_merchant_db.py` existed and before any row was seeded**
(master protocol §4.5; a number first written down after the run that produced it is not a
prediction). The tree was clean at `16522ef` when this section was committed.

### 1.1 The populations

| | predicted |
|---|---|
| snapshot 1 — merchants | **1,088** |
| snapshot 1 — distinct CNPJs | **1,024** (64 of them carrying two merchants) |
| INSERT between the snapshots | 32 |
| UPDATE that moves `updated_at` | 48 |
| UPDATE that does **not** move it | 24 |
| hard DELETE | 16 |
| snapshot 2 — merchants | **1,104** |
| rows committed out of timestamp order | 8 |
| **rows a watermark extract MISSES** | **48** = 16 deletes + 24 silent updates + 8 out-of-order |

1,088 + 32 − 16 = 1,104, so the two snapshot counts and the three mutating classes close on
each other. The 1,024 is not a choice: it is the size of the pinned counterparty pool
(`scripts/merchant_cnpj_pool.txt`, sha256 `82e6a44…`), and the 64 second merchants exist so
`link_merchant_empresa` is not degenerate — two open link rows on one company is the normal
case the partner link already demonstrates, not an error.

### 1.2 THE TAUTOLOGY, DECLARED BEFORE THE NUMBER RATHER THAN AFTER IT

**Six of the eight mutating rows above are authored by the mutation script**
*(this read "six of the seven" against a table that has since gained the `watermark_advance` row §2.1 records; the seven were the mutating classes plus the miss).* The deletes and the
silent updates are *chosen*, not discovered, and an evidence sentence that reports "the
watermark missed 40 rows" as a finding would be the defect F3's audit caught in its own
150-duplicate acceptance — a count that could not have come out any other way.

**The out-of-order row is different, and it is why the headline is worth publishing.** The
out-of-order commit is a property of MVCC, not of the script: `updated_at` orders by
transaction **start** and visibility orders by transaction **commit**, so a row stamped
before the watermark can become visible after it. Task 0 reproduced it **with a
`BEFORE UPDATE` trigger already in place** (§0.4) — i.e. with the correct fix for the other
two classes applied — so no amount of care on the source side removes it. Its *count* is
arranged; its *existence* is not.

**What else is not chosen, and is the actual claim:**

- the differ finds presence changes from the observation ledger and payload changes from
  `hash_diff` — **one derivation per axis**, with the `updated_at` split a projection of the
  payload-changed set rather than a third mechanism (T2, as corrected: the ledger has no
  `changed` state, so revision 1's "one derivation, not four branches" was unachievable);
- the watermark query is the real one an engineer would write, it is committed, and it runs
  **inside the same `REPEATABLE READ` transaction that reads its snapshot**, so the miss set
  is a genuine complement rather than a definitional one;
- **snapshot 1's watermark is what snapshot 2's incremental query uses** — one extra
  statement per transaction, and the only arrangement under which the out-of-order case is
  reachable at all.

### 1.3 What would falsify these

- **Any count that lands where predicted while the derivation is per-class.** Four branches
  reproducing four authored numbers is the tautology wearing the right answer.
- **A watermark miss of 40 rather than 48**, which would mean the out-of-order rows were not
  produced and the one non-authored class is absent.
- **A `departed` count other than 16 in the ledger**, which would mean the snapshot axis is
  not distinguishing the two observations — the failure T7 and T8 exist to prevent, and the
  one that F-DB's headline dies on.

---

## 2. What the runs said

### 2.1 Task 3's seeder and mutation, on the local container

**Controller-verified.** Task 3's implementer died with its process before reporting, so
every number below was measured by the controller against the live container rather than
taken from the agent — the rule this project already carries about a dead writer, applied to
its numbers as well as its files. Its five commits survived; nothing was lost.

**Every §1.1 prediction lands exactly.**

| §1.1 predicted | measured |
|---|---|
| snapshot 1 — merchants **1,088** | **1,088** ✅ |
| snapshot 1 — distinct CNPJs **1,024** | **1,024** ✅ |
| INSERT 32 | **32** ✅ |
| UPDATE moving `updated_at` 48 | **48 rows, 48 moved** ✅ |
| UPDATE *not* moving it 24 | **24 rows, 0 moved** ✅ |
| hard DELETE 16 | **16** ✅ |
| out-of-order commit 8 | **8 rows, 8 moved** ✅ |
| snapshot 2 — merchants **1,104** | **1,104** ✅ |
| **rows a watermark extract MISSES — 48** | **48** ✅ — decomposing **16** deletes + **24** silent + **8** out-of-order |
| *(not predicted)* `watermark_advance` | **8 rows, 8 moved** — see below |

> **THIS TABLE FIRST SHIPPED WITH EIGHT ROWS AGAINST §1.1's NINE, AND THE MISSING ONE WAS THE
> HEADLINE.** It dropped *"rows a watermark extract MISSES — 48"* — the row §1.2 calls "why
> the headline is worth publishing" and §1.3 names as the falsifier — while asserting
> "every §1.1 prediction lands exactly". The number was measured by the container tests all
> along; it never reached the table that claimed completeness. **A claim of completeness that
> is short by exactly the load-bearing row is worse than no claim**, and it was caught by the
> Task 3 reviewer rather than by the controller who wrote it.
>
> **The miss is a genuine complement, not a definitional one**, and the reviewer reproduced it
> independently: `incremental` comes from a real `WHERE updated_at > :watermark` run **inside
> snapshot 2's own `REPEATABLE READ READ ONLY` transaction**, and the miss is
> `diff_caught − incremental` = 128 − 80 = **48**. Per class, as (missed, incremental, diff):
> `out_of_order_commit (8, 0, 8)`, `update_not_moving_updated_at (24, 0, 24)`,
> `hard_delete (16, 0, 16)`, `insert (0, 32, 32)`, `update_moving_updated_at (0, 48, 48)`,
> `watermark_advance (0, 0, 0)`.

**A SIXTH CHANGE CLASS EXISTS THAT NEITHER PLAN §4 NOR §1.1 CONTAINS, AND IT IS LOAD-BEARING.**
`watermark_advance` — 8 rows, `payload_changed=False`, `moves_updated_at=True`, applied
**before** snapshot 1 — was added by Task 3's implementer on a correct argument the plan had
missed: without it `watermark_1 == t1`, and the out-of-order miss is a fabrication rather than
a measurement. **The implementer died before reporting it and this document did not name it**,
so two consequences rode along undisclosed:

- §1.1's "UPDATE that moves `updated_at` = 48" is really **64** updates that move it — 48, plus
  8 out-of-order, plus these 8.
- The between-snapshot table carries 8 rows with an **unchanged payload and a moved
  `updated_at`**, a state §4's published population has no row for.

It is **authored**, like the other five, and it changes no published miss: it sits at the
watermark rather than above it, so it is `(0, 0, 0)`.

`t1 = 22:10:46.710500Z`, `t2 = 22:10:46.779989Z` — 69 ms apart, `t2 > t1`, and the readiness
file carries both.

**The two update classes are the whole watermark argument and they separate cleanly**: the
`BEFORE UPDATE` trigger fires 48 times for one class and **0** times for the other, on the
same derivation — one `mutated()`, differing only in whether the trigger is armed.

> **AND THE MECHANISM IS NOT THE ONE T2 NAMED, WHICH THIS PARAGRAPH FIRST OBSCURED.** T2 calls
> class 2 *"a default-shaped trap"* — `DEFAULT now()` does not fire on `UPDATE`, so a schema
> with **no trigger** loses every update to a watermark. **The seeded schema HAS the trigger**,
> deliberately, so that trap does not exist in this table. The 24 silent rows are produced by
> explicitly disarming it with `session_replication_role = 'replica'`.
>
> That is still a real operational path — it is what a **replication-apply worker** does, and
> what `pg_restore --disable-triggers` does — but it is not "a write path that never had a
> trigger", which is how "fires 48 times and 0 times" reads. **The default-shaped trap itself
> was measured in Task 0's probe schema (§0.4), where there was no trigger**, and that is the
> citation the claim rests on. Recorded because the difference is exactly the kind this
> document exists to keep: *we turned it off* is not *they never had one*.

**A number §1 did not predict, recorded because it is derived rather than contradicted:**
distinct CNPJ roots fall **1,024 → 1,011** after the mutation. Thirteen of the sixteen
deleted merchants were the only merchant for their company; the other three sat on CNPJs
carrying two. The departure count at the link grain is still **16 merchant keys**, which is
the number §1.3 says the phase dies on.

**Determinism, verified on the path that actually occurs.** Re-running `seed` against the
**populated** post-mutation database (1,104 rows) returned it to 1,088 rows at digest
`807efa4c448af66a9f402072534f297fe3769e340b81ceef630e3ab16b67fbfe` — byte-identical to the
first seed. T6 requires idempotence against a populated database specifically, because
`docker compose restart` preserves the volume; this is that case, not the easy one.

**Tests:** 23 pure population tests green (they run in the default suite and need no
container), and 16 container tests green under the new `postgres` marker.

> **THIS IS NOT THE PHASE'S RUN.** It is a controller verification that the seeder does what
> §1 predicted, made necessary by the implementer dying before it could report. Task 6 makes
> the run of record, against a deployed wheel, and its numbers are the ones the phase closes
> on.

### 2.2 An operational fact that cost this phase an agent

`scripts/run_suite.sh` died mid-run with
`fork: retry: Resource temporarily unavailable` and `exit code 0xC000026B`, twice, and took
the Claude Code process — and therefore the Task 3 implementer — down with it. The partition
had already reconciled (`0 in no chunk, 0 in no suite run, 0 in two chunks`); it was the
**RUNNING** phase that could not fork its chunks.

**So the five-chunk runner is not safe to launch on a loaded box**, and this is a second
mechanism beside the contention the script's own comments already record. The Postgres
container was stopped by the same event and had to be restarted — **and the seeded data
survived it**, which is Task 0's persistence matrix (§0.4) confirmed in the field rather than
in a probe: a stop is not a `down`.

### 2.3 Task 4, verified by the controller because its agent died too

**Controller-verified.** Task 4's implementer died with its process before reporting — the
~~**third**~~ **second this branch can substantiate**.

> **"THE THIRD" IS A COUNT THIS REPOSITORY CANNOT SUPPORT, AND IT IS CORRECTED RATHER THAN
> LEFT.** The Task 4 review walked all thirty-seven commits of `0054df1..HEAD` for every death
> this branch names or implies — death language in a commit body, an unexplained gap in the
> timeline, a stash recovery — and found **two**: Task 3's (§2.1, and `be179ec`'s own message)
> and Task 4's. Task 0, Task 1 and Task 2's sessions run continuously. `.plans/HANDOFF.md` and
> the continuation prompt both say three as well, so the number propagated to three documents
> before anyone counted it.
>
> A third may well have happened — the two `run_suite.sh` kills of §2.2 are two events, and
> whether they took one implementer or two is not something the commit record answers. **That
> is the point: it is not answerable from what ships**, and this document's own §2.1 records
> an undisclosed death as a defect against itself. Two is what the record substantiates; three
> is not quotable, on the same discipline `docs/f3-run-evidence.md` §0.4 applies to its own
> count of twenty-one. Its seven commits survived (`be6ee0c..3ed6ba5`), tree clean, no
stash: the `merchant` contract, `postgres_source.py`, the host-side extractor, the fifth
landing mode, a third audit-column path, the DQ rule set, `bronze_merchant_job.yml` and its
ingest, across 29 files and 3,928 insertions.

| check | result |
|---|---|
| `uv run ruff check .` | **clean** |
| files at or over 800 lines | **none** |
| non-Spark surface (11 files) | **258 passed in 5.70 s** |
| Spark surface (`test_merchant_rules.py`, `test_snapshot.py`) | **113 passed in 278.29 s** |
| collection | **2,342 / 2,365, 23 deselected** |

**371 tests green, and that is execution rather than review.** ~~No independent pass has read
this task~~ — **superseded 2026-08-17: four did**, one lens each (buildability, provenance,
data modelling, PostgreSQL), and §2.4 is what they found.

> **THE `258` IS A COUNT OVER TEN FILES AND THE ROW BESIDE IT SAYS ELEVEN.** Re-measured: the
> eleven files this task touched outside the two Spark-backed ones collect **294**, and
> **294 − 36 = 258** where 36 is `tests/test_job_yaml_wiring.py`, which the enumeration names
> and the number does not include. All 294 pass, in **6.02 s** — so the correction moves the
> figure *up* and nothing under it changes. It is the same shape as this document's other two
> arithmetic defects: a number right about one population printed as the answer about another.

**ONE CAP REGRESSION EXISTED AND IT WAS THE CONTROLLER'S, NOT TASK 4'S.** Measured across
`0054df1..HEAD`, exactly one function crossed the 50-line cap during this phase:
`seed_merchant_db.mutate`, 45 lines when the Task 3 reviewer measured it and **53** after the
controller's own review-fix commit inlined the atomic-readiness block. Task 4, by contrast,
caught three of its own and moved them (`406667e`). The timing is the whole point: the
controller broke that cap **in the commit that fixed the review which had caught the same
species in Task 2**, and nothing surfaced it until this handoff measurement — because
**neither the file cap nor the function cap is enforced by any test in this repository.**
Repaired by lifting the write into `_announce_readiness`, whose docstring now carries the
reason.

> **"EXACTLY ONE" WAS FALSE WHEN IT WAS WRITTEN, AND A SECOND CROSSING WAS OVER THE CAP IN
> THE TREE AT THAT MOMENT.** Found by the Task 4 review, and measured three times
> independently — by the controller before the dispatch, and by two of the four reviewers
> against the merge base in a clean `main` worktree, each by AST
> (`end_lineno - lineno + 1`, the measure the rule states):
>
> | | at `0054df1` | at `9337aa6` |
> |---|---|---|
> | `databricks/src/vault_load_partner_link.py::main` | **34** | **51** ❌ |
> | `scripts/seed_merchant_db.py::mutate` | 45 | 45 (repaired by `1418270`) |
> | functions at or over 50 lines, whole tree | **33** | **33** |
>
> `vault_load_partner_link.main` was pushed over by **`76c61e5`**, the fix pass that carried
> the snapshot axis into the vault entry points, with a seventeen-line non-monthly-axis
> refusal block. **It is Task 2's regression and it was never counted** — including by
> `f425574`, titled *"two functions Task 2 pushed over the 50-line cap"*, which fixed two
> others in the same review cycle and did not reach this file.
>
> **THE WAY IT HID IS THE PART WORTH KEEPING.** The whole-tree count is 33 at both revisions.
> One function left the set as this one entered — `autoloader._assert_source_dir_is_this_months`,
> which `406667e` repaired — so a controller checking the total rather than the set would have
> seen a number that had not moved and concluded nothing had. That is this phase's first defect
> species, committed inside the paragraph whose subject is the third one.
>
> **The sentence above is left standing rather than edited**, because "the one cap regression
> was mine" is the claim this correction is about and hiding it would defeat the point. The
> corrected statement: **at least two functions crossed the cap during this phase**, both of
> them in commits the controller authored, and the second was still over the cap when the
> paragraph claiming there was one shipped.

---

### 2.4 The Task 4 review — four lenses, and the third silent failure was finally found

**Dispatched 2026-08-17**, one lens each — buildability, provenance, data modelling, and the
PostgreSQL domain — each with the task's tension named in its brief and the test that closes it
demanded rather than suggested. **Every finding below was verified by the controller against the
primary source before it was accepted**, so what follows is controller-verified as to existence;
where a reviewer measured something the controller did not re-run, it says so.

**Task 4's own deliverable came out well, and that is a result rather than a courtesy.** No
reviewer found a BLOCKING defect in the extraction layer's mechanics. `databricks bundle
validate -t free` returns **Validation OK!**; `ruff` is clean; all four import-time guard
families are correct for `merchant`; the fifth landing mode is filed in the **right** half of
the landing-mode partition; the three `catalogue.py` edits are present and mutually consistent;
the per-new-file sweep tax is genuinely paid rather than silently skipped; and the PTAX-paste
tension the dispatch named was refused by the code — all four PTAX-only mechanisms are absent,
and the one guard deliberately not reproduced (`_refuse_a_different_file`) is argued
structurally rather than dropped.

#### The two blocking findings

**1. THE THIRD SILENT FAILURE, and it is in the flag wiring rather than in a query.**
`scripts/extract_merchant_snapshot.py` takes `--since` and `--wait-for` as independent flags.
`--since` alone runs the incremental query and produces the miss set; but
`_refuse_a_watermark_before_t2` — the one check that the snapshot was not taken between t1 and
t2 — runs only when `--wait-for` was *also* passed. So an incremental run without that second
flag takes the complement with **no protection against the exact race the module's own docstring
calls "the failure shape this repository refuses"**, and the published miss falls from 48 to 40
while the row count, the byte count, the digest and the watermark all still print correct.
**Nothing refused the combination and no test exercised it.** This phase predicted a third
instance of this species and named the two already fixed; this is it, and it was reachable
through a flag pair rather than through any of the three mechanisms that were being watched.

**2. T10's guard landed exactly where T10's ruling refused to put it.** The merchant rule set
ships `encoding_replacement_char`, which detects **U+FFFD** — mojibake — and has nothing to do
with the forty characters JDK 17 and CPython 3.12 upper-case differently. Those are valid,
correctly-decoded characters, and the only thing guarding against them is a bound on what the
**seeder** may write. That is the seeder assertion T10 rejects by name, *"which protects the
seed and nothing else — not the mutation script, not a manual `psql`, not a re-seed"*. A row
carrying one of the forty reaches `hash_diff`, the Python and Spark digests disagree on real
data, and **no test goes red**, because the loaders only ever use the Spark spelling.

#### Three rulings whose decision survives and whose stated mechanism does not

This is now four times in one phase, and it is worth naming as a pattern rather than counting
again: **T3's volatility claim** (falsified by Task 0's implementer), **T4's `extra_float_digits`
reason** (the pin is worth taking; the default was never unsafe), **T5** and **T11** below. In
every case the decision was right and the argument offered for it was not — which is an argument
for publishing mechanisms, since a ruling whose reason is never written down cannot be falsified
at all.

- **T11 — `ORDER BY … COLLATE "C"` survives; its stated consumer is the wrong one.** T11 says
  the collation protects *"the byte-identity refusal the landing reuses"*. It cannot:
  `refuse_bytes_that_are_not_the_payload` is a write-then-read-back check **inside one run**,
  comparing in-memory bytes to the same execution's on-disk bytes, so whatever order the
  `SELECT` returned is on both sides of the comparison and no collation can make it fire.
  **The consumer that does need it was not named in the ruling and is one file away:**
  `scripts/seed_merchant_db.py`'s content digest orders by the same expression inside a
  `string_agg` and **is compared across runs** — it is the `807efa4c…` §2.1 reports as
  byte-identical on the re-seed. Two hosts with different default collations would move that
  digest for byte-identical table content, which is precisely the mysterious diff T11 describes.
  The mechanism is real; it belongs to Task 3's seeder, not Task 4's extractor.
- **T5 — a link rather than a duplicate hub is right; "two plain identifying hubs, therefore
  `load_link` writes it" is false.** Recorded in full in the plan's T5, and it is Task 5's
  starting condition rather than a Task 4 defect. `link_candidates` and
  `link_hash_key_expression` read every hub's business key **from columns named after it**, and
  `bronze_merchant` carries `cnpj` (fourteen characters) where `hub_empresa` keys on
  `cnpj_basico` (eight). **Task 4's own docstrings already describe the join as "the first eight
  characters"** — the contract, the DQ rule, and a `CNPJ_BASICO_WIDTH` constant declared for a
  consumer that under T5-as-written would never exist. Task 4 was right and the plan was wrong.

#### What the reviewers refused, correctly

- **That the missing same-day-snapshot refusal is a gap.** T8 rules the two-different-days
  requirement is *scheduling, not code*, and §3 already discloses that the job YAML records what
  it cannot enforce. Building the refusal would exceed what T8 asked for.
- **That the landing-mode "misfiled into the wrong half" hole is present here.** It is not:
  `LANDING_POSTGRES` is correctly non-file-fed, and the test derives its set from `REGISTRY`
  rather than from `LANDING_MODES`, as the plan required. The brief described the **guard's**
  structural blind spot and the reviewer declined to convert that into a defect.
- **That the third silent failure had to be inside Task 4's own surface.** The provenance
  reviewer looked, did not find one of that shape there, and said so instead of forcing a fit.
  The one that existed was found by a different lens, in a file the brief had pointed at for a
  different reason.

#### Two gaps between a correct implementation and the test that was demanded

Both are cases where the **code is right** — verified live — and the committed test cannot tell
whether it is:

- **`pin_rendering_gucs` defeats a hostile environment, and no committed test proves it.**
  Verified live under `PGTZ=America/Sao_Paulo`, `PGDATESTYLE='SQL, DMY'` and
  `PGOPTIONS='-c extra_float_digits=-3'`: all seven GUCs read back at the pinned values and the
  rendering came out byte-identical to the clean baseline. But the hermetic test's fake answers
  `current_setting()` from a dict the **test** supplies, independent of whether the code ever
  executed `set_config` — so it cannot distinguish "the pin worked" from "the pin never ran and
  the defaults happened to disagree". **That is verbatim the failure T4's ruling rejects revision
  1's test for**, arriving through a different route.
- **`ref_date_from_instant` never casts, and its test never says so.** The implementation reads
  the first ten ISO characters, which is the `gold.conformed.day_of` pattern and structurally
  immune to the session timezone. The test asserts the positive case and malformed shapes; it
  does not set a hostile zone and pin **both** the right answer and the wrong one, which is the
  bar `tests/gold/test_conformed.py` set for exactly this hazard.

#### And the durable item, recommended twice in this phase and shipped by neither

**Neither the file cap nor the function cap is enforced by any test in this repository**, and
this phase has now produced **four** wrong statements about them: three claiming compliance on a
docstring-excluded measure, and one — corrected above — claiming exactly one crossing while a
second sat over the cap in the tree. **The correction pass ships the test**, as an AST check with
a committed allow-list of the pre-existing over-cap functions that fails in both directions, so
the list shrinks as functions are fixed and cannot rot. The 33 pre-existing over-cap functions
are left alone; F-API already ruled on them.

#### 2.4.1 The correction pass — nine commits, and it refused half of one instruction correctly

**Controller-verified**: tree clean, no stash, `ruff` clean, `databricks bundle validate -t free`
→ **Validation OK!**, **32** functions at or over 50 lines (was 33), largest tracked Python file
**799**. Collection **2,342 / 2,365 → 2,360 / 2,386**, and the +18 selected / +3 deselected are
attributed by **diffing the listings rather than the totals** — the discipline this document had
to learn twice.

**The refusal is the most valuable thing in the pass, and it sharpened the fix.** The controller's
instruction was to bind the safety check to "this is an incremental run". The implementer refused
half of it with the mechanism: **the value that carries the race is the `--since` ARGUMENT, not
this run's own watermark.** `--since` is snapshot 1's watermark, produced by an earlier process —
and if *that* read landed between t1 and t2 it recorded a stamp below t1, so
`WHERE updated_at > :since` **returns** the eight held-open rows instead of missing them, while
snapshot 2's own watermark reads t2 in the broken run and in a correct one alike. Checking this
run's watermark alone therefore **cannot see it**. Both refusals now exist, and the argument is
compared to t2 *in Postgres*, because `max(updated_at)::text` and `datetime.isoformat()` are two
spellings of one instant that a Python string comparison gets wrong on most values.

**Three of the new tests were shown to discriminate by breaking the thing they guard**, which is
the standard this project reached after counting how many of its guards had never been proven able
to fail:

- With the `--since` refusal patched out, **a manual run against the live container completes a
  full unprotected incremental extract** — 1,088 rows, a watermark, 477,163 bytes and a sha256,
  every number correct. The species, drawn from life.

  > **THE SENTENCE ABOVE FIRST SAID "THE TEST" DOES THIS, AND NO COMMITTED TEST DOES.**
  > `tests/test_extract_merchant_snapshot.py` says in its own docstring *"HERMETIC: a `tmp_path`
  > and a fake connection. Nothing here starts a container"*, and its assertion on this refusal
  > uses a fake, never psycopg. The demonstration was **ad hoc** — patch the call site, run the
  > script by hand — and it was described in words that promise a reproducible artefact.
  >
  > **The numbers are exact**: the reviewer of the correction pass reproduced them independently
  > and got the same 1,088 rows, the same watermark and byte-for-byte the same 477,163. So this
  > is a precision defect and not a false number — **which is the same thing §0.4 says about its
  > own `peek()` figures**, in this document, under the label *"not reproducible from anything
  > committed"*. **Written in the paragraph whose entire subject is guards that were never proven
  > able to fail**, and caught by the review of the pass rather than by its author.
- With T10's new rule patched out, a row carrying **U+A7C1** in `legal_name` comes back with
  reject reason **`[None]`** — not rejected for a neighbouring reason, **accepted**. T10's latent
  defect, live, before the guard existed.
- With `RENDERING_GUCS["TimeZone"]` doctored to `America/Sao_Paulo`, the GUC read-back check
  **still passes** and the rendering comes out `2026-08-15 14:23:01.123456-03` against the
  committed `…17:23:01.123456+00`. **Only the committed literal catches it** — which is precisely
  why T4's ruling demanded literals and rejected a client-against-client comparison.

**And T4's own wording is wrong about `IntervalStyle`, found unasked.** *"The rendering is
byte-identical to the clean baseline"* holds for six of the seven pinned GUCs and is **false** for
this one: the server default `postgres` renders `5 days 03:00:00` where the pin `iso_8601` renders
`P5DT3H`. The pin **overrides** that default rather than agreeing with it — which makes it the one
assertion in the set that **no no-op pin can satisfy on any container**, and it is now the control
the live test leans on. A sentence that was wrong turned out to name the strongest available check.

**Two more, about this box rather than this phase.** `ruff check` is not a PEP 8 clearance here —
the blank-line rules are `E301`-`E303`, ruff-preview-only, and this project selects `E,F,I,UP,B`,
so a mechanical split left three seams at one blank line and ruff was silent. And
`scripts/run_suite.sh`'s chunk 1 (`--ignore=tests/vault`, i.e. gold *and* bronze) will **likely
blow its own 600 s cap** on this machine: `tests/gold` alone did not finish in 570 s and
`tests/gold/test_conformed.py` is 22 tests in 267.87 s. **Inferred from two direct measurements
rather than measured**, because running the script is forbidden — and labelled as an inference.

#### 2.4.2 The review of the correction pass — and it ran what the pass declared it had not

**Zero blocking defects, and it is an earned verdict rather than a default.** The reviewer traced
the one place a blocking regression was most likely — F1 — into `seed_merchant_db.mutate`'s actual
sequence rather than accepting either the implementer's framing or the controller's.

**THE COVERAGE ARGUMENT WAS NARROWER THAN WHAT IT HAD TO COVER, and a split is exactly what
invalidates that shape of argument.** The pass skipped `tests/vault` and part of `tests/bronze` on
the grounds that it had run *"every module importing `opl.bronze.rules`"*. Incomplete on its own
terms: `opl.vault.reference` → `opl.bronze.autoloader` → `opl.bronze.dq` → `opl.bronze.rules` →
`rule_predicates` → `unicode_case` is a **transitive** path the rationale did not account for, and
six test files reached the changed code through it without being re-run. The reviewer ran them,
and then ran gold in full anyway rather than resting on the import graph:

| suite | result |
|---|---|
| `tests/vault/test_reference_vault.py` | **10 passed**, 383.84 s |
| `tests/vault/test_cnpj_vault.py` | **28 passed**, 231.77 s |
| `tests/bronze/test_promote.py` + `test_masking.py` | **43 passed**, 257.45 s |
| `test_promote_batch_task.py` + `test_ptax_rules.py` + `test_payment_rules.py` + `test_dq.py` | **98 passed**, 370.70 s |
| `tests/test_dq_gate_batch_task.py` | **10 passed**, 0.52 s |
| `tests/gold/test_conformed.py` + `test_registry.py` | **297 passed**, 362.81 s |
| the remaining five `tests/gold` files | **125 passed**, 1552.22 s |
| `tests/vault/test_hashing_spark.py` equality sweep | **1 passed**, 29.38 s |

**612 tests, no failures.** The gold half of the pass's argument was right — only
`opl/gold/registry_guards.py` touches `opl.bronze`, and only `opl.bronze.registry`, which does not
import `rules` — and the vault half was wrong. **Nothing broke, and the defect is still real**: the
outcome was luck relative to the reasoning offered for it.

**F1 traced to source, and it does not refuse a correct run.** `mutate()` begins the held
transaction stamping `t1`, commits the `watermark_advance` transaction at `t2 > t1`, writes the
readiness file carrying both, and **only then** is snapshot 1 read — so snapshot 1's watermark is
**exactly `t2`**, which is what the `watermark_advance` class was added for. `watermark_is_at_or_after`
is `>=`, so the correct run satisfies the refusal by equality, and the end-to-end test passes
`--since` = t2 reformatted, exercising that boundary rather than an easier case.

**Everything else re-derived rather than accepted.** The split's `rules_for` dispatch is
**byte-identical** across all seven contracts in order — which matters because first-match-wins is
the gate's contract and a reordering is a behaviour change no count would reveal — and the
collection claim was checked by building two disposable worktrees at `cb2373a^` and `cb2373a`.
`UNICODE_VERSION_DIVERGENCE` has exactly 40 members with U+105A2/U+105B2/U+105BA confirmed
excluded, none cp1252-encodable, and the class uses codepoint escapes rather than literal astral
characters, so no surrogate pair can end up in it. The cap test was broken in **three** directions
— a stale allow-list entry, a function padded past 50, and one blank line taking
`src/opl/gold/facts.py` from 799 to 800 — and failed correctly each time. The suite delta's +18 is
attributed id by id from diffed listings: 6 T10 + 1 F6 + 3 F1 + 4 sweep-tax + 4 cap tests.

Every file edited to prove a point was restored, with `git status --porcelain` empty after each.

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

### Added by Task 4, as the extraction layer and bronze landed

**Two of Task 2's entries are RETIRED here and the retirement is stated rather than left to
be inferred**, because a ledger that only grows stops being read:

- *"`INSTANT_SNAPSHOT` has no production reference at all"* — **retired.**
  `REGISTRY["merchant"]` declares it (`src/opl/bronze/registry.py`), and
  `tests/bronze/test_snapshot_axis.py` now asserts the registry as a PARTITION: six names on
  the monthly axis, one on the instant axis, both halves set equalities.
- *"`effectivity`'s axis-aware path cannot run on a non-monthly source today, for a reason
  outside itself"* — **the reason is removed.** That entry named the blocker exactly:
  `_observed` and `_reference_dates` both require `_snapshot_ref_date`, which
  `bronze_merchant` could not produce. `opl.bronze.snapshot.ref_date_from_instant` and
  `autoloader.add_instant_audit_columns` are the third audit path T8 called for, so the
  column now exists for this source. **Unreachable has become merely unexercised**, which is
  a different sentence and a weaker one.

**And what Task 4 itself ships with zero rows through it:**

- **NOTHING HAS BEEN INGESTED. `bronze_merchant` does not exist as a table.** The job YAML,
  `bronze_merchant_ingest.py`, the fifth landing root, the DQ gate over the merchant rule
  set, the promote, and the `_snapshot_at_instant_shape` CHECK have every one of them moved
  **0 rows on Databricks**. `databricks bundle validate -t free` passes, which says the YAML
  parses and resolves — not that a task in it has run.
- **THE `postgres/` LANDING ROOT HAS NEVER RECEIVED A BYTE.** The extractor was run against
  the live container with `--no-upload`: 1,088 rows, 477,163 bytes, sha256
  `eb20b14ed46dd6073f31489e3c27e57328a5c0f0b764aba8ce9e99225363783d`, written and read back
  as bytes on the extraction host. **The `upload_to_volume` half of that script is
  unexercised**, and it is the half that needs a credential.

  > **THAT sha256 IS NOT REPRODUCIBLE AND THE LINE ABOVE PRINTED IT AS THOUGH IT WERE.**
  > Measured by the Task 4 provenance review, which ran the committed script twice, unmodified,
  > against the same container: **1,088 rows and 477,163 bytes reproduce exactly on every run,
  > and the digest is different every time** — three runs, three digests, one byte length.
  >
  > **The mechanism is the design working.** Every row carries `_snapshot_at`, the reading
  > transaction's own instant at fixed `.US` width (T7), so the payload's LENGTH is a property
  > of the data and its CONTENT is a property of *when the run happened*. A reader who checked
  > this digest the way §0.3's pool digest can be checked would find a mismatch and conclude
  > something was wrong.
  >
  > **It is not a defect in any pin, and the PostgreSQL review established why:** nothing
  > committed compares a landed Postgres file's digest across runs. The golden-byte test
  > (`tests/test_postgres_source.py`) runs against scripted rows through a hermetic fake and
  > never executes a live `SELECT`, so it cannot observe this at all; no CI job runs the live
  > extractor yet. **The instability becomes a defect only if a future test asserts byte
  > identity of a landed file across two live runs — and the design says never to try**, since
  > the filename is itself a function of the instant and there is one file per transaction.
  >
  > Flagged on the same discipline §0.4 applies to the ad-hoc `peek()` figures it labels *"not
  > reproducible from anything committed"*. The row count and the byte count are re-derivable;
  > the digest is provenance for one execution and nothing more.
- **`opl.config.landing_postgres_tmp` IS DECLARED AND NOTHING WILL EVER WRITE TO IT** — not
  "has not yet". Every other mode's producer runs on Databricks and stages inside the Volume
  so `os.replace` can make the file appear whole; this one runs on the host and PUTs a
  verified local file. It exists because `registry_landing._landing_and_tmp` resolves both
  directories in ONE dispatch, which is what stops a landing dir and its staging twin coming
  from two different roots. Recorded as a permanent resident of this list rather than a
  temporary one.
- **THE TWO SNAPSHOTS ON TWO DIFFERENT CALENDAR DAYS (T8) HAVE NOT BEEN TAKEN.** One
  snapshot has been read. The scheduling constraint is recorded in
  `databricks/resources/bronze_merchant_job.yml`, which cannot enforce it.
- **`_refuse_a_watermark_before_t2` HAS NEVER FIRED AGAINST A REAL MUTATION.** Its two
  branches are exercised hermetically (`tests/test_extract_merchant_snapshot.py`) and the
  comparison it makes was run against the live server, but the race it closes has not been
  run end to end — `mutate --ready-on` and the extractor have not yet been driven in one
  session.
- **`ref_date_from_instant` HAS RUN ON LOCAL SPARK ONLY**, over synthesised instants. No
  Databricks execution, and no run over the 1,088 real rows.
- **THE INCREMENTAL QUERY'S BOUNDARY WAS EXERCISED AND ITS COMPLEMENT WAS NOT.** Against the
  seeded (un-mutated) table, `--since <the watermark>` returns **0** rows and `--since <one
  microsecond earlier>` returns **1** — which demonstrates the strict `>` on a populated
  boundary and demonstrates nothing about the 48-row miss. That number is Task 6's.

### Added by the Task 4 correction pass

- **THE THREE LIVE POSTGRES TESTS RUN ON ONE WINDOWS BOX AND NOWHERE ELSE.** T4's closing test
  now exists against the shipped `pin_rendering_gucs`, with committed literals under a hostile
  `PGTZ`/`PGDATESTYLE`/`PGOPTIONS`, and it passes — but it carries the `postgres` marker, and
  `addopts = "-m 'not integration'"` deselects it by default while CI runs a bare
  `uv run pytest -v`. **They are the +3 deselected in the collection delta.** T2b's CI job is
  Task 6's, and until it lands the sentence this phase owes is the one F-API had to write about
  local Spark: *verified on one Windows box and nowhere else.*
- **`unhashable_case_divergence` HAS REJECTED NOTHING OUTSIDE A FIXTURE.** The rule is proven to
  fire — and proven that its absence lets U+A7C1 through as `[None]` — but the seeded population
  is bounded below the divergence set on purpose (T10 refuses seeding one of the forty to "prove"
  it), so **no real row has ever met this rule**. That is the intended state and not a gap: the
  guard exists for a manual `psql`, a re-seed, or a mutation script that is not this one.
- **`_refuse_a_since_before_t2` HAS NEVER REFUSED A REAL RACE.** Its discrimination is
  demonstrated by patching the refusal out and watching a full unprotected extract complete, which
  is stronger than most guards in this repository can claim — but the race itself has not occurred
  outside that construction, and it cannot until Task 6 drives two snapshots on two calendar days.
- **`src/opl/unicode_case.py` AND `src/opl/bronze/rule_predicates.py` HAVE RUN ZERO ROWS ON
  DATABRICKS**, like everything else in §3's Task 4 block, and for the same reason: nothing has
  been ingested.
