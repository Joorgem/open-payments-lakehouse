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

The plan's Task 0 asked for eight measurements. F0 owed five of them: `docs/f0-validation-report.md`
carries five premises — the egress claim, UC Volume upload, `bundle validate`, CNPJ snapshot
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

**The boundary is age, not the id.** Both F-API ids resolve; both older ids do not, across two
different phases and two different authors. So this is Databricks' statement-history retention
expiring somewhere between one and two days, **not** the "resolves to nothing" species this
repository has struck twice.

**What follows, and it is a fact about every evidence document in this repository:** the
statement ids published in `docs/f2-wave-1-*.md` and `docs/f3-*.md` — the ids that are the
stated provenance for the vault's and the star's headline numbers — **can no longer be
fetched by anyone.** F-API §3.1 already recorded that a `CLOSED` statement's rows cannot be
read; what is new here is that after ~two days the statement cannot be found at all.

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
**`01f1986b-af6c-1eb4-8585-f5edb3e11638`**, `from_cache: None`:

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

**Two cross-checks that were not asked for and are worth more than the number:**

- **69,062,849 is exactly the published `hub_empresa` count**, so the bronze-side distinct key
  count and the vault-side hub count agree without either being derived from the other.
- **433,702 is not a new number.** `docs/f1.4b-pr-b-run-evidence.md:2324` already published it
  as the empresas month-over-month delta. This measurement reproduces it from a different
  decomposition — a `GROUP BY` over bronze rather than F1.4b's row-count table — and
  68,629,147 + 433,702 = 69,062,849 closes exactly. **It was very nearly published here as a
  discovery**; the grep that caught it is the same one this project's retraction rule is built
  on, run before writing rather than after.

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
because `src/opl/generator/cnpj_pool.py:22` deliberately imports no pyspark and no SDK.

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
| sha256 of the sorted key body | `82e6a447c28befd565eaedf0556bba1752da7b3ba7bdc8b87474cf2eba8aff18` |
| min / max | `00057343` / `98418478` |
| **keys carrying a LEADING ZERO** | **142 of 1,024** |

Committed as `scripts/merchant_cnpj_pool.txt` (`98a5bc9`), with its provenance in the file's
own header. `scripts/` is outside the bundle sync root, so the pin never ships to Databricks.

**The 142 is a design input, not a curiosity.** Any column, cast or parse that treats a CNPJ as
a number destroys 13.9% of this pool silently — which is precisely the failure F1b's "100%
counterparty CNPJ resolution" check exists to catch (*"a cast to numeric eating leading zeros
is the failure it actually catches"*). It is why the Postgres `merchant.cnpj` column is `text`,
and the plan's §4 now says so with this number behind it.

### 0.4 The Postgres side

*(Task 0's container, isolation, GUC, stamp-gap and out-of-order-commit measurements, and the
`scripts/probe_postgres.py` artefact. Written when they land.)*
