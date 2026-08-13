# F3 — Gold / Kimball, and the as-of join that has to change its answer

**This document holds the PREDICTIONS and Task 0's pre-build measurements.**
`docs/f3-workspace-run-evidence.md` holds what the five runs said, every prediction
marked, the vault→star derivation, and what is still unexercised. The two are split for
`docs/f2-wave-1-run-evidence.md` / `docs/f2-wave-1-workspace-run-evidence.md`'s reason
and at the same seam: F2 measured that 751 KB is past what one reviewer reads carefully,
and one file over this project's 800-line limit is the same failure one size down.

**Controller-verified** means the controller ran the command and read the output.
**Reported** means a task's own stdout said it. **Everything in this document is
controller-verified**; the Reported items are in the companion and are labelled there.
The sentence this replaced promised that both "appear below and are labelled" and, until
the runs were written up, none did.

Predictions are published **before** the run that tests them (master protocol §4.5). A
number first written down after the run that produced it is not a prediction.

---

## 0. Task 0 — the four measurements taken before anything is built

The phase plan asked for two. It gets four, because an audit before Task 0 found that
one of the two was unsatisfiable by construction, and that the measurement which
actually decides whether the phase can succeed was absent. Those two corrections and the
rest of the audit are recorded in §0.4, which also says how much of it this repository can
substantiate and how much of it it cannot.

### 0.1 The predictions, published before any query ran

Every value below is derived from declarations in the repository and from counts already
published in `docs/f2-wave-1-workspace-run-evidence.md` and `docs/f1b-run-evidence.md`.
None of them needed a session.

| # | prediction | derived from |
|---|---|---|
| **P1** | `min(event_time)` over `bronze_payments` = **2026-08-01T00:00:00.000Z** | `profiles.py:193` `_WINDOW_START` |
| **P2** | `max(event_time)` = **2026-08-01T13:53:15.000Z** | 9,999 × 5,000 ms after P1 |
| **P3** | `COUNT(DISTINCT event_time)` = **10,000** | both promoted profiles share one window, interval and event count, so their instant sets are *identical* |
| **P4** | payments with `event_time` **before** 2026-07-11 = **0** | P1 |
| **P5** | payments with `event_time` on/after 2026-07-11 = **20,150** | P4 and the published row count |
| **P6** | counterparty companies carrying **two** `sat_empresa_dados` versions = **2** | λ = 1024 × 139,968 / 69,062,849 = **2.075** |
| **P7** | `sat_estabelecimento_dados` rows at `applied_date` 2026-07-11 = **1,656,354** | 1,211,834 changed + 444,520 July-only |
| **P8** | `sat_estabelecimento_endereco` rows at 2026-07-11 = **1,014,134** | 569,614 changed + 444,520 July-only |
| **P9** | both satellites' rows at 2026-06-13 = **71,874,448** each | the June-observed key count |
| **P10** | keys whose two satellites **both** carry a 2026-07-11 row (`B`) ≈ **9,604** | independence: 1,211,834 × 569,614 / 71,874,444 |
| **P11** | the naive `(hk, applied_date)` equi-join at 2026-07-11 returns **≈ 454,124** | 444,520 + `B` |
| **P12** | keys in force at 2026-07-11 (what a PIT table must return) = **72,318,968** | every key has a version at or before that date; 0 departures were measured |

**P7, P8 and P9 are deductions, not estimates**, and they close exactly against the
published totals — which is why they are worth publishing rather than measuring blind:

```
sat_estabelecimento_dados     71,874,448 + 1,656,354 = 73,530,802  ✓ published total
sat_estabelecimento_endereco  71,874,448 + 1,014,134 = 72,888,582  ✓ published total
```

**P6 is the only prediction that gates the phase, and it is genuinely probabilistic.**
The counterparty pool is 1,024 keys drawn by `ORDER BY sha2(concat(cnpj_basico, seed), 256)`
over all 69,062,849 hub keys (`cnpj_pool.py:165-169`) — a uniform sample, because the
digest is order-isomorphic to the 256-bit integer and independent of every satellite
attribute. Only 139,968 companies (0.2027%) carry a second version.

**Binomial throughout — one distribution, which the first spelling of this block was
not.** It wrote `P(X = 0)` as the binomial `(1 − p)^1024` and `P(X = 2)` as the Poisson
`λ²e^(−λ)/2 = 0.2703`, two models in four lines, and rounded both tail values down rather
than to nearest. Corrected against the same `n` and `p`:

```
n = 1024   p = 139,968 / 69,062,849 = 0.0020267   λ = np = 2.0753

P(X = 0) = (1 − p)^1024 = 0.1253      P(X = 1) = 0.2605
P(X = 2) = 0.2706                     P(X ≥ 1) = 0.8747
```

**The correction moves no decision**: `P(X = 0)` is ≈ 1/8 under either model, and that is
the only property §0.3's pre-decided branch rests on. It is recorded because a block that
switches distributions between two adjacent lines is a block whose reader cannot check
any of it.

**So roughly one pool in eight contains no two-version company at all**, and in that case
T3's closing test is unsatisfiable no matter what window any payment profile declares —
`POOL_SIZE` and `POOL_SEED` are module constants precisely so that every profile shares
one company universe (`profiles.py:46-52`). The branch is pre-decided in §0.3 rather than
discovered.

**P10 is the one number here that rests on an assumption.** Independence between an
establishment's `_dados` changes and its `_endereco` changes is not measured anywhere;
a relocation plausibly touches both, which would push `B` above 9,604. **Nothing in the
phase depends on its value** — the collapse population is `1,781,448 − 2B`, which is at
least **642,220** for any `B ≤ 569,614`. P10 is published so that a surprise is visible,
not because a decision waits on it.

### 0.2 What each measurement decides

| measurement | decides |
|---|---|
| P1–P5 | whether Task 4 needs a payment profile with an earlier window (T3) |
| P6 | **whether T3 is achievable at all** — and if not, whether the pool must change |
| P7–P12 | whether the PIT table demonstrates timeline collapse, or is unexercised (T2) |

### 0.3 The pre-decided branches, taken before the answers are known

Per master protocol §A4 — a fork an implementer has to ask about is a plan that was
incomplete.

- **If P4 holds (0 payments before 2026-07-11)**, Task 4 gets **one additional profile
  whose window sits entirely between the two `applied_date`s** — `window_start =
  2026-06-20T00:00:00.000Z`. The existing streams already supply the "after" side, so no
  straddling window is needed.

  > **CORRECTION — this bullet originally claimed a straddle was *refused*, and that was
  > false. Authored by this phase's controller.** It said: "spanning the 28 days with
  > 10,000 events needs `event_interval_ms ≈ 242,000`, and `defects._require_defects_fit`
  > rejects any `late_by_ms` that does not exceed the event interval." The guard is
  > `late_by_ms <= event_interval_ms → raise`. One lateness window is
  > `LATENESS_WINDOW_MS = 3,600,000` ms and the straddling interval is
  > `2,419,200,000 // 9,999 = 241,944` ms, so **3,600,000 exceeds it by 14.9× and the
  > guard passes**. Falsified by Task 4's implementer, who called
  > `_require_defects_fit` on exactly that spec and watched it not raise; re-verified
  > independently by the controller before this correction was written. **A straddling
  > profile could carry all three defect classes.**
  >
  > **The decision stands on a different argument**, which is now the one in the code: a
  > straddle needs **48×** the other profiles' `event_interval_ms`, so the fourth stream
  > stops being *the same delivery moved in time*, and any difference the as-of join
  > showed would have two candidate causes instead of one.

  This is a small **code** change — `_profile()` hardcodes
  `window_start=_WINDOW_START` and its docstring asserts the three streams differ only in
  id, seed and defects — not the pure-data change the phase plan implied.
- **If P6 comes back 0**, the fourth profile gets **its own stratified pool** drawn from
  the two-version stratum, leaving the existing three streams' bytes and F1b's 1,024/1,024
  resolution evidence untouched. `POOL_SEED` is **not** re-rolled: that would re-draw every
  company, destroy F1b's byte-identity and carry the same 12.5% risk a second time.
- **SCD2 `valid_to` closes with `F.lead(applied_date)` in the same window pass,
  append-only.** No MERGE. There is no MERGE anywhere in this repository — every loader is
  `mode("append")` — and proving a new write pattern on serverless at 69.2M rows is not a
  cost this phase takes on to save one window function the vault already runs
  (`loading.py:297`).
- **Half-open intervals, never `BETWEEN`.** `BETWEEN` is inclusive at both ends, so an
  event at exactly a version boundary matches two versions — the multi-match T1's own
  acceptance forbids, manufactured by the operator the plan prescribed.
- **Both sentinels, not one.** The open version's `valid_to` gets a high sentinel *and*
  the first version's `valid_from` gets a low one.

  > **CORRECTION — the reason originally given for the floor was wrong. Authored by this
  > phase's controller.** It said flooring only the top "cannot fire today, and the T3
  > remedy above is precisely the thing that would arm" it. **It is not**: the T3 profile
  > sits at 2026-06-20, which is *after* the earliest `applied_date` of 2026-06-13, so
  > every one of its payments resolves to version 1 whether or not a floor exists. The
  > floor matters only below 2026-06-13, and nothing plans to go there. Falsified by
  > Task 1's implementer.
  >
  > **The floor stays, on a stronger argument the controller did not have.** The tempting
  > conditional version — floor only the versions sitting on the earliest `applied_date` —
  > makes `valid_from`, and therefore any surrogate key hashed over it, **move under a
  > backfill of an earlier snapshot**, silently re-keying a dimension that facts already
  > reference. The unconditional floor is what makes the surrogate key stable.
- **The ghost row is `company_sk = -1` with a NULL `cnpj_basico`, and is never a join
  target.** It is reached as `COALESCE(<as-of lookup>, GHOST_SK)` at fact-build time. It
  must **not** be keyed on `'00000000'`: that is `hub_empresa`'s real lowest key
  (`f1b-run-evidence.md` §2.4), and using it would silently merge every unresolved payment
  onto a real company.

### 0.4 The defects found in the phase plan before Task 0 ran

Recorded because the plan is an artefact of this project like any other, and because
**two of the seven things F2 wave 1 falsified were authored by its own controller, one of
them a correction that overshot** (`docs/f2-wave-1-workspace-run-evidence.md` §5.1).

**The audit.** Three reviewers plus the controller read
`.plans/2026-08-10-f3-gold-kimball.md` before a table was built, one lens each:
arithmetic traceability, buildability, dimensional modelling. **The plan's three rulings
all reach the right decision, and two of them reach it from a premise that is false or a
test that cannot fire.** That combination is why the audit was worth running on a plan
nobody thought was wrong.

**On the count, and this is the honest part.** The working file records **twenty-one**.
Fifteen are named below — every one with a consequence a reader can check against a
committed artefact. **The remaining six are minor and live only in a git-ignored working
file, so the total is a number this repository cannot substantiate**, and it is stated
that way rather than published as if it could. The previous version of this section was
an empty placeholder pointing at `.plans/`, which no outside reader can open, in a
repository that is public.

#### The three that changed what Task 0 measured

| # | the plan said | what is true |
|---|---|---|
| 1 | payments were generated "over a window in 2026-06", so an as-of join risks resolving everything to the **first** version | `profiles.py:193` declares `_WINDOW_START = 2026-08-01T00:00:00.000Z`; the 2026-06 is the **landing-path month parameter**. Every payment is **after** both `applied_date`s, so the join degenerates to `WHERE valid_to = <sentinel>` — the mirror failure, and the worse one, because nothing distinguishes its answer from the naive one |
| 2 | *(nothing)* | **The measurement that decides the phase was absent.** The 1,024-key pool is a uniform draw over 69,062,849 hub keys and only 0.2027% of companies carry a second version, so P(no two-version company in the pool) ≈ 1/8 — and no new payment profile can fix a zero, because `POOL_SIZE`/`POOL_SEED` are module constants shared by every profile on purpose. Published as **P6** in §0.1 and pre-decided in §0.3 |
| 3 | T2 closes on "a key whose two estabelecimento satellites changed on **different** `applied_date`s" | There are exactly two dates and a second version can only land on the later one, so **the test cannot fire**. The plan would have retired a real mechanism on a false negative. Replaced before Task 0 ran with the naive-join / as-of gap (§0.5 T2) |

#### Nine that would have stopped the build

4. **No task in the plan creates a job resource, deploys, or runs anything.** Twelve
   files are needed for one deployable gold table; the plan named two.
5. **There is no MERGE anywhere in this repository** — every loader is `mode("append")` —
   so SCD2 `valid_to` closing had **no pre-decided write pattern**. Settled in §0.3 and
   argued in full in [ADR 0015](adr/0015-as-of-known-time-and-append-only-scd2.md).
6. **Creating the job YAML turns CI red on file creation** (`tests/test_job_yaml_wiring.py`
   asserts the set of job files against its own list).
7. **No gold namespace and no cross-layer collision guard.** A gold table silently
   sharing a Delta name with a vault or bronze table was reachable and unpoliced.
8. **The fact has TWO role-playing foreign keys into one dimension and the plan never
   said so**, which makes its closing test ("every row resolves to exactly one
   `dim_company` version") ill-formed: a correct row resolves to **two**, one per role.
   [ADR 0014](adr/0014-dim-company-at-empresa-grain.md) is where that ruling now lives.
9. **`BETWEEN`, which the plan prescribed, manufactures the multi-match the plan's own
   acceptance forbids** — it is inclusive at both ends, so a payment landing exactly on a
   version boundary matches the closing version and the opening one.
10. **The ghost row contradicted Task 1's own predicted count.**
11. **The 150-duplicate acceptance is a tautology** — `COUNT(*) − COUNT(DISTINCT <grain>)`
    is what deduplication does, by definition of the operation — and **the real trap is
    the 1,600 legitimate repeats** a "natural key" over the business attributes would
    delete while every number still looked plausible.
12. **No cost prediction for a 69.2M-row dimension.** One was written before the run, and
    it was falsified by 17–50× (`docs/f3-workspace-run-evidence.md` §9.1).

#### And one about provenance, which is the one that repeated

13. **Statement id `01f196af-4569-1b11-b465-98b476da2c8b`, offered in the plan as
    controller-verification for six measurements across five tables, resolves to
    nothing.** `grep` finds it in the plan and nowhere else. The six *values* all trace to
    F2 and F1b evidence; the id does not. **Struck rather than re-published** — and then
    committed a second time by the same controller three hours later, which §0.5 records
    at the point it bit.

#### The two §0.3 corrections, both the controller's own

14. **The straddle was said to be *refused* by `_require_defects_fit`. It is not** — the
    guard passes with 14.9× headroom. Falsified by Task 4's implementer; the decision
    stands on a different argument, in §0.3.
15. **The `valid_from` floor's stated reason was false** — the T3 profile sits after the
    earliest `applied_date`, so no payment of it could ever have armed the floor.
    Falsified by Task 1's implementer; the floor stays on a stronger argument, in §0.3
    and in [ADR 0015](adr/0015-as-of-known-time-and-append-only-scd2.md).

### 0.5 The measurements

**Controller-verified**, warehouse `13cf10c85b0f189d`, **six statements**, all
`from_cache: None` — so none of these is the DBSQL result cache answering an earlier
question (`.plans/HANDOFF.md`: "check `result_from_cache` or you will measure the cache").

> **CORRECTION — this said "all seven statements" and published six ids. Authored by this
> phase's controller.** The distinct ids below are `268e`, `2d8f`, `4e3b`, `55c2`, `7388`
> and `b82f`: **six**. The unaccounted seventh produced the six-row per-company table
> further down, **and that table carries no statement id at all.** The id was not
> recorded and is recoverable from no artefact in this repository.
>
> **This is the struck `01f196af` defect inverted, and it is left inverted rather than
> repaired by guesswork.** There, six values wore an id that did not measure them; here,
> six measured rows wear none. Assigning them to the plausible neighbour — `2d8f`, which
> counted the same three companies — would be committing the original defect a third
> time. The table is published as **unattributed**, and a reader is entitled to weigh it
> accordingly.
>
> **The six ids below are 13-character prefixes and a 13-character prefix does not
> resolve.** The full 36-character ids were not recorded at Task 0 and cannot be
> recovered; every id in the companion document is full-length, which is the standard
> from here on.

| # | prediction | **actual** | verdict | statement |
|---|---|---|---|---|
| P1 | `min(event_time)` = 2026-08-01T00:00:00.000Z | **2026-08-01T00:00:00.000Z** | confirmed | `01f196b3-268e…` |
| P2 | `max(event_time)` = 2026-08-01T13:53:15.000Z | **2026-08-01T13:53:15.000Z** | confirmed | `01f196b3-268e…` |
| P3 | `COUNT(DISTINCT event_time)` = 10,000 | **10,000** | confirmed | `01f196b3-268e…` |
| P4 | payments before 2026-07-11 = 0 | **0** | confirmed | `01f196b3-268e…` |
| P5 | payments on/after 2026-07-11 = 20,150 | **20,150** | confirmed | `01f196b3-268e…` |
| P6 | pool companies with two versions = 2 | **3** | **FALSIFIED** | `01f196b3-2d8f…` |
| P7 | `sat_estabelecimento_dados` at 2026-07-11 = 1,656,354 | **1,656,354** | confirmed | `01f196b3-4e3b…` |
| P8 | `sat_estabelecimento_endereco` at 2026-07-11 = 1,014,134 | **1,014,134** | confirmed | `01f196b3-4e3b…` |
| P9 | both satellites at 2026-06-13 = 71,874,448 | **71,874,448** each | confirmed | `01f196b3-4e3b…` |
| P10 | `B` ≈ 9,604 | **69,984** | **FALSIFIED, 7.3×** | `01f196b3-55c2…`, `01f196b3-7388…` |
| P11 | naive equi-join at 2026-07-11 ≈ 454,124 | **514,504** | **FALSIFIED** | `01f196b3-55c2…` |
| P12 | keys in force at 2026-07-11 = 72,318,968 | **72,318,968** | confirmed | `01f196b3-b82f…` |

**Nine confirmed, three falsified. Nothing was adjusted to agree.**

#### The establishment key population, and where each number actually comes from

Several derivations above lean on a three-way split of `hub_estabelecimento`'s keys. **It
was not measured by any statement in this section**, and saying so is the point: the
controller quoted it in a Task 2 dispatch attributed to the four statement ids above, an
implementer checked and found those statements do not carry it. That is the same defect
this phase struck from the plan — six values wearing an id that did not measure them —
**committed a second time by the same controller, three hours after writing the correction.**

**Every row below is a count over PROMOTED BRONZE, never over the RFB source**, and the
labels now say so. The previous version said "observed in 2026-06" with no layer named,
which reads as a statement about what the Receita published — and for the four-key row
that is precisely the wrong reading, because those four **are** in the July RFB file.

| | keys | source |
|---|---|---|
| in promoted bronze for 2026-06 | **71,874,448** | P9 above, measured — both satellites' row count at 2026-06-13 |
| in promoted bronze for **both** months | **71,874,444** | `f2-wave-1-run-evidence.md` §12, F2-era |
| in promoted bronze for 2026-06 only | **4** | 71,874,448 − 71,874,444, and published directly by F2's five-state ledger as `rejected_by_our_gate` for estab 2026-07 (`f2-wave-1-run-evidence.md:647`) |
| in promoted bronze for 2026-07 only | **444,520** | published directly by F2 (`f2-wave-1-run-evidence.md:516` and `:647`, as `absent_before` for estab 2026-06) **and** re-derived twice below |
| in promoted bronze for 2026-07 | **72,318,964** | 71,874,444 + 444,520, and F2's ledger row for estab 2026-07 |
| **hub total** | **72,318,968** | P12 above, measured |

**The derivation is not circular and it closes three ways**, which is why it is trustworthy
even though it is a derivation rather than a measurement:

```
71,874,448 + 444,520 = 72,318,968   ✓ equals the measured hub
P7: 1,656,354 − 1,211,834 = 444,520 ✓ from the _dados layer
P8: 1,014,134 −   569,614 = 444,520 ✓ from the _endereco layer, independently
```

**The 4 are still in force at 2026-07-11, and the reason first published here was the
wrong mechanism.**

> **CORRECTION — this said the four survive because "the RFB retains baixadas". Authored
> by this phase's controller, and it inverts ADR 0010's central result.** F2 measured
> these four specifically and says the opposite: they are **in the July RFB file** and
> absent from July **bronze** because **our own DQ gate rejected them**
> (`docs/f2-wave-1-workspace-run-evidence.md` §2.2). The five-state ledger classified
> them **`rejected_by_our_gate`, not `absent_after_observation`** — `f2-wave-1-run-
> evidence.md:647` publishes the row: estab 2026-07, `rejected_by_our_gate` **4**,
> `absent_after` **0**. Baixada retention is why *empresas* end-dating never fired; it is
> not why these four did.
>
> **The conclusion is unchanged and the argument is replaced.** The four survive because
> **no window closed on them** — which is exactly what ADR 0010's ledger exists to
> guarantee: a key our own quarantine removed must never read as a disappearance. Citing
> RFB retention instead credits the source with a property our gate is responsible for,
> and it is the ADR's headline claim turned inside out.

Which is why P12's "keys in force" is the hub's whole key set (72,318,968) and not July's
bronze-observed count (72,318,964). A PIT table built on the observed count would silently
drop those four.

#### T3 is achievable, and the plan's premise about why is retired

P1–P5 confirm the audit rather than the plan. **Every payment sits on 2026-08-01, after
both `applied_date`s** — so the failure the plan named (everything resolving to the
*first* version) was impossible, and the failure that was actually present is its mirror:
everything resolves to the **last** version, which makes an as-of join byte-for-byte
identical to `WHERE valid_to = <sentinel>`. That is the worse of the two, because nothing
distinguishes its answer from the naive one.

P3 is the sharpest of the five and was not in the phase plan at all: **the two promoted
streams share one window, one interval and one event count, so their instant sets are
identical** — 20,150 rows over 10,000 distinct `event_time` values. Any as-of query that
assumes payment instants are unique across batches is wrong here.

#### P6 — falsified, and it is the falsification that unblocks the phase

Predicted **2** (λ = 2.075); the pool holds **3**. P(X = 3) = 18.7%, so 3 was always a
likely draw and the prediction was the distribution's mode-and-mean rounded, not a bound.
It is recorded as falsified because it is: a prediction that is "close" is a prediction
that was wrong.

**The decision it gated is unchanged, and that is the point of having taken it in
advance.** X ≥ 1, so T3's closing test is reachable with the existing pool, **the
stratified-pool branch in §0.3 does not fire, and F1b's generated bytes are not touched.**

The three, with razão social withheld — two of the three carry `natureza_juridica` 2135
(empresário individual), where the razão social **is** a private individual's name.
**This table carries no statement id**; see the correction at the head of §0.5.

| `cnpj_basico` | `applied_date` | `hash_diff`[:12] | natureza | porte | `capital_social` | razão social |
|---|---|---|---|---|---|---|
| `30115555` | 2026-06-13 | `345f0e1b23d8` | 2135 | 01 | 5000,00 | *(masked)* |
| `30115555` | 2026-07-11 | `afaea4f8ef37` | 2135 | 01 | 5000,00 | *(masked)* |
| `44822028` | 2026-06-13 | `2e2c68cfc3b0` | 2135 | 01 | 5000,00 | *(masked)* |
| `44822028` | 2026-07-11 | `40451c15406a` | 2135 | 01 | **15000,00** | *(masked)* |
| `47070968` | 2026-06-13 | `bb3fc3fb3c8e` | 2062 | 03 | 50000,00 | *(masked)* |
| `47070968` | 2026-07-11 | `de2bae5386ce` | 2062 | 03 | **370000,00** | *(masked)* |

**The character counts this table used to publish are retracted.** `45/44/37/36/26/26`
told a reader how long two private individuals' names are and how much one of them
changed by — a leak of the shape masking exists to prevent, in the two rows where the
razão social is a natural person. `docs/f2-wave-1-workspace-run-evidence.md` §11.2's
precedent is a bare `(masked)`, and the differing `hash_diff` prefixes already carry the
whole demonstration: `30115555`'s payload changed and the visible columns did not.

**`47070968` is the headline case for T3**: `natureza_juridica` 2062 is a sociedade
empresária limitada, not a natural person, and its change is `capital_social`
**50000,00 → 370000,00** — a large, quotable, non-PII attribute. `44822028` is the
control at 5000,00 → 15000,00. `30115555` changes **only** its razão social, so it
demonstrates the mechanism and cannot be quoted; it is listed to show the test set was
not cherry-picked down to the convenient rows.

#### P10 and P11 — the independence assumption was wrong by 7.3×, and it was checked twice

`B`, the establishments whose `_dados` **and** `_endereco` both carry a 2026-07-11 row,
was predicted at **9,604** on an explicit independence assumption and measured at
**69,984**.

**It was measured twice, by two different queries, because 69,984 is exactly half of
139,968** — the two-version company count that is this phase's headline number, from an
unrelated table. A coincidence that neat is the shape of a query defect, so the second
route did not reuse the first's decomposition:

| route | statement | `_dados` 2nd versions | `_endereco` 2nd versions | overlap |
|---|---|---|---|---|
| by `applied_date` = 2026-07-11, minus the 444,520 July-only keys | `01f196b3-55c2…` | 1,656,354 − 444,520 | 1,014,134 − 444,520 | **69,984** |
| by `GROUP BY hk HAVING COUNT(*) = 2` | `01f196b3-7388…` | **1,211,834** | **569,614** | **69,984** |

The two agree, and the second route independently re-derives the change counts **the F2
loaders actually wrote**: `_dados` **1,211,834** and `_endereco` **569,614**, both
NORMALISED. It does *not* reproduce `f2-wave-1-run-evidence.md` §12's published
`_endereco` rate of **570,075** — that figure is a RAW comparison, retracted at
`docs/f2-wave-1-workspace-run-evidence.md` §2.3 where the loader came in 461 rows short
of a prediction built on it (347 case-only changes, 114 whitespace-only). Naming which of
the two is meant matters here for the reason it mattered there: the vault compares
`strip().upper()`, so a derivation that quotes the raw number quietly disagrees with the
table it claims to describe. **The coincidence is a coincidence.**

**What it means: an establishment that changed its registration data is 7.3× more likely
than chance to have also moved.** That is a real correlation in the RFB's monthly delta
and it was not documented anywhere — the phase plan asserted the two satellites had
"independent change rates", which is now measured false. They have *different* rates and
*correlated* changes.

#### T2 — the collapse the PIT table exists for is real in this vault, measured on the two satellites before the table was built

> **CORRECTION — this section was headed "the PIT table is exercised", in the present
> tense, about a table §0.6 records fifty lines later as not existing. Authored by this
> phase's controller.** What Task 0 measured is a property of the **vault data**: two
> satellites of one hub whose change sets barely overlap. The **table** was unexercised
> until it ran, and standing decision §4.6 — a path that ran zero rows through it is not
> a path that works — was refuted here by this phase's own §0.6.
>
> **The claim is now true, and it is true because of the run and not because of an
> edit.** `pit_estabelecimento` was built at 144,193,416 rows and the same collapse was
> re-measured **on the built table**: `docs/f3-workspace-run-evidence.md` §7.
> The wording is corrected rather than deleted, because a reader is owed the difference
> between "measured on the source before the build" and "measured on the artefact", and
> this document had quietly spent one on the other.

The phase plan's own closing test ("a key whose two satellites changed on **different**
`applied_date`s") could not have fired: there are two dates, and a second version can only
land on the later one. Replaced before Task 0 ran with the gap between the naive join and
the as-of answer, **both measured over the two SATELLITES**:

```
naive (hk, applied_date) equi-join at 2026-07-11  =      514,504   measured, sat x sat
keys in force at 2026-07-11 — what a PIT table
must return, measured over the two satellites     =   72,318,968   measured, sat x sat
                                                     ------------
the gap a PIT table is built to close                   71,804,464   rows
```

> **The right-hand number was labelled "PIT-based as-of answer … measured". It was
> not — no PIT table existed.** P12 in §0.1 is careful ("what a PIT table *must*
> return") and this block dropped the qualifier. Restored above.

Decomposed, because a gap that large invites the suspicion that the two queries are not
asking the same question. They are: at 2026-07-11 the naive join can only see keys that
happen to carry a row **in both satellites on that exact date**, while every one of the
72,318,968 keys has a version in force.

- **1,141,850** establishments (1,656,354 − 514,504) whose registration data changed in
  July while their address did not. A naive LEFT join returns a **NULL address** for each
  — an address that is perfectly well known, sitting in the June row, still in force.
- **499,630** (1,014,134 − 514,504) the other way round: moved in July, registration data
  unchanged, so their `_dados` goes NULL.
- **1,641,480** keys in exactly one change set, which is `1,781,448 − 2B` exactly.

**Timeline collapse is not a hypothetical in this vault.** The plan was one query away
from recording the opposite and retiring the table. Whether the **table** demonstrates it
was still open at this point and is settled in `docs/f3-workspace-run-evidence.md` §7.

### 0.6 The "before" baseline, recorded so the build is a measured transition

**Controller-verified** (`01f1973c-331c-101d-8b85-dec1f1121f4f`), taken before any gold
job was deployed or run:

```sql
SELECT table_name FROM workspace.information_schema.tables
WHERE table_schema='default'
  AND (table_name LIKE 'dim_%' OR table_name LIKE 'fact_%' OR table_name LIKE 'pit_%')
```

**Zero rows.** No dimension, no fact and no PIT table existed in `workspace.default` at
this point. §9's first condition is that every artefact the phase promised exists **built
by its own code**; a phase that never records the empty "before" can only assert that,
where this makes it a transition somebody can check. It is also the reason the gold
loaders' idempotence claims mean something later: every table they write, they write onto
nothing.

### 0.7 What Task 0 decided

| question | answer | consequence |
|---|---|---|
| Do the payments straddle 2026-07-11? | **No** — all 20,150 are after both dates | Task 4 gets one profile at `window_start = 2026-06-20T00:00:00.000Z` |
| Can T3's test be satisfied by the existing pool? | **Yes**, 3 companies | the stratified-pool branch does **not** fire; F1b's bytes are untouched |
| Is timeline collapse real here? | **Yes**, on 1,641,480 keys | Task 2 builds the PIT table; it is not skipped |
| Is the PIT table worth its cost? | 71,804,464-row gap | yes, and the number is the argument |

### 0.8 `dim_company` = 69,202,818, which this document never published

**The phase's headline row count appeared nowhere in this evidence file** — only in
`databricks/resources/gold_dim_company_job.yml`'s header (commit `adfe4f4`) and in a
git-ignored working file. It is published here, before the run that tests it, with the
F2 measurements it is derived from, so that a reader can check the derivation instead of
taking the total on trust.

```
hub_empresa                                       69,062,849
companies carrying a SECOND satellite version        139,968
                                                  ----------
sat_empresa_dados versions                        69,202,817
+ the one ghost row                                        1
                                                  ----------
dim_company                                       69,202,818
```

| number | where it was measured |
|---|---|
| `hub_empresa` 69,062,849 | predicted `f2-wave-1-run-evidence.md:1744` (`01f1943d-11d7-16bf-ac0d-316e1828fe92`), **confirmed by the run** `f2-wave-1-workspace-run-evidence.md` §1.1 |
| `sat_empresa_dados` 69,202,817 | predicted `f2-wave-1-run-evidence.md:1745` (`01f1943d-6606-1f0a-b21d-f1f998e9bd3c`), **confirmed by the run** §1.1 |
| 68,922,881 one-version + 139,968 two-version companies | `f2-wave-1-run-evidence.md` §11.3, the satellite-row histogram (`01f19274-e39d-15ff-b5e6-6b104baa93fe`) — no other bucket, summing to 69,062,849 |

**What is counted is satellite VERSIONS, not companies**, and the histogram is what makes
that distinction checkable rather than rhetorical: 68,922,881 companies contribute one row
each and 139,968 contribute two.

> **69,202,818 is also, by coincidence, a RETRACTED figure for `sat_empresa_dados`
> itself** — the raw-payload overcount at `f2-wave-1-run-evidence.md:1773`, one row above
> the true 69,202,817, off a `changed_rows` of 139,969 instead of 139,968 because exactly
> one company in 69 million changed its razão social's case and nothing else. Here it is
> `dim_company`: 69,202,817 versions **+ 1 ghost**. The same integer for two unrelated
> reasons, flagged so the next reader to grep it is not misled.
> [ADR 0015](adr/0015-as-of-known-time-and-append-only-scd2.md) records the same warning.

