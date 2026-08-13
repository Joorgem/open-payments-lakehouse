# F3 — Gold / Kimball, and the as-of join that has to change its answer

**Controller-verified** means the controller ran the command and read the output.
**Reported** means a task's own stdout said it. Both appear below and are labelled.

Predictions are published **before** the run that tests them (master protocol §4.5). A
number first written down after the run that produced it is not a prediction.

---

## 0. Task 0 — the four measurements taken before anything is built

The phase plan asked for two. It gets four, because an audit before Task 0 found that
one of the two was unsatisfiable by construction, and that the measurement which
actually decides whether the phase can succeed was absent. Both corrections, and the
nineteen others, are recorded in §0.4.

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

```
P(X = 0) = (1 − p)^1024 = 0.1252      P(X = 1) = 0.2605
P(X = 2) = 0.2703                     P(X ≥ 1) = 0.8748
```

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
  straddling window is needed. A straddle is not merely unnecessary, it is **refused**:
  spanning the 28 days with 10,000 events needs `event_interval_ms ≈ 242,000`, and
  `defects._require_defects_fit` (`defects.py:230-236`) rejects any `late_by_ms` that does
  not exceed the event interval. This is a small **code** change — `_profile()` hardcodes
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
  the first version's `valid_from` gets a low one. Flooring only the top leaves a payment
  before 2026-06-13 resolving to no version of a perfectly well-known company — which
  cannot fire today, and which the T3 remedy above is precisely the thing that would arm.
- **The ghost row is `company_sk = -1` with a NULL `cnpj_basico`, and is never a join
  target.** It is reached as `COALESCE(<as-of lookup>, GHOST_SK)` at fact-build time. It
  must **not** be keyed on `'00000000'`: that is `hub_empresa`'s real lowest key
  (`f1b-run-evidence.md` §2.4), and using it would silently merge every unresolved payment
  onto a real company.

### 0.4 Twenty-one defects found in the phase plan before Task 0 ran

Recorded because the plan is an artefact of this project like any other, and because six
of the previous phase's falsified statements were authored by its own controller.

*(filled in at Task 5 — the audit is summarised in `.plans/HANDOFF.md` until then)*

### 0.5 The measurements

*(pending — this section is written by the run, not before it)*
