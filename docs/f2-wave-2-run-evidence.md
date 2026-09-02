# F2 wave 2 — run evidence

**Phase:** the payment enters the Data Vault as a transactional link, with a descriptive
satellite carrying its measures.
**Branch:** `f2w2/payment-link`. **Written:** 2026-09-02.

---

## 0. READ THIS FIRST — THE PHASE DID NOT RUN IN THE WORKSPACE, AND IS NOT CLOSED

**No table described in this document was built by its own code.** Nothing in this phase
executed in the Databricks workspace. Every measurement below was taken on **LOCAL SPARK**, on
the developer machine, against **fixtures** — not against Receita Federal data, not against the
synthetic payment stream at volume, and not against any Delta table in `workspace.default`.

**Protocol §9 cannot be satisfied by this phase.** Conditions 1 and 4 require the tables to
exist, built by their own code, with evidence from a run. Neither is met. The phase merges
without closing, and **no controller may declare it closed.**

Standing decision 6 of the master protocol is the rule this document is written under:
*a path that ran zero rows through it is not a path that works.* What follows reports
unexercised things as unexercised.

### The workspace refusal — MEASURED BY THE F8 SESSION, NOT BY THIS ONE

| operation | state |
|---|---|
| reads, `jobs/update`, `bundle deploy` of an **existing** resource | work |
| `bundle deploy` creating a **NEW** job | **403 `PERMISSION_DENIED`** |
| `jobs/run-now`, SQL warehouse start | **refused** |

Last run terminating `SUCCESS`: **2026-08-28T18:32:13Z**.

**This table is carried from the F8 session's measurement and was NOT re-derived here.** This
phase is credential-free by design: `.env` is deliberately absent from the worktree, and
obtaining a credential is a human gate. An attempt to establish the 403 first-hand was refused
by permission and was not worked around. **So the 403 is reported as F8's measurement, dated,
and not as this session's.**

---

## 1. What is on the branch

```
18d38bd  fix(f2w2): the row that vanished, and seven counts deleted rather than corrected
d15c245  docs(f2w2): ADR 0022, the ledger, and a prediction the tree declined to satisfy
7b4b925  feat(f2w2): the loader routes on the parent's kind, and a lock whose green is not evidence
096e101  feat(f2w2): the payment's measures enter the vault on a link, and two locks that could not fail
cae3eff  feat(f2w2): the payment enters the vault as a transactional link, and a guard that stopped being able to fail
```

`git diff --shortstat origin/main..HEAD` → **51 files changed, 6878 insertions(+), 1029
deletions(-)**.

Two vault tables are registered that were not there before: **`link_payment`**, a
self-referencing transactional link on `hub_empresa` under roles `payer`/`payee` with
`transaction_id` as a width-less dependent-child key; and **`sat_link_payment`**, a DESCRIPTIVE
satellite on that link carrying `amount`, `currency`, `payment_method`.

**`hub_account` and `hub_customer` were NOT built, and that is a refutation rather than a cut.**
Those columns ARE `cnpj_basico`, which `hub_empresa` already keys on, and the digest is taken
over the padded key components without the hub's name — so a second hub on that column would
produce a byte-identical key. ADR 0022 records the argument and supersedes the statements in
earlier accepted documents that promised them.

---

## 2. LOCAL measurements — every one of these is local Spark on fixtures

**Command and count stated together, because a count without its selection is not a
measurement.**

```
uv run pytest tests/vault/test_payments_satellite.py tests/vault/test_payments_vault.py \
              tests/vault/test_satellite_applied_date.py -q --no-header
→ 47 passed in 173.60s          [LOCAL SPARK, FIXTURES]
```

```
uv run pytest tests/test_readme_counts.py tests/triage_agent/test_blast_radius_lock.py \
              tests/test_vault_job_wiring.py -q --no-header
→ 15 failed, 53 passed in 13.68s     [LOCAL, NO SPARK]
```

**The full suite was NOT run on this machine and no full-suite number appears in this
document.** It is measured at roughly seven hours here and was abandoned twice; CI is the
authority for "the suite passes". No CI run is quoted either, because this branch has not been
pushed.

### The fifteen red tests, each attributed

| count | file | why |
|---|---|---|
| 10 | `tests/test_readme_counts.py` | **Deliberate.** The README's counts must be re-derived on the MERGED tree and never on this branch — deriving on the branch is what made PR #32 red twice in F7. |
| 4 | `tests/triage_agent/test_blast_radius_lock.py` | Compare the bundle against the declaration. No job task exists for the two new tables. |
| 1 | `tests/test_vault_job_wiring.py::test_every_registered_vault_table_is_loaded_by_exactly_one_task` | The same fact, stated as totality. |

**The last five are the workspace gap, visible as a test.** They are red because
`databricks/resources/` declares no task for `link_payment` or `sat_link_payment`, and none was
written: the workspace 403s on creating a NEW job resource, and hanging the loader off an
EXISTING job's task list to dodge that would be a distortion outliving the outage.

---

## 3. What exists, what runs, and what has never run

| | state |
|---|---|
| `link_payment` / `sat_link_payment` registered in the vault registry | **yes** |
| their loaders exercised on fixtures, local Spark | **yes** — see §2 |
| a runnable entry point (`vault_load_satellite.py` routes on the parent's kind) | **yes**, as of `7b4b925` |
| a bundle job task | **NO** |
| deployed to the workspace | **NO** |
| **built as Delta tables by their own code** | **NO** |
| **rows loaded in the workspace** | **NONE. Zero.** |

---

## 4. Things known and deliberately not exercised

Recorded in `docs/unexercised-ledger.md` rather than asserted away. The phase opened eight rows
of new debt and closed one. Among them:

- **`fact_payment` still reads `bronze_payments` directly.** The payment is in the vault; the
  FACT does not read it. Re-pointing it is a gold refactor with its own risk and was out of
  scope.
- **`fdb:1504` did NOT close, though the phase plan predicted it would.** The exerciser conflated
  "a second link with a declared derivation on an identifying end" with "the field's second
  consumer". `link_payment` has two derivations, but its only satellite is transactional, builds
  no observation grain and derives no ledger — so `link_merchant_empresa` is still the only link
  whose prefixes reach a grain. **A plan predicting a closure is not evidence of one.**
- **The transactional satellite's delta detector is effectively inert** on today's data, and the
  claim that this is structural was measured false: it is data-dependent.
- **A declared residual in the entry-point lock**, plus the stale-name sweep this phase wrote,
  whose first run was a false green.

---

## 5. How this phase was verified, and what that verification is worth

Every task ran the project's method: implementer, independent reviewer told it did not write the
code, correction, review of the correction. Every new or changed lock was **mutation-tested** —
broken, confirmed red, restored by file copy.

**The method found, in code that was green and self-verified:** a guard that had lost the ability
to fail; a lock that killed zero tests and cancelled against its own companion edit; an
anti-vacuity guard that could not detect vacuity; a loader accepting a pairing that loaded rows
with no ledger and no refusal; and the defect in §4's list — a satellite row silently discarded
because the phase moved a declaration without the DQ control it depended on.

**A caveat on this document's own standard.** The phase ran more review rounds than the method
prescribes — the method's four steps ARE its stopping rule, and treating every new finding as
grounds for another round produced a loop with no exit. Severity fell monotonically: the early
rounds found locks that could not fail, the late ones found sentences with wrong counts. The
residual prose findings were recorded as ledger rows rather than chased, which is what the ledger
is for.

---

## 6. What a person must do before this is worth anything

1. **A workspace that will accept a new job resource.** Everything else waits on it.
2. Then: the bundle job tasks for both tables, a deploy, and a run — after which this document is
   superseded by one that reports rows, not fixtures.
3. `PHASES["0022"]` in `scripts/adr_index.py` must be re-declared with the real merge sha once
   the PR merges; it reads `unmerged` today, and the lock refuses that word the moment git says
   the ADR reached `main`.

**Until step 1 happens, the honest sentence about this phase is the one the README will carry:
the payment is in the vault, and the fact does not yet read it.**
