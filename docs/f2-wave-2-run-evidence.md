# F2 wave 2 — run evidence

**Phase:** the payment enters the Data Vault as a transactional link, with a descriptive
satellite carrying its measures.
**Branch:** `f2w2/payment-link`. **Written:** 2026-09-02.

---

## 0. THE RUN HAPPENED. This document reports rows.

**Both tables were built by their own code, in the workspace, from a deployed wheel.**

```
Run URL:  https://dbc-56b752e9-bb3f.cloud.databricks.com/jobs/1026518074461012/runs/432076088843817
2026-09-02 18:11:49  "[dev jorge_molinadavid_jm] opl-vault-payments"  RUNNING
2026-09-02 18:14:05  "[dev jorge_molinadavid_jm] opl-vault-payments"  TERMINATED SUCCESS
```

| task | result |
|---|---|
| `assert_deployed_revision` | OK — the installed wheel was built from `8ba13cc`, the revision the run was launched for, and a deploy from a modified tree would have stamped `+dirty` |
| `link_payment` | **+40,000 rows** into `workspace.default.link_payment` from `bronze_payments` over `['2026-06','2026-08']`, joining `['hub_empresa','hub_empresa']`; target previously held 0 |
| `sat_link_payment` | **+40,000 rows** into `workspace.default.sat_link_payment`, keyed on `link_payment`; target previously held 0 |

### THIS DOCUMENT REPLACES ONE THAT SAID THE OPPOSITE, AND THAT IS THE FIRST THING TO KNOW

An earlier version of this file, committed at `2f44796`, opened with *"the phase did not run in the
workspace, and is not closed"* and stated that protocol §9 could not be satisfied. **That was true
when written and is false now.** The workspace returned `403 PERMISSION_DENIED — "Organization …
has been cancelled or is not active yet"` from `POST /api/2.2/jobs/create`, measured by the F8
session on 2026-08-28. **On 2026-09-02 that call succeeded**, verified before anything was built:

```
databricks jobs create --json '{"name":"zz-f2w2-permission-probe", ...}'  ->  {"job_id": 326784013760523}
databricks jobs delete 326784013760523                                    ->  exit 0
```

The probe job was deleted. **Disclosure:** the probe was written as create-then-delete in one
command and its regex did not allow for the space in `"job_id": 326784013760523`, so the delete did
not fire on the first pass and a stray job briefly existed in the workspace. It was deleted in the
next command and the job count returned to 21. Nothing else was touched.

---

## 1. What is on the branch

```
8ba13cc  feat(f2w2): the job that runs the payment vault, and a ledger row closed by its own exerciser
5398461  docs(f2w2): a ledger row that named a dead owner and a lifted outage
2f44796  docs(f2w2): the run evidence, which reports that nothing ran     <- superseded by this file
18d38bd  fix(f2w2): the row that vanished, and seven counts deleted rather than corrected
d15c245  docs(f2w2): ADR 0022, the ledger, and a prediction the tree declined to satisfy
7b4b925  feat(f2w2): the loader routes on the parent's kind, and a lock whose green is not evidence
096e101  feat(f2w2): the payment's measures enter the vault on a link, and two locks that could not fail
cae3eff  feat(f2w2): the payment enters the vault as a transactional link, and a guard that stopped being able to fail
```

**`link_payment`** is a self-referencing transactional link on `hub_empresa` under roles
`payer`/`payee`, with `transaction_id` as a width-less dependent-child key. **`sat_link_payment`**
is a DESCRIPTIVE satellite on that link carrying `amount`, `currency`, `payment_method` — the first
such table in this vault, and the first table in this repository where a table's KIND does not
determine its loader.

**`hub_account` and `hub_customer` were NOT built, and that is a refutation rather than a cut.**
Those columns ARE `cnpj_basico`, which `hub_empresa` already keys on, and the digest is taken over
the padded components without the hub's name — so a second hub there would produce a byte-identical
key. ADR 0022 records the argument and supersedes the earlier accepted statements that promised
them, without rewriting them.

---

## 2. The numbers, and the one that is not 40,150

**Measured in the workspace after the run, one SQL statement:**

```sql
SELECT (SELECT COUNT(*) FROM bronze_payments)                    AS bronze_all,
       (SELECT COUNT(DISTINCT transaction_id) FROM bronze_payments) AS distinct_txn,
       (SELECT COUNT(*) FROM link_payment)                       AS link_rows,
       (SELECT COUNT(*) FROM sat_link_payment)                   AS sat_rows
```
```
bronze_all | distinct_txn | link_rows | sat_rows
40150      | 40000        | 40000     | 40000
```

**Bronze holds 40,150 rows and 40,000 distinct transaction ids. The link holds 40,000, and that is
correct rather than a loss.** `transaction_id` is IN the link's key, so 150 redelivered rows fold
onto the ids they redeliver. Nothing was dropped: `40150 − 40000 = 150` is exactly the redelivery
count.

**The redeliveries are intra-month, not cross-month** — measured, because the two readings differ:

```sql
SELECT COUNT(*) FROM (SELECT transaction_id FROM bronze_payments
                      GROUP BY transaction_id HAVING COUNT(DISTINCT _snapshot_month) > 1)
-> 0
```

So no id appears in both `2026-06` and `2026-08`; the 150 duplicates are all inside `2026-06`
(20,150 rows, 20,000 ids) and `2026-08` carries 20,000 of each.

**Prerequisite, measured before the run rather than assumed:** `hub_empresa` held **69,062,849**
rows. `refuse_unloaded_hubs` runs inside `load_link` before anything is written and refuses an
empty hub, so this is an existence check that passed rather than referential integrity.

**No satellite row carries a NULL `applied_date`:**

```sql
SELECT COUNT(*) FROM sat_link_payment WHERE applied_date IS NULL   ->   0
```

That guard was added by this phase after a closing review found that a payment whose `event_time`
yields no day would be **silently discarded** by `changed_rows`' `left_semi` — `NULL = NULL` is not
true. **It did not fire on this run, and that is not the same as it being unnecessary:** today's
generator always emits a well-formed instant, so the path is unexercised, and the count above is
what "unexercised" looks like when you measure it instead of assuming it.

---

## 3. What the satellite printed, and why it is worth quoting

> `sat_link_payment +40000 rows … THE FOLD COUNT WAS NOT MEASURED (report_diagnostics=false, the
> default), so this run reports none -- which is not it being zero. It is the ONLY count this flag
> buys on a transactional satellite … NO departure count, because this satellite is TRANSACTIONAL
> and derives no observation ledger at all -- an event does not depart, so there is no key that
> could reach absent_after_observation and no window to close. Not a zero, and not a skipped
> measurement either.`

Both sentences are corrections this phase's reviews forced. The first arm existed and said
*"neither diagnostic was measured … re-run to measure them"* — false here, because a departure
count does not exist at any flag setting on a transactional satellite. The second was added because
the run would otherwise have printed `None candidate departures, which is ZERO BY CONSTRUCTION over
a one-month window`: a sentence about a ledger this load never built.

---

## 4. Test state

```
uv run pytest tests/test_vault_job_wiring.py tests/triage_agent/test_blast_radius_lock.py \
              tests/test_job_yaml_launch_guards.py tests/test_job_yaml_wiring.py \
              tests/test_vault_entry_points.py tests/test_size_caps.py
-> 270 passed, 14 skipped        [LOCAL, NO SPARK]

uv run pytest tests/vault/test_payments_satellite.py tests/vault/test_payments_vault.py \
              tests/vault/test_satellite_applied_date.py
-> 47 passed in 173.60s          [LOCAL SPARK, FIXTURES]
```

**The five locks that were red because no job task existed are now GREEN**, closed by the YAML this
document reports running. `tests/test_readme_counts.py` stays red **on purpose**: its counts must be
re-derived on the MERGED tree and never on this branch — doing it on the branch is what made PR #32
red twice in F7. The job added one bundle job and three bundle tasks, so those counts moved.

**The full suite was NOT run here** (~7 hours on this machine) and no full-suite number is claimed.

---

## 5. What still does not happen, and it is the honest limit

**`fact_payment` still reads `bronze_payments` directly.** The payment is in the vault; the FACT
does not read it. Re-pointing it is a gold refactor with its own risk and was explicitly out of
scope — **and it was out of scope by decision, not because of the outage**, so the 403 lifting does
not change it. An outage lifting removes an excuse; it does not create a mandate.

So the honest sentence is now: **the payment is in the vault, and the fact does not yet read it.**

Other things known and not exercised are in `docs/unexercised-ledger.md` rather than asserted away.
One row closed on this run — `vwiring` asked for a YAML task naming `sat_link_payment` and got one —
and it is marked closed and KEPT rather than struck, because a row closed by its own stated
exerciser is evidence the mechanism works.

---

## 6. How this was verified, and what that verification is worth

Every task ran implementer → independent reviewer → correction → review of the correction, and every
new or changed lock was **mutation-tested**: broken, confirmed red, restored by file copy.

**The method found, in code that was green and self-verified:** a guard that had lost the ability to
fail; a lock that killed zero tests and cancelled against its own companion edit; an anti-vacuity
guard that could not detect vacuity; a loader accepting a pairing that loaded rows with no ledger and
no refusal; and the NULL `applied_date` row that vanished.

**A caveat on this document's own standard.** The phase ran more review rounds than the method
prescribes — the four steps ARE the stopping rule, and treating every finding as grounds for another
round produced a loop with no exit. Severity fell monotonically: early rounds found locks that could
not fail, late ones found sentences with wrong counts.

**And the largest correction in this file is to itself.** Every artefact this phase produced stated
the blocker in the present tense, and all of it was true when written and false a day later. The
phase's own ADR 0022 Decision 6 names that shape; this document is its largest instance.

---

## 7. What a person must still do

1. **`PHASES["0022"]` in `scripts/adr_index.py` reads `unmerged`** and the lock refuses that word the
   moment git says the ADR reached `main`. It must be re-declared with the real merge sha.
2. **The README's counts** must be re-derived on the merged tree — deliberately not done here.
3. **PR #36's title and body** still say the phase cannot satisfy §9 and must not be merged to close
   it. **That is now false** and is corrected separately; it is recorded here because a reader may
   reach the PR before the correction.
