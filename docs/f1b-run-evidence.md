# F1b — the synthetic payment stream, and the three defects proven in the workspace

Second of the four sources the job posting names: **event streams**. The generator is
deterministic and its defects are injected on purpose; this document records what the
workspace said when they were ingested.

**Controller-verified** means the controller ran the command and read the output.
**Reported** means a task's own stdout said it. Both appear below and are labelled.

**Every PREDICTION in §2 was published before the run that tested it** — computed from
the profile declarations alone (`opl.generator.profiles`). The *actual* column is
post-run by definition, and the CNPJ resolution in row 10 has no pre-run half at all:
it is a measurement of the extraction and the ingest, not of the declaration. Corrected
after review of PR #16 caught the sentence claiming more than it could. This is why
`drifted_row_count` refuses a profile combining drift with duplicates rather than growing
into something only a Spark session can evaluate.

---

## 1. The runs

Job `opl-bronze-payments`, month `2026-06`.

**Provenance, recorded rather than asserted** — the previous version of this section said
"verified by artefact" and left a truncated digest, which review of PR #16 called out as a
claim without its evidence.

| | value |
|---|---|
| revision the runs were launched for | `e7a4bf13ab30757a26008364226abd6f4e9e17e1` |
| revision stamped in the deployed wheel | `e7a4bf13ab30757a26008364226abd6f4e9e17e1` |
| deployed wheel sha256 | `97fc57d3789220114a4503b96318c748ad5b328eb9f73e10621a7028c0dcac87` |
| local wheel sha256 | *identical* — compared byte-for-byte after downloading the deployed artefact |
| `+dirty` suffix | absent |

The **guard's own output**, not a controller claim (`assert_deployed_revision`, quoted
verbatim from runs `709375879796911` and `592660596679630` — identical in both):

```
assert_deployed_revision: OK -- the installed wheel was built from
e7a4bf13ab30757a26008364226abd6f4e9e17e1, which is the revision this run was
launched for.
```

Both new modules (`opl/contracts/payments.py`, `opl/bronze/generated_landing.py`) were
confirmed present **inside the downloaded wheel**, not merely in the tree.

| profile | run | result | tasks |
|---|---|---|---|
| `clean` | `709375879796911` | **SUCCESS** | promote ran, `fail_on_dq` EXCLUDED |
| `promotable` | `390235151929464` | **SUCCESS** | promote ran, `fail_on_dq` EXCLUDED |
| `drifting` | `592660596679630` | **FAILED** — *by design* | promote EXCLUDED, `fail_on_dq` FAILED |

**The gate forces TWO runs; the third is a control this validation chose to add.** The DQ
gate is all-or-nothing and drift is a reject, so one stream carrying all three defect
classes could never get its duplicates and late arrivals into bronze — they would sit in
staging behind a promote the gate skipped. So `promotable` and `drifting` are the minimum,
and a plan expecting dup, late and drift all measurable in `bronze_payments` would have
found only the first two there. `clean` is **not** forced: it exists so every defect count
has a zero to be measured against, which is what makes 150 and 100 evidence rather than
arithmetic. Corrected after review of PR #16 read the original as claiming the gate
required all three.

---

## 2. Every prediction, marked

**Controller-verified** (`01f19697-2ccd…`, `01f1969a-42f4…`, `01f1969a-5974…`,
`01f1969a-f428…`, `01f1969b-0239…`, `01f19697-47a7…`):

| # | prediction | **actual** | verdict |
|---|---|---|---|
| 1 | `clean` delivers 10,000 rows | **10,000** | confirmed |
| 2 | `clean` carries 0 duplicates | **0** | confirmed |
| 3 | `clean` carries 0 late arrivals | **0** | confirmed |
| 4 | `promotable` delivers 10,150 rows | **10,150** (20,150 total) | confirmed |
| 5 | `promotable` injects **150** duplicates | **150** | confirmed |
| 6 | `promotable` injects **100** late arrivals | **100** | confirmed |
| 7 | `drifting` drifts **2,000** rows | **2,000 quarantined** | confirmed |
| 8 | drift is **quarantined, not absorbed** | `rescued_data_present`, 2,000, nothing else | confirmed |
| 9 | the drift run does **not** promote | `bronze_payments` unchanged at 20,150 | confirmed |
| 10 | **100%** of counterparty CNPJs resolve to `hub_empresa` | **1,024 / 1,024**, 0 unresolved | confirmed |

**Ten predictions, ten confirmed, none adjusted.**

### 2.1 Duplicates — and why the number means something

`COUNT(*) − COUNT(DISTINCT transaction_id)` = **150** across the promoted streams, which
is exactly the injected count.

> **CORRECTION, 2026-08-13 — this said "across BOTH promoted streams" and there are now
> three.** F3 added a fourth payment profile, `between-snapshots`, so that the star's
> as-of join had a "before" side of 2026-07-11 to find; it promotes, and
> `bronze_payments` is now **30,150 rows over 3 batches**
> (`docs/f3-workspace-run-evidence.md` §5). **Only the sentence moves. The number does
> not:** `COUNT(*) − COUNT(DISTINCT transaction_id)` is **150** over all four declared
> profiles and over the three that promote — verified by review at full length before the
> fourth stream ran, and consistent with the run's own `30,150 − 30,000 = 150`.
>
> **The correction waited for the run deliberately.** Written earlier it would have been a
> prediction about a stream that had not landed, published in a document whose whole
> contract is that it records what was measured. Written later it would have been lost:
> the fourth stream is what makes the old sentence false, so the moment it landed was the
> moment the sentence had to change.

**Legitimate repeats do not appear in it**: the clean stream
carries repeats — different `transaction_id`, identical business attributes — and
contributes **0** to this figure. A `transaction_id` derived from a hash of the business
attributes would have collapsed the two and made this number unfalsifiable; the contract
forbids that shape and a test refuses it.

### 2.2 Late arrivals — measured per batch, because a cross-stream comparison is not lateness

| `_batch_id` | rows | late arrivals |
|---|---|---|
| `709375879796911` (clean) | 10,000 | **0** |
| `390235151929464` (promotable) | 10,150 | **100** |

A row is late when its `event_time` precedes the greatest `event_time` already emitted
**within its own batch**. Measuring across the whole table would have let two independent
streams' emission ranges overlap and manufacture lateness neither contains. `_batch_id` is
the run id, so every row traces to the run that produced it without a second identifier.

### 2.3 Drift — the one link that had never run anywhere

Task 3 reasoned this path from source and said so plainly: `cloudFiles` is Databricks-only,
so the **JSON** spelling of the rescue had never executed anywhere, and everything above
the reader was tested while the reader's JSON behaviour was a prediction. **This run is its
first exercise, and it holds.**

- **2,000** rows quarantined — the predicted drifted count exactly.
- **2,000** of 2,000 carry a non-NULL `_rescued_data`. The undeclared `payment_channel` was
  rescued rather than absorbed or dropped.
- `_dq_reject_reason` is **`rescued_data_present` for all 2,000, and nothing else**. The
  highest-precedence rule fired, which is what separates "the gate noticed" from "the gate
  noticed for the right reason".
- `bronze_payments` stayed at **20,150**. The remaining 8,000 undrifted rows of that stream
  sit in staging, unpromoted, because the gate is all-or-nothing.

**Why the column had to stay undeclared.** A drift column *declared* in the contract is not
drift: `events.record_of` walks `COLUMNS`, so it would be emitted on every row; and with a
schema supplied, an undeclared key is what `rescuedDataColumn` catches. Declaring it would
have made "caught, not absorbed" unreachable while every test stayed green. The contract
refuses it at import, in three separate tuples.

### 2.4 The CNPJ integration is real, and the measurement says what it measures

**1,024 distinct counterparties, 1,024 resolved in `hub_empresa`, 0 unresolved.**

Stated precisely: at the generator level this is a tautology — the generator draws only
from the pool it is handed, and a test asserts that directly. **What the workspace query
actually proves is the extraction and the ingest**: that the pool came from the real
69,062,849-key hub, and that no step between generation and bronze cast a key to a number.
`hub_empresa`'s lowest key is `00000000` (**controller-verified**, `01f19689-858a…`), so
the adversarial case is inside the data rather than hoped for — a numeric round trip would
turn it into `0` and drop resolution below 100%. The bronze DQ rules refuse a
non-eight-character key *before* the table holds it.

---

## 3. Cost

| table | rows | files | size |
|---|---|---|---|
| `bronze_payments` | 20,150 | 2 | **0.85 MB** |
| `bronze_payments_quarantine` | 2,000 | 1 | **0.10 MB** |

Task durations, **reported**: generate 27 s, ingest 31 s, gate 24 s, promote 33 s, guard
35 s. **Under a minute per task at 10,000 events.**

**Cost was NOT measured** — these are durations and stored bytes, not compute or storage
price, and ~~Free Edition exposes no billing figure this project has ever read~~. The earlier
wording called this source "free" against the vault's 33.13 GB and ~7.7 h, which is a price
claim drawn from a duration; review of PR #16 was right to refuse it. What can be said is
narrower and still useful: **three orders of magnitude less data and two fewer of wall
clock than the vault**.

> **THE STRUCK CLAUSE WAS FALSE WHEN IT WAS WRITTEN, AND THE DISPROOF WAS ALREADY IN THIS
> FOLDER.** Corrected 2026-08-18, during F4's measurement pass. Free Edition **does** expose
> billing figures and this project **had already read them**:
> ~~`docs/f1.4b-pr-b-run-evidence.md` quotes `ESTIMATED_DBU` values off
> `system.billing.usage` and `system.storage.predictive_optimization_operations_history`,
> measured **2026-07-30**~~ — **the table named there is wrong, corrected the same day before
> this merged.** `grep -i billing docs/f1.4b-pr-b-run-evidence.md` returns **zero hits**: every
> `ESTIMATED_DBU` in that document comes from
> `system.storage.predictive_optimization_operations_history` alone, and **nothing had read
> `system.billing.usage` before 2026-08-18.** The `2026-07-30` is the PO *operation's*
> timestamp; that section was committed **2026-08-03**.
>
> **The retraction survives on the corrected dates and that is why this is a citation fix
> rather than a withdrawal:** a DBU figure was read and published on **2026-08-03**, and the
> sentence claiming none had ever been read was written on **2026-08-12**. The disproof still
> predates the claim by nine days. Caught by the F4 plan's provenance audit.
>
> Re-measured now: `system.billing.usage` holds **1,329 rows** over 2026-07-23 → 2026-08-18,
> with `PREMIUM_JOBS_SERVERLESS_COMPUTE` **93.4172 DBU**, `PREMIUM_SERVERLESS_SQL_COMPUTE`
> **78.2649 DBU** and `PREMIUM_DATABRICKS_STORAGE` **95.5245 DSU**.
>
> **The sentence around it survives and is why this is a correction rather than a retraction:**
> cost was not measured *for this phase*, and refusing to draw a price claim from a duration
> was right — PR #16's review was right to refuse it. What was wrong was the reason given, and
> it was wrong in the direction that closes an avenue: it said the figure could not be had.
>
> **This is the species F-DB's Task 0 was explicitly warned about** — *"F-API published a
> 'first' that was false with the disproof eleven days old in the same folder. Do not inherit a
> first."* Same shape, one document over, and it stood for six days because nobody grepped a
> sibling evidence file before asserting a capability was absent. **A claim that something
> cannot be measured is itself a measurement, and it needs the same evidence as any other.** That is the argument for 10,000 events rather than 50,000: generation
is pure driver-side Python, measured at 50 s per profile at 50,000, and 10,000 at a 5 s
interval preserves the same 13.9-hour span and thirteen one-hour windows that make lateness
mean something.

---

## 4. What is still unexercised

- **Value drift.** Task 2 built schema drift deliberately and deferred value drift; no rule
  exists for a class nothing generates.
- **`reclaim_landing` for a generated source.** There is no reclaim task on this job, and
  that is the landing mode's consequence rather than an omission: reclaim refuses non-zips
  because the sibling archive is the way back to the source. **A generated table's way back
  is the seed.**
- **The three defect classes in one promoted stream.** Structurally impossible while the
  gate is all-or-nothing. Recorded so nobody reads §2 as claiming it.
- **`max_retries: 0` still does not prevent a retry.** `fail_on_dq` ran twice on the
  drifting run, exactly as it did on the vault's effectivity failure. The task is
  side-effect-free, which is why it costs nothing here.
