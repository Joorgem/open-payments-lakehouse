# F3 — running the star, and what the runs said

Companion to `docs/f3-run-evidence.md`, which published the **predictions** and Task 0's
pre-build measurements. This document records what happened when the gold loaders ran.
Where the two disagree, the disagreement is written down rather than resolved by editing
the prediction.

**Controller-verified** means the controller ran the statement and read the output.
**Reported** means a task's own stdout said it. **Everything below is controller-verified**
and every figure carries the statement id that produced it — and that is a gap as much as
a virtue. **No task's stdout was captured from this phase's runs**, so the one number the
gold loaders report that no query afterwards can recover — the **per-dimension orphan
count** — is absent from this record. §9.4 says so where it matters. F2's evidence carried
both kinds; this one carries one, and it does not pretend otherwise by labelling a
controller measurement as a task's own.

**Every statement id here is the full 36 characters.** `docs/f3-run-evidence.md` §0.5
carries 13-character prefixes because the full ids were not recorded at Task 0 and cannot
be recovered; that is a defect, it is named there, and this document does not repeat it.

**What was built.** Six gold tables in `workspace.default` — five in the star and one
outside it — onto the empty baseline
`docs/f3-run-evidence.md` §0.6 recorded:

```
  dim_company (SCD2, 69,202,818)
        ^                                pit_estabelecimento (144,193,416)
        |  payer_company_sk                  |
        |  payee_company_sk                  +-- hub_estabelecimento
  fact_payment (30,000) --> dim_date         +-- sat_estabelecimento_dados
                        --> dim_channel      +-- sat_estabelecimento_endereco
                        --> dim_currency
                                          NOTHING IN THE STAR JOINS TO IT
```

`pit_estabelecimento` is a **mechanism demonstration outside the star** and is drawn
apart on purpose. `dim_company` is at empresa grain, `dim_merchant` at estabelecimento
grain is deferred, `dim_geography` is twice blocked — so no fact and no dimension in this
lakehouse reaches the PIT table. [ADR 0014](adr/0014-dim-company-at-empresa-grain.md) is
the ruling; `src/opl/gold/pit.py` states the one change that would pull it in.

---

## 1. The deploy, verified by artefact — twice, and the second check went further

Standing decision §4.2: verify a deploy by artefact, never by its success line. It was
verified twice because a review finding forced a redeploy between them — `opl/gold/pit.py`
stamped its `load_date` through `F.lit(datetime)`, i.e. the **driver's** operating-system
zone, where every other instant gold writes is parsed by Spark in the **session** zone.
Fixed in `fcd199b`.

| | first deploy | redeploy after the `instant_literal` fix |
|---|---|---|
| revision | `2eb93f25d9a26c4cc3cb94c6cee152d458c60630` | `fcd199bc466482473889dc7843fedd204d5c6dc4` |
| local wheel sha256 | `b948986c192d4121f9d66e795beed79a0eac7db2fa6e9e7421d8c93fd4a9c593` | `5e5440d06b8e62705afc570b0e2ec16544ee862d7db760dfa737a3596ec7c8f5` |
| deployed wheel sha256 | **identical to local** | **identical to local** |
| `+dirty` stamp | absent | absent |
| gold modules inside the wheel | 9 | 9 |

**The second check did something the first did not, and it is the check worth copying.**
A matching sha256 proves the bytes on the workspace are the bytes on the box; it does not
prove the *fix* is in them. So the **downloaded** artefact was opened and `opl/gold/pit.py`
read out of it: it contains `instant_literal(load_date)` and **does not** contain
`F.lit(load_date)`. The fix is verified in the thing that ran, not in the tree it was
written in.

---

## 2. The five runs

Sequenced as `.plans/HANDOFF.md` prescribed: the fourth payment stream first, so the fact
has a "before" side of 2026-07-11 to find; then the dimension the fact's two role keys
resolve into; then the conformed three; then the fact; then the PIT table, last and
droppable.

| # | job | run id | result | task durations |
|---|---|---|---|---|
| 1 | `opl-bronze-payments` (`profile=between-snapshots`, `month=2026-08`) | `959200938801308` | SUCCESS | not captured |
| 2 | `opl-gold-dim-company` | `30161267975961` | SUCCESS | guard 31 s, **`dim_company` 120 s** |
| 3 | `opl-gold-conformed-dimensions` | `867440677025467` | SUCCESS | guard 32 s, channel 32 s, currency 32 s, **date 35 s** |
| 4 | `opl-gold-fact-payment` | `783065297599432` | SUCCESS | not captured |
| 5 | `opl-gold-pit-estabelecimento` | `276634505933379` | SUCCESS | guard 32 s, **`pit_estabelecimento` 168 s** |

**Two runs' task durations were not captured and are not reconstructed.** Runs 1 and 4
succeeded; how long their tasks took is unrecorded, so `gold_fact_payment_job.yml`'s
published cost interval (5–40 min) is **unmarked below** rather than marked confirmed.
A prediction nobody measured is not a prediction that held.

**The provenance guard is the dependency root of all five jobs and all five runs
succeeded, so it accepted five more times** — **eleven** across F2 and F3, never once
refusing. Three of the five durations were captured (31 s, 32 s, 32 s); the other two are
part of the two runs above whose task durations are not recorded. Its **refusal** half is
still unexercised in the workspace after this phase too (§9.4), and five more accepts are
not evidence about it.

---

## 3. `dim_company` — the SCD2 dimension, and the number the whole phase is grained on

**Controller-verified** (`01f19748-a274-100a-8c7f-640ea1e2adf6`):

| | predicted | **actual** | |
|---|---|---|---|
| rows | 69,202,818 | **69,202,818** | ✅ |
| distinct `company_sk` | = the row count | **69,202,818** | ✅ **injective** |
| ghost rows | 1 | **1** | ✅ |
| NULL `valid_to` | 0 | **0** | ✅ |
| distinct companies | 69,062,849 | **69,062,849** | ✅ **= `hub_empresa`** |

The derivation of 69,202,818 is published at `docs/f3-run-evidence.md` §0.8, with the F2
statements each of its parts was measured by. **Three counts were asked, not one**, for
`hub_empresa`'s reason in F2 §1.1: a correct total with a repeated surrogate key would
pass a row count and mean two dimension versions had collided onto one key — which every
fact joining on it would then match twice, silently. Distinct keys equalling rows is what
makes this evidence of injectivity rather than evidence of a row count.

**Distinct companies equalling the hub exactly** is the other half: `_bounded` joins
`how="inner"`, so a satellite version whose hash key matched no hub row would be dropped
and the load would still report success. That path is now closed by
`_refuse_a_count_that_is_not_every_version_plus_the_ghost`, and the measurement above is
what says the guard had nothing to catch.

**No `valid_to` is NULL.** Both ends carry a sentinel — 1970-01-01 and
2999-12-31 23:59:59.999999 — and neither is NULL, which is the commoner spelling and is
refused twice over in [ADR 0015](adr/0015-as-of-known-time-and-append-only-scd2.md): NULL
compares false in a join, so the omission **loses rows silently** rather than failing.

---

## 4. The three conformed dimensions, and the two numbers that say what they are

**Controller-verified** (`01f1974c-6fb2-1965-8ef2-5cbda9e7cc2b`, which counted all six
gold tables in one statement):

| dimension | members predicted | **members** | rows incl. ghost | fact-side cardinality |
|---|---|---|---|---|
| `dim_date` | 50 | **50** | 51 | **2** |
| `dim_channel` | 5 | **5** | 6 | **5** |
| `dim_currency` | 1 | **1** | 2 | **1** |

**The fact-side cardinality is the number this trio exists to publish**, and it is
counted from the fact on every load rather than described. `dim_currency` has one member
and the fact reaches one of them: a dimension of cardinality 1 **cannot be wrong, and no
test over it can fail**. `dim_date` holds 50 days and the fact touches **two** of them —
2026-08-01 from the three original streams and 2026-06-20 from `between-snapshots`.
`dim_channel` is the one of the three that is not a constant column wearing a dimension's
name: five declared rails, five reached.

**`dim_date`'s span needed no rebuild between runs 1 and 3, and the published reason for
running them in that order was wrong.** `covered_span` reads the fact's own days **and**
`sat_empresa_dados`' `applied_date`s, and anchors the low end on the earlier of the two —
which is **2026-06-13**, the June RFB snapshot, and that precedes the fourth stream's
2026-06-20. So the calendar covered the new stream's days before the stream existed. The
ordering was harmless; the rationale ("building it before the June stream lands gives a
calendar that does not cover the payments the fact will carry") is false, and it is
corrected here so nobody "repairs" a correct calendar on it.

---

## 5. `fact_payment` — the grain, the repeats, and the two role keys

**Controller-verified** (`01f19749-caeb-18cf-bb38-ad65df90e3be` for the counts,
`01f19749-c41a-1890-9b9d-8191ffa95577` for the schema,
`01f19749-e7e7-1d69-9c5c-68524985aab7` for the tuples and repeats,
`01f19748-c3dc-17ed-a2d4-944a622778c3` for bronze after the fourth stream):

| | predicted | **actual** | |
|---|---|---|---|
| `bronze_payments` after the 4th stream | 30,150 | **30,150** | ✅ |
| …distinct `transaction_id` | 30,000 | **30,000** | ✅ |
| …`_batch_id` values | 3 | **3** | ✅ |
| …payments **before** 2026-07-11 | 10,000 | **10,000** | ✅ *(0 at Task 0)* |
| `fact_payment` rows | 30,000 | **30,000** | ✅ **grain enforced** |
| …distinct `transaction_id` | 30,000 | **30,000** | ✅ |
| …distinct business tuples | 27,600 | **27,600** | ✅ |
| **bronze**'s distinct business tuples | 27,600 | **27,600** | ✅ dedup changed none |
| …legitimate repeats | 2,400 | **2,400** | ✅ `= 3 × 800` |
| …rows resolving to the ghost, both roles | 0 | **0** | ⚠️ **UNEXERCISED, not success** |
| …distinct `event_date_key` | 2 | **2** | ✅ |
| …channels / currencies reached | 5 / 1 | **5 / 1** | ✅ |

**`30,150 − 30,000 = 150` is the tautology and `2,400` is the number that means
something.** "The build removed 150 duplicates" is `COUNT(*) − COUNT(DISTINCT grain)` by
definition of the operation and comes out right whichever column deduplication was taken
over. What is published instead is the **legitimate repeats**: rows the fact holds beyond
its 27,600 distinct business tuples. Three promoted streams inject 800 repeats each — a
different `transaction_id`, an identical business-attribute tuple — and a fact
deduplicated on a "natural key" over (payer, payee, amount, currency, method) would have
deleted all 2,400 real payments and returned a plausible 27,600 rows with every other
number intact.

**Bronze's own distinct-tuple count is the control**, and it is 27,600 on both sides: the
deduplication removed 150 redeliveries and changed **what the payments were** not at all.

**The ghost on both role keys is 0 and that is reported as unexercised, not as success.**
See §9.4.

---

## 6. The as-of join changes its answer — the phase's headline

**Controller-verified** (`01f19749-d757-1803-907e-e390bcb3295f`). Company `47070968`,
`natureza_juridica` **2062** — a sociedade empresária limitada, **not** a natural person,
which is why it is the quotable case of the three. Razão social masked throughout; the
other two pool companies with two versions are `natureza_juridica` 2135, empresário
individual, where the razão social **is** a private individual's name.

| side of 2026-07-11 | `company_sk` | `capital_social` | validity interval | `is_current` | payment legs |
|---|---|---|---|---|---|
| **before** | `-8897288640841010596` | **50000,00** | [1970-01-01, 2026-07-11) | false | 18 |
| **on/after** | `7138330321006406353` | **370000,00** | [2026-07-11, 2999-12-31 23:59:59.999999) | true | 38 |

**One company, two surrogate keys, two attribute values, chosen by nothing but the
payment's own `event_time`.** This is the sentence the phase existed to be able to write,
and until run 1 landed it was unwritable: at Task 0 every payment in bronze sat on
2026-08-01, **after both RFB `applied_date`s**, which made the as-of lookup bit-identical
to `WHERE valid_to = <sentinel>` — a join with extra columns and no way to tell its answer
from the naive one. The `between-snapshots` profile put 10,000 payments in the gap, and
the "before" side above is what they resolve to.

**Both sentinels behave as designed and both are visible in one row each.** The floored
`valid_from` of 1970-01-01 is what gives the earlier version an interval a 2026-06-20
payment can fall inside at all; the 2999-12-31 ceiling is what makes `is_current` a
derived column rather than a NULL test. Neither is Kimball's conventional 1900/9999, and
[ADR 0015](adr/0015-as-of-known-time-and-append-only-scd2.md) records that this is a
measured limit of this project's dev box written into a production table.

**What the two keys assert is narrower than it looks**, and 0015 is where that is said:
`applied_date` is the RFB's snapshot reference date, so the star answers as of **known**
time. The 18 payments on the "before" side get June's `capital_social` **correctly, as a
statement about what was known** — the real change lies somewhere in the 28-day interval
`(2026-06-13, 2026-07-11]` and this source cannot narrow it.

---

## 7. `pit_estabelecimento` — the collapse, now measured on the built table

**Controller-verified** (`01f1974c-3010-1f0c-a49b-81c51c51323d` for the layers,
`01f1974c-5f64-1fdb-a8cc-377cdddeddc3` for the collapse):

| | predicted | **actual** | |
|---|---|---|---|
| `pit_estabelecimento` rows | 144,193,416 | **144,193,416** | ✅ |
| …at `as_of_date` 2026-06-13 | 71,874,448 | **71,874,448** | ✅ |
| …at `as_of_date` 2026-07-11 | 72,318,968 | **72,318,968** | ✅ **= `hub_estabelecimento`** |

```
naive (hub_estabelecimento_hk, applied_date) equi-join at 2026-07-11 =      514,504
pit_estabelecimento as-of answer at 2026-07-11                       =   72,318,968
                                                                        ------------
rows the PIT table recovers                                          =   71,804,464
```

**This is what closes the highest-severity defect the docs review found in this phase's
own evidence.** Before the run, `docs/f3-run-evidence.md` was headed "the PIT table is
exercised" — present tense, about a table its own §0.6 recorded fifty lines later as not
existing. What Task 0 had measured was a property of the **vault data**: two satellites
whose July change sets overlap on only 69,984 keys. **It is now a property of the table**,
and the correction in that document says so rather than deleting the tension.

**The two layers are not the same size and that is the mechanism, not an accident.** At
2026-06-13 the table holds 71,874,448 rows — the keys promoted bronze had observed by
then. The 444,520 establishments that first appear in July are **absent** from that layer,
not NULL in it: a key with nothing to say at an instant must not appear at all, which is
what a PIT owes where a dimension owes an unknown member. At 2026-07-11 it holds the
hub's whole key set, **including the 4 our own DQ gate rejected in July**, whose June
versions are still in force.

**Nothing downstream breaks if this job is never run**, and no other gold job in this
repository can say that. It ran anyway, on a 71,804,464-row argument.

---

## 8. The vault → star derivation, column by column

Master route §3 requires this phase to produce "the derivation documented, vault → star",
and it was absent from every artefact on this branch. This is it.

**The shape of the derivation, in one line each:**

- **satellite → SCD2 version chain.** `sat_empresa_dados` holds one row per (company,
  `applied_date`). Ordering those rows per business key and taking `F.lead(applied_date)`
  in the **same window pass** turns a set of snapshots into a chain of half-open
  intervals. No `MERGE`: the close is a **column**, computed before anything is written.
- **version chain → half-open interval.** `valid_from <= t < valid_to`, never `BETWEEN`,
  which is inclusive at both ends and would match a boundary payment against the closing
  **and** the opening version.
- **interval → surrogate key.** `company_sk = xxhash64(cnpj_basico, applied_date)` — over
  the pair the **vault** already keys a version on, and deliberately **not** over
  `valid_from`, which is a TIMESTAMP and moved with `spark.sql.session.timeZone`.
- **hub → business key → role FK.** `hub_empresa` supplies `cnpj_basico`;
  `bronze_payments` carries two of them, one per role; each resolves independently as of
  the payment's own instant into the same dimension.

### 8.1 `dim_company` ← `sat_empresa_dados` + `hub_empresa`

| gold column | vault source |
|---|---|
| `company_sk` | `xxhash64(hub_empresa.cnpj_basico, sat_empresa_dados.applied_date)` |
| `cnpj_basico` | **`hub_empresa`** — the satellite does not carry it, so this is a join and not a projection, and that join is the price of DV2's own decomposition |
| `razao_social`, `natureza_juridica`, `capital_social`, `porte_empresa` | `sat_empresa_dados` payload (`src/opl/vault/domains/cnpj.py:140-144`) |
| `valid_from` | `sat_empresa_dados.applied_date` as an instant — or the **1970-01-01 floor** on a company's first version |
| `valid_to` | `F.lead(applied_date)` over (business key, ordered by `applied_date`) — or the **2999-12-31 ceiling** on the open version |
| `is_current` | derived: `valid_to == ceiling` |
| `load_date` | the job's `{{job.start_time.iso_datetime}}` — one instant per run, shared by every task |
| `record_source` | `sat_empresa_dados.record_source`; `opl.gold.dimensions:ghost` on the ghost row |

The ghost is `company_sk = -1` with a **NULL** `cnpj_basico`, spanning all time. It is
never a join target: it is reached only as `COALESCE(<as-of lookup>, -1)` at fact-build
time. It is **not** keyed on `'00000000'`, which is `hub_empresa`'s real lowest business
key — using it would have merged every unresolved payment onto a real company.

### 8.2 `fact_payment` ← `bronze_payments` + `dim_company` + the conformed three

| gold column | source |
|---|---|
| `payer_company_sk` | as-of lookup into `dim_company` on `payer_cnpj_basico` where `valid_from <= event_instant < valid_to`, then `COALESCE(…, -1)` |
| `payee_company_sk` | the same lookup on `payee_cnpj_basico` — **the same dimension, played twice** |
| `event_date_key` | **derived, not joined**: `yyyyMMdd` of the first ten ISO characters of `event_time` |
| `channel_key` | **derived**: `xxhash64(payment_method)` |
| `currency_key` | **derived**: `xxhash64(currency)` |
| `transaction_id` | `bronze_payments` — a **degenerate dimension**, a key with no dimension table |
| `amount` | `bronze_payments.amount` cast to `decimal(18,2)`, the scale being BRL's minor unit |
| `event_time` | `bronze_payments.event_time`, ISO-8601 text carrying `Z` |
| `load_date` / `record_source` | the run's instant / bronze's record source |

**The three conformed keys are derived rather than looked up**, which is a pre-decision of
the conformed module and not a shortcut the fact took: a day's key **is** its calendar
position and an enumerated member's key is a hash of the member, both computable from a
column the fact row already carries. **What it costs is stated in §9.4** — a derived key
has no lookup to coalesce onto a ghost, so an out-of-domain value produces an **orphan**,
counted after the write, not an unknown member.

**The instant is parsed with its zone REQUIRED** — `to_timestamp(text,
"yyyy-MM-dd'T'HH:mm:ss.SSSXXX")`, never `CAST(… AS TIMESTAMP)`. The cast is not wrong
about the instant while the text carries `Z`; it is wrong about the **contract**, because
it accepts text carrying no offset and resolves it through the session timezone. The
explicit parse yields NULL there and the build refuses, naming the rows.

### 8.3 The conformed three ← the contract and the two tables the star dates against

| dimension | members from |
|---|---|
| `dim_date` | **derived**: `min`/`max` over the fact's own `event_time` days **and** over `sat_empresa_dados.applied_date` — because `dim_company`'s version boundaries sit on the latter, and a calendar that cannot name the day a version opened is one the star's own history falls outside of |
| `dim_channel` | **declared**: `opl.contracts.payments.PAYMENT_METHODS`, never `SELECT DISTINCT` over the fact |
| `dim_currency` | **declared**: `opl.contracts.payments.CURRENCIES` |

Members are the contract's domain and never the fact's observed values, because a
dimension that cannot contain an unobserved member can never report that a rail went
unused — its member count and its fact-side cardinality would be one number written twice.

### 8.4 `pit_estabelecimento` ← `hub_estabelecimento` + both its satellites

| gold column | vault source |
|---|---|
| `hub_estabelecimento_hk` | `hub_estabelecimento` |
| `as_of_date` | **measured, never declared**: the distinct union of `applied_date` over both satellites — {2026-06-13, 2026-07-11}, the only instants at which any answer in this vault changes |
| `sat_estabelecimento_dados_applied_date` | `max(applied_date <= as_of_date)` over that satellite |
| `sat_estabelecimento_endereco_applied_date` | the same over the other |
| `load_date` / `record_source` | the run's instant / this module |

**No ghost row, and the absence is a decision.** Nothing holds a foreign key to a PIT
table — it is read by joining to it from the hub side, never by dereferencing a key stored
elsewhere — so an unknown member would be a row no query could arrive at. What a PIT owes
instead is that a key with nothing to say at an instant is **absent**, which is the
444,520-row difference between its two layers.

---

## 9. Every prediction, marked

| # | prediction | **actual** | verdict |
|---|---|---|---|
| 1 | `bronze_payments` after the 4th stream = 30,150 | **30,150** | confirmed |
| 2 | …distinct `transaction_id` = 30,000 | **30,000** | confirmed |
| 3 | …batches = 3 | **3** | confirmed |
| 4 | …payments before 2026-07-11 = 10,000 | **10,000** | confirmed |
| 5 | `dim_company` = 69,202,818 rows | **69,202,818** | confirmed |
| 6 | …distinct `company_sk` = the row count | **69,202,818** | confirmed, injective |
| 7 | …ghost rows = 1 | **1** | confirmed |
| 8 | …NULL `valid_to` = 0 | **0** | confirmed |
| 9 | …distinct companies = 69,062,849 | **69,062,849** | confirmed, = the hub |
| 10 | `dim_date` / `dim_channel` / `dim_currency` = 50 / 5 / 1 members | **50 / 5 / 1** (51 / 6 / 2 rows with ghosts) | confirmed |
| 11 | `fact_payment` = 30,000 rows | **30,000** | confirmed |
| 12 | …distinct `transaction_id` = 30,000 | **30,000** | confirmed, grain enforced |
| 13 | …distinct business tuples = 27,600 | **27,600** | confirmed |
| 14 | …**bronze**'s distinct tuples = 27,600 | **27,600** | confirmed, dedup changed none |
| 15 | …legitimate repeats = 2,400 | **2,400** | confirmed, `= 3 × 800` |
| 16 | …ghost on both role keys = 0 | **0** | **UNEXERCISED, not success** |
| 17 | …distinct `event_date_key` = 2 | **2** | confirmed |
| 18 | …channels / currencies reached = 5 / 1 | **5 / 1** | confirmed |
| 19 | `pit_estabelecimento` = 144,193,416 rows | **144,193,416** | confirmed |
| 20 | …at 2026-06-13 = 71,874,448 | **71,874,448** | confirmed |
| 21 | …at 2026-07-11 = 72,318,968 | **72,318,968** | confirmed |
| 22 | naive equi-join at 2026-07-11 = 514,504 | **514,504** | confirmed |
| 23 | PIT as-of answer at 2026-07-11 = 72,318,968 | **72,318,968** | confirmed |
| 24 | **`dim_company` wall clock 2,100–6,000 s** | **120 s** | **FALSIFIED, 17–50× fast** |
| 25 | **`pit_estabelecimento` wall clock 1,800–10,800 s** | **168 s** | **FALSIFIED, 11–64× fast** |
| 26 | `fact_payment` wall clock 5–40 min | *not captured* | **unmarked** |

**Twenty-two confirmed, one reported as unexercised rather than confirmed, two falsified,
one unmarkable.** Nothing was adjusted to agree.

**Every row-count prediction in this phase landed exactly.** That is worth one sentence
and no more: they are derivations from counts the vault had already published, and a
derivation that closes is a derivation, not a discovery. The two numbers that were
genuinely uncertain — the two wall clocks — are both in the falsified rows.

### 9.1 Two cost extrapolations falsified, and kept falsified

| | published before the run | published where | **actual** | error |
|---|---|---|---|---|
| `dim_company` | ~35 min – 1 h 40 m (2,100–6,000 s) | `.plans/HANDOFF.md`'s run sequence | **120 s** | **17–50× over** |
| `pit_estabelecimento` | 0.5–3 h (1,800–10,800 s), central ~1.5 h | `databricks/resources/gold_pit_estabelecimento_job.yml` | **168 s** | **11–64× over** |

**Both were built from F2's satellite loads and both overestimated by more than an order
of magnitude, for one reason.** F2 measured the vault's satellites at 5,428–5,635 s for
~72M rows and both extrapolations scaled that on **row count**. But
`docs/f2-wave-1-workspace-run-evidence.md` §2.4 had already said where the cost actually
lives, on real data: a three-component hub key cost **6.6× a one-component key for 1.05×
the rows**, and the effectivity satellite — which re-derives its entire input twice
because serverless forbids caching — is the **cheapest per row** of any loader there.
**The cost is hashing width × row count, not passes and not rows alone.**

**The satellites hash every source row into a new digest. The gold loaders read Delta and
hash a DATE or a short string.** `dim_company` hashes `(cnpj_basico, applied_date)` — an
8-character key and a date — where `sat_empresa_dados` normalises and length-prefixes a
four-column payload for all 69.2M rows. `pit_estabelecimento` hashes **nothing at all**:
it reads hash keys the vault already computed and moves dates.

**The extrapolations ignored their own source's conclusion.** They cited the F2 cost
tables and used the row counts out of them while leaving behind the sentence those tables
were written to support.

**This is recorded as a failure and not as a happy surprise.** A prediction wrong by 50×
in the harmless direction is still wrong, and the next phase that extrapolates gold cost
from vault cost will be wrong the same way and might not be lucky about the direction.
The correct anchor for the next one is: **what does this loader hash, and how wide?**

### 9.2 What this phase falsified, and who authored each error

Nine, counting Task 0's three. **Five were only reachable by running something** — the
three Task 0 measurements and the two cost extrapolations — which is the argument for
running the phase rather than reasoning about it. **The other four were reachable by
reading, and a reviewer read them.** All four are the controller writing about its own
work, which is the uncomfortable half of this list.

1. **P6 — two two-version companies in the pool. There are three.** A distribution's mode
   rounded and published as a point value. Recorded as falsified because a prediction that
   is *close* is a prediction that was wrong; the decision it gated was pre-decided and is
   unchanged. *Authored by this phase's controller.*
2. **P10 — the two estabelecimento satellites change independently. They do not.**
   Predicted overlap 9,604 on an explicit independence assumption; measured **69,984**, a
   7.3× correlation nobody had documented. *The "independent change rates" claim is the
   phase plan's; this phase's controller published P10 on it and flagged the assumption as
   an assumption before the run, which is why the surprise was visible.*
3. **P11 — the naive equi-join returns ≈454,124. It returns 514,504.** A consequence of
   2, not an independent error.
4. **`dim_company`'s cost, 17–50× over.** *Authored by this phase's controller.*
5. **`pit_estabelecimento`'s cost, 11–64× over.** *Authored by this phase's controller.*
6. **"The PIT table is exercised" — present tense about a table that did not exist.**
   Standing decision §4.6 is the rule it broke, and the phase's own §0.6 is the evidence
   that broke it. *Authored by this phase's controller.*
7. **The 4 June-only establishments credited to RFB baixada retention.** They are **in**
   the July RFB file; our own DQ gate rejected them, and the ledger classified them
   `rejected_by_our_gate`. The conclusion was right and the argument **inverted ADR 0010's
   central result**. *Authored by this phase's controller.*
8. **"All seven statements" published beside six ids**, with the seventh table carrying
   none — **the struck `01f196af` defect inverted**, in the document that struck it.
   *Authored by this phase's controller.*
9. **"Only `dim_date`'s ghost is reachable even in principle."** Commit `49b9344` said it;
   **`b05a435`, five commits later, states the mechanism that makes it false** — a derived
   key has no lookup to coalesce onto a ghost, so a day outside the calendar's span
   produces an **orphan**, not an unknown member. **None of the three conformed ghosts is
   reachable**, `dim_date`'s included. Retracted here by name. *Authored by this phase's
   controller.*

**SIX DEFECTS IN THIS PHASE WERE THE CONTROLLER WRITING ABOUT ITS OWN WORK**, and that is
a different and worse category than getting a prediction wrong. Getting a number wrong is
what predictions are for; getting the **record** wrong defeats the mechanism the whole
project runs on. The six are **items 6, 7 and 8 above — the three HIGH defects the docs
reviewer found, all three in the controller's own evidence prose** — plus the provenance
repeat and the two corrections that overshot, all three recorded below. **Item 9 is a
seventh of the same species** and is listed separately only because it lives in a commit
body rather than in evidence: a claim that a later commit of this same phase contradicted,
and that nothing retracted until now.

- **A provenance error committed after writing the correction for the identical error.**
  The controller struck `01f196af` from the plan for attributing six unmeasured values to
  one statement id — and then, three hours later, quoted a three-way key split in a Task 2
  dispatch attributed to four statement ids that do not carry it. An implementer checked
  and found it. `docs/f3-run-evidence.md` §0.5 records it at the point it bit.
- **Two corrections that were themselves wrong.** §0.3's straddle bullet and its
  `valid_from` floor bullet were both written as corrections and both were false — the
  first claimed a guard refuses a spec it accepts with 14.9× headroom, the second claimed
  a floor could be armed by a profile that cannot reach it. Both were falsified by
  implementers, and both decisions survive on **different** arguments. That is the fourth
  and fifth instance of "corrections overshoot" in this project's record.

**Five more are filed as RECORDED-NOT-REPAIRED**, because they live in job-YAML header
comments that this documentation pass did not edit. They are listed rather than left in a
git-ignored review file, so that whoever next touches those headers has the list:

- **`gold_fact_payment_job.yml:143`** publishes a `5–40 min` interval whose stated upper
  anchor is 5,630 s — **94 minutes** — and calls the 40/5 ratio "11×" where it is 8×.
  **Neither stated endpoint produces the published interval.** This is why row 26 above is
  unmarked rather than compared against a number that does not reconcile with its own
  derivation.
- **`gold_fact_payment_job.yml:93-94`** says `dim_date`'s span is measured "from this very
  table". It is not: `covered_span` reads **`bronze_payments`** and
  **`sat_empresa_dados`**. The conformed job's own header is right; this one contradicts
  it, and §4 above is the measurement that settles it.
- **Both gold cost blocks cite `f2-wave-1-workspace-run-evidence.md` §4** for the cost
  table. It is **§2.4**. §4 is the reference tables, whose own cost table carries
  plausible-looking numbers that would not alert a reader to the mis-citation.
- **`gold_pit_estabelecimento_job.yml`** calls its 0.5–3 h interval "set by a disagreement
  in the measurements". It is a **judgement anchored on** measurements (499–5,630 s =
  0.14–1.56 h) and reaches well past both of them. The same block says the two 72.3M-row
  loads are "72.3M rows each"; `sat_estabelecimento_dados` is **73,530,802**.
- **`§A6` does not exist.** The master protocol has A1–A5; the rule those headers mean is
  **standing decision §4.6** ("a path that ran zero rows through it is not a path that
  works"). Cited wrongly at four sites, including commit `49b9344`'s body.

### 9.3 What was checked and found TRUE

**A pass reporting only hits is indistinguishable from one that stopped early.**

- **Every gold table was built onto nothing.** The empty "before" baseline was recorded
  first (`01f1973c-331c-101d-8b85-dec1f1121f4f`, zero rows for `dim_%`, `fact_%`, `pit_%`),
  so all six builds are transitions somebody can check rather than assertions.
- **The deployed artefact matched the tree on both deploys**, by sha256 **and** stamped
  revision, never by the deploy's success line — and the second check went inside the
  downloaded wheel to confirm the fix by source.
- **The provenance guard is the dependency root of all five jobs and all five runs
  succeeded**, so it accepted five more times, naming what it read. Its refusal half
  remains unexercised (§9.4).
- **The surrogate key is injective over `dim_company`'s whole 69,202,818 rows** — measured
  on the written table, not assumed, because `xxhash64` over that many rows carries a
  ~1.3e-4 birthday chance and a collision is the silent wrong answer a star cannot recover
  from.
- **Every satellite version reached the dimension**: distinct companies equals
  `hub_empresa` exactly, so the inner join dropped nothing.
- **No `valid_to` is NULL** anywhere in the dimension — both sentinels, both written.
- **The fact's grain is one row per delivered identity**, enforced rather than observed:
  30,000 rows against 30,000 distinct `transaction_id` in bronze.
- **Deduplication removed deliveries and not payments** — bronze and the deduplicated
  frame hold the same 27,600 distinct business tuples, and the 2,400 legitimate repeats
  survived.
- **The as-of join returns two different versions of one company on two sides of one
  date**, with two different surrogate keys and two different attribute values, decided by
  nothing but `event_time`.
- **Both sentinels behave as designed** — a 2026-06-20 payment falls inside a floored
  interval, and `is_current` is derived from the ceiling rather than from a NULL test.
- **Both PIT layers reconcile**: 71,874,448 + 72,318,968 = 144,193,416, and the later
  layer equals `hub_estabelecimento` exactly.
- **The PIT recovers 71,804,464 rows over the naive join, on the built table**, closing
  the claim that was previously made about the source data.
- **`dim_date` needed no rebuild after the fourth stream landed**, because `covered_span`
  anchors on the June RFB snapshot — checked, and it falsified the published reason for
  the run ordering rather than confirming it.
- **`dim_channel` reaches all five declared rails**, so the one non-constant conformed
  dimension is exercised across its whole domain.
- **CI is green on PR #18** at the time of writing: `test` **pass** (13 m 46 s),
  `secret-scan` **pass**.

### 9.4 What is still unexercised, after everything ran

**Standing decision §4.6: a path that ran zero rows through it is not a path that works.**
There was no unexercised-paths list anywhere on this branch before this section, which is
the sixth of protocol §9's conditions and the one nothing had produced.

- **The ghost row on BOTH role keys of `fact_payment`.** 0 rows resolved to it, and that
  is **structural rather than lucky**: F1b measured 1,024 of 1,024 counterparties
  resolving to `hub_empresa`, `dim_company` carries one chain per hub key, and the
  unconditional `valid_from` floor means every payment instant falls inside some
  version's interval. **`COALESCE(<lookup>, GHOST)` cannot fire on this data.** No
  unresolvable payment was manufactured to make the number non-zero.
  [ADR 0015](adr/0015-as-of-known-time-and-append-only-scd2.md) names the one change that
  would exercise it — flooring only the earliest observed versions — and rejects it for a
  stronger reason than the ghost is worth.
- **The three conformed ghosts, all of them.** `dim_date`'s was published as "reachable
  even in principle"; it is not (§9.2, item 9). All three conformed keys are **derived**
  from the fact's own columns, so there is no lookup to coalesce onto a ghost in any of
  the three.
- **The conformed orphan counters.** They are what replaces the ghost for a derived key,
  and they cannot fire here either: `dim_channel` and `dim_currency` hold the contract's
  declared domains and the generator draws from those same domains, while `dim_date`'s
  span is measured from the fact itself. **No orphan count was captured into this phase's
  run facts, so this document publishes no number for them** — which is a gap in the
  record, not a zero.
- **`dim_currency` at fact-side cardinality 1, and `dim_date` at 2.** A dimension the fact
  reaches one member of is a **constant column with a surrogate key on it**: no test over
  it can fail and no query over it can be wrong. `dim_date` is barely better — 2 of 50
  members reached. These are published as numbers rather than as the word "thin" precisely
  so this line can be written.
- **`_refuse_a_target_the_source_has_outgrown`.** The append-only SCD2's whole safety
  argument, and it has never fired: every run in this phase built onto an empty table or
  re-ran against an unchanged source. It fires the day a third RFB snapshot lands, and
  what it costs then is a drop-and-rebuild of a 69.2M-row table — 120 s, now that the
  extrapolation is retired.
- **The PIT's out-of-order-backfill refusal.** A snapshot loaded **between** two the table
  already holds is the one case a PIT is not monotone under, and it is refused before the
  first write. This vault holds exactly two snapshots and neither was backfilled, so the
  refusal has never been reached.
- **The provenance guard's REFUSAL half, in the workspace.** Eleven accepts across F2 and
  F3 and not one refusal. A wheel from another commit, or a `+dirty` stamp, is still
  proven locally only. **This phase did not change that, and its five extra accepts must
  not be read as if it had.**
- **Everything F2 left unexercised is still unexercised**: end-dating on a descriptive
  satellite, the satellite dedup tie-break, reference-table history, and
  `reclaim_landing`'s wired path. Gold reads the vault; it does not exercise it.

### 9.5 Cost

| task | wall clock | rows written |
|---|---|---|
| `dim_company` | **120 s** | 69,202,818 |
| `dim_date` | 35 s | 51 |
| `dim_channel` | 32 s | 6 |
| `dim_currency` | 32 s | 2 |
| `pit_estabelecimento` | **168 s** | 144,193,416 |
| **sum of the five captured builds** | **387 s** | 213,396,293 |
| the three captured `assert_deployed_revision` guards | 31 + 32 + 32 = **95 s** | — |

`fact_payment`'s task duration was not captured, so it is absent from this table rather
than estimated into it; so are two of the five guard durations.

**Five of the six gold builds have captured durations, and they cost 387 s of task time
plus 95 s of captured guards**, against F2's ~7.7 h for the vault they read. Two facts
drive that and both are worth carrying forward:

- **The three conformed builds are 32–35 s each and almost none of it can be work** —
  they write 51, 6 and 2 rows. F2 §4.4 attributed a per-task floor of **~80 s** on the
  reference tables to serverless session startup; these come in at **under half that**, so
  the ~80 s is a measurement of one day and **not a constant to quote as one**. What both
  agree on is the shape: a job that writes fifty rows costs what starting a session costs,
  whatever that happens to be.
- **`pit_estabelecimento` wrote 144M rows in 168 s while `dim_company` wrote 69M in 120
  s.** Row count is not the driver. The PIT hashes nothing and the dimension hashes an
  8-character key and a date, and both are an order of magnitude below anything in the
  vault, which hashes whole payloads.

**Storage was not captured for any gold table.** The vault's per-table sizes are in
`docs/f2-wave-1-workspace-run-evidence.md` §5.4; the equivalent table for gold does not
exist and is not reconstructed here.

---

## 10. How this phase ends, against protocol §9's six conditions

| # | condition | status |
|---|---|---|
| 1 | every artefact the phase promised exists, built by its own code | ✅ six gold tables, built onto the empty baseline of §0.6 |
| 2 | every prediction marked, the falsified ones kept | ✅ §9 — two falsified, kept, and both are the cost ones |
| 3 | **CI green on the MERGED PR** | ✅ **closed** — PR #18 merged 2026-08-13T20:47:42Z as `abee2bb`; final CI at `8fc6fc7` passed all three checks (`test` **14 m 45 s**, `secret-scan`, CodeRabbit) |
| 4 | `docs/<phase>-run-evidence.md` exists, controller-verified separated from reported | ✅ this file and `docs/f3-run-evidence.md` |
| 5 | `.plans/HANDOFF.md` updated, including deleting what the phase made false | ✅ |
| 6 | what remains unexercised is listed as unexercised | ✅ §9.4 |

**Condition 3 is the only one this document cannot close**, and it is stated as open
rather than rounded up. PR #18's `test` and `secret-scan` checks pass; the merge has not
happened.

**On review, and this matters more than the check marks.** CodeRabbit reporting `pass` is
**not** a review — protocol §A5 names "Review rate limited" as **absence, not approval**,
and a branch whose only review is a rate-limited bot has not been reviewed. **The review
of record for this branch is the split whole-branch pass**: two independent reviewers,
code and docs as **disjoint packages**, because F2 measured that 751 KB is past what one
reviewer reads carefully. The code reviewer said it would merge. **The docs reviewer found
three HIGH defects, all three in the controller's own evidence prose**, and every one of
them is corrected in `docs/f3-run-evidence.md` with the correction recorded rather than
the text quietly replaced.
