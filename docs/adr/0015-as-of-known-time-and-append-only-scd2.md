# ADR 0015 — the star answers as of KNOWN time, and its SCD2 is append-only

## Status
Accepted. Implemented in `src/opl/gold/dimensions.py` (the version chain),
`src/opl/gold/columns.py` (the sentinels and the floor) and
`src/opl/gold/facts.py` (the half-open as-of join); locked by
`tests/gold/test_dim_company.py` and `tests/gold/test_fact_payment.py`. Written
in F3 after the layer existed. Every argument below was already in the repository
— at length, in module docstrings and commit bodies — and **none of it was
findable**: a reader asking "what does `payer_company_sk` actually mean?" had to
read a loader to find out that the answer is dated by an RFB snapshot and not by
the payment.

**What it decides.** What the as-of join asserts, what it cannot assert, and the
load shape that follows from both.

## Context

### `applied_date` is a snapshot reference date, so the lookup is as-of KNOWN time

`dim_company` is versioned on `applied_date` — the **RFB's own declared reference
date for the monthly snapshot**, not a date on which anything happened to the
company. `fact_payment` resolves each role by finding the version whose interval
contains the payment's `event_time`. What that returns is therefore:

> **the most recent registry assertion available as of the payment** —
> not the company's attributes at the instant the payment happened.

The two coincide only when the registry is current. For the **139,968** companies
that changed between the two snapshots
(`docs/f2-wave-1-workspace-run-evidence.md`: 69,202,817 − 69,062,849), the real
change lies somewhere inside the **28-day** interval `(2026-06-13, 2026-07-11]`,
and **this source cannot narrow it**. A payment on 2026-06-20 against one of those
companies resolves to the June version — correctly, as a statement about what was
known — while the attribute may already have changed in the world.

This is the standard distinction between valid time and transaction time, and the
star has only the second. Naming it matters because the alternative reading —
"the company's state at payment time" — is what a reader assumes a
Kimball as-of join gives them, and it is the reading every downstream analysis
would be built on.

### Bi-temporality is not deferred here; it is unbuildable from this feed

The usual answer is a bi-temporal dimension carrying both a business effective
date and a load date. **`sat_empresa_dados` has no business effective date to
carry.** Its payload is four columns —
`razao_social`, `natureza_juridica`, `capital_social`, `porte_empresa`
(`src/opl/vault/domains/cnpj.py:140-144`) — and not one of them is a date. The
empresas feed states what is true at snapshot time and says nothing about when it
became true.

`data_situacao_cadastral` — the one RFB column that *is* a business effective date
— exists only on the **estabelecimento** satellite
(`SAT_ESTABELECIMENTO_DADOS`), which the fact cannot reach at all: the
counterparty is 8 characters and names a company, not an establishment. That is
[ADR 0014](0014-dim-company-at-empresa-grain.md)'s grain ruling, and this is one
of its consequences.

So bi-temporality here is not a phase-two feature. It needs a **different source
column that this feed does not contain**, and no amount of work in the gold layer
produces one.

### The load shape follows from what an append can and cannot do

An SCD2 version's `valid_to` is the next version's `valid_from`. Closing a version
therefore means **updating a row that is already written**, which is a `MERGE` or
a delete-then-append. This repository executes **no Delta `MERGE` anywhere** —
every occurrence of the word under `src/` is prose arguing against one, except a
last-one-wins **dict** merge in `opl.contracts.catalogue` — and the SCD2
loader is no exception: `F.lead(applied_date)` closes each interval **in the same
window pass** that orders the versions, so the closed chain is derived rather than
patched, and the write is a single append of rows that are already final.

## Options considered

### 1. `MERGE` on each load, the textbook SCD2

**Rejected.** It writes the new version and then UPDATEs the previous one's end
date. On a 69.2M-row dimension that is a rewrite of files to reach a state the
derivation already knows in full, and it makes every load a two-statement
operation whose failure between the statements leaves a chain with two open
versions. The derivation has the whole chain in one window; there is nothing for
the second statement to discover.

### 2. Append-only, with a refusal when the source has grown

**Chosen.** `_refuse_a_target_the_source_has_outgrown` checks
`(surrogate key, valid_to)` against the target **before the first write**, and
stops. The surrogate key does not move when a company gains a version — it is
hashed over that version's own `applied_date`, which is unchanged — so what a new
snapshot changes is exactly the previously-open row's END, and a missing pair is
that signal.

**The accepted cost is stated rather than softened: a later snapshot forces a
drop-and-rebuild of a 69.2M-row table.** That is expensive and it is the right
trade, because the alternative is not "a cheaper MERGE" but a silent one — an
append that adds a second interval to a surrogate key that already has one, after
which every as-of lookup for that company returns **two rows** and the fact,
which stores `company_sk`, joins to both. The refusal fires before anything is
written and names the repair; the corruption would be discovered by a downstream
query returning duplicate rows, months later.

The refusal also names a second cause, which is the one an operator would
otherwise misdiagnose: `valid_to` is an **instant**, so a rebuild under a moved
`spark.sql.session.timeZone` reproduces every row with its bounds shifted and
trips the same check. The message says to check the zone **before dropping
anything**.

### 3. Floor `valid_from` only on the earliest observed versions

**Rejected, and this is the option that deserved the most argument**, because it
is more honest about our ignorance and would exercise the ghost with real data
instead of leaving it structurally unexercised. A company first observed in July
would start on 2026-07-11, and a payment before that would resolve to the unknown
member — which is the truth: we know nothing about that company then.

It is rejected because it makes `valid_from` depend on a **global aggregate over
the source**. The day a phase backfills an earlier snapshot, every first version's
`valid_from` moves — and with it every `company_sk`, which is hashed over
`valid_from`. That **silently re-keys a dimension that facts already reference**,
under a load nobody thought was destructive. The unconditional floor is stable
under a backfill; the conditional one is not, and a surrogate key that moves
underneath a written fact is the worse failure by a wide margin.

**The unconditional floor claims less than it looks like it claims**, and the
module says so: `valid_from = 1970-01-01` on a first version does not assert that
the company existed then. It is a **lookup convention** — "for any as-of time up
to the next version, this is the state this dimension can offer". An RFB snapshot
is a state, not a birth certificate.

### 4. Kimball's conventional sentinels, 1900-01-01 and 9999-12-31

**Rejected on a measured platform limit, and the rejection is a local artefact
accepted deliberately.** PySpark converts a timestamp between Python and the
engine through the C runtime, and on this project's Windows dev box the
round-trippable window is **1970-01-01 .. 3000-12-31 and nothing outside it**.
Measured, pyspark 3.5.9, both directions and both paths, one value per year
(`src/opl/gold/columns.py:52-63`): 1899, 1900, 1969, 3001 and 9999 all fail —
`OverflowError` from `time.mktime` on the write path via `F.lit(datetime)`, and
`OSError [Errno 22]` from `fromtimestamp` on the read path via an ISO cast and
`collect()`. 1970, 2999 and 3000 round-trip.

**What is being accepted is worth saying exactly.** In-engine comparison works for
every one of those values, so a 9999 sentinel would be perfectly **writable and
joinable on Databricks** and would fail only when a Python test tried to read a
row back. The choice is therefore not "9999 is broken" — it is that **a loader
whose tests cannot run is a loader asserted nowhere**, and a sentinel is arbitrary
either way. 1970-01-01 is the lowest instant this stack can round-trip and sits 53
years below the RFB's open-data series (2023-05); 2999-12-31 is 973 years above
anything this star can be asked about.

The high sentinel is **2999-12-31 and not 3000-12-31** although 3000 round-trips:
3001 is the real boundary, and a sentinel one year inside a measured cliff is not
a margin worth having.

### 5. `BETWEEN` for the as-of predicate

**Rejected.** `BETWEEN` is inclusive at both ends, so a payment landing exactly on
a version boundary matches **both** the closing and the opening version. The fact
loader's own acceptance forbids a multi-match — one row per payment, one version
per role — so an inclusive predicate manufactures the very state the build then
refuses, at a boundary instant no random fixture would hit. It is hit
deliberately instead:
`test_a_payment_landing_exactly_on_a_boundary_matches_the_opening_version_only`
is the lock. The predicate is half-open everywhere: `valid_from <= t < valid_to`.

Both ends carry a **sentinel** and **neither is NULL**, which is the commoner spelling and
is refused twice over: a NULL `valid_to` makes every as-of predicate a three-way
`OR valid_to IS NULL` that a reader forgets exactly once, and NULL compares false
in the join, so the omission **loses rows silently** rather than failing.

## Decision

**`dim_company` is an append-only SCD2 versioned on `applied_date`, with
half-open intervals closed by `F.lead` in the ordering window, an unconditional
`valid_from` floor of 1970-01-01, a `valid_to` ceiling of 2999-12-31, and no
`MERGE`. The star answers as of KNOWN time, and cannot answer as of valid time
from this feed.**

## Consequences

- **Every `payer_company_sk` and `payee_company_sk` means "the registry's most
  recent assertion available then".** Any downstream reading of the star as "the
  company's state at payment time" is wrong by up to the snapshot interval — 28
  days for the pair this project holds, and one month in general for as long as
  the RFB series stays monthly.
- **The uncertainty is bounded and nameable, which is the useful part.** It is
  never worse than the gap between consecutive `applied_date`s, and it is zero for
  the 68,922,881 companies that did not change between the two snapshots.
- **A third snapshot is a drop-and-rebuild, planned rather than discovered.** The
  refusal makes the cost visible before the write; an operator meeting it should
  check the session timezone first and drop second.
- **The ghost is structurally unexercised on this data, and that is reported as
  such rather than engineered around.** The unconditional floor means every
  payment falls inside some version's interval, so `COALESCE(<lookup>, GHOST)`
  cannot fire. No unresolvable payment is manufactured to make the number
  non-zero. Option 3 is the change that would exercise it, and it was rejected for
  a stronger reason than the ghost is worth.
- **The sentinels are a dev-box constraint written into a production table**, and
  a reader is entitled to know that rather than to infer a modelling opinion from
  1970. If this project ever stops running its Spark tests on this platform, the
  values remain fine and the *reason* recorded here expires.
- **Bi-temporality needs a new source, not a new loader.** The entry point for it
  is a company-grained business effective date. `data_situacao_cadastral` is the
  RFB's, it is on the establishment, and reaching it means
  [ADR 0014](0014-dim-company-at-empresa-grain.md)'s grain problem first.

### What would change this decision

- An RFB feed carrying a business effective date at empresa grain. The dimension
  becomes bi-temporal and this ADR is superseded, not amended.
- A snapshot cadence fine enough that the known/valid gap stops mattering. It
  would not change the model, only the size of the caveat.
- A dimension large enough that drop-and-rebuild stops being affordable. That is
  the argument for a `MERGE`, and it is a real one at a scale this project has not
  reached: `dim_company` — **69,202,818 rows**, one per satellite version plus the
  ghost — **built in 120 s** on Free Edition serverless, against an extrapolation
  of 2,000–6,000 s. **The extrapolation was falsified and the measurement is what
  this bullet rests on**; a rebuild costs two minutes, so the refusal above is
  cheap to comply with and `MERGE` buys nothing yet.

  > Controller-reported from the F3 workspace run, not yet in
  > `docs/f3-run-evidence.md` when this was written. **69,202,818 is also, by
  > coincidence, a retracted figure for `sat_empresa_dados`** — the raw-comparison
  > overcount `docs/f2-wave-1-run-evidence.md` records against a true 69,202,817.
  > Here it is `dim_company`: 69,202,817 satellite versions **+ 1 ghost**. The two
  > are the same integer for different reasons, and the next reader to grep it
  > deserves to be told so.
- Tests that no longer read timestamps back into Python on a platform with this
  limit. 9999-12-31 would then be available, and would be worth taking only for
  conventional familiarity.
