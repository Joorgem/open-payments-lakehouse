# ADR 0010 — derive an observation ledger, because our extract is lossy and Data Vault assumes it is not

## Status
Accepted. The derivation is implemented in `src/opl/vault/observation.py` and
locked by `tests/vault/test_observation.py`, whose closing probe was run against
two deliberately degenerate implementations and goes red against each (recorded
in the F2 wave-1 task report). **The five-state model was then replicated in SQL
against real bronze, independently of the implementation, and reproduces exactly**
— cost and state distribution both, below. **A satellite now consumes it.** This
sentence read *"what has not happened yet is a satellite consuming this ledger —
Tasks 3, 4 and 5 do that, and until then the ledger is a correct answer nobody
has asked in anger"*, which was true when the ADR was written at Task 2 and false
from Task 5 onward. Task 5's `sat_eff_company_partner` is the first table whose
CONTENT depends on which state the ledger returns; the last bullet of
[Consequences](#consequences) records what each of Tasks 3, 4 and 5 actually
wired. Corrected in Task 7's pass — a Status section asserting that something has
not happened yet is the first thing a reader trusts and the last thing anyone
revisits.

## Context

### The hole in an ordinary satellite

A Data Vault satellite is delta-driven on a `hash_diff`: it writes a row only
when the payload changes. A business key that is **absent from a load** produces
no comparison and therefore no row — byte-identical, in the satellite, to a key
whose payload was **unchanged**. The two are indistinguishable in the one place a
reader would look.

That is not hypothetical here. Four establishments are in 2026-06's bronze and
absent from 2026-07's, because a DQ rule widened between the runs (`caed88e`) and
quarantined them on `encoding_replacement_char`
(`01f19061-1041-1e56-b6c3-a9ac80655d7c`). Nothing in a satellite built the
ordinary way would say so, and the same shape arises from any DQ rejection, any
missed file, and — differently, and this is the distinction that matters — from
an entity genuinely leaving the registry.

### What Data Vault 2.0 prescribes, and what it presumes

The literature has three named artefacts for "the row is not in this load", and
they are different things (full quotes, URLs and evidence strength in
`.plans/f2-research-snapshot-dv2.md`):

- **Record tracking satellite (RTS)** — no payload at all, "one row per hashkey
  per load date that it appeared" (DataVault4dbt, *How to Track Effectivity*,
  directly fetched). Written unconditionally every load, so absence becomes a
  missing row rather than a silence. It asserts only *seen / not seen*.
- **Status tracking satellite (STS)** — "tracks the appearance, updates, and
  disappearance of business objects in the source system" and is "invaluable"
  "when dealing with full extracts (non-CDC data)" (Scalefree, *CDC, Status
  Tracking Satellite, and Delta Lake*, directly fetched). It asserts *created /
  updated / deleted*, which is a claim about the world.
- **Effectivity satellite** — two incompatible definitions in circulation
  (AutomateDV's driving-key window over a link; DataVault4dbt's `is_active`
  appearance tracking). **ADR 0011 adjudicated it**: the driving-key definition
  cannot express the partnership relationship at all, because 49.7% of companies
  with partners have more than one simultaneous partner. Named here only so this
  ADR is not read as covering it.

The canonical detection mechanic for a full extract is a lookback into the
current staging load: *"Perform a lookup back into the staging area to check
whether the business key still exists. If not, add a record with the deleted
information … into the Effectivity Satellite"* (Scalefree, *Handling Validation
of Relationships in Data Vault 2.0*, directly fetched).

**Every one of those presumes the extract is complete.** DataVault4dbt states the
assumption outright for its effectivity satellite: *"It only works if the source
data delivery always includes a full load of the data. It does not work, if the
data delivery only includes deltas."* The failure mode we have is a third case
that sentence does not contemplate — a delivery that is nominally full and has
had rows removed **downstream of the source, by us**. Absence from the extract is
taken, everywhere, to mean absence from the source.

### The literature's actual position on what we do is that we should not do it

This is the uncomfortable part and it belongs in the record rather than in a
footnote:

> "If any of these checks fail, the entire file should be rejected — not just
> individual records. Why? Because partial loads introduce ambiguity and audit
> challenges."
> — Scalefree, *Dealing with Corrupted Loads in Data Vault* (directly fetched)

That sentence names our exact failure mode — *partial loads introduce ambiguity*
— and prescribes file-level all-or-nothing rejection to avoid it. It is
reinforced by the broader loading doctrine, which is that the raw vault takes
everything, "the good, the bad, and the ugly" (Linstedt & Olschimke, via
ScienceDirect's *Raw Data Vault* topic page — **[via search snippet; the page
returned 403 to direct fetch, so the exact wording is unconfirmed, though the
formulation is widely and consistently attributed]**), and by the Data Vault
Alliance's placement of business rules "on the way out of the Data Vault model"
rather than during ingestion (directly fetched).

So: **a row-level DQ gate in front of the raw vault is a documented deviation
from DV2 doctrine, and the ambiguity this ADR models is the predicted cost of
that deviation, not a surprise.** ADR 0006 took the gate decision on its own
merits; this ADR is the bill arriving.

Note also where our quarantine sits in the literature's own vocabulary: it is an
**Error Mart** in all but name — "a catch-all for rejected records that fail to
load", "auditable, traceable, and eventually resolvable" (Scalefree, *Defining
the Error Mart in Data Vault*, directly fetched). The literature treats that as a
place rows go to be **reprocessed**, not as a permanent hole in the extract. We
are using it as both.

## Options considered

**1. Move the gate — comply.** Load good, bad and ugly into the raw vault,
quarantine nothing ahead of it, express the encoding check as a soft rule
downstream. This is the by-the-book answer and it makes the extract complete
again, so every pattern above works exactly as documented.
*Rejected*, and not lightly: it is a rewrite of the F1 bronze layer that is
already deployed, measured and evidenced, and it moves a fail-closed guard into a
report. The gate is what stopped ADR 0005's silently-damaged rows. Deferring the
whole class of quality decisions to "the way out" would trade a modelling
inconvenience for a correctness one.

**2. Escalate the gate to file level — comply literally.** If any row fails,
reject the file and do not load the month. Scalefree's own prescription.
*Rejected*: one bad row in 72.3M would discard a month. In 2026-07 that is four
rows on estabelecimentos out of 72,318,964, and it would have thrown away the
entire snapshot — the very snapshot that makes `hash_diff` demonstrable and this
whole phase possible.

**3. Keep row-level quarantine and build the missing state.** Then the record
tracking has to key on **what the pipeline observed**, never on what the source
published — because what the source published is precisely the thing the pipeline
no longer knows.
*Chosen.* Within it, two sub-options:

**3a. Materialise a record tracking satellite.** One row per key per load, the
documented artefact.
*Rejected on cost.* At our grain that is roughly 169M rows per month — ~69M
companies + ~72M establishments + ~28M partners — growing linearly and forever,
carrying no payload, on a Free Edition workspace that peaked at 39.7 GB this
phase and whose retention control does not run through the wired path.
DataVault4dbt acknowledges the growth ("can lead to significant data growth over
time") and offers no mitigation; **the search behind this ADR looked for a
compression pattern for an RTS and found none** (`.plans/f2-research-snapshot-dv2.md`,
"What I looked for and could not find" #5 — a negative search result, not a claim
that none exists), and inventing one would break the insert-only property that is
why the standard does not do it.

**3b. Derive the ledger from what is already stored.** *Chosen — see below.*

## Decision

**Derive the observation ledger. Do not materialise a record tracking satellite.**

The RTS pattern exists because staging is transient. **Ours is not.** Bronze is
append-only, carries `_snapshot_month` and `_snapshot_ref_date`, and is never
dropped; the quarantine tables carry the rejects with the same keys and the same
durability, and every quarantined row carries a usable business key (0 NULL
`cpf_cnpj_socio` in socios quarantine, 0 in empresas —
`01f19061-aade-12eb-aee4-b43e66b22c3a`). Presence per key per month is therefore
**already recorded twice over**, and an RTS would be a third copy of a fact we
have.

### The states

The plan carried four. Measurement made it five, and the fifth is not a
refinement — it is a defect the first four would have shipped.

| in bronze for M | in quarantine for M | state |
|---|---|---|
| yes | no | `observed` |
| yes | yes | `observed_with_rejected_siblings` |
| no | yes | `rejected_by_our_gate` — not observed, and **we** are the reason |
| no | no | an absence, and there are two of them — below |

**Row two is not a corner case.** Of 680 quarantined sócio keys in 2026-07, **679
are also in bronze that same month**; at link grain, 5 of 1,786
(`01f19061-b62b-16c8-8c94-bca654ea0c54`). Every grain in this vault is **coarser
than a source row** — one partner key spans many partnership rows, and one row
can be rejected while its siblings pass — so a three-state derivation would have
had to pick a winner silently. It is named instead.

**The absence split.** Running the four-state derivation against real bronze
produced this distribution, and the 2026-06 column is what condemned it:

| table | month | observed | with rejected siblings | rejected | absent |
|---|---|---|---|---|---|
| estabelecimentos | 2026-06 | 71,874,448 | 0 | 0 | **444,520** |
| estabelecimentos | 2026-07 | 72,318,964 | 0 | 4 | 0 |
| socios (link grain) | 2026-06 | 27,832,321 | 5 | 1,792 | **219,370** |
| socios (link grain) | 2026-07 | 27,986,258 | 5 | 1,781 | 65,444 |

Those 444,520 establishments and 219,370 partnerships are **not candidate
deletes. They are entities that did not exist yet** — exactly the keys whose
first appearance is 2026-07. A grid of every key against every month marks every
future entity as absent in every earlier month, so a single `absent` state
meaning "candidate delete" would have the ledger assert 444,520 false departures
in 2026-06 on estabelecimentos alone. Hence:

- **`absent_before_first_observation`** — no month at or before this one, within
  the window read, showed this key at all. There was nothing to depart from.
- **`absent_after_observation`** — we saw this key earlier and did not see it
  here. **A candidate delete, and never an asserted one.**

### The five states, measured against real bronze

The split is not a modelling preference; it was re-measured after the fact, with
`first_observed_month` as `min(_snapshot_month)` over bronze ∪ quarantine and the
branch order below (quarantine before the absence split), over the two months:

| table | month | state | rows |
|---|---|---|---|
| estabelecimentos | 2026-06 | `observed` | 71,874,448 |
| estabelecimentos | 2026-06 | `absent_before_first_observation` | **444,520** |
| estabelecimentos | 2026-07 | `observed` | 72,318,964 |
| estabelecimentos | 2026-07 | `rejected_by_our_gate` | **4** |
| socios (link grain) | 2026-06 | `observed` | 27,832,321 |
| socios (link grain) | 2026-06 | `observed_with_rejected_siblings` | 5 |
| socios (link grain) | 2026-06 | `rejected_by_our_gate` | 1,792 |
| socios (link grain) | 2026-06 | `absent_before_first_observation` | 219,370 |
| socios (link grain) | 2026-07 | `observed` | 27,986,258 |
| socios (link grain) | 2026-07 | `observed_with_rejected_siblings` | 5 |
| socios (link grain) | 2026-07 | `rejected_by_our_gate` | 1,781 |
| socios (link grain) | 2026-07 | `absent_after_observation` | **65,444** |

`absent_after_observation` is **zero on estabelecimentos in both months**. Every
one of the 444,520 keys the four-state version called a candidate delete is
pre-birth, and that table has **no true departures at all** — its only four
absences are our own gate's. All five states occur across the two tables, and
none of the twelve rows above is a state the model cannot name.

### Defined on the past only

The split could have been "before first observation / after **last**
observation", which reads more naturally and is unstable: a key absent in July
and back in August is "after the last observation" while July is the end of the
data, and something else once August lands. A row already written would change
meaning as new months arrived. Comparing each month against the key's **first**
observation can never be revised by a later load, and
`test_a_key_that_returns_keeps_the_label_it_had_before_the_later_month_arrived`
holds it there.

The price is that the states are **relative to the window the ledger was asked
for**, and the ledger cannot warn a caller who narrows it: months it was not
given are months it never read. `months=None` — every month both tables hold — is
the default for exactly that reason, and `first_observed_month` is an output
column so the truncation is at least visible.

### The cost, measured, because that is what decides materialisation

The plan said to materialise only if the derivation proves too slow to serve the
satellites, and to record the number rather than decide on taste. Controller-run
against real bronze, both first-run and not cache-served:

| grain | scale | four states | **five states** | statement (five-state run) |
|---|---|---|---|---|
| estabelecimentos, hub grain | 72.3M keys × 2 months = 144.6M grid rows | 93 s | **93 s** | `01f191f3-6c96-15d2-84db-514bfcff2ce5` |
| socios, link grain | 28.05M keys × 2 months | 24 s | **26 s** | `01f191f3-ad7e-1edf-b0bd-063c4f1b7db6` |

Ninety-three seconds is cheaper than writing 169M rows a month forever, and it is
cheap enough that a satellite load can consult the ledger without a materialised
copy. **Derive, do not materialise** therefore rests on those two numbers, and
the number is what a later phase should re-measure before overturning it.

**The absence split is free.** The five-state derivation adds one aggregation
over the key space (`min(_snapshot_month)` per key, for `first_observed_month`)
and carries one extra string column through the final fold, and it costs 0 s on
estabelecimentos and 2 s on socios against the four-state runs the earlier
columns record — inside the noise of a single observation either way. The fifth
state was bought for nothing measurable, which is worth saying plainly: the
argument for it is entirely about correctness, and it did not have to be traded
against cost.

### What the implementation refuses to make easy

- **There is no state whose name contains "delete", and no `is_departure`
  helper.** A caller who wants a departure signal maps `absent_after_observation`
  onto one in their own code, where the choice is visible in review. A key that
  vanished because our own gate rejected it must never reach a satellite as a
  departure from the registry, and the way to make that hard to get wrong is to
  make the wrong thing require typing.
- **The grain is a parameter, not a code path.** `ObservationGrain` carries the
  two tables and the key columns; hub grain and link grain differ only in
  `key_columns` and share every line of the derivation. They are not
  interchangeable in use — a partner who loses one of two partnerships is
  `absent_after_observation` at link grain and plainly `observed` at hub grain,
  which is why Task 5's effectivity satellite must read the link grain — and the
  tests assert that divergence over a single table.
- **No join on a business key, anywhere.** `NULL = NULL` is NULL in SQL, so an
  equality join would fail to match a key to itself wherever a component is NULL
  — 12,824 foreign partners carry `cpf_cnpj_socio` NULL — and would report those
  keys absent in every month while they sat in bronze the whole time. Presence is
  folded with `groupBy`, which treats NULL as a value equal to itself.

### Why the acceptance test spans two tables

In one line: **estabelecimentos supplies a departure caused by our own gate and no
true departures; socios supplies 65,444 true departures and no departure caused by
our gate.** Departures are what a satellite and an effectivity window consult, and
no single table's departures carry both causes:

- estabelecimentos 2026-06 → 2026-07 has exactly **4** departures and **all four**
  are our own gate's (`01f19061-707d-1eda-bbf1-a8302ffc3e79`);
- socios at link grain has **65,444** departures and **not one** is in July's
  quarantine (`01f19061-e234-1617-bc9a-19f854e7b204`);
- empresas has **zero** departures of either kind — all 68,629,147 keys of
  2026-06 are present in 2026-07, because the RFB retains baixadas rather than
  removing them (`01f19061-4f47-1b47-ab3a-1880491dda04`).

So a ledger that blames every departure on our gate passes an
estabelecimentos-only test in full, and one that blames every departure on the
source passes a socios-only test in full. Both degenerate implementations were
written and run; each turns the cross-table probe red while leaving the
single-table test for the other shape green.

> **A weaker claim was made first, corrected here rather than deleted — and the
> correction itself overshot, which Task 7's pass fixes in place.** An earlier
> statement of this argument read "no single table carries both absence states".
> That is false. The correction then read *"socios at link grain carries all five
> states in one month (27,986,258 / 5 / 1,781 / 65,444 in 2026-07)"* — **also
> false, and contradicted by this ADR's own table above**, which lists exactly
> four states for socios 2026-07. What the measurement supports is narrower and
> is what the original claim actually needed: socios at link grain carries
> `rejected_by_our_gate` **and** an absence state in the **same month**, and
> carries **both** absence states across the two months (219,370
> `absent_before_first_observation` in 2026-06, 65,444 `absent_after_observation`
> in 2026-07). **No month of any table can carry five**: a key is either before
> its first observation or after its last, never both, so the two absence states
> are mutually exclusive within a month by construction. The true statement is
> about the **cause of a departure**, not about which states appear.

## Consequences

- **The DQ gate's output is now a first-class vault input.** The quarantine
  tables must be retained per month with the same durability as bronze, or the
  distinction between "we rejected it" and "the source dropped it" is
  unreconstructable after the fact. Any future retention or vacuum policy that
  touches `*_quarantine` is touching a vault input, and this ADR is the reason it
  cannot be treated as an operational byproduct.
- **Every consumer pays the derivation.** It is a function returning a
  `DataFrame`, not a table: 93 s on the largest grain, per consultation, with no
  caching in this layer. A phase that consults it many times in one job should
  cache the frame it gets, and a phase where that stops being enough should
  re-measure before materialising.
- **This ledger is deliberately weaker than an STS.** It records what we saw and
  will not tell anyone what happened. The research notes that a derived delete is
  understood in the DV community as a weaker claim than a delivered one; here it
  is weaker still, because **some** of the silence is our own doing rather than
  the source's, and the ledger is what tells the two apart. Any satellite that
  wants to end-date has to say, in its own code, which state it is acting on.

  > **This bullet read "because a third of the absence signal is our own doing",
  > corrected in Task 7's pass. No reading of the table above yields a third.**
  > Over the two months measured, the gate-caused states total 4 + 1,792 + 1,781
  > = **3,577** against **732,921** non-`observed` rows — **0.49%**. Restricted to
  > DEPARTURES, which is what an end-dating satellite acts on, it is **4 of
  > 65,448 — 0.006%**. The argument does not need a fraction and is stronger
  > without one: what makes the derived delete weak is that **any** of the silence
  > can be ours, not how much, because the four estabelecimentos rejects are
  > **100%** of that table's departures.
- **The deviation is now two-sided and documented.** ADR 0006 admits row-level
  quarantine; this ADR admits that doing so leaves the raw vault unable to
  distinguish two very different silences, and builds the missing distinction
  rather than pretending the extract is complete. A reviewer holding DV2 doctrine
  should read the two together: the project departs knowingly, in one place, and
  pays for it in another.
- **`_snapshot_ref_date` is not in the ledger, on purpose.** For an absent
  (key, month) there is no row on either side to take a date from, so it would
  have to be asserted from the month — a date attached to a key that has no row.
  The month-to-ref-date mapping is one row per month and belongs wherever a
  caller needs a date.
- **~~Nothing consumes this yet.~~ Something does, as of Task 5.** Tasks 3 and 4
  wired the ledger into `load_satellite`, where it contributes a reported
  departure count and `_window`'s refusal of an unloaded month but where **no
  code branches on a state** — the filter that would have was dead by
  construction, and `opl.vault.satellites` says so. Task 5's
  `sat_eff_company_partner` is the first table whose CONTENT depends on which
  state the ledger returns: `absent_after_observation` closes a window and
  `rejected_by_our_gate` does not, and the state that authorised a close is
  written into the row. See ADR 0011.
