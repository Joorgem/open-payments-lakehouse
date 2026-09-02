# ADR 0022 — The payment is a transactional link between two companies, and its two promised hubs are refuted

## Status

**Accepted**, F2 wave 2, 2026-09-02. **And accepted over a phase that does not close** — the
Databricks workspace refuses to launch anything (Decision 7), so protocol §9's conditions 1 and 4
cannot be met and no controller may declare this phase closed. Every decision below was taken and
tested against local Spark; none of them has run on the deploy target.

This ADR records the decisions no existing note owns. The dependent-child-key idiom stays in
[ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md), which named the
deferral this phase consumes; the per-domain registry claim stays in
[ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) and
`opl.vault.registry`'s own docstring, and Decision 2 says which of their sentences this document
supersedes, by file and line.

> **There is no `docs/f2-wave-2-run-evidence.md` built from a run**, and Decision 7 is why. Where
> a number appears here it was measured locally, and it says so.

---

## Context

The README's *Honest limits* says, in as many words, **"The payment fact does not go through the
vault … there is no payment hub."** That sentence is the phase's subject. `fact_payment` is built
straight from `bronze_payments`, so the one table this repository triages most often — the
workspace's largest DQ incident is a payments batch — is the one table with no vault lineage at
all.

The plan for wave 2 had been written a long time before it ran, and it staked something specific:
the vault's per-domain registry exists so that **wave 2 adds `hub_account`, `hub_customer` and
`link_payment` with a git diff of "+N files, 0 modified"**. That claim was asserted in four
places in the tree's **live code**, and one of them **was** a test that built a throwaway
domain carrying those three names. **Four is the count of live sites and not of sites**: four
MORE assert the same claim in the dated record — ADR 0011's two `"+1 file, 0 modified"` table
rows and `docs/f2-wave-1-run-evidence.md`'s `:482` and `:1088` — and Decision 2's supersession
table names those by file and line rather than rewriting them. The live four were corrected;
the recorded four were superseded. Saying only "four" invited a reader to check one number
against the other and find a document undercounting itself.

**Past tense, and the first draft of this paragraph wrote it in the present.** The fixture no
longer carries those names — this phase renamed it, which the Consequences record — so the
sentence describing it went stale inside the document that states the rule against exactly that.
It is the defect T4 had just fixed in `payments_domain.py`, whose docstring was describing two
files in the present tense that T4's own edits had changed, reappearing one file later. Decision
6 is the rule; this is the rule's own document breaking it.

**The phase's most interesting result is that two thirds of that list cannot be built.** Not
deferred, not descoped — refuted, on three independent grounds, one of which is that the table
would produce a digest byte-identical to one this vault already holds. What is left is one link,
one satellite, and a set of loader changes nobody predicted, because the prediction had been made
about the vault and the cost landed on the repository.

---

## Decision 1 — `link_payment` is a TRANSACTIONAL link, self-referencing on `hub_empresa` under `payer` and `payee`

A payment is not a state two companies are in; it is an **event** between them. So the link is
keyed on the pair **and** on the event's own identifier: `transaction_id`, declared as a
`BusinessKeyColumn` in `dependent_child_keys`, which belongs to no hub. That is the idiom
`link_company_partner` already uses for the sócio grain, and the one
[ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) named for this
exact table when it deferred the loader work.

**Both ends are DERIVED and IDENTIFYING**, which is `link_merchant_empresa`'s pairing used twice on
one hub:

- **Derived**, because `bronze_payments` carries no column called `cnpj_basico`. `link_candidates`'
  default — read the hub's business key from the columns the hub is named after — finds nothing, so
  `LinkEnd.key_from` declares where the key really lives.
- **Identifying**, because a payment between A and B is not a payment between A and C. An end left
  non-identifying drops its counterparty out of the digest, and **every payment A ever made would
  share a link key with every payment made *to* A.**

**Order is the link's identity, not a listing convention.** `link_hash_key_expression` flattens the
components with no boundary marker, so swapping the two ends re-keys the whole table while both
reference columns stay correct and every join keeps working. Payer-then-payee is the direction the
relationship reads: money leaves the first and arrives at the second, and
`links.refuse_mismatched_hubs` is what stops a caller supplying them the other way round.

**The prefix width is declared and is a no-op on well-formed data, and that is the point.** The
counterparties are already the 8-character `cnpj_basico` root, so `KeyPrefix(column, width=8)`
truncates nothing. It is declared because a `KeyPrefix` is what the registry can **reason** about:
`registry._refuse_a_derivation_that_does_not_fit` cross-checks the width against `hub_empresa`'s own
at import, and `zero_padded_column` fails the query on an overlong value instead of silently
truncating a longer number onto another company's true key.

**`transaction_id` carries NO width**, which is `BusinessKeyColumn`'s "take the value as it is". A
width is a claim about a canonical form; the id is the processor's opaque string — a sha256 hex
digest today, and `opl.bronze.registry` deliberately declines to pin 64 there either — so padding
it would invent characters rather than recover a dropped zero. **A width-bearing dependent-child key
is now refused outright** by `links._refuse_a_width_bearing_dependent_child_key`, because the loader
would hash the padded value and write the raw one; the alternative (project the padded value into
the row) is named in that refusal's docstring together with the reason it was not taken.

**Rejected: `transaction_id` as a hub.** `opl.contracts.payments` is emphatic that the id
identifies the **event**, and a hub over it would be a hub with exactly one satellite, no second
consumer, and a business key that is a delivery receipt rather than a business entity. **Rejected:
a link on the pair alone**, with `transaction_id` in the satellite — two payments between one pair
on one day would then collide onto one link row and the satellite would carry two rows at one hash
key with no way to tell a redelivery from a second payment, which is the exact distinction
`opl.contracts.payments` spends thirty lines protecting.

**What reverses it:** an identifier arriving in the stream that is not a company and not the event — a settlement id, an account, a batch — at which point the pair-plus-event key stops being the whole grain and the link gains an end or the domain gains a table. That is a `SCHEMA_VERSION` change and therefore a scope decision, not a modelling one.

---

## Decision 2 — `hub_account` and `hub_customer` are REFUTED, not deferred

This is the phase's most interesting result, and it is a refutation because all three of the
following were accepted statements in this tree and they cannot all be true:

1. `opl.vault.registry`'s docstring stakes DV2's extensibility claim on wave 2 adding
   `hub_account`, `hub_customer` and `link_payment`.
2. `opl.contracts.payments` says the counterparties are `payer_cnpj_basico` / `payee_cnpj_basico`
   and that `hub_account` / `hub_customer` "are where they become keys".
3. `opl.vault.domains.cnpj` keys `hub_empresa` on `cnpj_basico`, and
   `loading.hash_key_expression` hashes the padded key components **and nothing else**.

The argument is written in full in `src/opl/vault/domains/payments_domain.py`'s module docstring,
under `WHY THERE IS NO hub_account AND NO hub_customer`. Its three grounds, restated here because
an ADR is where a reader looks for them:

- **A `hub_account` on `payer_cnpj_basico` would be `hub_empresa` under a second name.** The hub's
  name is not in the digest. Hashing the 8-character root under a different table name produces the
  **byte-identical** hash key `hub_empresa` already holds — two tables, one key space, and **no
  guard anywhere in this package refuses it.** This repository has rejected that shape twice
  already: [ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) for
  `hub_socio`, and `merchant_domain.py` for a CNPJ-keyed `hub_merchant` — *"a duplicate hub … two
  tables that look independent and are one."*
- **There is no account and no customer in the stream.** `opl.contracts.payments.COLUMNS` carries a
  transaction id, two timestamps, two counterparty roots, an amount, a currency and a method.
  Inventing an identifier means editing the generator and `SCHEMA_VERSION`, which is F1b/F5's
  byte-identity surface — a scope change, and Jorge's gate.
- **A role is not a hub.** "Payer" and "payee" are what one company *is* in one relationship, which
  is `LinkEnd.role`'s whole subject. The two roles are how the same hub appears twice without
  either reference being lost.

**Rejected: building them anyway to keep the plan's sentence true.** The plan's claim would have
been demonstrated by two tables that a later reader would have had to un-build, and the
demonstration would have been of the wrong thing: the mechanism registers a domain from a file, and
Decision 3 shows it doing so for a real one.

### What this supersedes, by file and line

**ADRs are immutable records and none of the following is rewritten.** Each statement was true when
it was written and is superseded here:

| where | what it says | superseded because |
|---|---|---|
| [`0011-…:380`](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) | table row `hub_account`, `hub_customer` → *"yes — registry and `load_hub` unchanged"* | the row is a correct statement about a table that will not be built; the two rows below it about `link_payment` are the ones that came true |
| [`0011-…:381`](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) | table row `their satellites` → *"yes — registry and `load_satellite` unchanged"* | falsified twice over: the hubs those satellites would have hung off are refuted by Decision 2, and `load_satellite` is **not** unchanged — Decision 3 gave it `link=`, `hubs=` and `axis=` and made `hub=` and `grain=` optional, measured against `cae3eff:src/opl/vault/satellites.py` where `hub: Hub` and `grain: ObservationGrain` were both required |
| [`0014-…:97`](0014-dim-company-at-empresa-grain.md) | *"`hub_customer` is F2 wave 2 and does not exist"* | the second half stays true permanently; the first half named a phase that has now refused it |
| [`0014-…:165`](0014-dim-company-at-empresa-grain.md) | `dim_customer` row: *"what would unblock it: `hub_customer`, which is F2 wave 2"* | **only the unblocking cell is superseded, and an earlier spelling of this row got that wrong.** It said `dim_customer` moves from *deferred behind a phase* to *out of scope* — but ADR 0014 never deferred it: its `:165` status cell already reads **out of scope**, under a heading at `:95` reading *"The other two are not deferrals at all"* and a sentence at `:99` reading *"is out of scope, not deferred."* Nothing about the status moves. What moves is what would UNBLOCK it: `hub_customer` is refuted by Decision 2, so the answer is no longer a phase but a payment stream carrying a customer — a `SCHEMA_VERSION` change, and Jorge's gate |
| `docs/f2-wave-1-run-evidence.md:482` | the three-table list, *"structural, not a measurement"* | a dated phase record; it is history and stays where it is |
| `docs/f2-wave-1-run-evidence.md:1088` | the same `"+1 file, 0 modified"` table row | the same |
| `docs/f-db-run-evidence.md:1505` | *"`link_merchant_empresa` is the only link in this vault with a declared derivation on an identifying end"* | false since Decision 1 — `link_payment` declares **two**. A dated phase record, so it is not rewritten, and there is a second reason not to: it sits **inside** the anchor window `fdb:1504` quotes (`ledger_sources.ANCHOR_WINDOW = 3`, so 1504–1506), and editing it would move a lock's subject as well as a record. The headline at 1504 is still true, which is why the ledger row stays open rather than closing |

**What is NOT superseded:** ADR 0011's `link_payment` rows, both of which came true — the
*without*-`transaction_id` row as written, and the *with* row, which said the loader refuses it and
that "it should be made by the wave-2 task that has a table to point at it". That is exactly what
Decision 1 did.

**What reverses it:** the payment stream carrying an identifier for a party that is not its CNPJ root — a customer number, an account number, a wallet — which would be a real entity with a key space of its own and a hub with something to hold. Nothing about the vault reverses this; the stream does.

---

## Decision 3 — the measures ride a satellite on the LINK, and that WIDENED a kind rather than adding one

`sat_link_payment` carries `amount`, `currency` and `payment_method` — descriptive facts about the
payment, hanging off `link_payment`. Without it the vault holds the payment *relationship* and not
the payment, and the README's limit does not narrow in the way this phase claims.

That table was **refused by name** when the link landed, and the refusal said what would have to
change: *"one parented on a LINK — which DV2 does allow — would be a registered table nothing in
this package can write. **The guard and that signature have to change together**."* Both halves
moved in one task:

- **The parent guard widened.** `assert_every_satellite_hangs_off_a_hub_or_a_link` replaces
  `_assert_every_satellite_hangs_off_a_hub`, and the widened guard gained **two refusals the old
  one never had** — a transactional flag on a hub parent, and its absence on a link parent.
- **`load_satellite` takes `link=` / `hubs=` beside `hub=`**, resolved by `_resolved_parent`.

**It is the SAME kind, not a fifth one, and the repository's own criterion decides that.**
`opl.vault.registry`'s docstring gives the test for when `EffectivitySatellite` had to be a separate
kind: *"a `Satellite` is delta-driven on a `hash_diff` over a payload and `load_satellite` takes a
`Hub`. This table has neither."* `sat_link_payment` meets **both** halves — it is delta-driven on a
`hash_diff` over a payload — and only the loader's signature mismatched. A fifth kind would have
duplicated `Satellite`'s guards to change one parameter's type.

**`transactional=True` is a fact stated where a loader can read it, not a switch.** A link-grain
observation ledger would report every payment of every earlier month as
`absent_after_observation` — a candidate delete for a stream in which nothing departed. That was
**measured, not argued**: a probe built the real link-grain `ObservationGrain` over the payments
fixture and got `absent_after_observation` = **2 of 4 keys**. The flag cannot be used to switch a
ledger off: `registry_satellites` refuses it on a hub parent and refuses its absence on a link one,
and — after a review finding — `satellite_grain.snapshot_axis_for` refuses the same pairing **at the
loader**, because the registry guard is not reached by a direct call and `load_satellite` is public.

**And this is where the phase's own prediction broke.** The plan predicted "+1 file, 0 modified".
That is true of the **link** and false of the **satellite**, and `payments_domain.py`'s docstring
now publishes the split rather than the headline: the link needed nothing new, the satellite needed
a kind's signature to change — *precisely* the case `opl.vault.registry`'s docstring has always said
does **not** clear the "+1 file" bar.

**The seven-module list belongs to `payments_domain.py`'s docstring, not to `registry.py`'s.**
`opl.vault.registry` states the bar and names no such list; the enumeration of `registry.py`,
`specs.py`, `hubs.py`, `satellites.py`, `observation.py`, `effectivity.py` and
`domains/__init__.py` is `payments_domain.py`'s own. **Six** of those seven were touched, not
five, and two new modules were added (`registry_satellites.py`, `satellite_grain.py`). **Only
`hubs.py` survives**: `effectivity.py` is in `096e101`'s own `--stat` at `2 +-`, carried in by
the rename sweep in that same commit — Decision 6's subject landing inside a count that this
document then reported wrong.

**Rejected: an `EffectivitySatellite`-shaped table.** An effectivity satellite records the window a
**relationship** held and closes it when the relationship stops being observed. A payment happened
once; there is no window to close, and a departure at this grain would mean the processor
un-delivered a payment. `sat_eff_*`'s whole mechanism is disappearance-driven
([ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md)), and a table
whose closing path can never have a producer is a table with an untestable half.

**What reverses it:** a second link-parented satellite whose behaviour a hub-parented one cannot have — a different delta rule, a different grain contract, a closing path. Two tables sharing a kind and disagreeing about what the kind does is the moment the kind splits, and `Link` itself entered this package that way.

---

## Decision 4 — `applied_date` becomes a DECLARED source, because `bronze_payments` deliberately carries no `_snapshot_ref_date`

`satellite_candidates` built `applied_date` from `_snapshot_ref_date` **unconditionally**, and every
one of the four satellites that existed before this phase reads it. **`bronze_payments` does not
have that column**, and its absence is argued in four independent places:

| site | what it says |
|---|---|
| `opl.bronze.autoloader` (`add_common_audit_columns`) | omits it for a generated source — *"stamping it anyway would have been the quiet failure"* |
| `opl.bronze.snapshot` | three answers to *"when is this row a snapshot of"*, one per KIND of source; a generated stream declares none |
| `opl.bronze.rules` (the payments set) | omits `unprovable_snapshot_ref_date` *"because THE COLUMN DOES NOT EXIST on those rows"* |
| `opl.dataops.cadence` | the same distinction, one layer up |

So the satellite could not get its `applied_date` the way the other four do, and the repair is
`opl.vault.specs.AppliedDateSource`: **a source column plus a rule for reading a calendar day out
of it**, defaulting to `_snapshot_ref_date` so the four existing satellites are behaviourally
unchanged, refusable at import, and refused again by
`_refuse_an_applied_date_the_source_cannot_provide` before any expression is built.

**Rejected: making `bronze_payments` stamp `_snapshot_ref_date`.** That changes a bronze
deliverable and forces the payments DQ rule set to take on `unprovable_snapshot_ref_date` — which is
the exact shape `autoloader.py` calls *"a control deliberately left out so a column it refuses can
be written"*. It would have made a satellite loadable by making a bronze table lie.

**Rejected: `SnapshotAxis` as the declaration's type.** That type answers *"when did we OBSERVE this
row"*; this one answers *"when was the FACT true"*. They are **different columns on the same table**
for payments (`_snapshot_month` and `event_time`) and for the RFB (`_snapshot_month` and
`_snapshot_ref_date`), and reusing the type would let a caller pass an axis where a fact date
belongs — where the two would agree on every source that has only one of them.

**And the reading is ten characters of ISO text, not `ref_date_from_instant`, on a MEASUREMENT.**
That function pins the 27-character microsecond rendering `opl.bronze.snapshot_axis` declares;
`opl.generator.instants.to_text` renders **24 characters with three fractional digits**, so it fails
both the width check and the pattern and **returns NULL for every payment row**. The link's own T1
fixture used a 27-character `event_time` *that this producer never emits* — so a satellite built on
that fixture would have passed the fixture and failed in production. The derivation taken is the one
the gold layer already uses on this exact column (`opl.gold.conformed.day_of`,
`to_date(substring(event_time, 1, 10))`), and never a `CAST`: a cast resolves the instant in the
**session** timezone, and `applied_date` is the satellite's ordering axis.

**What reverses it:** `bronze_payments` gaining a `_snapshot_ref_date` it can prove — which means the generated stream declaring a date in its own filename, the thing three bronze modules say a generated stream does not have. The declaration would then be redundant for this table and would still be the mechanism, because the four RFB satellites and the payments one would no longer be readable from one constant by luck.

---

## Decision 5 — `BlastRadius` carries a THIRD shape, and the two-shape model was emitting a false sentence

Before this phase every bronze table had exactly one leg. `payments` and `ptax` reached gold
directly with an empty vault tuple; everything else reached gold only through the vault. **One tuple
plus a boolean said everything**, and `bypasses_the_vault` could be spelled *"the vault leg is
empty"*.

**`link_payment` made `payments` the first table with BOTH legs, and the two-shape model answered
that state by reporting the union and attributing all of it to the vault leg.**
`blast_radius_note("payments")` emitted *"payments feeds `link_payment` in the vault, and through
them `dim_date`, `fact_payment` in gold"* — **false**: `link_payment` reaches no gold table at all.
That sentence goes into an issue body a person acts on, about the table behind the workspace's
largest incident.

Worse, and this is the part that makes it a decision rather than a bug fix:
**`_assert_no_bronze_table_reaches_nothing` lost its ability to fail.** The guard's demonstration
had been `payments` with its direct-to-gold edge removed; registering `link_payment` gave `payments`
a vault leg, so removing the direct edge no longer emptied its radius and the guard stayed green
over a mutation it exists to catch.

What changed:

- **`gold_direct` is its own field**, not a subtraction from `gold`. A gold table can be reachable
  **both** ways, so `gold - gold_direct` is not "the vault-reached gold" and any reader computing it
  would get a smaller set than the truth.
- **`bypasses_the_vault` reads the direct leg**, not an empty vault leg.
- **`blast_radius_note` gained a fourth arm** naming both legs, so the table with two legs gets two
  sentences rather than one wrong one.
- **`__post_init__` refuses a direct leg the union does not contain** — `gold` stays stored because
  it is the field the issue payload carries and `issue._radius_of` compares field-for-field, so the
  redundancy is **checked** rather than removed.
- **The guard's demonstration moved to `ptax`.** `ptax` is now the last bronze table with no vault
  leg, so **the guard is one table away from having no demonstration at all** — recorded in the test
  on purpose rather than discovered later.

**Rejected: keeping one gold tuple and a `bypasses` boolean set by the declaration.** That is a
second spelling of a fact the two declarations already carry, and `opl.config`'s month rule is this
repository's record of what two spellings of one value cost.

**What reverses it:** `fact_payment` re-pointed at `link_payment`, which empties `payments`' direct leg and leaves `ptax` as the field's only witness — at which point the third shape is carried by one table and the question of whether a declaration deserves a field it exercises once becomes live again.

---

## Decision 6 — a rename carries its own prose sweep, IN the rename commit, re-derived AFTER the edit

This phase and its sibling met one failure mode over and over, every instance green at the time.
The framing is the F8 session's and is better than this session's, so it is attributed and
quoted:

> **The population a check runs over is chosen by hand somewhere nobody re-derives.**

**The instances below are NAMED, NOT NUMBERED, AND NO TOTAL IS PUBLISHED — both of which are
corrections this Decision earned the hard way.** An earlier draft said the phase met the mode
*"four times"*; a correction round changed it to *"five"*. Both were counts over a population
assembled by hand from what one reader happened to recall, which is this Decision's own subject
committed inside the Decision. The list is what `.plans/2026-09-02-f2-wave-2-progress.md`
records; it is **not a closed set**, and a later reader should re-derive it from that log rather
than trust an enumeration here. And the names are load-bearing: **a cross-reference to an ordinal
rots the moment an instance is inserted**, which is exactly how *"Decision 6's fifth instance"*
came to denote two different things in two files of this same phase.

- **THE HAND-TYPED WRITER** (F2w2 T1). `issue.as_mapping` wrote the blast radius from **three
  hand-typed literals**. T1 added `gold_direct` to the dataclass and to the **reader** and not to
  that writer, so every payload round-trip raised `KeyError: 'gold_direct'`. **19 tests red in
  files T1's own verification never ran** — confirmed by the reviewer's mutation M7 (20 red, one
  of which was a lock that did not exist before the change). The irony is exact: T1's commit body
  says the reader *"fails loudly rather than comparing equal on three fields of four"*, and T1's
  writer then produced precisely such a payload on every call. The fix is to iterate
  `fields(BlastRadius)` rather than to type the field list.
- **THE NARROWER GLOB** (F8). A probe file left one module red while a second module's sweep
  reported green, because the second globbed a **narrower suffix set** than the first. **No count
  is quoted here, on purpose.** An earlier draft attributed a `76 passed` to this instance;
  `76 passed` is F2w2 **T3's** own measurement of `tests/test_vault_entry_points.py`, and the
  log's F8 entry carries no number at all. A measurement that migrated between tasks while
  keeping its authority is the same defect one level down, so the number is dropped rather than
  repaired.
- **THE CANCELLING PAIR** (F2w2 T2). The applied-date demand added to the job-wiring lock **killed
  zero tests** under mutation, and **cancelled exactly** against its own companion edit: removing
  both returned the file to its previous behaviour. A lock added over a population no test
  constructed.
- **STALE ON ARRIVAL** (F2w2 T2), the sharpest of the staleness family.
  `tests/vault/test_registry_satellites.py` named `satellites._grain_key_mismatch` when the same
  change had just moved that function to `opl.vault.satellite_grain`. **It was stale on arrival,
  in a file created by the very change that made it stale.** *And this entry used to cite that
  mention at `:166`, which had itself drifted to `:173` by the time a correction round checked
  it — a rotted line anchor inside the instance about rotted references. The citation is now the
  file and the symbol, which do not move when a docstring grows.*
- **THE PREDICTED CLOSURE** (F2w2 T4; framing F8). The phase plan instructed `fdb:1504` to close.
  T4 verified before closing and found the entry's own exerciser conflated *"a second link with a
  declared derivation on an identifying end"* with *"the field's second consumer"* — `link_payment`
  supplies the first and not the second, so the row stays open. The F8 session's formulation names
  why a plan is not enough, and it is quoted rather than paraphrased:

  > **A plan predicting a closure is not evidence of one.**

  **This is a DIFFERENT VARIANT from every instance above it, a distinction the F8 session drew
  and this session's controller had not.** Those are claims about what **IS** — a name, a
  population, a branch — false about the tree at the moment they are read, and reachable by a
  sweep over what the tree says. A predicted closure is a claim about what **WOULD BE**: on the
  day it is written it is false of nothing, so **no sweep of existing claims can ever reach it**.
  It goes false only when the phase it predicted arrives and does something else, by which time
  nobody is grepping for it. The check it needs is a **re-derivation of the prediction against the
  tree at the moment the phase ends**.
- **PER-DOCUMENT CONSISTENCY** (F2w2 T4; framing F8), and among the reachable variants it is the
  sharpest. A draft of this document's Consequences said *"`fdb:1504` closes"* while
  `docs/unexercised-ledger.md`, edited in the same round, said it does not — this ADR carrying
  verbatim the conflated reasoning that its own ledger row exists to diagnose. **It was caught
  inside the round and nothing shipped**, which corrects an earlier wording here that said it had.
  What let it survive as long as it did is the part worth keeping: **each document was internally
  consistent.** A reviewer reading the ADR finds nothing wrong with the ADR; a reviewer reading the
  ledger finds nothing wrong with the ledger; the defect exists only in the relation between them,
  and nothing in the suite locks ADR prose against ledger prose —
  `tests/test_unexercised_ledger.py`'s corpus is `docs/*-evidence.md`, and
  `tests/test_adr_index.py` reads titles, status and conditions.

  > **Internal consistency per document is not consistency, and no per-document review reaches
  > it.** — F8

- **THE GUARD THAT GUARDED THE SEAM AND NOT THE POPULATION** (F2w2 T3). An anti-vacuity guard was
  written precisely to stop a parametrised lock reporting green over an empty population. Emptying
  the population it guards — `_PAIRINGS = ()` — left **exit 0, 36 passed and the guard's own arm
  green**, with pytest printing `got empty parameter set`. The quoted framing at the top of this
  Decision, instantiated inside the guard written to prevent it.
- **THE DETECTOR WITH ITS OWN DEFECT** (the controller; caught by F8), and it generalises furthest.
  The controller wrote a detector for pure-EOL file flips, ran it, got no output, and reported that
  clean result to a peer. **F8 asked whether it fires on a planted flip. It cannot:** the detector
  iterated `git diff --name-only`, which under `core.autocrlf=input` emits nothing at all for a
  pure-EOL change, so a flipped file never enters the loop and the test inside it is unreachable. A
  green from it was never evidence. **Nor does the obvious replacement work, and the number first written
  here was itself the defect this instance is about.** An earlier draft said
  `git status --porcelain` "saw 6 and missed 11" of 17 CRLF-on-disk files. Re-measured by
  splitting the visible files by WHY they were visible: **not one was visible for its line
  endings** -- every one was reported because its CONTENT had changed, and the EOL state
  was incidental. **Blindness to a pure flip is total.** The mechanism, settled across two
  sessions: git compares the index entry's recorded SIZE first and never hashes on a match,
  so a just-flipped file is reported for its stale stat while a long-standing one is
  invisible. Both detectors fail the audit question; only a byte or hash comparison against
  the blob answers it. Two sessions each "reproduced" a mechanism from a single tree and
  reached opposite conclusions, and the controller then carried the refuted 6-and-11 into
  the brief that produced this paragraph, having already measured and written down its
  correction.

  > **The right question was not "did it report clean?" but "does it fire on a planted
  > positive?"** — F8

  — and its corollary, earned in that exchange: **a positive planted in ONE tree is a population of
  one.** What survives is neither detector but the rule: verify a probe by **byte or sha comparison
  against the blob**, never by asking `git status` or `git diff`, because under `autocrlf=input`
  neither is a dependable witness to a pure-EOL change. This is ADR 0018's house rule applied to a
  check its own author had written five minutes earlier and not tested.
- **THE FACT THAT ROTTED UNDER A PARALLEL TASK** (the controller — and this one is the controller's
  defect, not any task's). Decision 7's first bullet asserted, present tense, that
  `sat_link_payment` had **no runnable entry point at all**, and the whole argument under it rested
  on `databricks/src/vault_load_satellite.py` calling `domains.parent_hub` and refusing a link
  parent. **It was true when it was written, and a sibling task made it false while its author was
  still writing.** T3 and T4 were dispatched in PARALLEL on files chosen to be disjoint: the FILES
  were disjoint and **the FACTS were not**, so no file-level boundary could have prevented it.

  **What makes it a Decision entry rather than a fix:** T4 had already seen this trap and avoided
  it once. Its own report records WITHHOLDING a ledger row for exactly this reason — *"a row
  asserting the entry point is missing would be false on arrival, the exact defect Decision 6
  names"* — and it then wrote the same assertion into this ADR, which is the one artefact this
  repository does not rewrite. **The ledger was clean and this document was not.**

  **It is the same failure mode arriving from a new direction: concurrency rather than staleness.**
  Every instance above it is a claim whose subject had already moved when it was written and nobody
  re-derived. Here the subject moved AFTER it was written and BEFORE it was reviewed — so
  re-deriving *after the edit*, which is the rule this Decision states, would not have caught it
  either. Only re-deriving at REVIEW time does, against a tree that has stopped moving. **The
  answer is therefore not a check but a process decision, taken 2026-09-02: stop parallelising
  tasks whose FACTS overlap, and sequence them.** Decision 7 below is written against `7b4b925`,
  with the tree stationary.

  **And the claim had rotted in TWO files, which only the sweep found.** The review named
  Decision 7. Re-running the prose sweep after that edit — this Decision's own rule, applied to
  this Decision's own correction — found the identical assertion a second time in
  `src/opl/triage_agent/blast_radius.py`'s declaration comment, where it read *"IT HAS NO RUNNABLE
  ENTRY POINT AT ALL TODAY, WHICH IS MORE THAN A MISSING YAML TASK AND IS STATED SO T3 CANNOT READ
  IT AS LESS"* — a sentence written **to** T3 about the change T3 then made. Both are corrected.
  **A review that names a site is not a sweep**, and a correction round that repairs only the
  sites its review listed is running over a population somebody else chose by hand.

**So the rule this ADR states:**

> **A file created inside a change is not in any population anyone thought to enumerate.**

No sweep of pre-existing sites can reach it, because it did not pre-exist. **A rename must therefore
carry its own prose sweep in the rename commit, re-derived *after* the edit rather than before it**
— and the sweep must be wrap-tolerant, because a line-based `grep` cannot see a sentence that breaks
across two lines, which is how one stale site survived a correction pass in F8.

**And the cross-session check is recorded as it actually went, one error each.** F2w2 told F8 that
four `databricks/` comments naming a deleted private were stale; **F8 refused, correctly** — the
private is still defined on its branch, so those comments are true there, and editing them would
have made its prose false against its own code on a bet that an unmerged rename lands. F8 handed
over a 22-line, 13-file population with an action attached, **without partitioning it by which
module each mention named**; only 4 of the 6 mentions go stale under the split, and the other 2 name
`effectivity._grain_key_mismatch`, which still exists — striking them would have destroyed correct
sentences. **Both errors are the same shape as the defect above: an action asserted over a
population that was not derived, and both were caught by the other party re-running the grep instead
of trusting the message.**

**Rejected: a repository-wide mechanical check that every backtick-quoted dotted name resolves.**
It is the obvious generalisation and it is not available here: the corpus is full of names that are
deliberately absent (things refused, things on another branch, things a phase decided not to build),
so the check's population would itself be hand-curated — this defect wearing a test.

**What reverses it:** nothing reverses running a sweep after an edit. What would make it unnecessary is a check that derives its own population from the tree at the moment it runs, for the specific class of name being renamed — narrower than the rejected general one, and worth building the next time a rename touches more than a handful of files.

---

## Decision 7 — the phase ships UNCLOSED, and the README's limit narrows rather than closing

**The workspace refuses to launch anything.** Measured by the F8 session on 2026-09-01/02:

| operation | today |
|---|---|
| reads, `jobs/update`, `bundle deploy` of an **existing** resource | work |
| `bundle deploy` creating a **NEW** job | **403 `PERMISSION_DENIED`** |
| `jobs/run-now`, SQL warehouse start | **refused** |

Last run terminating `SUCCESS`: **2026-08-28T18:32:13Z**. Therefore protocol §9's conditions 1 and 4
cannot be met — there is no run-evidence document produced from a run, and the tables do not exist
built by their own code. **This phase merges without closing, and no controller may declare it
closed.** Standing decision 6: *a path that ran zero rows through it is not a path that works.*

**Two consequences are published rather than worked around:**

- **`sat_link_payment` HAS a runnable entry point since `7b4b925`, and has NO JOB TASK, and the
  difference is the whole of what the 403 costs this phase.** `databricks/src/vault_load_satellite.py`
  now resolves `domains.parent_of` and routes on the resolved **parent's kind** through a
  `parent_arguments` seam — a grain for a hub, `axis=` for a link — so the script calls
  `load_satellite` with arguments a link parent accepts. **What is missing is the YAML task, and
  the reason is the 403:** `bundle deploy` cannot create a new job resource, so no payments vault
  job exists to hold one. Measured 2026-09-02 on `7b4b925`: `grep -rn "sat_link_payment"
  databricks/resources/` returns **nothing**, and
  `tests/test_vault_job_wiring.py::test_every_registered_vault_table_is_loaded_by_exactly_one_task`
  is one of this phase's fifteen reds — the registered table that no task loads.

  **This bullet said the opposite until the correction round, and the reason it did is a Decision
  6 instance rather than a typo.** *"No runnable entry point at all"* was **true when it was
  written** — the script resolved `domains.parent_hub`, which refuses a link-parented satellite by
  name — and T3 made it false while T4 was still writing, the two having been dispatched in
  parallel on files that were disjoint while their FACTS were not. Decision 6 records it as *the
  fact that rotted under a parallel task*, and it is the controller's defect rather than either
  task's.

  **The order T3 took is the one that removes the hazard, and it is named because the opposite
  order was available and cheaper.** Adding only the YAML task would have cleared the totality red,
  passed the entry-point lock, passed the source lock and left the grain sweep stepping past it:
  **a fully green suite over a task that raises `ValueError` on the cluster** — this phase's own
  subject reproduced as a deliverable. The script was repaired first; the task is owed to a
  workspace that will accept one.
- **`fact_payment` still reads `bronze_payments` directly.** Re-pointing it at the link is a gold
  refactor with its own risk and its own decision, and it is **explicitly out of this phase's
  scope**. So the honest sentence after this ADR is *"the payment is in the vault and the fact does
  not yet read it"*, never *"payments go through the vault"*.

**Rejected: hanging the loader off an existing job's task list to dodge the 403.** It would be a
distortion that outlives the outage, and no later reader would know why the task is where it is.
Both this session's controller and the F8 session reached that conclusion independently.

**What reverses it:** a workspace that will launch a run — at which point `docs/f2-wave-2-run-evidence.md` can be produced from a run instead of from a local Spark session, and protocol §9's conditions 1 and 4 become answerable for the first time in this phase.

---

## Consequences

- **`PHASES["0022"]` in `scripts/adr_index.py` is declared `UNMERGED`, and it MUST be re-declared
  with the real merge sha once this branch's PR merges.** The sentinel is refutable on purpose:
  `tests/test_adr_phase_declaration.py` asserts git agrees there is no merge, and **the moment this
  ADR's adding commit becomes an ancestor of `origin/main` the declaration is stale and goes red.**
  Three properties a reader needs: the refuting arm **skips in CI** (`actions/checkout@v4` at its
  default `fetch-depth: 1`), so the red appears **locally only**; the declaration goes stale **at
  the merge**, so the local suite reddens while CI stays green; and `origin/main` is a
  remote-tracking ref, so **`git fetch` first** or the arm skips for the wrong reason.
- **The README's *Honest limits* sentence is now half false and half true**, and the half that
  stands is stated in Decision 7 rather than left to be discovered. Its counts move too — the vault
  gains two tables — and they must be **re-derived on the merged tree, never on this branch**, which
  is what made a PR red twice in F7.
- **`fdb:1504` does NOT close, and the phase plan predicted that it would.** It waited for *"a
  second link with a declared derivation on an identifying end"*, and `link_payment` does have
  **two**, both checked against `hub_empresa`'s width at import — but that exerciser conflated a
  second DERIVATION with a second CONSUMER of the field, and only the first arrived. Measured on
  the real registry: `identity_derivations_of(link_payment)` returns two `KeyPrefix`es, and the
  link's only satellite is `sat_link_payment`, which is `transactional=True`, takes `axis=` and
  reaches no observation grain at all. `satellite_grain._refuse_a_prefixed_hub_grain`'s own
  docstring states the routing — *"a link-parented satellite never reaches a grain at all:
  `snapshot_axis_for` refuses it above, by name, before this is called."* So
  `link_merchant_empresa` is still the only link whose non-empty `key_prefixes` reach a grain,
  the anchored headline at `docs/f-db-run-evidence.md:1504` is still true, and the row **stays
  open in the ledger's §3.2** with its exerciser corrected rather than closing. **An earlier
  draft of this bullet said it closed**, contradicting the ledger row in the same commit;
  Decision 6 records that defect under **PER-DOCUMENT CONSISTENCY** — named rather than
  numbered, because the ordinal it used to carry denoted a different instance in a different
  file of this same phase.
- **`vaultreg:5` splits.** Its registry half is confirmed — a real domain registered from one new
  file — and its `hub_account`/`hub_customer` half is **falsified** by Decision 2. A ledger entry
  that closes half and is refuted in the other half is a shape the ledger had not carried before.
- **Eight rows of NEW DEBT are opened by this phase**, enumerated by id rather than described,
  because an enumeration is checkable against the document and a description is not — which is
  this bullet's own failure mode, three times over now. **"New debt" is the definition and it
  is stated rather than left to be inferred:** rows this phase adds under STANDING LIMITS or
  STILL UNEXERCISED. This phase adds two further rows that are not debt — `vaultreg:5` into
  CLOSED and `vaultreg:16` into NO LONGER MEANINGFUL — and they are the bullet above, THIS SENTENCE USED TO TELL THE READER TO COUNT ADDED TABLE ROWS IN THE DIFF AND
  EXPECT TEN. Measured, that count is EIGHTEEN for the ledger alone and twenty-seven for
  the whole diff: it includes six `### 0.4` key rows and counts a row restated in place as
  an addition. No reading of it yields ten. The eight above is derived from the BUCKETS --
  two rows added under STANDING LIMITS and six under STILL UNEXERCISED -- which is the
  count this bullet means and the one the totals lock re-derives. An enumeration is
  checkable against the document; the RECIPE for checking it has to be checked too, and
  this one never was.

  **Six of the eight** were found by T2's correction round and its review rather than by any
  implementer, and the log lists them under *"FOR T4 — LEDGER ROWS THIS CORRECTION FOUND AND
  DID NOT WRITE"*: `jobdem:185`, `jobdem:203` and `vwiring:534`, the three undriven job-wiring
  branches; `vaultreg:527`, the unreachable *neither a hub nor a link* refusal in
  `registry.parent_of`; `vspecs:290`, the **inert** delta detector on `sat_link_payment` (the
  link hash key already carries `transaction_id`, so every payment is its own partition with
  one `applied_date` and the `lag` is always NULL — idempotence comes from
  `_without_persisted`'s anti-join alone); and `golddim:11`, **three** stale prose sites under
  `databricks/`, which is another session's area this phase and is therefore recorded rather
  than edited. The remaining two came from elsewhere: `paydom:159` is **T1's own declared
  gap** — `fact_payment` still reads `bronze_payments` directly, which Decision 7 puts out of
  scope — and `ventry:486` is **T3's**, declared as an uncovered residual in the commit that
  closed T3's own findings: `satellite_grain._transactional_axis` returns the axis it is handed
  **without checking it against `source_table`**, so a substituted axis reads a window on a
  column the source does not key on and nothing refuses it. T3 measured it and correctly did
  not repair it in a test, because the durable repair belongs in `src/opl/vault/`.

  **Three earlier spellings of this bullet were wrong, and each error is worth its sentence.**
  It enumerated a *missing entry point* row that was deliberately never written (no row in the
  ledger ever named one, and `7b4b925` landed the entry point itself), and it omitted
  `paydom:159`, which was written. It said **five** stale `databricks/` sites while
  `golddim:11` — the very row it was describing — says *"THREE, not the five an earlier
  hand-assembled list reported"*: this document carried the number the ledger had already
  refuted, in the document that states Decision 6. Re-derived on 2026-09-02 by grep and not by
  copying, `_refuse_a_mismatched_hub` survives at exactly **three** sites —
  `resources/gold_dim_company_job.yml:93`, `resources/vault_empresa_job.yml:119` and
  `src/gold_load_dimension.py:11`. And it said **five of seven** came from T2's correction
  round when the log's own list carries **six**, `golddim:11` being its fifth item: a
  provenance count re-derived from memory rather than from the record it cites.
- **The method found what a green suite could not, in every round of this phase but one**, and
  the exception is as much of the evidence as the hits. Re-derived from
  `.plans/2026-09-02-f2-wave-2-progress.md`, which is the record, because an earlier spelling of
  this bullet got every clause of its own evidence wrong:

  | round | mutations proved by the author | what the independent review then found |
  |---|---|---|
  | T1 | **14** new locks, all 14 red | **two BLOCKING** (plus two MEDIUM, several LOW), one a guard that had lost its ability to fail |
  | T2 | **43** | **two BLOCKING**: a flag that switched a ledger off at the loader, and a lock that killed zero tests |
  | T2's correction | **five** (A–E) | **nothing blocking** — the first round in the phase to come back clean |
  | T3 | its own set | **two BLOCKING**, one an anti-vacuity guard that guarded the seam and not the population |
  | T4 | **four** in its correction round and **five** in the second, each restored and byte-compared | **two BLOCKING**; the review of T4's correction then found **three**, one of them the controller's rather than the task's |

  The earlier spelling read *"T2's correction mutation-tested 43; **the review of it** still
  found two more."* **Every clause of that is false:** the 43 is T2's IMPLEMENTER's, the two
  blocking findings are the independent REVIEWER's, T2's correction's own record lists **five**,
  and the review of that correction found **no blocking defects at all**. A false statement
  inside this document's evidence for its own central methodological claim — which is why the
  table above is re-derived from the log and the correction is left visible rather than tidied
  away. Decision 6 is the generalisation, and ADR 0018's house rule is the short form: *when a
  check reports the expected value, ask what else would produce that value.*
- **A fixture stopped being able to lie.** `tests/vault/test_registry.py`'s throwaway domain was
  named after wave 2's three tables *because they were wave 2's three tables*; two of those names
  are now refuted and the third collides with a real table of a different shape. The fixture keeps
  its proof and loses the names, so a reader grepping for `hub_account` no longer finds a test that
  appears to register one.

## References

- `src/opl/vault/domains/payments_domain.py` — the refutation in full, and the module docstring this
  ADR's Decisions 1–4 compress.
- [ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md) — the
  dependent-child-key deferral this phase consumes, the duplicate-hub argument reused in Decision 2,
  and the `"+1 file, 0 modified"` table Decision 2 supersedes two rows of.
- [ADR 0014](0014-dim-company-at-empresa-grain.md) — `dim_customer`'s status, superseded by
  Decision 2.
- [ADR 0018](0018-dataops-derives-it-does-not-instrument-and-it-does-not-act.md) — *a check that
  reports the expected value because it could not look*, which is Decision 6's ancestor and the
  reason the phase declaration skips rather than passes under a shallow clone.
- [ADR 0020](0020-the-triage-accelerator-is-deterministic-and-the-model-runs-as-the-control.md) —
  Decision 5's blast-radius manifest, whose two-shape model this phase falsified.
- `docs/unexercised-ledger.md` — every path this phase leaves unexercised, including the ones it
  opened.
