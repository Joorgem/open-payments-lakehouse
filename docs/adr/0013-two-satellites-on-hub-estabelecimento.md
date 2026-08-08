# ADR 0013 — two satellites on `hub_estabelecimento`, split on measured change rate, and the sharper boundary that was not taken

## Status
Accepted. Implemented as
`SAT_ESTABELECIMENTO_DADOS` and `SAT_ESTABELECIMENTO_ENDERECO` in
`src/opl/vault/domains/cnpj.py`; the partition is locked by
`tests/vault/test_estabelecimento_vault.py::test_the_two_satellites_and_the_key_partition_the_estabelecimentos_contract`.
Written in Task 7 of F2 wave 1, after the fact: the split was argued in Task 4
before any measurement existed, measured by the controller after the commit, and
the numbers were then quoted into a module comment. This is the one modelling
decision of the phase whose justification arrived **after** the artifact — and,
having arrived late, it arrived **twice**: the first measurement's column scope
was narrower than the payloads it was quoted against, Task 7's correction pass
caught it, and the controller re-ran it over the full payloads. Both figures are
below, because which one you are reading matters.

## Context

### Why payload composition is a decision and not a layout preference

A DV2 satellite writes a new row whenever **any** column in its payload changes.
So the payload's composition is what determines the table's row count over time,
and putting a column that moves rarely beside one that moves often means the rare
column's value is rewritten at the frequent column's rate. The satellite stays
correct either way — this is a cost and a legibility decision, not a correctness
one, which is exactly why it needs a record rather than a preference.

Estabelecimentos' bronze contract has **thirty** columns: three business keys
(`cnpj_basico`, `cnpj_ordem`, `cnpj_dv`), sixteen modelled across the two
satellites (six and ten), and **eleven deliberately unmodelled** — declared in
`UNMODELLED_ESTABELECIMENTO_COLUMNS` rather than left absent, so that a column
missing from both payloads is distinguishable from a column somebody forgot, and
so that adding a column to the contract turns the partition test red.

### The measurement

**Controller-measured** against real bronze over the **71,874,444** establishments
present in both 2026-06 and 2026-07, over the **full payloads** exactly as
`domains/cnpj.py` declares them (`01f192de-b784-1e33-a64b-625fad698c1a`):

| payload | columns | changed | rate |
|---|---|---|---|
| `_dados` | 6 | **1,211,834** | **1.69%** |
| `_endereco` | 10 | **570,075** | **0.79%** |

**≈ 2.13×.** Per column — **controller-measured**, from the earlier run
(`01f192ac-d8be-1e59-99e5-05717e28efcc`), covering **four of `_dados`' six**:
`nome_fantasia` **31,912** · `cnae_fiscal_principal` **84,588** ·
`situacao_cadastral` **976,355** · `motivo_situacao_cadastral` **976,333**.

> **The first figures quoted here were a lower bound, and Task 7's correction pass
> found it.** `_dados` was originally quoted as **1,076,696 / 1.50%** against a run
> covering only four of its six columns — `cnae_fiscal_secundaria` and
> `data_situacao_cadastral` were omitted — while the module comment attributed the
> rate to the full six-column payload. The re-run above closes the gap, and
> **`_dados` was understated**: 1,076,696 → **1,211,834**, taking the ratio between
> the payloads from ~1.9× to **~2.13×**. **The split's justification is therefore
> stronger than this ADR first claimed, not weaker.** Worth recording in the
> Context rather than in a footnote: a measurement whose scope has drifted from the
> thing it is quoted against is not automatically an overclaim, and this one ran
> the other way.

> **`_endereco` needed no correction — 570,075 either way — and that is a finding
> in its own right.** The two columns the earlier run omitted are
> `nome_cidade_exterior` and `pais`, the foreign-address pair, and they changed on
> **zero rows across all 71,874,444 establishments**. The Decision below places
> them in `_endereco` on the argument that they *are* the address for an
> establishment outside Brazil; this supports that placement from a second
> direction — they belong with the address **and**, over this pair of snapshots,
> they cost nothing to carry there. **Measured after the fact**, and stated as such
> rather than as a reason the placement was chosen.

## Options considered

### 1. One satellite over all sixteen modelled columns

Simplest, and the DV2 default when nothing is known about change rates. Rejected
on the measurement: an address change is a physical move and a situação cadastral
change is a registry event; they arrive on different rows of the RFB's own
history for the same establishment. A single satellite rewrites the whole
sixteen-column payload whenever either moves, so it would write roughly the sum
of the two rates where two satellites write each rate against its own columns.

### 2. Two satellites, split address from status

Chosen. Cuts along the one boundary that is both a real-world distinction (where
the establishment is vs what the registry says about it) and visible in the data.

### 3. Three or more, split on the sharpest measured boundary

**Rejected, and this is the option that deserved the most argument, because the
data supports it better than it supports the cut that was taken.**

The sharpest rate boundary in this table is **not** the one the split rests on.
Inside `_dados`, `nome_fantasia` (31,912) sits beside `situacao_cadastral`
(976,355): a **30×** spread, against the ~2.13× the cut itself rests on. Every
status change rewrites a trading name that moved thirty times less often. The
re-measurement narrowed that gap — 30× against 2.13× rather than against 1.9× —
but nowhere near enough to change the argument, and the per-column figures behind
the 30× are unaffected by it.

Rejected for three reasons, in ascending force:

1. `situacao_cadastral` and `motivo_situacao_cadastral` have near-identical change
   counts (976,355 against 976,333) and the motivo is what *explains* the
   situação, so they belong in one payload. **That is an inference, not a
   measurement** — two columns can share a marginal count while changing on
   disjoint rows, and no cross-tab was run. It bounds where a third cut could go,
   not whether one should exist.
2. A third satellite is a **design opinion at this evidence level**. One month-pair
   of data is one observation of a rate; splitting a table on it commits the vault
   to a shape that a second month could argue against.
3. **The asymmetry of the cost decides it.** Splitting `_dados` later means
   rewriting `sat_estabelecimento_dados` and its history. **Adding** a satellite
   for one of the eleven unmodelled columns touches nothing — new table, new file
   entry, no existing table's shape changes. Deferring the finer cut is cheap;
   taking it early and being wrong is not.

## Decision

**Two satellites on `hub_estabelecimento`, cut between address and status,
carrying six and ten columns; eleven contract columns declared unmodelled; the
partition asserted as a total by test.**

The 30× ride-along inside `_dados` is **accepted, named in the code beside the
numbers, and not hidden behind the 2.13× the cut rests on.** The candidate third
satellite, if one is ever taken, is `nome_fantasia` plus the CNAEs — not a general
subdivision.

## Consequences

- **This is the template wave 2 will copy**, which is most of why it is worth an
  ADR. The reusable part is the method, not the cut: measure the per-column change
  rate before composing a payload, and state the widest intra-payload spread the
  composition accepts.
- **The partition is total and enforced.** Every one of the thirty contract
  columns is a business key, the payload of exactly one satellite, or a declared
  omission with a reason beside it. Adding a column to the bronze contract turns
  the partition test red rather than silently landing it in neither satellite.
- **The row-count consequence is the point of the split and is not yet observed
  at scale.** Loading both months writes, in the FIRST month, one row per
  establishment per satellite — ~71.9M keys × 2 satellites ≈ **143.7M rows** —
  and in the SECOND only what changed: **1,211,834 + 570,075 = 1,781,909**, so
  **≈ 1.8M**. The 1.8M is the second month's **delta**, not the total; a reader
  sizing these tables adds the two and gets ~145.6M. Nothing in this phase has
  run either loader against real bronze, so both figures are arithmetic from the
  measured rates above rather than observed row counts.

  > **This bullet read "~1.6M rows between them", which was the arithmetic of the
  > retracted measurement** — `1,076,696 + 570,075`, the pre-correction `_dados`
  > figure. The correction pass updated the tables, the retraction blocks, the
  > module comment and the run-evidence document, and missed the one number
  > *derived* from the old one, inside the ADR whose Status advertises the
  > correction. With this ADR's own figures it is 1,781,909. The sentence was
  > also ambiguous between the delta and the total, which is an ~80× difference
  > for anyone sizing the tables, so it now states both.
- **`nome_fantasia` history is over-written relative to its own change rate**, by
  a factor of up to thirty. A consumer that reads only trading names pays for
  status churn. That is the accepted cost, and it is the first thing to point at
  if a third satellite is ever proposed.
- **The foreign-address pair is free, over these two months.**
  `nome_cidade_exterior` and `pais` changed on **zero** rows, so their presence in
  `_endereco` adds no satellite row at all today. That is a fact about this
  month-pair, not a property — a wave of RFB address revisions abroad would make
  them ordinary `_endereco` columns, which is exactly where they already are.
- **A second month-pair could reverse this.** The rates are one observation. If
  `_endereco` ever churns at `_dados`' rate, the cut buys nothing and the argument
  for it disappears — the split would still be harmless, but it would no longer be
  justified, and this ADR should be updated rather than left as a rationale
  nobody re-checked.

### What would change this decision

- A later month-pair bringing `_dados` and `_endereco` close together. The cut
  would then be legible but not economic. (The re-run that closed the original
  scope gap moved them **further apart**, 1.9× → 2.13×, not closer.)
- A cross-tab showing `situacao_cadastral` and `motivo_situacao_cadastral` change
  on substantially disjoint rows. That breaks the argument bounding where a third
  cut could go.
- A downstream consumer of `nome_fantasia` alone, at a volume that makes the 30×
  ride-along a real cost rather than a stated one.
