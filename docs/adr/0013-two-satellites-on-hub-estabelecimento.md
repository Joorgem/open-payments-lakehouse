# ADR 0013 — two satellites on `hub_estabelecimento`, split on measured change rate, and the sharper boundary that was not taken

## Status
Accepted, with one measurement left open and named below. Implemented as
`SAT_ESTABELECIMENTO_DADOS` and `SAT_ESTABELECIMENTO_ENDERECO` in
`src/opl/vault/domains/cnpj.py`; the partition is locked by
`tests/vault/test_estabelecimento_vault.py::test_the_two_satellites_and_the_key_partition_the_estabelecimentos_contract`.
Written in Task 7 of F2 wave 1, after the fact: the split was argued in Task 4
before any measurement existed, measured by the controller after the commit, and
the numbers were then quoted into a module comment. This is the one modelling
decision of the phase whose justification arrived **after** the artifact.

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

### The measurement, and what it does and does not cover

Controller-run against real bronze over the **71,874,444** establishments present
in both 2026-06 and 2026-07 (`01f192ac-d8be-1e59-99e5-05717e28efcc`):

| payload | changed | rate |
|---|---|---|
| `_dados` | 1,076,696 | 1.50% |
| `_endereco` | 570,075 | 0.79% |

Per column: `nome_fantasia` **31,912** · `cnae_fiscal_principal` **84,588** ·
`situacao_cadastral` **976,355** · `motivo_situacao_cadastral` **976,333**.

> **⚠️ The measured column scope is narrower than the payloads, and these two
> aggregates are lower bounds.** The controller's record of the run scopes
> `_dados` to **four** columns (the four named above) and `_endereco` to **eight**
> (the domestic-address eight, without the `nome_cidade_exterior` / `pais` pair),
> against payloads of six and ten. The module comment attributed the rates to the
> full payloads until Task 7's correction pass. A payload's true rate can only be
> **higher** than a rate measured over a subset of it, so the direction of the
> argument survives; what does not survive is quoting **1.9×** as a ratio between
> the two payloads. Settling it needs one re-run over all sixteen modelled
> columns — the query is named in the F2 wave-1 Task 7 report and this ADR should
> be updated with its answer.

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
(976,355): a **30×** spread, against the ~1.9× the cut itself rests on. Every
status change rewrites a trading name that moved thirty times less often.

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
numbers, and not hidden behind the 1.9× the cut rests on.** The candidate third
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
  at scale.** Both satellites' first two-month load will write ~1.6M rows between
  them against ~71.9M keys. Nothing in this phase has run either loader against
  real bronze, so the predicted advantage over a single satellite is arithmetic
  from the measured rates, not an observed row count.
- **`nome_fantasia` history is over-written relative to its own change rate**, by
  a factor of up to thirty. A consumer that reads only trading names pays for
  status churn. That is the accepted cost, and it is the first thing to point at
  if a third satellite is ever proposed.
- **A second month-pair could reverse this.** The rates are one observation. If
  `_endereco` ever churns at `_dados`' rate, the cut buys nothing and the argument
  for it disappears — the split would still be harmless, but it would no longer be
  justified, and this ADR should be updated rather than left as a rationale
  nobody re-checked.

### What would change this decision

- The re-run over all sixteen modelled columns coming back with `_dados` and
  `_endereco` close together. The cut would then be legible but not economic.
- A cross-tab showing `situacao_cadastral` and `motivo_situacao_cadastral` change
  on substantially disjoint rows. That breaks the argument bounding where a third
  cut could go.
- A downstream consumer of `nome_fantasia` alone, at a volume that makes the 30×
  ride-along a real cost rather than a stated one.
