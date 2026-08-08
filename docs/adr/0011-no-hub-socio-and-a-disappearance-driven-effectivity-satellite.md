# ADR 0011 — there is no `hub_socio`, and the effectivity satellite is driven by disappearance rather than by replacement

## Status
Accepted. Implemented in `src/opl/vault/partners.py`,
`src/opl/vault/effectivity.py` and the three registry concepts they needed
(`LinkEnd`, `Link.dependent_child_keys`, `EffectivitySatellite`); locked by
`tests/vault/test_socios_vault.py` and the new guards in
`tests/vault/test_registry.py`. Three mutation probes were run against the
implementation and each turns the intended test red and leaves the others green
(recorded in the F2 wave-1 Task 5 report).

**This ADR is the second documented departure from the master spec's §4.2**, the
first being ADR 0010's observation ledger. Both departures are from the same
sentence-level source and both are measured rather than argued.

## Context

### §4.2 corrects a grain error and then makes the same one

The master spec's §4.2 opens with *"Grão correto do CNPJ (correção de domínio)"* —
`hub_empresa` keys on `cnpj_basico`, not on the fourteen-digit CNPJ completo,
because the eight-digit root is what identifies the company. One line below it
prescribes a `hub_socio` keyed on `cpf_cnpj_socio`.

Measured over 2026-07's 27,990,592 socios rows
(`01f19061-9328-1159-a4e8-63a8b433237e`):

| `identificador_socio` | rows | distinct `cpf_cnpj_socio` | shape |
|---|---|---|---|
| 1 — PJ (a company) | 717,650 | 310,374 | 14 digits |
| 2 — PF (a person) | 27,260,118 | **999,853** | `***NNNNNN**` |
| 3 — foreign | 12,824 | **0 — all NULL** | — |

The RFB masks a natural person's CPF to its six middle digits **at source**. The
key space is therefore 10⁶ = 1,000,000, and **999,853 of it is occupied — 99.99%
saturation**. A hub on that column does not have collisions; it is made of them.
27,260,118 partnership rows spread over 999,853 keys means roughly **27 unrelated
people share every key**, by construction and forever.

Building `hub_socio` as written would ship a hub whose every row merges people
who have nothing to do with each other, and every satellite hanging off it would
interleave their histories. Nothing would fail.

### Two kinds of masking, and only one of them is ours

This distinction has to be kept straight because conflating it would make a
future reader think the link's business key could change if a grant changed.

- **`cpf_cnpj_socio` is masked by the RFB, in the published file.** The
  `***NNNNNN**` shape is baked into the data we receive. It is irreversible, it
  is the same for every reader, and it is why the key space is 10⁶. It is also
  why the key is **computable and stable**: nothing about our workspace affects
  it.
- **`nome_socio_razao_social` and `nome_do_representante` are masked by Unity
  Catalog**, by the column mask ADR 0008 installs. Measured against live bronze
  (`01f192b4-a6f8-1849-9955-f321d9742180`), the mask applies on read **to the
  table owner included**: both columns return the literal three-character string
  `***`, not a partial value.

The second measurement closes the obvious repair for the first. The natural fix
for "27 people share one masked CPF" is to add the partner's name to the key.
**The name reads `***`.** Keying on `(company, masked CPF, '***')` is byte-identical
to keying on `(company, masked CPF)`, so the repair would produce a key that
*looks* disambiguated and is not — strictly worse than not attempting it, because
the next reader would believe the hole was closed. **The bucket is irreducible,
not merely unrepaired.**

### The second problem: "effectivity satellite" names two incompatible things

`.plans/f2-research-snapshot-dv2.md` §3 records both definitions in circulation,
and ADR 0010 deliberately left the adjudication to this one:

- **§3a — AutomateDV.** A window over a link with a **driving key**: *"In any Link
  there are two FK columns, one will change over time and the other will remain
  constant."* The close is triggered by **replacement** — *"The old record now
  needs to be end-dated so that we do not have 2 open Link records."*
  **This is the one §4.2 points at.**
- **§3b — DataVault4dbt.** `is_active` over a single tracked hash key, driven by
  the key's **appearance and disappearance** in a full extract.

## Options considered

### The hub

**1. Build `hub_socio` as §4.2 prescribes.**
*Rejected on the measurement above.* 99.99% key-space saturation is not a data
quality problem to be cleaned; it is what the source publishes.

**2. Build `hub_socio` on `(cpf_cnpj_socio, nome_socio_razao_social)`.**
*Rejected, and it is the one that had to be measured to reject.* The name column
reads `***` to every reader the mask function does not admit, including us. This
option produces a hub identical to option 1 with a longer key.

**3. Build `hub_socio` only for PJ partners.**
*Rejected as a duplicate hub.* All **310,374 of 310,374** distinct PJ partner
CNPJs have their eight-digit root present in empresas — **zero unresolved**
(`01f19063-44ef-132a-8aa7-9068b624b370`). A corporate partner is not a new
business object; it is an empresa already in `hub_empresa`. A second hub over the
same key space would be two tables that look independent and are one.

**4. No `hub_socio`. The relationship carries the grain.** *Chosen.*

### The satellite

**5. §3a, replacement-driven, as §4.2 points at.**
*Rejected on measurement, and not merely as incomplete.* Of the 16,644,534
companies with partners in 2026-07, **8,266,470 — 49.7% — have more than one
simultaneous partner**; the mean is 1.681 and the maximum 2,573
(`01f19061-d161-17c6-971d-23106c8d8bcf`). AutomateDV's mechanic closes the old
window so that there are never two open link records for one driving key. **Two
open link records is the normal case for half the companies in this table.**
Applied here, a company gaining a second partner would end-date the first. §3a
does not under-report on this relationship; it reports the opposite of the truth.

**6. §3b, disappearance-driven, gated on the observation ledger.** *Chosen.*

## Decision

### There is no `hub_socio`; `link_company_partner` carries the grain

`link_company_partner` is keyed on exactly the measured business key of a
partnership row — `(cnpj_basico, identificador_socio, cpf_cnpj_socio)` — which is
`hub_empresa`'s business key followed by **two dependent-child keys**. That is the
idiom §4.2 itself chooses for `transaction_id` on `link_payment`: a key component
that identifies no business object of its own.

**The link is self-referencing.** It carries two references to `hub_empresa`:

| end | role | reference column | identifying? |
|---|---|---|---|
| the company | `company` | `company_hub_empresa_hk` | yes |
| the partner, when the partner is a company | `partner` | `partner_hub_empresa_hk` | **no** |

The partner reference is **not part of the link's identity** and that is a
deliberate distinction rather than an optimisation. The partner company's
`cnpj_basico` is the first eight characters of `cpf_cnpj_socio`, which is already
a dependent-child key — so the reference is a **function of** the identity, not a
part of it. Hashing it as well would make the link's own key depend on a value
**we derived**, where every other component is one the source **delivered**, and
would re-key the whole table the day the derivation changed.

**The reference is NULL where there is no partner company.** `hash_key_column`
never returns NULL — a NULL component encodes to the token `N` and hashes to a
perfectly ordinary-looking digest — so an unconditional derivation would give all
27.2M PF partners and all 12,824 foreign ones one shared digest that joins to no
hub row, and would give a PF partner a digest of `substring('***777777**', 1, 8)`
= `'***77777'`. A hash of garbage is indistinguishable from a hash of a company.
The condition is **both** `identificador_socio = '1'` (the source's own delivered
statement) **and** a fourteen-character value (the guard beside it).

### Foreign partners are admitted, not excluded, and the cost is counted

12,824 rows carry `identificador_socio = '3'` and `cpf_cnpj_socio` NULL — **no
business key at all**. Three options, and the decision is explicit because none of
them is free:

- **Exclude them.** *Rejected.* The link's key universe would then differ from the
  observation ledger's at the same grain — and that ledger is what the effectivity
  satellite gates window closes on, so the two would disagree about which
  relationships exist. It would also silently drop a whole class of real
  relationship from the vault.
- **Admit them under a surrogate key** (a row number, a hash of the whole row).
  *Rejected.* A row number is not stable across loads, so idempotence goes with
  it; a whole-row hash invents an identity the source does not have and splits one
  relationship into two the moment any descriptive column moves.
- **Admit them under the hash standard's NULL token.** *Chosen.* The key is
  `(company, '3', NULL)`, which is well defined, stable and re-derivable. **The
  consequence is that all of one company's foreign partners collapse to one link
  row**, and that consequence is real: two foreign partners of one firm are one
  relationship in this vault.

The coarsening is **counted, not conceded**: `PartnerLinkLoadResult.collapsed_duplicates`
and `EffectivityLoadResult.collapsed_duplicates` report how many source rows were
folded, so the number is in the operator's log rather than only in this document.

### The dedup rule, stated because a silent `DISTINCT` is a decision nobody recorded

The source is **not unique on the link's own business key**. In 2026-07:
27,990,592 rows over **27,986,263** distinct
`(cnpj_basico, identificador_socio, cpf_cnpj_socio)` — **4,329 collisions**; adding
`qualificacao_socio` and `data_entrada_sociedade` still leaves **3,088 exact
duplicate rows** (`01f19063-53c0-1f06-89f1-6aade0691af8`).

- **The link's fold is free.** A link row carries no payload, so collapsing
  duplicates loses only the choice of `record_source`, which is settled by
  `earliest_record_source`'s `min` over (month, source) — deterministic, where a
  bare `DISTINCT` or a `first()` depends on partition order.
- **The satellite's fold costs something**, because it must choose ONE
  `data_entrada_sociedade`. **The earliest delivered entry date wins.** That rule
  is deterministic (two runs agree), order-independent (no dependence on partition
  layout), and — unlike `opl.vault.satellites`' lowest-`hash_diff` tie-break —
  **not arbitrary**: the open of a window is the earliest moment the source claims
  the relationship began.

### The effectivity satellite is §3b, and every close is gated on one ledger state

`sat_eff_company_partner` writes a row whenever `is_active` **changes** for a link
hash key, in `applied_date` order: the relationship's first appearance, its
disappearance, and its return. A relationship present in both months writes **one**
row, not two — it is delta-driven exactly as a descriptive satellite is, through
the same `changed_rows`.

**A window closes on `absent_after_observation` and on nothing else.** The other
four `ObservationState` values close nothing:

| ledger state | what it means | window |
|---|---|---|
| `observed` | the source published it | open |
| `observed_with_rejected_siblings` | published; some sibling rows failed our gate | open |
| `rejected_by_our_gate` | published, and **we** removed it | **stays open** |
| `absent_before_first_observation` | not yet in existence | nothing to close |
| `absent_after_observation` | seen earlier, not seen here | **closes** |

The third row is the whole point of ADR 0010 arriving here. **1,781 sócio keys at
link grain are `rejected_by_our_gate` in 2026-07**: the RFB published them and our
DQ gate quarantined them. Closing their windows would have the vault assert that
1,781 partnerships ended when the only thing that happened is that we rejected a
row.

**The gate is written into the data.** Every closing row carries
`closed_by = 'absent_after_observation'`. It is single-valued today, by
construction, and that is the point: a future version that closed on another state
would say so in the table, and a reader auditing a close does not have to read
this file to know what authorised it.

**The ledger must be at LINK grain and the loader refuses anything else.** A
partner who loses one of two partnerships is `absent_after_observation` at link
grain and plainly `observed` at hub grain — so a hub-grain ledger reports no
departure and the window stays open forever, with the satellite answering, writing
rows, and never closing anything. `load_effectivity_satellite` compares the
grain's key columns against the link's own identity columns, derived from the
spec, so the two cannot drift.

### The open is delivered; only the close is derived, and the table says which is which

`data_entrada_sociedade` is populated on **100%** of 2026-07's rows with **no
`00000000` sentinel** (`01f19063-53c0-1f06-89f1-6aade0691af8`). The entry date is
the RFB's own fact. The exit is our inference from an absence.

The research's position is that a **derived** delete is a weaker claim than a
**delivered** one, and here it is weaker still, because part of the absence signal
is our own gate's. So the two are not presented alike:

| column | whose claim | named by |
|---|---|---|
| `data_entrada_sociedade` | the RFB's | **the RFB** — the source's own column name, value carried verbatim |
| `is_active` | ours | us |
| `last_observed_on` | ours | us |
| `closed_by` | ours | us |

**`last_observed_on` is not an end date.** It is the `applied_date` of the last
month in which we *observed* the relationship. The date the partnership actually
ended is something this pipeline does not know and does not claim: the RFB
publishes monthly snapshots, so the true end is somewhere in a one-month interval,
and asserting a point inside it would be inventing precision.

## What this task carries of Task 2's acceptance proof, and what it does not

Socios supplies **65,444** link keys present in 2026-06 and gone from 2026-07, of
which **zero** are in 2026-07's quarantine (`01f19061-e234-1617-bc9a-19f854e7b204`).
Estabelecimentos supplies **four** departures, **all four** of them our own gate's,
and **zero** true departures.

**Neither table proves the ledger discriminates.** A ledger that blamed the source
for every disappearance passes `tests/vault/test_socios_vault.py` in full; one that
blamed our gate for every disappearance passes
`tests/vault/test_estabelecimento_vault.py` in full. The discrimination lives in
the cross-table probe
`test_observation.py::test_a_departure_reads_as_our_gate_on_one_table_and_as_the_sources_on_the_other`,
which Task 2 already built and against which two deliberately degenerate
implementations were run.

What Task 5 adds is that a satellite now **acts** on the distinction rather than
reporting it: one window closes and the other does not, in the same load, over the
same table.

## Consequences

- **A partnership is identified, a partner is not.** Any downstream model wanting
  "all companies this person is a partner of" is asking a question this vault
  cannot answer, and cannot be made to answer without inventing an identity the
  RFB withheld. The dimensional layer must not build a `dim_socio` on
  `cpf_cnpj_socio`; the honest artefact is a partnership bridge.
- **Two people can be one relationship, and only within one company.** The same
  masked CPF at two companies is two link rows (the company is part of the key);
  two rows carrying it inside one company are one. That is asserted in both
  directions and counted, not left to be discovered.
- **A descriptive satellite on the link is now the obvious next table and does not
  exist.** `qualificacao_socio`, `faixa_etaria`, `pais`, `representante_legal` and
  `qualificacao_representante_legal` are declared in
  `UNMODELLED_SOCIOS_COLUMNS` with reasons, so a column in no table is
  distinguishable from a column someone forgot. Building it needs
  `load_satellite` to accept a link, which is a real edit and not a copy.
- **The two UC-masked columns can never enter the vault while the mask stands.**
  A satellite payload containing either would store `***` on every row and its
  `hash_diff` would be constant — change detection that can never fire, with no
  error attached. They become usable only when `opl_pii_readers` exists AND the
  job's run-as principal is a member of it, which is F4's work (ADR 0008).
- **`load_link` now refuses a link it cannot write.** `link_company_partner` left
  unrefused would not crash: `link_candidates` computes every reference from the
  columns its hub is *named* after, so both ends would be hashed from
  `cnpj_basico` and every relationship would read as a company partnered with
  itself.
- **The extensibility claim is unchanged in substance and narrower in wording.**
  Task 5 edited `registry.py` to add a fourth table kind, which is what the file's
  own docstring said a new kind would cost and what Task 4 did for `Link`. Wave
  2's `link_payment` needs a dependent-child key for `transaction_id`, and that
  now exists — so wave 2 remains "+1 file, 0 modified" for hubs, satellites and
  links. What is *not* covered is a link whose hub reference must be derived from
  another column: that is `opl.vault.partners`, the one domain-specific loader in
  the package, and it says so in its first paragraph.
- **The window closes are only as good as the quarantine's retention.** ADR 0010
  already made the quarantine a vault input; this ADR makes it a vault input whose
  loss would cause **false closes** rather than merely a lost distinction. A
  vacuum policy that dropped `bronze_cnpj_socios_quarantine` would turn 1,781
  `rejected_by_our_gate` keys per month into `absent_after_observation`, and the
  satellite would end-date them.
