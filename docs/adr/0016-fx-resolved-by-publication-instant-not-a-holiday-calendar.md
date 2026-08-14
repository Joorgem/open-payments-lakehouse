# ADR 0016 — the FX rate is resolved by PUBLICATION INSTANT from the PTAX series, never from a holiday calendar

## Status

Accepted. Implemented in `src/opl/gold/fx.py` (the reduce, the interval table and the
as-of join), `src/opl/extraction/ptax_source.py` (the request, the response validation
and `BRASILIA`) and `src/opl/contracts/ptax.py` (the landed shape); locked by
`tests/gold/test_fact_payment_fx.py`, `tests/gold/test_fact_payment.py` and
`tests/bronze/test_ptax_rules.py`.

Written in F-API Task 5, after the layer existed **and ran**. Two source sites
(`src/opl/gold/facts.py` and `tests/gold/test_fact_payment_fx.py`) said these deviations were
"recorded in the T3 ADR" while `docs/adr/` stopped at 0015 — a citation to nothing, which is
this repository's own recurring defect species. **Both now name this file, and both were
repointed without growing `facts.py` past its 799/800 lines.**

**What it decides.** Five things, and the fifth is inherited rather than new:

1. how the rate for a payment is chosen — an **instant** comparison against the PTAX
   series, and explicitly **not** the holiday calendar master spec §4.3 names;
2. what zone `dataHoraCotacao` is read in, and why that zone must live **in the value**
   and not in a cluster setting;
3. the column-name deviations from master spec §4.3's stated column list — **three of them,
   where the code and the phase evidence both say two**;
4. that this star carries **two different "as of" semantics on one fact row**, which no
   earlier document says out loud;
5. the bronze→gold seam — `fact_payment` reads `bronze_payments` and `bronze_ptax`
   directly, with no vault hop.

---

## Context

### What the master spec asks for

Master spec §4.3 (`2026-07-22-flagship-lakehouse-design.md:113`):

> **FX/PTAX:** PTAX de venda de fechamento; **fallback para último dia útil (calendário de
> feriados BR)**; `fact_payment` guarda `amount_original + currency + fx_rate +
> fx_rate_date + amount_brl`. FX só cross-border (maioria BRL) — showcase, não caminho
> quente.

Three things in that sentence are deviated from below: the fallback mechanism, and two of
the five column names. The closing-sale-quote (`cotacaoVenda`) and the cross-border-only
scope are followed exactly.

### A holiday calendar is a SECOND SPELLING of "is there a quote"

The fallback the spec names needs to know which days are business days. The PTAX series
already answers that question — a day either carries a quote or it does not — so a
calendar is a *second* implementation of one predicate, maintained separately from the
data it describes, and the two can disagree. Both directions of disagreement are silent:

| the calendar says | the series says | what the join returns |
|---|---|---|
| holiday | a quote exists | the join steps **over** a published rate and hands the payment an older one |
| business day | no quote | the join finds nothing on a day BCB never published, and the fallback stops one day short |

Neither raises. Both change every converted amount by a business day's drift, which is the
class of error nobody re-derives.

**The disagreement is measured, not argued.** F-API Task 0 walked the series from
2023-01-01 to 2026-08-05 — 903 rows — and found the witness:

> **2023-11-20 carries a PTAX quote, and today's Brazilian national-holiday list calls it
> a holiday.** Black Consciousness Day (*Dia da Consciência Negra*) became a **national**
> holiday only from 2024, under Lei 14.759/2023. A 2026-vintage calendar applied to a 2023
> date disagrees with the series about a day the series has a quote for.

One disagreement in 903 rows is not a rare-corner argument — it is the *first* holiday
whose national status moved inside the window this project can reach. A calendar is not
merely redundant here; it is redundant **and versioned by a different authority on a
different schedule**.

**And the ruling opposes no existing mechanism.** Nothing in this repository carried a
holiday concept when it was taken: `src/opl/gold/members.py` derives `is_weekend` from ISO
weekday numbering and carries nothing else. There was no calendar to reuse and none was
built.

---

## Decision 1 — the rate is the most recent quote whose PUBLICATION precedes the payment

> **The rate a payment converted at is the quote whose `dataHoraCotacao` is the latest one
> strictly before — or exactly at — the payment's own `event_time`, read from the landed
> PTAX series itself.**

Three clauses, each of which does work:

1. **It is an INSTANT comparison, not a calendar-day one.** The `cross-currency` stream
   opens at `2026-06-22T08:00:00.000Z` and closes at `21:53:15.000Z`; the 2026-06-22 quote
   publishes at `dataHoraCotacao 2026-06-22 13:06:19.750415` read as BRT, i.e.
   `2026-06-22T16:06:19.750415Z`. So **two payments on one calendar day, in one stream, in
   one currency, convert at two different rates** — 5.14420 for the 5,836 events before the
   bulletin and 5.13950 for the 4,164 after it. No day-grain implementation can produce
   that. A day-grain implementation would also hand a payment at `2026-06-22T01:53Z` a rate
   published 14 h 13 m later — **a rate from the future, in the one project whose headline
   is as-of-KNOWN-time** (ADR 0015).

   > **The model this clause first shipped with was wrong and is recorded rather than
   > deleted.** It said PTAX venda is a closing quote, so the rate for quote date *q*
   > becomes known "at the end of *q* in BRT", putting the gap at ~19 h. **Measured: every
   > 2026 row publishes between 13:03 and 13:25 on the quote's own date** — 2026-06-19 at
   > 13:03:25.555497, 2026-06-22 at 13:06:19.750415, 2026-07-31 at 13:10:31.061071. Under
   > the retracted model a payment at 21:00 BRT on Friday 2026-07-31 would have been
   > **denied** the 07-31 quote published at 13:10 that same afternoon. The rule stands; its
   > model is now the one the source ships, per row.

2. **Gaplessness is REPORTED, not asserted.** T3's original clause 2 asked for an assertion
   that the landed series is contiguous in business days. **That item was declined, with its
   mechanism**: any bound on the gap between two consecutive quotes is either a Brazilian
   holiday calendar — the thing this ADR refuses, with the witness above — or a number drawn
   from this one extraction window, which is the species `ptax_source` already refuses for
   magnitude ceilings ("a bound that cannot be chosen from the data is not a guard, it is a
   guess"). Friday-to-Monday is three days, a holiday weekend four, Carnival five.
   `FxSeries` therefore carries the quote count and the first and last publication instant,
   and `FxCoverage` carries the two numbers over the rows that actually consult a quote —
   **conversions that took the series' last landed quote**, and **the widest fallback any
   conversion took**. A truncated or holey extraction is five numbers in the run log rather
   than an invisible stale rate.

3. **A payment below the series' first publication is a REFUSAL, not a NULL.** The FX
   interval table's low end is **not floored**, which is the deliberate opposite of
   `dim_company` (ADR 0015 floors its first version at the epoch). A lookup convention about
   a company we know something about beats "unknown"; **a rate that did not exist yet cannot
   be applied.** `fx.refuse_payments_no_rate_can_be_resolved` stops the build, for
   `facts._refuse_payments_no_instant_can_be_read`'s reason: a NULL rate gives a NULL
   `amount_brl` and lowers every total by an amount nobody can name.

### Two supporting decisions the rule needs to be implementable

- **The quote date is carried from the REQUEST, because the API does not return it.** The
  Olinda response ships exactly three fields per row — `cotacaoCompra`, `cotacaoVenda`,
  `dataHoraCotacao` — and `dataHoraCotacao` is a **publication** instant, not the quote's
  date. Across all 73 probed days of 2026 the two coincide; **they do not coincide in
  1984**, where a quote requested for 1984-11-28 comes back stamped `1984-12-03 11:29:00.0`.
  A contract landing only what the API returns makes this whole rule degrade silently into
  the calendar-day comparison it exists to forbid. `opl.contracts.ptax` therefore splits
  `REQUEST_COLUMNS` from `RESPONSE_COLUMNS`, and the extraction is **one single-day request
  per calendar day of the window** rather than one range call — a range response is
  unattributable many-to-one (1984-11-28 and 1984-11-29 both publish on 1984-12-03). **The
  measured cost of this window is 60 requests of ~220 bytes in 52 s** — 42 quotes and 18
  empty envelopes, *not* the "42 requests" every document in the phase published until the
  run said otherwise (`docs/f-api-run-evidence.md` §2.5).
- **The FX side is reduced to one row per `(currency, quote_date)` BEFORE the join, and two
  rows that disagree are REFUSED — never `max()`, never `min()`.** `bronze_ptax` is written
  `mode("append")`, so a second extraction over the same window lands a second row for every
  date it covers; `ptax_source.sole_quote` reduces one *response* and cannot see across runs.
  Without the gold-side reduce an ordinary re-run doubles every USD fact row.
  `facts._refuse_a_row_count_that_is_not_one_per_delivered_identity` catches a fan-out only
  after the append, in a message whose own words are "THE TABLE ON DISK IS ALREADY WRITTEN".
  Two rows that **agree** reduce to the **earlier** publication stamp: the rate is identical
  either way and only its availability moves, so keeping the later stamp would deny a payment
  between the two a rate that had already been published.

---

## Decision 2 — `dataHoraCotacao` is read as BRASÍLIA TIME (UTC−03:00)

BCB publishes the stamp with **no zone designator at all**. The reading is a decision, and
it is measured rather than assumed:

- Every 2026 row publishes at **~13:0x**, which is the PTAX bulletin hour in Brasília. Read
  as UTC the same values would put the bulletin at ~10:0x local — before it exists.
- **BRT is also the fail-safe direction.** It places publication three hours *later*, so a
  wrong zone makes a payment fall back to an **older** rate rather than use one that had not
  yet been published. The failure this ADR exists to prevent is a rate from the future; the
  chosen zone errs away from it.

The offset has **one spelling in the codebase**: `opl.extraction.ptax_source.BRASILIA`, from
which `opl.gold.fx._BRASILIA_OFFSET` is *rendered* rather than restated. A second spelling of
this offset would be a second spelling of the ruling, and it would be silent — three hours is
not a shape any test notices, it is a rate one business day old.

**The verdict does not rest on this zone being right.** Read the stamp as UTC instead and the
publication boundary moves from event index 5,836 to 3,676: 1,801 USD rows fall back and
3,104 resolve same-day, against the 2,864 / 2,041 the run measured under BRT. **Both
populations are non-empty under either reading**, so the two-rates-on-one-day property
survives a wrong zone; only the counts move. *(The UTC pair is a DERIVATION over the
generated stream — `docs/f-api-run-evidence.md` §1.1 — and not a workspace measurement: no
run has ever been made under the fail-open reading, and none should be.)*

---

## Decision 3 — the zone lives IN THE VALUE, because `to_utc_timestamp` does not cancel the session zone

**This is the finding that turned Decision 2 from a ruling into an implemented one, and it
was published three times as its own opposite.**

The shipped spelling was `to_utc_timestamp(to_timestamp(text), 'UTC-03:00')`, described in
the module docstring, the function docstring and two commit messages as one in which
"`to_utc_timestamp` cancels the session zone on both sides, so the instant does not move with
`spark.sql.session.timeZone`".

**It does not cancel.** Spark's `to_utc_timestamp` renders its input **in UTC**, not in the
session zone (`convertTz(micros, from=tz, to=UTC)` reads `getLocalDateTime(micros, UTC)`),
while `to_timestamp` over text carrying no offset parses **in the session zone** — so only one
of the two ever varied. Measured through `opl.spark.local_session` (pyspark 3.5.9),
`unix_micros` of one landed stamp under three session zones:

| session zone | `2026-06-19 13:03:25.555497`, shipped spelling | offset appended to the text |
|---|---|---|
| `UTC` (the pin) | 1781885005555497 = **16:03:25.555497Z** ✅ | 1781885005555497 ✅ |
| `America/Sao_Paulo` | 1781895805555497 = **19:03:25.555497Z** ❌ | 1781885005555497 ✅ |
| `Asia/Tokyo` | 1781852605555497 = **07:03:25.555497Z** ❌ | 1781885005555497 ✅ |

**No landed number was ever wrong, and that is the finding rather than the reassurance.**
`opl.config.SESSION_TIMEZONE` pins the session to UTC in both gold entry points and in the
local session — so **the whole of Decision 2 rested on a cluster setting**, in the layer whose
own prose says the only defence that has ever worked here is not depending on one.

**The fix appends the offset to the text**: `to_timestamp(concat(data_hora_cotacao,
'-03:00'))`, the offset rendered from `ptax_source.BRASILIA`. That is
`opl.gold.fact_guards.event_instant`'s discipline — which *requires* a zone designator in the
payment's text — applied to the side BCB leaves zoneless. Measured identical under all three
zones above, at all three fractional-second widths the series carries (1 digit in 1984, 3 in
2025, 6 in 2026), and identical to the old value under the pin, so nothing landed moves.

**Why nothing caught it, which is the transferable half.** The zone *fix* was pinned and the
zone *invariance* was not: the only non-UTC session test had a **BRL-only** fixture, so the FX
interval bounds were computed there and discarded, and the one assertion on a publication
instant read `.microsecond` — blind to the whole-hour shift that is the only thing a wrong
zone does. Both are repaired; the repaired test is what produced the table above, by failing.

> **The zone hazard is met by every new column that answers "which day", not once and
> closed.** In the same pass, a reporting-currency row's `fx_rate_date` was written
> `to_date(<event instant>)` — a *rendering* — so under `America/Sao_Paulo` every
> midnight-UTC payment dated its own identity conversion to the previous day.
> `tests/gold/test_fact_payment.py::test_the_fact_is_unchanged_when_it_is_built_under_a_non_utc_session_zone`
> refused it. It reads `day_of(event_time)` now, the same ten characters `event_date_key`
> comes from. This is the third instance in this lakehouse after `company_sk` and
> `event_date_key` (`.plans/HANDOFF.md`, F3).

---

## Decision 4 — THREE deviations from §4.3's column list, where the code says two

§4.3 asks for `amount_original + currency + fx_rate + fx_rate_date + amount_brl`. **Two** of
the five land under exactly those names. **Three do not**, and the third was found by reading
the rebuilt table's schema in F-API Task 5 rather than by reading the code:

| §4.3 asks for | the star carries | why |
|---|---|---|
| `amount_original` | **`amount`** | It is `amount` renamed. The payments contract already ships `amount`, and shipping both would put **two byte-equal columns in one fact forever**. The spec's name is satisfied by a documented mapping, not by a duplicate. |
| **`currency`** | **`currency_key`** (`bigint`), and **no currency column at all** | A conformed foreign key into `dim_currency`, whose member column is spelled `currency_code`. So the string `currency` names a **bronze** column and nothing in gold. |
| `fx_rate_date` | **`fx_rate_date_key`** (`int`, `yyyyMMdd`) | A bare date column is one the star cannot group by or filter against the calendar — the "decorative column" charge this repository levels elsewhere. It is a **second role-playing foreign key into `dim_date`**, beside `event_date_key`. **No bare `fx_rate_date` column is projected at all.** |

> **THE COUNT OF TWO WAS SHORT BY ONE, ON THIS REPOSITORY'S OWN CRITERION.**
> `src/opl/gold/facts.py` called `fx_rate_date_key` "the **second** deviation … the first
> being `amount_original`" until Task 5 corrected it to three in the same pass that wrote
> this ADR; `docs/f-api-run-evidence.md` §1.3 still says two, deliberately — **it is a
> predictions section and predictions are not edited after the run that tests them**, so the
> correction lives in §2.9 rather than on top of the claim it corrects.
> `currency` → `currency_key` is *exactly the same species* as `fx_rate_date` →
> `fx_rate_date_key`: a bare business column replaced by a conformed key, satisfied through a
> dimension rather than in the fact. It **predates this phase** — F3 built the fact with
> `currency_key` and no currency column, when that column would have held one constant value —
> which is why nobody counted it: at fact-side cardinality 1 the deviation was invisible. It
> becomes visible the moment the fact reaches two currencies, which is what this phase did.
> Recorded here because it was in no ADR, no evidence document and no docstring.
> **Controller-verified** by the post-rebuild schema
> (`docs/f-api-run-evidence.md` §2.9, statement `01f19831-8df6-18d9-b4ff-f113b0fb05c9`).

Three consequences worth stating because a reader will otherwise file them as defects:

- **`amount_brl` is the fact's declared additive measure; `amount` is additive ONLY WITHIN A
  CURRENCY.** Before this phase `amount` was the single declared measure and every row was
  BRL, so `SUM(amount)` was meaningful by accident. With 4,905 USD rows it is a
  mixed-currency number, and nothing about the old declaration would have failed.
- **`SUM(amount_brl) != SUM(amount) × rate` to the cent, and that is arithmetic rather than a
  defect.** `amount_brl` is `amount * fx_rate` rounded **HALF-UP to two decimals AT THE ROW**,
  so the two differ by up to half a centavo per converted row. Rounding once over a total
  would hide which rows moved; carrying the product's seven decimals into the fact would give
  `amount_brl` a scale no currency explains.
- **`fx_rate` is `decimal(18, 5)` and is NOT `AMOUNT_TYPE`, and the arithmetic is the
  argument.** `decimal(18, 2)` would round 5.14420 to 5.14 and put `amount_brl` about **0.08%
  wrong on every USD row** — plausible in magnitude, in a column nobody re-derives. Five is
  the scale the series publishes at every magnitude it has ever carried: 5.14420 in 2026,
  2828.00000 at the 1984 floor, 0.82900 at the 1994 low, 71153.00000 at the 1993 high. It is
  **non-additive**: a ratio that must never be summed and whose unweighted mean is wrong.

---

## Decision 5 — this star carries TWO "as of" semantics on one fact row

Nothing before this ADR said so, and a reader is entitled to assume a Kimball star is
uniform in this respect.

| the row's | is resolved as of | which is |
|---|---|---|
| `payer_company_sk` / `payee_company_sk` | the RFB's declared **`applied_date`** for the snapshot | a **reference date** — not the instant the registry assertion became downloadable, and not the instant the company changed (ADR 0015) |
| `fx_rate` / `fx_rate_date_key` | the quote's **publication instant**, per row | the instant the value **became knowable**, to the microsecond |

Both are as-of-**known**-time in spirit; they differ in how precisely each source dates its
own knowledge. The FX side is the sharper of the two because BCB stamps every row it
publishes, and the company side cannot be sharpened from this feed at all — ADR 0015 records
why. **A query joining the two is answering at two different resolutions**, and the coarser
one is the 28-day interval between RFB snapshots.

---

## Decision 6 — the bronze→gold seam, inherited rather than taken here

`fact_payment` reads `bronze_payments` **and now `bronze_ptax`** directly. There is no
`link_payment`, no `hub_moeda`, no `sat_ptax_cotacao` and no `fact_fx_rate`.

**This is not this phase's first vault bypass, and it is not an oversight.** Master route §7
pre-decided exactly this seam for `fact_payment` — "F3 can build a fact without the DV2 link
if the fact reads the payment bronze directly; **decision: do that, and record it as the seam
F2 wave 2 closes**" — because `link_payment` needs `links.py` edited (ADR 0011:371-376), which
is wave-2 work off the MLV path. `docs/f3-workspace-run-evidence.md` §8.2 records the payments
half. This ADR records that the PTAX half takes the same seam for the same reason, and adds
one of its own:

**A vault hop for FX was proposed by the modelling review and REJECTED on measurement.** A
`hub_moeda` + `sat_ptax_cotacao` + `fact_fx_rate` triple would be a conversion fact of ~42
rows at fact-side cardinality **2** — the constant-column disease this project already names
in `dim_currency`'s own evidence, built to satisfy a pattern rather than a measurement. The
FX series is **reference data with an interval**, and the interval table `fx.rate_intervals`
builds is the same mechanism `opl.gold.dimensions` writes for `dim_company`, read with the
same half-open predicate. Adding two vault tables would produce a second spelling of that
mechanism and no new answer.

**What the seam costs, stated so wave 2 can price it:** `fact_payment` is the only gold entry
point that reads **another layer's** table, and after this phase it reads two of them. Its
`_QUALIFIED_NAMES` entry went 4 → 5 for exactly that reason. Closing the seam means the
payments half becomes `link_payment` and the FX half stays where it is — the argument above is
independent of `links.py`.

---

## Consequences

### What this buys — measured, on the run of 2026-08-14

Every number here is from `docs/f-api-run-evidence.md` §2, with statement ids.

| | value |
|---|---|
| USD rows that **fell back** to Friday 2026-06-19 at venda **5.14420** | **2,864** |
| USD rows that resolved **same-day** on 2026-06-22 at venda **5.13950** | **2,041** |
| distinct `fx_rate` on `event_date_key = 20260622` (one calendar day) | **3** — two of them among the USD rows |
| widest fallback any conversion took | **3 days** |
| conversions past the series' last landed quote | **0** |
| BRL rows at `fx_rate` exactly 1.00000, consulting no quote | **35,095** |

- The two-rates-on-one-calendar-day property is **measurable on real fact rows**, not
  asserted.
- **A consequence the prediction missed, and it is worth carrying:** `event_date_key` equals
  `fx_rate_date_key` on **37,136** rows, not on the 35,095 reporting-currency ones. Every
  same-day resolution makes the two keys agree, whatever the currency — so the two-key
  agreement is *not* a test for "identity conversion", and the fallback population is its
  complement (2,864) rather than the USD population (4,905).
- A bounded or holey extraction is visible as numbers in the run log rather than as a rate
  that quietly resolved.
- The publication instant is a function of the landed bytes, not of a cluster setting.

### What ships UNEXERCISED, and must not be reported otherwise

Standing decision §4.6: a path that ran zero rows through it is not a path that works.
`docs/f-api-run-evidence.md` §3 is the full ledger; the entries this ADR is directly
responsible for:

- **The disagreeing-duplicate refusal, at both layers.** Two duplicate pairs exist in 3.6
  years of series (2025-04-23, 27 ms apart; 2001-12-21, identical stamps) and **both agree**.
  The refusal has no witness. It is two unexercised refusals and not one counted twice —
  bronze appends, so `ptax_source.sole_quote` and `fx.rate_intervals` see different
  populations.
- **The holiday crossing, on fact rows.** No Brazilian national holiday falls between
  2026-06-13 and 2026-08-01 (Corpus Christi 2026 is 2026-06-04; the next is 2026-09-07), so
  **no payment in this lakehouse can cross one.** The holiday case is exercised over the
  extracted **series** in the unit suite only. **The witness for this ADR's central argument
  is a series row, not a fact row**, and that distinction is the honest scope of the claim.
- **The below-the-series refusal.** Nothing in this phase's payment range sits below
  2026-06-03; only a fixture reaches it.
- **`fx_beyond_series` non-zero.** The number is 0 on this data, which means the series
  reached past every conversion — **not** that the truncated-window path was exercised. It is
  a report, not a refusal, and a zero is not coverage of a branch.

### What was declined rather than deferred

T3 clause 2's **gaplessness assertion** — see Decision 1 clause 2. It is refused with its
mechanism, and the substitute is five reported numbers. Recorded as a refusal, not an
omission.

---

## Alternatives considered

| option | why not |
|---|---|
| A Brazilian holiday calendar, as §4.3 states | A second spelling of "is there a quote", disagreeing with its own source on a measured witness (2023-11-20). Both directions of disagreement are silent. |
| A calendar-**day** join on `quote_date` | Correct for every row this phase lands and wrong in principle: it cannot produce two rates on one day, and it hands a 01:53Z payment a rate published 14 h later. |
| Deriving the quote date from `dataHoraCotacao` | Degrades to the day comparison above, correctly for every 2026 row and wrongly in 1984 — i.e. it would have shipped green. |
| `to_utc_timestamp(..., 'UTC-03:00')` | Does not cancel the session zone. Measured: 16:03Z / 19:03Z / 07:03Z under three zones. |
| Adding three hours as an interval | Correct only while the session is UTC, which is where the retracted spelling started. |
| `max()` / `min()` over disagreeing duplicate quotes | Silently picks the rate every payment on that date converts at. `max()` additionally denies a payment a rate already published, which is the model this ADR retracts. |
| `hub_moeda` + `sat_ptax_cotacao` + `fact_fx_rate` | A ~42-row conversion fact at fact-side cardinality 2, and a second spelling of an interval mechanism the gold layer already has. |
| Shipping `amount_original` beside `amount` | Two byte-equal columns in one fact, forever. |
| A bare `fx_rate_date` column | A date the star cannot group by or filter against the calendar. |

---

## References

- `src/opl/gold/fx.py` — the rule, the reduce, the interval table, the coverage report.
- `src/opl/extraction/ptax_source.py` — the request shape, `BRASILIA`, `sole_quote`.
- `src/opl/contracts/ptax.py` — `REQUEST_COLUMNS` versus `RESPONSE_COLUMNS`.
- `docs/f-api-run-evidence.md` — §0 (Task 0's five measurements), §1 (the predictions),
  §2 (what the runs said), §3 (the unexercised ledger).
- [ADR 0015](0015-as-of-known-time-and-append-only-scd2.md) — as-of-known-time, and the
  drop-and-rebuild this phase's `fact_payment` rebuild relies on.
- [ADR 0014](0014-dim-company-at-empresa-grain.md) — the star's grain.
- [ADR 0011](0011-no-hub-socio-and-a-disappearance-driven-effectivity-satellite.md):371-376 —
  the `link_payment` blocker behind the seam.
- Master spec §4.3 and master route §7.
