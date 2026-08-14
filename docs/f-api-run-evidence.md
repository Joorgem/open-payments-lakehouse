# F-API — PTAX, and the measurements taken before anything was built

**This document holds Task 0's measurements and, as the phase runs, its predictions and
what the runs said.** It exists in `docs/` and not in `.plans/` deliberately: `.plans/` is
git-ignored and this repository is public, and F3 shipped a section pointing a public reader
at that directory, where they reached nothing.

**Controller-verified** means the controller ran the command and read the output.
**Reported** means a task's own stdout or a subagent's report said it. Every claim below
carries one of the two labels. The distinction is not decorative: F3 recorded six defects
that were the controller writing about its own work, three of them HIGH findings in its own
evidence prose.

**Predictions are published BEFORE the run that tests them** (master protocol §4.5). A
number first written down after the run that produced it is not a prediction.

---

## 0. Task 0 — five measurements taken before a table existed

The phase plan asked for three. It gets five, because an audit before Task 0 found that one
of the three landed **zero rows** by construction, and that the measurement deciding the
whole architecture — whether a serverless task can reach the API at all — was absent.

### 0.1 The artefact, and why this section exists at all

**`scripts/probe_ptax.py`** is committed and re-runnable (`uv run python scripts/probe_ptax.py`,
~90 s). It reads its endpoint and the `MM-DD-YYYY`-in-single-quotes date format from
`scripts/validate_cnpj_snapshots.py:28-32` rather than re-deriving them, and it parses with
`Decimal` from the raw response text.

**It exists because the phase plan published two PTAX rates as "measured" when no artefact in
this repository carried them.** That is the third appearance of a provenance defect this
project has struck twice before — a statement id in the F3 plan that resolved to nothing, and
the same controller committing the identical error three hours after writing its correction
(`docs/f3-run-evidence.md` §0.5). The rates were, in the event, **correct**. The provenance
was missing, and correct-with-no-provenance is the state that cost this project two prior
retractions.

### 0.2 The two published predictions — both CONFIRMED

**Reported** by the Task 0 implementer, from requests reproducible via the script above:

| date | predicted venda | **API returns** | compra | published (`dataHoraCotacao`) |
|---|---|---|---|---|
| 2026-07-31 | 5.07730 | **5.07730** ✅ | 5.07670 | `2026-07-31 13:10:31.061071` |
| 2026-06-19 | 5.14420 | **5.14420** ✅ | 5.14360 | `2026-06-19 13:03:25.555497` |

Exact to the trailing zero. **The trailing zero is the point**: `json.loads` on the live body
yields the float `5.0773` and `str()` drops it, so a test pinning the published string fails
on a *correct* extraction. `parse_float=Decimal` from the raw text preserves `5.07730`.

### 0.3 The series, and the one weekday that is missing

**Reported.** 73 calendar days walked, 2026-05-25 → 2026-08-05: **52 quotes, 21 absences.**

- **20 of the 21 are weekend days.**
- **Exactly one weekday absence: 2026-06-04 (Thursday), Corpus Christi** — confirmed absent,
  falling back to **2026-06-03, venda 5.04150**.
- Widened to 2023-01-01 → 2026-08-05 (903 rows, 902 distinct dates): **zero gaps that are
  neither a weekend nor a national holiday.** No unpredicted fallback case exists.
- **Series floor: 1984-11-28** (compra 2814.00000, venda 2828.00000).

**Extraction range: 2026-06-03 → 2026-08-01 inclusive — 42 quotes, gapless in business days.**
2026-06-03 is a hard floor: starting at 06-04 leaves the holiday case with nothing to fall
back to, which the phase's T3 clause 3 makes a refusal rather than a NULL.

### 0.4 THE FINDING THAT CHANGED THE CONTRACT: the quote date is not in the response

**Reported, and it is the sharpest thing Task 0 produced.** The API ships exactly three
fields per row — `cotacaoCompra`, `cotacaoVenda`, `dataHoraCotacao` — and **`dataHoraCotacao`
is a PUBLICATION instant, not the quote's date.**

Across all 73 probed days of 2026 the two coincide. **They do not coincide in 1984**, where a
quote requested for 1984-11-28 comes back stamped `1984-12-03 11:29:00.0` — five days later.

Three consequences, none of which any version of the phase plan predicted:

1. **The quote date must be carried from the REQUEST.** A contract landing only what the API
   returns makes the phase's instant-comparison rule degrade silently into the calendar-day
   comparison it exists to forbid — correctly for every 2026 row, and wrongly in 1984.
2. **The single-day request shape is FORCED, not chosen.** A *range* request returns rows
   with no attributable quote date, so the extraction is **42 requests of ~220 bytes**, not
   one wide call.
3. **The fractional-second width is not stable**: 1 digit in 1984, 3 in 2025, 6 in 2026. Any
   fixed-width slice works on this phase's range and breaks on the series.

### 0.5 A weekend and a failed request are indistinguishable

**Reported.** 2026-06-20 answers **HTTP 200 with `"value":[]`** — the same zero rows an HTML
interstitial or an OData error object would produce. Because an absence is resolved by
falling back, a failure read as an absence makes the fallback cross a hole nobody can see.
The response envelope is therefore validated and **refused**, rather than returned as no rows.

**The sharpest instance is not an outage — it is the date format.** An **ISO** date in `@di`
returns **HTTP 200 with `"value":[]`**, on every day of a span. So the one format mistake this
repository polices hardest fails by wearing a Saturday's exact shape, silently, all the way
through: `2026-06-03` looks precisely like a day with no quote. That is the argument for
refusing a wholly-empty span rather than returning nothing, and it is what the refusal message
now says.

**And Olinda's real error body is a JSON object inside a comment wrapper** —
`/*{ "codigo": …, "mensagem": … }*/`, served without a content-type, reproduced from three
different bad requests. The fixture originally written for this case used the OASIS
`{"error": …}` shape, which takes the *missing-member* branch, so **the branch that fires on a
real API error had never been exercised** until it was replaced with the live body.

### 0.6 T3's premise, measured rather than argued

**Reported.** The phase rules that the fallback reads the PTAX series itself and never a
Brazilian holiday calendar, on the ground that a calendar is a second spelling of "is there a
quote" and the two can disagree.

**They do disagree, and the case is in the series: 2023-11-20 carries a quote while today's
national-holiday list calls it a holiday.** Black Consciousness Day became a national holiday
only from 2024 (Lei 14.759/2023). One calendar-versus-series disagreement in 903 rows.

**No Brazilian national holiday falls between 2026-06-13 and 2026-08-01** (controller-verified
by computation; Corpus Christi 2026 is 2026-06-04, the next is 2026-09-07). So no *payment*
can cross a holiday, and the holiday case is exercised over the extracted series in the unit
suite only. §3 records that as unexercised rather than reporting it as working.

### 0.7 A duplicate row exists, and it agrees

**Reported.** **2025-04-23 carries two rows**, publication stamps 27 ms apart
(`13:06:30.416` / `13:06:30.443`), **agreeing on both rates**. So a reduce-to-one-row step is
required. Its *disagreement* branch has **no witness in 3.6 years of series** and ships
unexercised; §3 says so rather than calling it tested.

### 0.8 Serverless egress — REACHABLE, and it decided the architecture

**Reported**, run `1112844532335593` (task run `1111515407481178`), `SUCCESS`, driver log
untruncated:

```
PROBE requests: AVAILABLE 2.32.2
PROBE dns olinda.bcb.gov.br: 150.171.109.72
PROBE ptax: status=200 bytes=220 head=b'{"@odata.context":"https://was-p.bcnet.bcb.gov.br/...
PROBE ptax: json_ok keys=['@odata.context', 'value']
PROBE control-pypi: status=200 bytes=192973
```

Nothing in `databricks/src/` had ever made an outbound HTTP call, so this was a real question.
**The answer is yes**, so the PTAX fetch is a task inside the bronze job rather than a
host-side PUT on the `lookup` pattern. Three things it settled beyond the question asked:

- The body is genuinely BCB (`was-p.bcnet.bcb.gov.br` in the `@odata.context`), so this is not
  a proxy interstitial answering 200.
- **A second, unrelated host returned 192,973 bytes.** Egress is not an olinda-specific
  allowlist entry and is not capped at trivial bodies. **This narrows a standing finding**:
  `pyproject.toml`'s note that Free Edition could not complete a 300 MB download is about
  **install budget**, not about outbound HTTP being governed off.
- Runtime Python is 3.12.3 and `requests 2.32.2` is in the serverless base image.

The probe left no trace: the git tree was untouched and the uploaded workspace file was
removed afterwards.

### 0.9 What Task 0 decided

| question | answer | consequence |
|---|---|---|
| Can a serverless task reach the API? | **Yes** | the fetch is a job task; the file inventory stands |
| Are the two published rates right? | **Yes**, to the trailing zero | they now have a request behind them |
| Are there unpredicted gaps in the series? | **No**, in 903 rows | the only weekday absence is 2026-06-04 |
| Does the API return the quote date? | **No** | it is carried from the request, and the extraction is 42 single-day calls |
| How far back must the extraction reach? | **2026-06-03** | earlier leaves the holiday case unresolvable |

---

## 1. Predictions — published before the runs that test them

*(Written as Tasks 2–4 land, before any run. Empty by design at Task 0's close: a prediction
about a table whose loader does not exist yet would be a guess about code nobody has read.)*

### 1.1 Task 3 — the fifth profile, before it was declared

**Every number in this section was computed on the tree at `f564f57`, which does not contain
the fifth profile, and written down before the declaration was committed.** They are
arithmetic on the profile's *intended* declaration — a seed, a stream id, a window, an event
count and a currency tuple — and they need no Spark, no CNPJ pool and no workspace, because
`opl.generator.derivation` is counter-based: every drawn value is a pure function of
(seed, stream id, index, salt). The derivation script is `scripts/`-free on purpose; it is
three imports of the committed generator, and the two cross-checks below are what make it
worth more than an assertion.

**The declaration these numbers are computed from:**

| field | value |
|---|---|
| key / `name` | `cross-currency` |
| `stream_id` | `F-API-CROSS-CURRENCY` |
| `seed` | `20260817` |
| `window_start` | `2026-06-22T08:00:00.000Z` |
| `last_event_time` | `2026-06-22T21:53:15.000Z` (9,999 × 5,000 ms — the F1b span unchanged) |
| `currencies` | `("BRL", "USD")` — a **literal**, never `payments.CURRENCIES` |
| `placement` | `between` (strictly inside 2026-06-13 .. 2026-07-11) |
| `defects` | none |

#### The stream itself

| prediction | value | derivation |
|---|---|---|
| delivered rows | **10,000** | `event_count + duplicate_count`, and there are no duplicates |
| drifted rows | **0** | `drift_from_index is None` |
| base events / legitimate repeats | **9,200 / 800** | `event_count − repeat_count`, `repeat_count` |
| landed bytes | **2,926,588** | see below |
| distinct `event_date_key` it adds | **1** (2026-06-22) | the window sits inside one calendar day in **both** UTC and BRT |

**The byte count needed no new code, and that is the point.** `BRL` and `USD` are both three
characters, so the fifth stream's bytes are the bytes of the *same* (seed, stream id, window)
stream generated by code that only knows `("BRL",)` — which is the tree at `f564f57`. Measured
there: 10,000 rows, **2,926,588 bytes**, line widths 284–298. The emission after the change
must reproduce that number exactly, and a different one means the currency draw moved
something other than the currency.

#### The currency split — exact, and the repeats are why it is not a parity count

`pick` is `items[draw_below(...) % len(items)]`, so with `currencies = ("BRL", "USD")` the
index is `draw % 2` — a pure function of (seed, stream id, index). **But a legitimate repeat
does not draw its own currency**: `stream.generate` copies an earlier BASE event's whole
attribute tuple, currency included, at an index chosen under `PURPOSE_REPEAT_SOURCE`. So the
split is not "5,000 / 5,000 up to hash noise" over 10,000 indices — it is a draw over the
**9,200 base positions** plus an inheritance over the 800 repeat positions. No version of the
phase plan said so.

| population | BRL | USD |
|---|---|---|
| 9,200 base events (drawn at salt 0) | 4,675 | **4,525** |
| 800 legitimate repeats (inherited) | 420 | **380** |
| **10,000 delivered rows** | **5,095** | **4,905** |

Both the repeat *positions* and the repeat *sources* are pool-independent
(`_repeat_positions` digests positions; `PURPOSE_REPEAT_SOURCE` draws against `len(bases)`),
so the whole table above holds for any pool — **provided no base event needed a salt above
0**, which is the one place the pool can enter. At 1,024 companies the attribute space is
≈5.2 × 10¹³ and 9,200 draws collide with probability ≈8 × 10⁻⁷. The closing test asserts the
generated stream reproduces the table, which is what tests that assumption rather than
restating it.

#### The FX resolution split — two rates on one calendar day

The 2026-06-22 quote publishes at `dataHoraCotacao 2026-06-22 13:06:19.750415`, **read as
BRT** (T3), i.e. `2026-06-22T16:06:19.750415Z` — **29,179,750.415 ms** after the window
opens. Events land on whole 5,000 ms steps, so the first index whose own instant *follows*
publication is `ceil(29,179,750.415 / 5,000)` = **5,836**. Nothing lands on the boundary, so the
`<=`-versus-`<` question cannot decide a row.

> **The reason first published here was not the operative one.** It said the **415 µs**
> remainder is what puts the boundary strictly between two events. It is the **.750 s**:
> 29,179,750 mod 5,000 = **4,750**, so publication already sits **249.585 ms** clear of index
> 5,836 before the microseconds are considered. The conclusion holds and the mechanism was
> misattributed — found by Task 3's reviewer, and corrected here rather than in the three source
> comments alone, because this is the copy a reader quotes.

| | rows | of which USD (these resolve a quote) | of which BRL (`fx_rate = 1.0`, no quote consulted) |
|---|---|---|---|
| indices 0 – 5,835, **before** publication → falls back to **2026-06-19, venda 5.14420** | 5,836 | **2,864** | 2,972 |
| indices 5,836 – 9,999, **after** publication → resolves **same day, 2026-06-22, venda 5.13950** | 4,164 | **2,041** | 2,123 |

**2,864 + 2,041 = 4,905**, the USD total above. **These are the two numbers T2's closing test
is marked against**, and they are the FX-resolving populations — a refinement of the phase
plan's own 5,836 / 4,164, which is the *row* split by the publication boundary and counts BRL
rows that consult no quote at all.

**The verdict does not rest on the timezone ruling.** Read `dataHoraCotacao` as UTC instead —
the fail-open direction the plan rejects — and the boundary moves to index **3,676**: 1,801
USD rows fall back and 3,104 resolve same-day. Both populations are non-empty under either
reading, so the two-rate property survives a wrong zone; only the counts move.

#### What §5's inherited numbers become

| published (`f3-workspace-run-evidence.md`) | becomes | derivation |
|---|---|---|
| `bronze_payments` **30,150** rows | **40,150** | `drifting` still never promotes; the fifth stream delivers 10,000 |
| distinct `transaction_id` **30,000** | **40,000** | 150 of the 40,150 are injected redeliveries, unchanged |
| `_batch_id`s **3** | **4** | one ingest per landed file |
| `fact_payment` **30,000** rows | **40,000** | one fact row per distinct delivered payment |
| legitimate repeats **2,400 = 3 × 800** | **3,200 = 4 × 800** | `repeat_count` is shared by every profile |
| distinct business tuples **27,600 = 3 × 9,200** | **36,800 = 4 × 9,200** | the fifth stream contributes 9,200 base tuples; see the note below on which of them could collide |

> **The 36,800 derivation argued about the wrong population, and is restated.** It said the
> 4,905 USD rows cannot collide with an existing row because no existing row is USD. True, and
> irrelevant: the rows that *could* collide are the fifth stream's **4,675 BRL base tuples**
> against the existing 27,600, and 4,905 is the *delivered* USD count rather than a base count.
> The number holds — the attribute space is 1024 × 1023 × 4,999,901 × 2 × 5 ≈ 5.2 × 10¹³, so the
> collision probability is ≈5 × 10⁻⁶ — but as first published it did not establish 36,800. Found
> by Task 3's reviewer.
| distinct `event_date_key` **2** | **3** | 2026-06-20, **2026-06-22**, 2026-08-01 |
| `dim_date` members **50** | **50** | `covered_span` anchors on 2026-06-13 and 2026-06-22 is inside it, so the conformed re-run appends zero date rows |
| `dim_currency` members **1** | **2** | `members=payments.CURRENCIES`, and the domain gains USD |
| `dim_currency` fact-side cardinality **1** | **2** | 4,905 USD fact rows and 35,095 BRL ones |
| channels reached **5** | **5** | the fifth profile draws from the same `PAYMENT_METHODS` |

**`dim_currency` at fact-side cardinality 1 "cannot be wrong" is retired by these numbers**,
which is what T1 exists to do.

#### The byte-identity baseline this task must not move

Four files, emitted from `f564f57` against a **synthetic** 1,024-key pool
(`00000001 … 00001024`). **The pool decides WHICH company gets which payment and therefore the
sha256, but not the byte count** — every `cnpj_basico` is exactly eight characters. So the byte
counts below are comparable with anything, and **the digests are a tree-to-tree comparator
only**: they are not the digests of the files in the Volume, which were derived from the real
`hub_empresa` pool.

| profile | stream id | rows | bytes | sha256 |
|---|---|---|---|---|
| `clean` | `F1B-CLEAN-2026-08` | 10,000 | 2,925,069 | `fccd6c48088909cd2f7f13fe1500a948ec670c5c8bfa29b3ae9e268a57b3dbea` |
| `promotable` | `F1B-PROMOTABLE-2026-08` | 10,150 | 2,969,937 | `5603cdd48c612f229afc5ff01d77134b5d42cdb82c15b45fb77a82c5df4aa77d` |
| `drifting` | `F1B-DRIFTING-2026-08` | 10,000 | 2,989,447 | `54db876f678396631edf7c2287cbf83c3b59d52360730612f611f760cc921425` |
| `between-snapshots` | `F3-BETWEEN-SNAPSHOTS` | 10,000 | 2,926,409 | `3381ba267d857f3fbb7cc7b25ff0df1bb87b25f0a340719dfb44f1d8d8be9dac` |

The first three byte counts are the ones `.plans/HANDOFF.md` published at F3's close and they
reproduce **exactly**, which is the evidence that this probe is the same probe. The fourth was
never published and is recorded here so the next edit to `profiles.py` has four rows to
compare against rather than three.

### 1.2 What Task 3 then measured locally — every §1.1 number reproduced

**Reported by the Task 3 implementer.** This is not §2: no workspace run has happened, and the
predictions above still stand as predictions about the run Task 5 makes. What is recorded here
is that they survived the local derivation they describe.

**The byte-identity probe, re-run from the implemented tree against a worktree at `f564f57`
created outside the repository root** (a stray worktree under it turns
`tests/test_revision_stamp.py`'s watched-paths test red locally and is invisible to CI). **It
is now a committed artefact, `scripts/probe_byte_identity.py`** (`uv run python
scripts/probe_byte_identity.py`, ~68 s, exit 0 when nothing moved) — the same repair §0.1
made for the PTAX rates, applied to a claim that had been published twice with no artefact
behind it:

```
clean              F1B-CLEAN-2026-08      rows= 10000 bytes=  2925069 sha256=fccd6c48…
promotable         F1B-PROMOTABLE-2026-08 rows= 10150 bytes=  2969937 sha256=5603cdd4…
drifting           F1B-DRIFTING-2026-08   rows= 10000 bytes=  2989447 sha256=54db876f…
between-snapshots  F3-BETWEEN-SNAPSHOTS   rows= 10000 bytes=  2926409 sha256=3381ba26…
cross-currency     F-API-CROSS-CURRENCY   rows= 10000 bytes=  2926588 sha256=a527b61c…
```

`cmp` is clean on all four pre-existing files, at both stages of the change: once after the
mechanism commit (the domain widened, the draw moved, no profile declaring a mix) and again
after the fifth profile landed. The committed probe reports `IDENTICAL` on all five and
`0 of 5 profile(s) differ from the baseline`. **The fifth file's 2,926,588 bytes is the number
predicted in §1.1 before the `currencies` field existed**, reproduced by an emission in which
4,905 of the 10,000 rows carry `USD` — which is the check that the currency draw moved the
currency and nothing else.

| §1.1 predicted | measured | where |
|---|---|---|
| 5,095 BRL / 4,905 USD | **same** | the closing test, and `grep -c` on the landed file |
| 4,525 USD drawn / 380 USD inherited | **same** | the closing test, splitting base from repeat |
| 5,836 rows before / 4,164 after publication | **same** | the closing test |
| 2,864 USD fall back / 2,041 USD same-day | **same** | the closing test |
| 10,000 rows, 2,926,588 bytes | **same** | the probe and the closing test |
| distinct `event_date_key` 3, `dim_date` members 50 | **same** | `tests/gold/test_conformed.py`, 22 passed in 822 s local |

**No base event needed a salt above 0**, which was §1.1's one stated assumption: the counts
computed pool-free on the baseline tree and the counts read off a stream generated against a
1,024-key pool agree exactly, and a collision retry would have moved them.

#### The third rate now has a request behind it, and §0.2 had only two

**Reported by the Task 3 implementer**, three live single-day requests through
`opl.extraction.ptax_source.quote_url`:

```
2026-06-19  200  "value":[{"cotacaoCompra":5.14360,"cotacaoVenda":5.14420,"dataHoraCotacao":"2026-06-19 13:03:25.555497"}]
2026-06-22  200  "value":[{"cotacaoCompra":5.13890,"cotacaoVenda":5.13950,"dataHoraCotacao":"2026-06-22 13:06:19.750415"}]
2026-06-20  200  "value":[]
```

**§0.2 publishes 2026-07-31 and 2026-06-19 and not 2026-06-22**, and the whole of T2's window
rests on the third one: `13:06:19.750415` read as BRT is what puts the boundary at index
5,836, and `5.13950 != 5.14420` is what makes two payments on one calendar day carry two
rates. Its only provenance in this repository was a test fixture. Checked rather than
inherited, because **three windows in this phase were published and falsified** and the
surviving one is the only artefact that would not have said so. The Saturday's
`"value":[]` is the same envelope §0.5 records — one more instance of an absence and a
failure being indistinguishable to a forgiving reader.

### 1.3 Task 4 — the gold layer, and what the star will hold after the rebuild

**Reported by the Task 4 implementer.** No workspace run has happened; every number below is
derived from a declaration in this repository or reproduced locally, and each says which.

#### The three columns the fact gained, and their types

| column | type | additivity | derived from |
|---|---|---|---|
| `fx_rate` | `decimal(18, 5)` | **non-additive** | `currency`, `event_time` |
| `amount_brl` | `decimal(18, 2)` | **additive** — the one measure a reader sums | `amount`, `fx_rate` |
| `fx_rate_date_key` | `int` (`yyyyMMdd`) | *(a foreign key, not a measure)* | the resolved quote date |

**`fx_rate` IS NOT `AMOUNT_TYPE`, and the arithmetic is the argument.** `decimal(18, 2)`
would round 5.14420 to 5.14 and put `amount_brl` about **0.08% wrong on every USD row** —
plausible in magnitude, in a column nobody re-derives. Five is the scale the series itself
publishes at every magnitude it has ever carried: 5.14420 in 2026, 2828.00000 at the 1984
floor, 0.82900 at the 1994 low, 71153.00000 at the 1993 high (§0.3 and `ptax_source`'s own
measurements).

**`SUM(amount_brl)` IS NOT `SUM(amount) × rate` TO THE CENT, and that is arithmetic rather
than a defect.** `amount_brl` is `amount * fx_rate` rounded **HALF-UP to two decimals AT THE
ROW**, so the two differ by up to half a centavo per converted row — about 4,905 rows' worth
on this data. Rounding once over a total instead would hide which rows moved, and carrying the
product's seven decimals into the fact would make `amount_brl` a column whose scale no
currency explains. **Controller-checkable locally:** `1.23 × 1.50000 = 1.845` resolves to
`1.85` (half-even would give 1.84), pinned in
`tests/gold/test_fact_payment_fx.py::test_the_rate_keeps_five_decimals_and_the_converted
_amount_rounds_half_up`.

**`amount` is retained under its contract name and declared ADDITIVE ONLY WITHIN A CURRENCY.**
Master spec §4.3 asks for `amount_original`; it is satisfied by a documented mapping onto
`amount` rather than by a duplicate, because shipping both would put two byte-equal columns in
one fact forever. **`fx_rate_date` is the second deviation from that column list**: the star
carries `fx_rate_date_key` and no bare date column at all. Both renames belong in the T3 ADR.

#### The FX split, REPRODUCED by the gold implementation

**§1.1's two numbers are what this task was marked against, and they reproduce exactly** —
counted off the frame `opl.gold.fx` writes, over the fifth profile's real 10,000 generated
rows against the three measured quotes:

| | rows | rate | quote date |
|---|---|---|---|
| USD, before the 2026-06-22 bulletin | **2,864** | 5.14420 | 2026-06-19 |
| USD, after it | **2,041** | 5.13950 | 2026-06-22 |
| BRL (no quote consulted) | **5,095** | 1.00000 exactly | the payment's own day |

`tests/test_payment_profiles.py` already pinned 2,864 / 2,041 against a **Python-side**
comparison of the same rule; what Task 4 adds is that the **Spark** implementation reproduces
them, which is the only version of the claim that is about the code the workspace will run.
Both populations are non-empty, which standing decision §4.6 requires and which each of this
phase's three earlier windows would have failed.

**Two payments on ONE calendar day carry two different rates.** Every row of the fifth stream
falls on 2026-06-22 in both zones, and the resolved rate is not one number — which is the
property no calendar-day implementation can produce, and the reason T3's rule is an instant
comparison. Asserted directly:
`test_two_payments_on_one_calendar_day_carry_two_different_rates`.

#### What the star holds after the rebuild

| | value | derivation |
|---|---|---|
| `fact_payment` rows | **40,000** | one per distinct delivered `transaction_id` |
| distinct `fx_rate` values | **3** | 1.00000, 5.14420, 5.13950 |
| rows at `fx_rate = 1.00000` exactly | **35,095** | every BRL row, by definition and not by lookup |
| distinct `event_date_key` | **3** | 20260620, 20260622, 20260801 |
| distinct `fx_rate_date_key` | **4** | 20260619 and 20260622 from the USD rows; 20260620, 20260622 and 20260801 from the BRL ones, whose conversion is dated to their own day |
| rows where the two date keys AGREE | **35,095** | the BRL population — an identity conversion is dated to its own day |
| orphaned rows, per fact key (four of them) | **0** | every resolvable quote date is inside `dim_date`'s 2026-06-13 .. 2026-08-01 span, the earliest being 2026-06-19 |
| reduced PTAX quotes read | **42** | 2026-06-03 .. 2026-08-01, one row per `(currency, quote_date)` |

> **`fx_rate_date_key` READ 5 IN THE FIRST DRAFT OF THIS TABLE, AND IT INCLUDED 20260731,
> WHICH NO ROW CAN CARRY.** 2026-07-31's quote is only reachable by a USD payment on
> 2026-08-01, and the only USD payments in this lakehouse are the fifth profile's, all of which
> fall on 2026-06-22. The extraction window covers 2026-07-31 because the series has to be
> gapless past what the fact needs, not because a row resolves to it — which is the distinction
> the count got wrong. Caught by re-deriving it against the four streams' own days before this
> document was committed, and corrected rather than deleted.

#### The operational order, and why `fact_payment` must be DROPPED

`opl.gold.facts._appended` writes `mode("append")` with **no `mergeSchema`**, so the three new
columns make the append FAIL — which is the safe half. **With `mergeSchema` it is worse:**
`_new_rows` anti-joins on `transaction_id`, so the 30,000 rows already written would keep NULL
FX **forever** while every counter in the run log reported clean. ADR 0015 already accepts a
drop-and-rebuild at 69.2M rows and 120 s; this table is 40,000 rows.

1. land + ingest the fifth payment batch (`opl_bronze_payments`, `month=2026-06`)
2. fetch + ingest the PTAX window (`opl_bronze_ptax`)
3. re-run `opl_gold_conformed_dimensions` — **append-safe**: `dim_currency` gains USD,
   `dim_date` appends **zero** rows because `covered_span` anchors on 2026-06-13 and
   2026-06-22 is already inside the span
4. **`DROP TABLE fact_payment`**
5. re-run `opl_gold_fact_payment` → 40,000 rows

Step 3 before step 5 and not after: the orphan counts are measured against the conformed
tables as they stand, so a fact built first would REPORT 4,905 orphaned currency keys —
correctly, and about a state whose repair is to re-run step 3 and read the numbers again.

#### What Task 4 refused, and what an existing test caught

**Gaplessness in business days is REPORTED, not refused, and that is a declined item.** Plan
T3 clause 2 asks for the assertion. Any bound on the gap between two consecutive quotes is
either a **Brazilian holiday calendar** — the second spelling of "is there a quote" that T3's
own ruling refuses, with the witness in the series (§0.6: 2023-11-20 has a quote and today's
list calls it a holiday) — or a number **drawn from this one extraction window**, which is the
species `ptax_source` already refuses for magnitude ceilings ("a bound that cannot be chosen
from the data is not a guard, it is a guess"). Friday-to-Monday is three days, a holiday
weekend four, Carnival five. So `FactLoadResult` carries the quote count and the first and last
publication instant instead, and the run log prints all three: a bounded or holey extraction is
visible as three numbers rather than hidden behind a rate that resolved.

**AND AN EXISTING TEST CAUGHT A REAL DEFECT IN THE FIRST DRAFT, which is worth recording
because it is this repository's own recurring hazard.** A reporting-currency row's
`fx_rate_date` was written `to_date(<event instant>)` — a **rendering** — so under
`America/Sao_Paulo` every midnight-UTC payment dated its own identity conversion to the
**previous day** and `fx_rate_date_key` became a function of a cluster setting.
`tests/gold/test_fact_payment.py::test_the_fact_is_unchanged_when_it_is_built_under_a_non_utc
_session_zone` refused it. It is `day_of(event_time)` now — the same ten characters
`event_date_key` is read from. The zone hazard is not one this layer met once and closed; it is
met by **every new column that answers "which day"**, and the only defence that has worked here
is a test that SETS the wrong zone rather than inheriting it.

---

## 2. What the runs said

*(Task 5.)*

---

## 3. What ships UNEXERCISED

**Standing decision §4.6: a path that ran zero rows through it is not a path that works.**
Accumulated as the phase runs rather than reconstructed at its end.

- **The duplicate-quote disagreement branch.** One duplicate pair in 3.6 years, and it agrees
  (§0.7). The refusal has no witness.
- **The holiday crossing, on fact rows.** No Brazilian national holiday falls in this phase's
  payment range (§0.6), so it is exercised over the series in the unit suite only.
- **The below-the-series refusal.** Nothing in this phase's range sits below 2026-06-03.
- **The provenance guard's REFUSAL half**, still, in the workspace. Eleven accepts across F2
  and F3, zero refusals. This phase's runs will add accepts, and accepts are not evidence
  about the refusal.

Added by Task 2, as the bronze layer was built:

- **The fetch's non-200 branch.** Exercised against a test double only; the live endpoint
  answered 200 every time it was asked.
- **The refusal of a fetch window that yields no quotes.** No window in this phase's range
  produces one.
- **The refuse-a-different-file-under-one-name branch, for PTAX.** It fires when BCB
  *revises* a rate for a window already landed. No witness in 3.6 years of series.
- **`encoding_replacement_char` is SHADOWED on four of the five columns it is folded
  over**, which is a stronger and different statement from the near-tautology below and was
  not recorded when the rule set was written. First-match-wins is the gate's contract, and an
  earlier CONTENT rule sits on every column but `currency`: a U+FFFD in `quote_date` breaks
  the ISO regex, in either rate it makes the decimal cast NULL, and in `data_hora_cotacao`
  it breaks the publication-instant shape. **No row escapes** — every one of the five is
  rejected — but the reason a triager filters on names the content rule, so
  `encoding_replacement_char` can only ever be the recorded reason for `currency`. Measured
  per column in `tests/bronze/test_ptax_rules.py`. The fold stays total over the contract
  for the reason it is derived rather than listed: a v2 column arrives covered, and the four
  shadowed columns are shadowed by rules that describe the same row correctly.
- **Every PTAX DQ rule except `bad_quote_date_shape` and `unparseable_data_hora_cotacao`
  is a near-tautology against the live source.** The landed record is built by this
  repository from an already-validated response, so the gate re-asserts at the table what
  the extraction refused at the row. Each rule's docstring says so. **A green gate here is
  not evidence about BCB** — it is evidence that our own fetch did not drift, which is a
  smaller claim wearing the same colour. *(The instant rule left this list in the fix pass:
  the landing directory is a re-ingestible surface with no `reclaim_landing`, so the value
  it now refuses is one the extraction cannot be asked about — see §3.1.)*

Added by Task 3, as the fifth profile and the currency domain landed:

- **The `before` placement.** `StreamProfile.placement` is one of `before` / `between` /
  `after`, and **no declared profile is `before`** — the F1b three sit after both
  `applied_date`s and the two June streams sit between them. Its branch in
  `profiles.observed_placement` is exercised in `tests/test_payment_profiles.py` and by nothing
  else. It is not speculative padding: without it the guard's dispatch would be a two-branch
  check that silently accepts any window below the earlier date. Reported as a suite-only
  path rather than as a working one.
- **The straddling window.** `observed_placement` returns `None` for a window containing an
  `applied_date`, which the guard turns into a refusal because `None` equals no declared
  placement. No declaration takes it, and F3 measured why it never will at today's shape: a
  straddle needs 48× the shared `event_interval_ms`. Suite-only.
- **`stream._require_currencies`' four refusals.** Outside the domain, out of domain order,
  a repeated member, and a bare `str` — every one of them is refused only in the suite,
  because a live declaration reaching any of them would fail the build. This is the same
  class as the gate's near-tautologies below: the guard protects an edit, not a data source.
- **The disagreement between `dim_currency`'s member count and its fact-side cardinality
  existed for exactly one commit.** The domain gained USD before any profile drew it, so the
  numbers were 2 and 1; the fifth profile made them 2 and 2. That is not an unexercised
  path — it is the opposite, a window in which the two numbers were provably different — and
  it is recorded here because the assertion that they can differ is otherwise only a comment.

Added by Task 4, as the gold layer was built:

- **The below-the-series refusal, on fact rows.** T3 clause 3 makes a payment for which no
  quote had yet been published a REFUSAL rather than a NULL rate, and nothing in this phase's
  payment range sits below 2026-06-03. The only population that reaches it is a fixture — a USD
  payment on 2026-06-20 against a series whose first quote publishes on 2026-07-31. Suite-only.
- **The disagreeing-duplicate refusal, now at the GOLD side too.** §0.7 records that the
  extraction layer's version has no witness in 3.6 years; the reduce over the LANDED table has
  the same absence for the same reason, and it is a second unexercised refusal rather than the
  same one counted twice — bronze appends, so the two see different populations. The
  *agreeing*-duplicate path IS exercised (a re-run is an ordinary event, and the reduce is what
  makes it one).
- **The unreadable-rate refusal in `rate_intervals`.** A `cotacao_venda` that will not cast to
  `decimal(18, 5)`, a `data_hora_cotacao` from which no instant can be read, or a `quote_date`
  that is no calendar day. All three are refused by the DQ gate one layer up, so this fires only
  on a bronze row that did not come through it. Suite-only, and deliberately not removed: the
  gate's own justification for its five `null_or_empty_*` rules is exactly this boundary.
- **`_refuse_a_derived_role_this_loader_cannot_produce`.** A conformed dimension reached through
  a derived role over any column but `fx_rate_date`. It cannot fire on the live registry, which
  is the point — it fires on an EDIT, before a session starts, where the alternative is an
  `AnalysisException` naming a column rather than the declaration that asked for it. Same class
  as `stream._require_currencies`' four refusals (§ Task 3 above): the guard protects a diff,
  not a data source.
- **The one-rate branch of `gold_load_fact.py`'s FX note.** It reports a star where every row
  converted at 1.00000 as the state the phase exists to end. After the rebuild the count is 3,
  so the branch that fires is the mixed one; the single-rate sentence is exercised in
  `tests/test_gold_entry_points.py` and by no run.

### 3.1 A gate rule weaker than its name — and the number this document first published was WRONG

Spark's format-agnostic `to_timestamp` **parses a bare time**, so
`unparseable_data_hora_cotacao` accepts `"13:03:25.555497"`. That much is true and was
measured. **What this section first published about the resulting value was false, and its
stated consequence was backwards.**

> **RETRACTED.** This said the value becomes `1970-01-01T13:03:25`, "a real instant fifty-six
> years early that every payment sorts after", and called the whole thing an accepted limit.
> The number came from Task 2's implementer, was repeated in two commit messages and in
> `rules.py`'s own docstring, and was accepted by **the first of two independent reviewers and
> by the controller**. Nobody measured it until the second review.
>
> **Controller-verified** through `opl.spark.local_session` (pyspark 3.5.9, session timezone
> UTC; the driver renders local, which is the −3 h in the display):
>
> | landed text | `to_timestamp` |
> |---|---|
> | `13:03:25.555497` | **2026-08-14** 10:03:25.555497 — *the date the query ran* |
> | `2026-06-19 13:03:25.555497` | 2026-06-19 10:03:25.555497 |
> | `1984-12-03 11:29:00.0` | 1984-12-03 08:29:00 |
> | `2025-04-23 13:02:31.416` | 2025-04-23 10:02:31.416000 |
>
> **Both consequences are worse than the retracted claim.** The value is
> **non-deterministic** — the same landed bytes yield a different instant tomorrow, and being
> a function of its input is bronze's entire contract. And the ordering is **inverted**:
> today's date is *later* than every payment in this phase's June/July window, so the row is
> **excluded from every as-of set** and the payment silently resolves to an **older** quote,
> which is verbatim the failure the rule's own docstring says it exists to prevent.
>
> **Why three readers inherited it.** The first reviewer reported the claim "verified by
> execution" — and what its execution verified was that the **rule does not fire**: the test
> asserts `== [None]`, which is true under either value. **A test that pins the verdict does
> not pin the value.** A docstring carrying a number no assertion checks is how it survived
> an implementer, a reviewer and the controller in sequence.

**This is therefore not an accepted limit.** A judgement about a known, stable behaviour can
be accepted and recorded; a publication instant that renders differently on two days cannot be
carried by this lakehouse at all.

**FIXED, and here is what the rule is now.** `unparseable_data_hora_cotacao` is two checks
where it was one, on `bad_quote_date_shape`'s own pattern — a **shape** the landed text must
have, and the format-agnostic **parse** it must still satisfy:

```
^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?$
```

Each half catches what the other misses. The shape refuses every value whose instant its own
text does not determine — a bare time (today's date), a bare date (midnight, an instant BCB
never published), a `T` separator, seven fractional digits, surrounding whitespace: **all six
return a non-NULL timestamp from `to_timestamp` and all six were accepted before.** The parse
refuses `2026-13-45 11:00:00`, which has the shape exactly and names no instant, so the parse
half stays load-bearing rather than becoming decorative behind the regex.

**It is not a pinned `to_timestamp` pattern, and that part of the original reasoning survives**
(below). The fractional group is `{1,6}`-or-absent, which is the set the series uses, and it
is **strictly stricter** than `ptax_source.PUBLICATION_FORMATS` — because whether a spelling is a
publication instant is one decision spanning the extraction layer and the gate, and a gate
looser than the extraction tolerates exactly the values a bug between the two could produce.

> **"Matches exactly" was false and is retracted.** Python's `%m`, `%d`, `%H`, `%M` and `%S` all
> accept **unpadded** fields, so the extraction validates `2026-6-19 13:03:25`,
> `2026-06-9 13:03:25`, `13:3:25` and `13:03:5` where the regex's `[0-9][0-9]` groups refuse
> them — **five spellings, not the three first reported.** `%Y` is the one field demanding four
> digits, so `26-06-19 13:03:25` is refused by both. The divergence runs in the **safe**
> direction, and the asymmetry is now pinned by a nine-spelling table asserting both verdicts
> against both real implementations, so the claim cannot go stale again the way this one did.

**And the test asserts the VALUE.** `test_a_bare_time_resolves_to_TODAYS_DATE_and_is_therefore_
refused` compares the resolved instant's date against `current_date()` in the same session —
not against a literal, which would be a second number going stale the way the retracted one
did — and then asserts the rule refuses the row. The first assertion fails if the resolved
instant ever changes; the second fails the moment anyone reverts to the format-agnostic parse.
A literal `!= 1970-01-01` sits between them, refusing the retracted claim by name.

**And "closed upstream" was false at the one boundary the gate exists to police.** The
extraction does refuse a bare time — but `bronze_ptax_ingest` reads a **directory** against
`struct_for("ptax")` and never imports the extraction module, and this table deliberately has
no `reclaim_landing`, so a landed file persists indefinitely. A file written by a wheel built
from another revision, hand-repaired, or copied in meets no extraction guard whatsoever. That
is precisely the boundary the five `null_or_empty_*` rules are justified by — "something
between that validation and bronze emptied a column" — and the same argument refuses leaving
this rule weak. The accurate scope is *closed for records this repository's fetch task
builds*; the residual is the landing directory as a re-ingestible surface.

**A pinned format pattern is still the wrong fix**, and that part of the original reasoning
survives: the fractional-second width is 1, 3 or 6 digits across the series (§0.4), so any
single pattern rejects real rows — `1984-12-03 11:29:00.0` and `2025-04-23 13:02:31.416`
among them.
