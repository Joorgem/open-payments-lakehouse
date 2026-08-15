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

**Extraction range: 2026-06-03 → 2026-08-01 inclusive — 60 calendar days, 43 weekdays, 42
quotes, gapless in business days *except 2026-06-04*.** The qualifier is not pedantry: the
bullet two lines above this one names 2026-06-04 as the single weekday absence, and
"**42 quotes, gapless in business days**" unqualified — the spelling this line, `ptax_window.py`
and `bronze_ptax_job.yml` all carried — contradicts it inside six lines. The holiday IS the
gap, it is the reason the floor is where it is, and a reader who takes the short form at face
value concludes the series has no holes and stops looking for the mechanism that resolves one.

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
   with no attributable quote date, so the extraction is **~~42~~ 60 requests of ~220 bytes**,
   not one wide call.
3. **The fractional-second width is not stable**: ~~1 digit in 1984, 3 in 2025, 6 in 2026~~
   — **2 to 6 digits inside this phase's own extraction window**, see below. Any fixed-width
   slice works on some of this phase's range and breaks on the rest of it.

> **BOTH NUMBERED CLAIMS ABOVE ARE MARKED FALSE HERE, WHERE THEY LIVE.** §2.8 said the two
> non-§1 falsifications were "corrected where they live rather than marked here" and named
> this section; nothing marked it, so a reader arriving at §0.4 — the section a reader
> consults *about the request shape* — met the false number unqualified. That is this phase's
> signature defect committed about its own correction, and the marker is the repair.
>
> - **The request count is 60, not 42** (§2.5, run log). `quote_dates` walks every CALENDAR
>   day of the span: 2026-06-03 .. 2026-08-01 is **60 calendar days**, of which **43 are
>   weekdays**, **42 answer a quote** and **18 answer HTTP 200 with `"value":[]`** (17
>   weekend days plus Corpus Christi 2026-06-04). 42 was a count of *quotes* published as a
>   count of *calls*, in the sentence that justified the cost. The single-day shape is
>   unaffected: the argument is attribution, not volume.
> - **The fractional-second enumeration `{1, 3, 6}` is false** — §3.1's closing note carries
>   the measurement. Its *conclusion* survives and is strengthened.
>
> §0.9's decision table carried the same "42 single-day calls" and is corrected there too.

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
  `pyproject.toml` attributes the failed 300 MB `pyspark` fetch to "Free Edition's **governed
  egress / serverless install budget**" — **both**, as alternatives it could not separate —
  and this measurement rules the first one out. What is left is the install budget.
  *(This bullet read "is about install budget, **not** about outbound HTTP being governed
  off", which reports the narrowing as though the note had never named egress. It named it
  first. The distinction matters because the note is what a reader consults before asking
  whether a task may call an API at all, and "it was only ever about install budget" would
  make Task 0's measurement look redundant rather than decisive. `pyproject.toml` now carries
  the answer beside the question.)*
- Runtime Python is 3.12.3 and `requests 2.32.2` is in the serverless base image.

The probe left no trace: the git tree was untouched and the uploaded workspace file was
removed afterwards.

### 0.9 What Task 0 decided

| question | answer | consequence |
|---|---|---|
| Can a serverless task reach the API? | **Yes** | the fetch is a job task; the file inventory stands |
| Are the two published rates right? | **Yes**, to the trailing zero | they now have a request behind them |
| Are there unpredicted gaps in the series? | **No**, in 903 rows | the only weekday absence is 2026-06-04 |
| Does the API return the quote date? | **No** | it is carried from the request, and the extraction is ~~42~~ **60** single-day calls — one per CALENDAR day, of which 42 answer a quote (§0.4, §2.5) |
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
>
> > **AND TWO OF THOSE SOURCE COMMENTS STILL CARRIED THE RETRACTED MECHANISM UNTIL THE
> > CONSOLIDATION PASS** — `src/opl/generator/profiles.py` ("The 415 us remainder puts the
> > boundary strictly between two events") and `tests/test_payment_profiles.py` ("the
> > publication carries a 415 us remainder"). Only `opl.gold.fx` was moved to 249.585 ms.
> > Correcting "the copy a reader quotes" and leaving the copies a reader *runs* is this
> > phase's signature defect in its purest form, and neither whole-branch reviewer named it.
> > Re-derived: 29,179,750 mod 5,000 = 4,750, so publication is **249.585 ms before index
> > 5,836** and **4,750.415 ms after index 5,835** — drop the microseconds entirely and the
> > clearance is still **250 ms**. The 415 µs cannot be what separates them.

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
> The number holds — the collision probability is ≈5 × 10⁻⁶ — but as first published it did not
> establish 36,800. Found by Task 3's reviewer.
>
> > **AND THE ≈5 × 10⁻⁶ DOES NOT FOLLOW FROM THE SPACE THIS NOTE STATES, WHICH IS THE SAME
> > DEFECT ONE LAYER DOWN.** It gives the space as 1024 × 1023 × 4,999,901 × **2** × 5 ≈
> > 5.2 × 10¹³ — the ×2 being the currency domain. Over that space the expected cross-stream
> > collisions are 4,675 × 27,600 / 5.2 × 10¹³ ≈ **2.5 × 10⁻⁶**, half what is printed. The
> > figure printed is right, and it follows from the **BRL-conditioned** space: both
> > populations in this comparison are BRL by construction — the existing 27,600 tuples
> > because no earlier stream draws USD, the fifth stream's 4,675 because that is the BRL half
> > of its base events — so the currency factor is **1**, the space is 2.6 × 10¹³, and
> > 4,675 × 27,600 / 2.6 × 10¹³ = **4.9 × 10⁻⁶**. The corrected derivation still argued about a
> > population wider than the one it counted. The conclusion is unchanged in either
> > reading: both are ≈10⁻⁶ and 36,800 is a prediction that can fail.
> > *(§1.1's other probability, ≈8 × 10⁻⁷ for the fifth stream's 9,200 base draws colliding
> > with each other, IS right against the ×2 space — those draws do vary the currency.
> > 9,200² / (2 × 5.2 × 10¹³) = 8.1 × 10⁻⁷. Two probabilities, two spaces, and only one of them
> > was stated correctly.)*
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

> **MARKED, NOT EDITED — "every USD row" IS FALSE AND ADR 0016 CARRIES THE CORRECTION.**
> 5.14420 → 5.14 is **0.0816%**, on the **2,864** rows carrying that rate. The other 2,041
> USD rows carry **5.13950**, which rounds to 5.14 as well and is **0.0097%** off in the
> *opposite* direction — an order of magnitude smaller. The argument survives on the worse
> half and the type is unchanged. **Four source and test sites carried the wider phrasing
> after ADR 0016 retracted it** (`bronze/rules.py`, `gold/fx.py`, `gold/registry.py`,
> `tests/gold/test_fact_payment_fx.py`); all four now carry the split, which is the point of
> marking a prediction rather than quietly editing one.

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
| last publication instant printed | **2026-07-31 16:10:31.061071+00:00** | §0.2's `13:10:31.061071` read as BRT, rendered as an aware **UTC** instant on every driver (below). The first one is 2026-06-03's bulletin and is **not measured** — no prediction is made for it |
| conversions past the last landed quote | **0** | every USD payment falls on 2026-06-22 and the series runs to 2026-07-31, so nothing converted at the open end |
| widest fallback any conversion took | **3 days** | Monday 2026-06-22 back to Friday 2026-06-19 for 2,864 rows; 0 for the 2,041 same-day ones |

> **THE LAST THREE ROWS ARE NEW, AND THE FIRST VERSION OF THIS TABLE COULD NOT HAVE BEEN
> CHECKED WITHOUT THEM.** The quote count and the publication span describe the SERIES, and
> nothing in `FactLoadResult` described the payment window they are supposed to be compared
> against — `fx_last_published` was carried, printed and never compared. **An extraction
> stopping one business day short of the payments produces every other number in this table
> unchanged** (40,000 rows, grain enforced, four zero orphan counts, `fx_rates_used` 3) with up
> to 10,000 rows converted at a stale rate, because the last quote's interval is closed at
> `VALID_TO_CEILING` and a payment after it matches THAT quote rather than nothing.
> `fx_beyond_series` and `fx_widest_fallback_days` are the other side of that comparison, and
> both are **REPORTED, not refused**: a payment after the most recent bulletin is the normal
> case — 20,000 fact rows fall on Saturday 2026-08-01, later than any quote a correct
> extraction can hold — so telling a truncated window from a Saturday evening needs the
> business-day calendar T3 refuses on the record. A refusal would refuse this phase's own
> correct build. Closed by
> `tests/gold/test_fact_payment_fx.py::test_a_series_that_stops_short_of_the_payments_is
> _REPORTED_by_the_run_it_cannot_refuse`, which builds a real fact from a one-quote series
> against payments 43 days later and asserts every other count clean.

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

> **AND THE FIRST VERSION OF THAT SUBSTITUTE COVERED ONLY THE LOW END, WHICH THE FIX PASS
> REPAIRED RATHER THAN RE-ARGUED.** Declining the *bound* was right; reporting the series' own
> three numbers while nothing described the PAYMENT window is one side of a comparison. Two
> numbers were added, both over the rows that actually consult a quote: **conversions that took
> the series' last landed quote** (0 predicted here) and the **widest fallback any conversion
> took** (3 days predicted here), which is what an INTERIOR hole moves when the high end reads
> zero. The high end is reported and not refused for T3's own reason — a payment after the most
> recent bulletin is normal, 20,000 fact rows fall on Saturday 2026-08-01, and separating that
> from a truncated window needs the calendar the ruling refuses. §1.3 carries the predictions
> and the test that closes it.
>
> **Two further refusals were added in the same pass, both with witnesses.** An EMPTY reduced
> series was accepted — over zero rows `max`/`min` are NULL, so both existing branches are
> skipped and a fact builds against an absent PTAX series at `fx_rate = 1`, i.e. the pre-phase
> state printed as "converted at ONE rate over 0 quotes published None .. None". A `quotes == 0`
> refusal needs no bound from the data. And the interval window's `orderBy` had no tie-break, so
> two quote dates sharing one publication instant (2001-12-21 in the series; 1984-12-03/04/05
> all published on 1984-12-05) left it undetermined which one carried the open range and which
> got the empty interval `[t, t)` — no fan-out either way, so every count stayed right while the
> surviving rate was arbitrary. Its test drives the same two landed rows in both orders and was
> confirmed to FAIL against the old `orderBy`.
>
> > **MARKED, NOT EDITED — BOTH WITNESSES IN THAT SENTENCE ARE MISREAD, AND §3 CARRIES THE
> > MEASUREMENT.** 2001-12-21 is ONE quote date with two rows; 1984-12-03/04/05 publish on one
> > DATE at three different instants (11:31 / 12:40 / 18:50). No two distinct quote dates share
> > a publication instant anywhere in 1984-11-28 .. 2026-08-13. The guard and its test are
> > unchanged and correct; only the claim that the series witnesses the case is false. The
> > sentence is left standing because §1 is not edited after the run — §0.4's remedy applies
> > here too: **mark it where it lives**, and put the correction where a reader of §3 will find
> > it against the ledger it belongs on.

#### THE ZONE INVARIANT `opl.gold.fx` ASSERTED ABOUT ITSELF IS FALSE — measured in the fix pass

**`_published_instant` was SESSION-ZONE DEPENDENT and its own docstring said it was not.** The
shipped spelling was `to_utc_timestamp(to_timestamp(text), 'UTC-03:00')`, described in the
module docstring, the function docstring and two commit messages as one in which
"`to_utc_timestamp` cancels the session zone on both sides so the instant does not move with
`spark.sql.session.timeZone`".

**It does not cancel.** Spark's `to_utc_timestamp` renders its input **in UTC** and not in the
session zone (`convertTz(micros, from=tz, to=UTC)` reads `getLocalDateTime(micros, UTC)`),
while `to_timestamp` over text carrying no offset parses **in the session zone** — so only one
of the two varied. Measured through `opl.spark.local_session` (pyspark 3.5.9), `unix_micros` of
one landed stamp under three session zones (and at three of the fractional widths the series
carries — the parse is format-agnostic, so it is indifferent to the width; "all three" was
this sentence's own version of the enumeration §3.1 retires):

| session zone | `2026-06-19 13:03:25.555497` shipped | offset appended to the text |
|---|---|---|
| `UTC` (the pin) | 1781885005555497 = **16:03:25.555497Z** ✅ | 1781885005555497 ✅ |
| `America/Sao_Paulo` | 1781895805555497 = **19:03:25.555497Z** ❌ | 1781885005555497 ✅ |
| `Asia/Tokyo` | 1781852605555497 = **07:03:25.555497Z** ❌ | 1781885005555497 ✅ |

**No landed number is wrong and nothing in §1.1 or §1.2 moves**, because
`opl.config.SESSION_TIMEZONE` pins the session to UTC in both gold entry points and in the
local session — which is exactly the problem: **the whole of T3's zone ruling rested on a
cluster setting**, in the layer whose own prose says the defence that has ever worked is not
depending on one. The fix appends the offset to the text
(`to_timestamp(concat(data_hora_cotacao, '-03:00'))`, the offset rendered from
`ptax_source.BRASILIA` so there is still one spelling of it), which is
`fact_guards.event_instant`'s discipline applied to the side BCB leaves zoneless. Measured
identical under all three zones above and identical to the old value under the pin.

**Why nothing caught it, which is the transferable half.** T3 asked to "pin it in a test" and
the zone FIX was pinned while the zone INVARIANCE was not: the only non-UTC session test
(`test_fact_payment.py`) has a **BRL-only** fixture, so the FX interval bounds are computed
there and discarded, and the one assertion on a publication instant read `.microsecond` —
blind to the whole-hour shift that is the only thing a wrong zone does. Both are repaired:
`test_two_landed_rows_that_agree_reduce_to_the_earlier_publication_instant` asserts the **full
instant** and re-derives the series under `America/Sao_Paulo`. That test is what produced the
measurement above, by failing.

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

**Ran 2026-08-14, all four jobs SUCCESS, at revision `6cfe0f00c6f62720feb711a9478c005a33b3b7e7`.**
Everything below is **controller-verified** unless a line says *Reported*: the run ids and task
durations come from `databricks jobs get-run`, the quoted lines from
`databricks jobs get-run-output <TASK run id>`, and every count from `.plans/sql.sh` with its
36-character statement id and its `from_cache` flag both read before the number was written
down. **F3 published 13-character statement-id prefixes that resolve to nothing and recorded
it as a defect; these are full ids.**

### 2.0 The deploy, verified BY ARTEFACT and not by its success line

`databricks bundle validate -t free` → **Validation OK**; `databricks bundle deploy -t free` →
**Deployment complete**. That sentence is not the verification. Three checks are:

| check | value |
|---|---|
| local `dist/…whl` sha256 | `c0beb381459337d4b1dac6bcb3af7a0002ba46322e3f7832638597d089d4a8de` |
| **deployed** wheel, downloaded back out of the workspace, sha256 | `c0beb381459337d4b1dac6bcb3af7a0002ba46322e3f7832638597d089d4a8de` — **identical** |
| revision stamped **inside** the downloaded wheel (`opl/_revision.py:9`) | `REVISION = '6cfe0f00c6f62720feb711a9478c005a33b3b7e7'` = `git rev-parse HEAD`, with no `+dirty` suffix |

**And the source was read out of the DOWNLOADED wheel, not out of the tree** — F3's second
check, copied because a digest match proves the two files are the same file and not that
either one carries the fix. `opl/gold/fx.py` was unzipped from the downloaded artefact and
`cmp`-ed against `src/opl/gold/fx.py`: **identical**. It carries both of this phase's last two
fixes by source:

- **`_published_instant`**'s
  `stamped = F.concat(F.col(ptax.PUBLISHED_AT_COLUMN), F.lit(_BRASILIA_OFFSET))` — the offset
  appended to the text, i.e. the repair for the session-zone dependency §1.3 records. **The
  retracted `to_utc_timestamp(to_timestamp(text), 'UTC-03:00')` spelling is gone as
  EXECUTABLE code**; the literal string still appears three times in the deployed bytes, in
  the docstrings that retract it, which is the intended state and not a residue.
  *(This bullet first named `_as_micros`, a Python-side helper over a `datetime` forty lines
  above. Caught by an independent audit; §1.3 and ADR 0016 both had it right.)*
- `FxSeries`' corrected span sentence (last publication **2026-07-31**, not the 2026-08-01 the
  extraction *range* ends on), which is the diff `6cfe0f0` carried in this file.

**No commit was made between the deploy and any of the runs** (§4.1). The tree was clean at
`6cfe0f0` at deploy time and every job was launched with `--params …,revision=$(git rev-parse HEAD)`.

### 2.1 The run sequence

| # | job | run id | wall clock | result |
|---|---|---|---|---|
| 1 | `opl-bronze-payments` (`profile=cross-currency`, `month=2026-08`) | `1110464707906055` | **186.0 s** | **SUCCESS** |
| 2 | `opl-bronze-ptax` (`month=2026-08`) | `607801051136099` | **181.3 s** | **SUCCESS** |
| 3 | `opl-gold-conformed-dimensions` | `225673262734612` | **68.3 s** | **SUCCESS** |
| 4 | `DROP TABLE workspace.default.fact_payment` | `01f19831-3044-1af3-ac60-24c7b8d0344f` | — | **SUCCEEDED** |
| 5 | `opl-gold-fact-payment` | `202942563444320` | **72.0 s** | **SUCCESS** |

Runs 1 and 2 were launched **overlapping** — they write different tables and share no batch —
so the wall clocks above are not additive. First launch `2026-08-14T22:31:47.432Z`, last
termination `2026-08-14T22:42:20.327Z`: **632.9 s** end to end, which includes the operator
time between runs and the drop.

**`month=2026-08` is the LANDING month and not the event window**, for both bronze jobs. The
`cross-currency` stream's events are on 2026-06-22; launched under `month=2026-06` the June
stream would never have reached `bronze_payments`. The handoff records this as an F3 fact and
it cost nothing here because it was read first.

### 2.2 Task-level durations, captured from the run output rather than reconstructed

**F3 captured five of its six gold builds and recorded the two absences rather than
estimating them; this closes the same gap on the smaller scale.** *(An earlier draft of this
line said F3 "published no durations for its gold runs", which is false in both halves:
`docs/f3-workspace-run-evidence.md` §9.5 publishes `dim_company` 120 s, `pit_estabelecimento`
168 s and the three conformed builds at 32–35 s, summing to **387 s**. What F3 recorded as
absent was `fact_payment`'s duration and two of its five guard durations. Caught by an
independent audit of this section.)*

| job | task | execution | setup |
|---|---|---|---|
| `opl-bronze-payments` | `assert_deployed_revision` | 30 s | 4 s |
| | `generate` | 46 s | 1 s |
| | `ingest` | 39 s | 1 s |
| | `dq_gate_batch` | 26 s | 2 s |
| | `promote` | 30 s | 1 s |
| | `check_bad_rows` (condition) / `fail_on_dq` | 0 s / **SKIPPED** | — |
| | **sum** | **171 s** | |
| `opl-bronze-ptax` | `assert_deployed_revision` | 28 s | 4 s |
| | `fetch` | 52 s | 1 s |
| | `ingest` | 30 s | 1 s |
| | `dq_gate_batch` | 24 s | 1 s |
| | `promote` | 33 s | 1 s |
| | `check_bad_rows` (condition) / `fail_on_dq` | 0 s / **SKIPPED** | — |
| | **sum** | **167 s** | |
| `opl-gold-conformed-dimensions` | `assert_deployed_revision` | 29 s | 3 s |
| | `dim_channel` / `dim_currency` / `dim_date` (parallel) | 31 / 30 / 32 s | 1 / 2 / 2 s |
| | **sum** | **122 s** (68.3 s wall — the three run concurrently) | |
| `opl-gold-fact-payment` | `assert_deployed_revision` | 26 s | 3 s |
| | `fact_payment` | **40 s** | 1 s |
| | **sum** | **66 s** | |

**526 s of measured task execution, of which 113 s — 21.5% — is the provenance guard.** Four
runs, four `assert_deployed_revision` tasks at 26–30 s each, every one of them a cold
serverless start doing one string comparison. That is the guard's real price at this scale.

**The like-for-like comparison with F3 runs the OTHER way, and the first draft of this
sentence had it backwards.** F3's 95 s is **three captured guards**, not five runs — 31 + 32
+ 32, with two of its five never captured (`docs/f3-workspace-run-evidence.md` §9.5). So F3's
guard cost **31.7 s per captured run** against F-API's **28.25 s**, i.e. this phase's guard is
marginally *cheaper* per run, not half again as expensive. **The guard's cost is a per-run
constant of ~30 s, and neither phase's builds are big enough to hide it.** That is the durable
statement and the per-run figure is the one to quote.

> **THE SHARES WERE OFFERED AS A SECOND, CORROBORATING ROUTE — 95 / 482 = 19.7% at F3 against
> 113 / 526 = 21.5% here — AND THE TWO DENOMINATORS ARE NOT THE SAME KIND OF NUMBER.** F3's
> **482 is gold only**: 387 s of five gold builds plus its three captured guards. It excludes
> F3's own step 1 (`opl-bronze-payments`) entirely, and excludes `fact_payment`'s duration,
> which F3 records as not captured. F-API's **526 is all four jobs**, two of them bronze —
> 338 of the 526 s. So a near-identical percentage is read off a gold-only set and a
> bronze-plus-gold set, and the agreement is a coincidence of composition rather than
> corroboration. **Struck as evidence.** The per-run constant stands on the 31.7-versus-28.25
> comparison, which is like for like because a guard task is a guard task in any job — and
> the percentage only ever means anything beside the build it is a fraction of, which is
> exactly why two differently-composed fractions may not be compared.

**NO DURATION PREDICTION WAS PUBLISHED BEFORE THESE RUNS, so no duration below is marked.**
They are measurements, not confirmations. §1 predicted rows and rates and said nothing about
time, and a cost number written after the run that produced it is not a prediction (§4.5) —
which is exactly the standard this document applies to everything else. F3's lesson stands
unexercised here rather than restated: **do not extrapolate gold cost from vault cost on row
count.**

### 2.3 Storage — a figure F3 never published for gold at all

*(F3's evidence contains no `sizeInBytes`, `numFiles` or `DESCRIBE DETAIL` for any gold table,
and F2 published per-table bytes for **ten** of the fourteen vault tables.)*

> **THE PARENTHESIS ABOVE WAS A CORRECTION THAT REVERSED A TRUE STATEMENT, AND IT WAS WRONG
> IN ITS OTHER HALF TOO.** It read: *"F2 published per-table bytes for **all fourteen** vault
> tables and the gold phase published none. An earlier draft said F3 'recorded that as a gap'
> — **it did not record it either way**, which is a weaker and more accurate statement."*
>
> - **F3 recorded it, in those words.** `docs/f3-workspace-run-evidence.md` §9.5 closes with
>   "**Storage was not captured for any gold table.** The vault's per-table sizes are in
>   `docs/f2-wave-1-workspace-run-evidence.md` §5.4; the equivalent table for gold does not
>   exist and is not reconstructed here." The earlier draft was right and the correction
>   overshot it.
> - **F2 published ten of fourteen, not all fourteen.** Eight vault tables carry a size in a
>   §-level table (`hub_empresa` 2.459 GB, `sat_empresa_dados` 5.716, `hub_estabelecimento`
>   2.753, `sat_estabelecimento_dados` 5.585, `sat_estabelecimento_endereco` 6.496,
>   `link_empresa_estabelecimento` 7.201, `link_company_partner` 1.932,
>   `sat_eff_company_partner` 0.988) and two more in prose (`ref_cnae` 23,635 B, `ref_municipio`
>   41,665 B). **`ref_motivo`, `ref_natureza_juridica`, `ref_pais` and `ref_qualificacao` have
>   no per-table figure** — the six reference tables are aggregated as "< 0.001 GB" for the
>   whole task.
>
> Both halves propagated to `.plans/HANDOFF.md`, where they are corrected in the same pass.
> **"Check a correction as hard as the claim it replaces" is a bullet this repository already
> carries, and this is its fourth instance.**

**Controller-verified**, `DESCRIBE DETAIL` per table:

| table | files | bytes |
|---|---|---|
| `bronze_payments` (40,150 rows, 4 batches) | 4 | **1,690,371** |
| `bronze_ptax` (42 rows) | 1 | **4,765** |
| `dim_currency` (2 members + ghost) | 3 | **4,960** |
| `dim_date` (50 members + ghost) | 2 | **6,880** |
| `dim_channel` (5 members + ghost) | 2 | **3,472** |
| `fact_payment` **before** the drop (30,000 rows, 10 columns) | 1 | **1,336,929** |
| `fact_payment` **after** the rebuild (40,000 rows, 13 columns) | 1 | **1,926,707** |

`1,690,371` bytes of `bronze_payments` against **11,748,003** bytes of landed JSONL
(2,925,069 + 2,969,937 + 2,926,409 + 2,926,588, the four promoted profiles' published byte
counts) is the all-string bronze table's Parquet compression at **6.9×** (6.94996), not a
loss. The codec is measured and not assumed: `DESCRIBE DETAIL`'s `properties` carries
`"delta.parquet.compression.codec":"zstd"`. The fact grew 44% for a 33% row increase and
three added columns, one of which is a `decimal(18,5)` taking three distinct values.

### 2.4 Step 1 — `opl-bronze-payments`, `profile=cross-currency`

**Reported**, `generate` task run `87731639120985`:

```
generate_payments: profile=cross-currency stream_id=F-API-CROSS-CURRENCY written at /Volumes/workspace/default/landing/generated/2026-08/payments/F-API-CROSS-CURRENCY.jsonl
generate_payments: rows=10000 (declared 10000) drifted=0 bytes=2926588 sha256=8bf65d61fe08186c91bf88036ac82bc35d404501f9f71981501e39306f18d831
generate_payments: event_time 2026-06-22T08:00:00.000Z .. 2026-06-22T21:53:15.000Z (10000 events, 5000 ms apart)
generate_payments: currency BRL=5095 USD=4905 (declared BRL, USD)
```

**`bytes=2926588` is the number §1.1 published on the tree at `f564f57`, before the
`currencies` field existed**, reproduced by a workspace emission against the **real
`hub_empresa` pool** rather than the synthetic 1,024-key one. The sha256 differs from §1.2's
`a527b61c…` for exactly the reason §1.1 stated in advance: the pool decides *which* company
gets which payment and therefore the digest, but every `cnpj_basico` is eight characters, so
it cannot move the byte count.

**AND THE CURRENCY MIX IS IN THE RUN LOG, WHICH THE PLAN SAID IT WOULD NOT BE.** Task 3's
handover recorded that `generate_payments._report` "prints no currency mix, so **Task 5 cannot
read the 5,095 / 4,905 split out of the run log**". It was repaired before the deploy —
`_currency_mix` prints the counts in the order the profile *declares* its currencies — so the
split has a run-log witness as well as a table one. Recorded because the plan's own text still
says otherwise.

**Reported**, `dq_gate_batch` `347135863764864` and `promote` `62861209205868`:

```
dq_gate_batch: batch=1110464707906055 good=10000 bad=0
promote_batch: appended 10000 rows (batch 1110464707906055) to workspace.default.bronze_payments
```

`fail_on_dq` was **SKIPPED**, which is the gate passing.

**Controller-verified**, `01f19830-72e1-1e79-8544-b867ef0c1002`, `from_cache: None`:

| | predicted | **actual** | |
|---|---|---|---|
| `bronze_payments` rows | 40,150 | **40,150** | ✅ |
| …distinct `transaction_id` | 40,000 | **40,000** | ✅ |
| …`_batch_id` values | 4 | **4** | ✅ |

**Controller-verified**, `01f19830-7bba-17e5-82d6-a0fa0446cd89`, `from_cache: None` — the
currency split at the table rather than at the emitter:

| currency | rows |
|---|---|
| BRL | **35,245** = 30,150 pre-phase + **5,095** |
| USD | **4,905** |

**None of the 150 injected redeliveries is USD**, which is not a coincidence and is worth one
line: they come from `promotable`, a BRL-only stream, so `40,150 − 40,000 = 150` and
`USD = 4,905` are consistent by construction rather than by luck.

**Marked from the same log**, because a number quoted inside a code block is not a marked
prediction:

| §1.1 predicted | **actual** | | |
|---|---|---|---|
| delivered rows | **10,000** | ✅ | `rows=10000 (declared 10000)` |
| drifted rows | **0** | ✅ | `drifted=0` — this profile declares no defects |
| landed bytes | **2,926,588** | ✅ | predicted on `f564f57`, before `currencies` existed |
| BRL / USD, delivered | **5,095 / 4,905** | ✅ | twice: the emitter's own count and `01f19830-7bba-…` at the table |
| distinct `event_date_key` it adds | **1** (2026-06-22) | ✅ | the fact went 2 → 3 (§2.8) |

#### THE ONE §1.1 PREDICTION NO WORKSPACE RUN CAN MARK, said rather than dropped

§1.1's sharpest refinement — the one it introduced with "No version of the phase plan said
so" — is that the currency split is **not** a parity draw over 10,000 indices, because a
legitimate repeat *inherits* its source base event's whole attribute tuple:

| population | BRL | USD |
|---|---|---|
| 9,200 base events (drawn at salt 0) | 4,675 | **4,525** |
| 800 legitimate repeats (inherited) | 420 | **380** |

**Neither row is markable from the workspace, and the reason is structural: nothing landed
carries a base-versus-repeat flag.** A repeat is a distinct `transaction_id` with an identical
business tuple; bronze holds the tuple and the id and no marker of which row was drawn and
which inherited, so no query over `bronze_payments` or `fact_payment` can partition 9,200 from
800. Their sum, **5,095 / 4,905**, is marked ✅ above; the decomposition is confirmed only by
the local closing test (§1.2) and by the aggregates it implies at the fact — `36,800 = 4 ×
9,200` and `3,200 = 4 × 800` (§2.8), which pin the *sizes* of the two populations without
splitting either by currency.

**Recorded as UNMARKABLE, which is itself a verdict.** F3 closed with "22 confirmed, 1
unexercised, 2 falsified, **1 unmarkable**", and the fourth category exists so that a
prediction no run can reach is written down rather than quietly omitted. This is F-API's one.
*(It was omitted from the first draft of §2 and found by an independent audit, which is the
argument for the category.)*

### 2.5 Step 2 — `opl-bronze-ptax`

**Reported**, `fetch` task run `690868124844405`:

```
fetch_ptax: window 2026-06-03 .. 2026-08-01 (60 calendar days) -> 42 quote(s), 18 day(s) with none
fetch_ptax: written at /Volumes/workspace/default/landing/api/2026-08/ptax/usd-2026-06-03_2026-08-01.jsonl
fetch_ptax: rows=42 bytes=6156 sha256=0dce4f1354f93d09f47e7c54b731bb0e7745003c2ccedfa89eb3cdad0ef466b8
```

**SIXTY requests from a serverless task in 52 s, and every published version of this number
said forty-two.** Task 0's egress measurement (§0.8) is now a production path and not a
probe: this is the first job in this repository that fetches its own input over HTTP.

> **THE REQUEST COUNT IS 60, NOT 42, AND THE RUN LOG IS WHAT SAYS SO.** §0.4 of this
> document, the phase plan in three places, and `ptax_source.fetch_series`' own docstring
> ("ONE REQUEST PER QUOTE DATE … the phase's span is 42 quotes") all state the extraction as
> **"42 requests of ~220 bytes"**. `quote_dates` yields **every calendar day** in the span —
> it must, because a caller cannot know which days carry a quote without asking — so the
> window 2026-06-03 .. 2026-08-01 is **60 requests**, of which 42 answer a quote and **18
> answer HTTP 200 with `"value":[]`**. The run log prints both numbers correctly and nobody
> had read them against the published claim.
>
> **The number that was wrong is the one describing the COST**, which is the number the
> single-day request shape was justified against: the cost of attribution is **43% higher**
> (60 / 42) than every document in this phase says. It is still trivial — at most ~13 KB of
> response bodies, 52 s wall — so the ruling does not move — but "42 requests" was a count of *quotes* wearing the label
> of a count of *calls*, which is the same species as §1.3's falsified row below: a number
> right about one population, published as an answer about another. **Twice in one phase.**
>
> **And it has one good consequence:** the `"value":[]` envelope §0.5 records as
> indistinguishable from a failure was **received 18 times in the workspace** and read as an
> absence each time, so the no-quote-for-this-day branch is no longer fixture-only. **That is
> not the same as exercising the refusal** — a *wholly* empty span is what
> `_refuse_a_span_with_no_quote_at_all` fires on, and 42 of the 60 answered, so it did not
> fire. §3 keeps it on the unexercised list.

```
dq_gate_batch: batch=607801051136099 good=42 bad=0
promote_batch: appended 42 rows (batch 607801051136099) to workspace.default.bronze_ptax
```

**Controller-verified**, `01f19830-e50d-15e7-b00e-584e0fb95074`, `from_cache: None`:

| | predicted | **actual** | |
|---|---|---|---|
| `bronze_ptax` rows | 42 | **42** | ✅ |
| …distinct `quote_date` | 42 | **42** | ✅ one row per date, no reduce needed on a first landing |
| …first / last `quote_date` | 2026-06-03 / — | **2026-06-03 / 2026-07-31** | ✅ |
| …distinct `currency` | 1 | **1** | ✅ USD only |

**The last landed quote date is 2026-07-31 and the extraction RANGE ends 2026-08-01**, which
is not a shortfall: 2026-08-01 is a Saturday and has no bulletin. That is the distinction
`6cfe0f0` corrected in `fx.py`'s own docstring hours before this run, and the run reproduces
it exactly.

**Gaplessness in business days, measured rather than assumed** — every weekday in
2026-06-03 .. 2026-08-01 anti-joined against the landed dates.
**Controller-verified**, `01f19830-f021-10c9-be20-27c9c092c12a`, `from_cache: None`:

| missing business day | weekday |
|---|---|
| **2026-06-04** | Thu |

**One row, and it is the one Task 0 named** — Corpus Christi 2026, absent from BCB's series,
falling back to 2026-06-03 (venda 5.04150). Zero unpredicted gaps in the landed window. This
is the assertion T3 clause 2 *declined to build into the loader* (a bound on the gap is either
a holiday calendar or a number drawn from this window — see ADR 0016), taken here as an
operator measurement instead, which is what the decline said would happen.

### 2.6 Step 3 — `opl-gold-conformed-dimensions`, append-safe

**Reported**, the three task logs verbatim:

```
gold_load_conformed_dimension: workspace.default.dim_currency +1 rows (2 members + 1 ghost, 3 distinct currency_key values, which is every row); drawn from 'currency', and the fact reaches 2 of them
gold_load_conformed_dimension: workspace.default.dim_date +0 rows (50 members + 1 ghost, 51 distinct date_key values, which is every row); drawn from 'event_time', and the fact reaches 3 of them
gold_load_conformed_dimension: workspace.default.dim_channel +0 rows (5 members + 1 ghost, 6 distinct channel_key values, which is every row); drawn from 'payment_method', and the fact reaches 5 of them
```

| | predicted | **actual** | |
|---|---|---|---|
| `dim_currency` gains USD | **+1** row, 2 members | **+1**, **2 members** | ✅ |
| `dim_currency` fact-side cardinality | 2 | **2** | ✅ **the "cannot be wrong" column is retired** |
| `dim_date` appends | **zero** rows | **+0**, still **50 members** | ✅ |
| `dim_date` fact-side cardinality | 3 | **3** | ✅ |
| `dim_channel` | unchanged, 5 members | **+0**, **5**, fact reaches **5** | ✅ |

**`dim_date` appending zero is a prediction and not a tautology.** `covered_span` anchors its
low end on 2026-06-13, and 2026-06-22 sits inside the existing span, so a calendar built for
three streams already covered the fourth and the fifth. Had the fifth profile's window been
one day outside it, this run would have appended and every count downstream would still have
been right — which is why the number was published in advance.

### 2.7 Step 4 — the drop, measured before it happened

**Pre-decided in the plan and in ADR 0015, not discovered here.** `opl.gold.facts._appended`
writes `mode("append")` with no `mergeSchema`, so three new columns make the append fail —
and *with* `mergeSchema` it is worse: `_new_rows` anti-joins on the grain, so the 30,000
existing rows would keep NULL FX **forever** while every counter in the run log reported
clean.

**The transition is measured rather than asserted. Immediately before the drop**
(`01f19831-1f1e-1ab4-ac90-855f2988dd66` for the counts,
`01f19831-2a32-1140-a024-e1a01e99596c` for the storage, both `from_cache: None`; the schema is
`01f1982f-9173-1fc1-99cd-d8e003dad8a9`):

| | before the drop | after the rebuild |
|---|---|---|
| rows / distinct `transaction_id` | 30,000 / 30,000 | **40,000 / 40,000** |
| columns | **10** | **13** |
| distinct `event_date_key` | 2 | **3** |
| distinct `currency_key` | **1** | **2** |
| files / bytes | 1 / 1,336,929 | 1 / **1,926,707** |
| Delta table id | `dba54eea-be46-422e-a3ad-9958b8c665a4` | **`6342e221-0c68-4790-85f9-06dfb8cb62d7`** |

`DROP TABLE workspace.default.fact_payment` → statement
`01f19831-3044-1af3-ac60-24c7b8d0344f`, **SUCCEEDED**; `SHOW TABLES … LIKE 'fact_payment'`
returned zero rows (`01f19831-36f7-1775-b947-71f98a492630`, `from_cache: None`). **The two
different Delta table ids are the evidence that this was a rebuild and not an append.**

The three columns the rebuild added, with their declared types confirmed at the table
(`01f19831-8df6-18d9-b4ff-f113b0fb05c9`, `from_cache: None`):

| column | predicted type | **actual** |
|---|---|---|
| `fx_rate` | `decimal(18, 5)` | **`decimal(18,5)`** ✅ |
| `amount_brl` | `decimal(18, 2)` | **`decimal(18,2)`** ✅ |
| `fx_rate_date_key` | `int` (`yyyyMMdd`) | **`int`** ✅ |

### 2.8 Step 5 — `opl-gold-fact-payment`, and the phase's headline

**Reported**, `fact_payment` task run `314037919408885`, in full because every clause of it is
a marked prediction:

```
gold_load_fact: workspace.default.fact_payment +40000 rows from 40150 bronze payments over
40000 distinct transaction_id keyed on dim_company and ['dim_date', 'dim_channel',
'dim_currency']; 80000 (row, role) references over 40000 rows, all resolved and NONE on the
ghost (payer_company_sk: 0, payee_company_sk: 0) -- so the unknown-member path is UNEXERCISED
rather than proven: every counterparty is drawn from hub_empresa's own key space, so nothing
in this data can fail to resolve; and 3200 legitimate repeats survived it (36800 distinct
business tuples), which is the count a deduplication over the business attributes would have
deleted; every derived conformed key names a member that exists; and 3 distinct amount_brl
conversion rates were used over 42 reduced (currency, quote_date) quotes published
2026-06-03 16:06:26.540000+00:00 .. 2026-07-31 16:10:31.061071+00:00, no conversion past the
last quote and widest fallback taken 3 day(s); SUM(amount_brl) is the only additive total
(amount is additive only within a currency) and it does NOT equal SUM(amount) x rate to the
cent, because the conversion rounds half-up at the row
```

#### THE FX SPLIT — the two populations counted separately

**Controller-verified**, `01f19831-a0bf-17d9-a6ce-815a9b45ce74`, `from_cache: None`. The fact
carries `currency_key`, not a currency string, so this is joined through `dim_currency`:

| currency | `fx_rate` | `fx_rate_date_key` | **rows** | predicted |
|---|---|---|---|---|
| BRL | **1.00000** | 20260620 | 10,000 | — |
| BRL | **1.00000** | 20260622 | **5,095** | **5,095** ✅ |
| BRL | **1.00000** | 20260801 | 20,000 | — |
| **USD** | **5.14420** | **20260619** | **2,864** | **2,864** ✅ |
| **USD** | **5.13950** | **20260622** | **2,041** | **2,041** ✅ |

**2,864 fell back across a whole weekend to Friday 2026-06-19; 2,041 resolved same-day on
Monday 2026-06-22. Both populations are non-empty, and they carry different rates.** That is
T2's closing test, on real fact rows, and it is the property three earlier windows in this
phase were published for and falsified before this one survived.

**Two payments on ONE calendar day carry two different rates** —
`01f19831-ab40-1e2a-bfca-677cce8a0046`, `from_cache: None`:

| `event_date_key` | rows | distinct `fx_rate` | distinct `fx_rate` among USD rows |
|---|---|---|---|
| 20260620 | 10,000 | 1 | 0 |
| **20260622** | **10,000** | **3** | **2** |
| 20260801 | 20,000 | 1 | 0 |

**One calendar day, one stream, one currency, two rates.** No calendar-day implementation can
produce that row, and it is the reason the fifth profile exists.

**And the boundary itself is at the predicted index** —
`01f19831-fb02-158e-ac6e-1fc670ad4182`, `from_cache: None`, splitting 2026-06-22's rows at the
2026-06-22 bulletin read as BRT (`2026-06-22T16:06:19.750415Z`):

| | predicted | **actual** |
|---|---|---|
| rows before the bulletin | 5,836 | **5,836** ✅ |
| rows after the bulletin | 4,164 | **4,164** ✅ |

#### 2,864 RE-DERIVED BY TWO ROUTES THAT SHARE NO DECOMPOSITION

F3's precedent: a number too neat to trust once is re-measured by a query sharing nothing with
the first. **Controller-verified**, `01f19835-01f6-1f07-bafe-49b045df14de`, `from_cache: None`,
over the 4,905 rows with `fx_rate <> 1.00000`:

| route | reads | **answer** |
|---|---|---|
| group by `fx_rate` / `fx_rate_date_key` (above) | the resolved rate and its date key | **2,864** / 2,041 |
| compare `event_time` against the publication instant | the payment's own instant, and nothing the FX join produced | **2,864** ✅ |
| recover the rate as `ROUND(amount_brl / amount, 4)` | the two money columns only | **2,862** / 2,040 ⚠️ |

**The third route loses 3 of the 4,905 converted rows — two from the 2,864 bucket and one
from the 2,041 — and the three rows are the argument rather than a defect.**

> **THIS SENTENCE FIRST READ "the third route is 3 rows short", AGAINST A COLUMN WHOSE OWN
> SUBTRACTION IS 2** (2,864 − 2,862). Three rows fail to recover, but only two of them are in
> the 2,864 population: the `6.35` row below carries **5.13950**, so it falls out of the 2,041
> bucket instead (2,041 − 2,040 = 1). **That is the same "argues about the wrong population"
> defect as the falsified §1.3 row two subsections below**, committed in the paragraph written
> to demonstrate a cross-check, and caught by an independent audit rather than by its author.
> **Three times in one phase.**

`01f19835-1a4c-1c19-ba3c-457ecc44d3ac`, `from_cache: None`, lists all three in full:

| `amount` | `fx_rate` | `amount_brl` | recovered ratio |
|---|---|---|---|
| 6.35 | 5.13950 | 32.64 | 5.14015748… |
| 31.40 | 5.14420 | 161.53 | 5.14426751… |
| 73.06 | 5.14420 | 375.84 | 5.14426498… |

They are short for exactly the reason §1.3 published in advance: `amount_brl` is rounded
HALF-UP at the row, so half a centavo is a larger fraction of a small amount and moves the
recovered ratio in the fourth decimal. `6.35 × 5.13950 = 32.635825 → 32.64`, recovering
5.1402.

> **AND THEY ARE NOT SIMPLY "THE THREE SMALLEST", WHICH IS WHAT THIS PARAGRAPH FIRST
> ASSERTED WITHOUT MEASURING IT.** `01f19836-e291-134d-b4e6-de3ceac8823f`, `from_cache: None`,
> lists **all eight** converted rows under R$100 with their recovered ratios: 6.35 → 5.1402
> ✗, 31.40 → 5.1443 ✗, 46.75 → 5.1395 ✓, **73.06 → 5.1443 ✗**, 87.03 → 5.1442 ✓, 92.40 →
> 5.1395 ✓, 92.40 → 5.1442 ✓, 99.76 → 5.1395 ✓. **Five of the eight smallest recover
> correctly and one of the three failures is the fourth-smallest.** Size raises the
> probability and does not determine it — what determines it is where `amount × rate` falls
> relative to the half-centavo boundary, which is a property of the product's third decimal
> and not of the amount's magnitude. `92.40 × 5.14420 = 475.32408 → 475.32`, barely rounded
> at all; `73.06 × 5.14420 = 375.835252 → 375.84`, rounded up by 0.0047.

**The rate is NOT recoverable from the two money columns**, which is why `fx_rate` is a column
and not a derivation — and this is the sharpest available demonstration that the rounding
claim describes real rows rather than a bound nobody reached.

#### THE COUNTS

**THE PROVENANCE IS PER ROW AND NOT PER TABLE, because half of these are SQL against the
built table and half are the loader's own run log.** This repository has recorded six defects
that were a controller labelling its own prose as verification, so the label is a column here
rather than a heading. `C` = **Controller-verified**, with the statement id; `R` =
**Reported**, from `gold_load_fact`'s log quoted above. All statements `from_cache: None`.

| | predicted | **actual** | | provenance |
|---|---|---|---|---|
| `fact_payment` rows | **40,000** | **40,000** | ✅ **grain enforced** | **C** `01f19831-80ec-1859-ae3d-86a3d31f523e` |
| …distinct `transaction_id` | 40,000 | **40,000** | ✅ | **C** `…80ec` |
| …distinct `event_date_key` | **3** | **3** | ✅ 20260620, 20260622, 20260801 | **C** `…80ec` |
| …distinct `fx_rate_date_key` | **4** | **4** | ✅ 20260619, 20260620, 20260622, 20260801 | **C** `…80ec` |
| …distinct `fx_rate` values | 3 | **3** | ✅ 1.00000, 5.14420, 5.13950 | **C** `…80ec` |
| …channels / currencies reached | 5 / 2 | **5 / 2** | ✅ | **C** `…80ec` |
| **bronze**'s distinct business tuples (the control) | 36,800 | **36,800** | ✅ dedup changed none | **C** `01f19831-b024-13c1-9f75-f789b1dd0695` |
| …rows at `fx_rate` exactly 1.00000 | 35,095 | **35,095** | ✅ every BRL row, by definition and not by lookup | **C** `…b024` |
| …**fact-side** distinct business tuples | 36,800 | **36,800** | ✅ `= 4 × 9,200` | **R** |
| …legitimate repeats | 3,200 | **3,200** | ✅ `= 4 × 800` | **R** |
| …rows resolving to the ghost, both roles | 0 | **0** | ⚠️ **UNEXERCISED, not success** | **R** |
| …orphaned rows per fact key (four of them) | 0 | **0** | ⚠️ unexercised, see §3 | **R** |
| reduced PTAX quotes read | 42 | **42** | ✅ | **R** |
| last publication instant printed | 2026-07-31 16:10:31.061071+00:00 | **2026-07-31 16:10:31.061071+00:00** | ✅ to the microsecond | **R** |
| `fx_beyond_series` | 0 | **0** | ✅ *(a report, not a path — see §3)* | **R** |
| `fx_widest_fallback_days` | 3 | **3** | ✅ Monday 06-22 back to Friday 06-19 | **R** |
| rows where the two date keys AGREE | 35,095 | **37,136** | ❌ **FALSIFIED — see below** | **C** `…b024`, decomposed at `01f19831-bc82-1694-b89b-b83e3f1db092` |
| `SUM(amount_brl)` ≠ `SUM(amount) × rate` | stated in words | **0.241039 apart** | ✅ | **C** `01f19831-c979-10b7-8baf-9562377117dc` |

**Fact-side 36,800 and bronze-side 36,800 are two different measurements and only the second
is a statement.** The first is the loader reporting on the frame it just wrote; the second is
SQL over bronze, taken as the control precisely because a loader agreeing with itself is not
evidence that the dedup preserved what the payments *were*.

**`SUM(amount_brl)` is not `SUM(amount) × rate` to the cent, and here is the arithmetic**
rather than the sentence: `SUM(amount_brl) = 1,501,572,707.34` against a
multiply-the-per-rate-subtotals answer of `1,501,572,707.5810390` — a gap of **0.241039** over
4,905 converted rows, against a bound of half a centavo per row (24.525). `SUM(amount)` is
`997,161,462.62` and **is not a currency total at all** now that 4,905 rows are USD, which is
precisely why `amount_brl` is the declared measure.

#### THE ONE FALSIFIED §1 PREDICTION, AND IT IS THE MOST USEFUL ROW IN THIS SECTION

*(The only falsified **§1 prediction**. Two other published claims were falsified in this
pass and are not §1 predictions, so they are corrected where they live rather than marked
here: **§0.4's "42 requests"**, measured at **60** (§2.5), and **§2.8's own "3 rows short"**,
which is 2 in the column it was written against (above). **This sentence was true of the
second and false of the first for the whole of the branch's life**: §0.4 carried no marker
of any kind until the consolidation pass put one there, so the claim that it had been
"corrected where it lives" was itself the surviving stale claim. It is true now, and it is
checkable by opening §0.4.)*

> **§1.3 predicted 35,095 rows "where the two date keys AGREE", derived as "the BRL
> population — an identity conversion is dated to its own day". The measurement is 37,136.**

**Controller-verified**, `01f19831-bc82-1694-b89b-b83e3f1db092`, `from_cache: None`:

| currency | `fx_rate` | rows where `event_date_key = fx_rate_date_key` |
|---|---|---|
| BRL | 1.00000 | 35,095 |
| **USD** | **5.13950** | **2,041** |
| | | **37,136** |

**The prediction's derivation was a sufficient condition offered as a necessary one.** An
identity conversion is indeed dated to its own day — but so is **any USD payment that
resolved to the SAME day's quote**, and this document predicted 2,041 of those, four
paragraphs above the row that forgot them. The two date keys agree for every same-day
resolution, whatever the currency, and "same-day resolution exists" is the entire point of
T2's window.

**Nothing is adjusted to match.** The number 35,095 is right about the population it names
(BRL rows) and wrong as an answer to the question asked (rows where the keys agree). The
error is not in the loader, the join or the star: **it is in a prediction that reasoned about
one population while counting another** — which is the same species Task 3's reviewer caught
in §1.1's 36,800 derivation, in this same document, and which was corrected there rather than
learned from. Twice now, the defect has been the derivation and not the number.

**Restated correctly for whoever inherits it:** rows where `event_date_key = fx_rate_date_key`
= every reporting-currency row (35,095) **plus** every converted row that resolved same-day
(2,041) = **37,136**, and the complement — 2,864 — is exactly the fallback population.

#### The as-of join's headline case, re-measured

§5 required `f3-workspace-run-evidence.md` §6's payment-leg counts for company `47070968` to
be re-measured after the fifth stream. **Controller-verified**,
`01f19831-dedc-1ae5-ad58-9526630be145`, `from_cache: None`:

| side of 2026-07-11 | `company_sk` | `capital_social` | `is_current` | payment legs (F3) | **payment legs (now)** |
|---|---|---|---|---|---|
| **before** | `-8897288640841010596` | 50000,00 | false | 18 | **39** |
| **on/after** | `7138330321006406353` | 370000,00 | true | 38 | **38** |

**The "before" side gained 21 legs and the "after" side gained none**, which is the shape the
fifth profile forces rather than a surprise: its 10,000 payments are on 2026-06-22, before
2026-07-11, so every leg it contributes resolves to the June version. The "after" side is fed
only by the 2026-08-01 streams, which did not change.

### 2.9 A THIRD DEVIATION FROM MASTER SPEC §4.3'S COLUMN LIST, and the repository says two

**Controller-verified** by the post-rebuild schema (`01f19831-8df6-18d9-b4ff-f113b0fb05c9`,
`from_cache: None`). §4.3 asks the fact to hold
`amount_original + currency + fx_rate + fx_rate_date + amount_brl`. The star holds:

| §4.3 asks for | the star carries |
|---|---|
| `amount_original` | `amount` — **recorded** as a deviation |
| **`currency`** | **`currency_key` (`bigint`), and no currency column at all** — **recorded nowhere** |
| `fx_rate` | `fx_rate` ✅ |
| `fx_rate_date` | `fx_rate_date_key` — **recorded** as a deviation |
| `amount_brl` | `amount_brl` ✅ |

`src/opl/gold/facts.py` **called** `fx_rate_date_key` "the **second** deviation … the first
being `amount_original`", and §1.3 of this document still does. **On this repository's own
criterion it is the third of three.** `currency` is exactly the same species as
`fx_rate_date`: a bare business column replaced by a conformed foreign key, satisfied through
a dimension rather than in the fact. It predates this phase — F3 built the fact with
`currency_key` and no currency column, at a time when the column would have held one constant
value — but it becomes visible only now that the fact reaches two currencies, and it is not in
any ADR, evidence document or docstring. **ADR 0016 records all three, and `facts.py`'s
docstring and `test_fact_payment_fx.py`'s assertion message were corrected to say three in
the same pass** — the two sites that had been citing "the T3 ADR, which Task 5 writes"
against a `docs/adr/` that stopped at 0015. **§1.3 is deliberately NOT corrected**: it is a
predictions section, and a prediction edited after the run that tests it stops being one.
*(`dim_currency` spells its member `currency_code`, so the string `currency` names a bronze
column and nothing in gold.)*

### 2.10 The provenance guard: four more accepts, then a deliberate refusal

**Reported**, `assert_deployed_revision` task run `736340098682648`, one of four identical:

```
assert_deployed_revision: OK -- the installed wheel was built from 6cfe0f00c6f62720feb711a9478c005a33b3b7e7,
which is the revision this run was launched for. That is a claim about the WHEEL; the
entry-point files under databricks/src were synced by the same deploy, and a deploy made from
a modified tree would have stamped +dirty.
```

**Eleven accepts across F2 and F3 became FIFTEEN**, and this phase then made the guard refuse
— see below, together with the retraction of what that was first published as.

### 2.10.1 The refusal, re-confirmed under this phase's code — NOT for the first time

> **RETRACTED, AND THE RETRACTION IS THE POINT. This section was published as "THE REFUSAL
> HALF FIRED, FOR THE FIRST TIME IN THIS WORKSPACE". It was not the first time, and the
> proof was already on `main`, in this same directory.** `docs/f1.4b-pr-b-run-evidence.md`
> §12 records the guard refusing on **2026-08-03** across six executions — including run
> `788625093349052`, the deliberate incident shape, which refused with **8 tasks SKIPPED**
> against this run's 1 — and [ADR 0009](adr/0009-deployed-revision-provenance.md)'s own
> **Status** paragraph has said so for eleven days.
>
> **The three things this section claimed "fifteen accepts could not establish" were all
> established there**: that it raises, that downstream tasks are skipped, and that the
> message names both revisions. And "ADR 0009's central claim is now a measurement rather
> than a design intent" was already true when it was written.
>
> **Authored by this phase's controller**, in the branch's final commit, *after* the
> independent audit that was dispatched to catch exactly this. The path in: `.plans/HANDOFF.md`
> and F3's evidence both carried "the provenance guard's REFUSAL is still unexercised in the
> workspace", **which was itself already false** — F3 inherited it and this phase inherited
> it from F3. Nobody read the primary source. That is the species this document names three
> times about other people's work, committed here about its own.

**What the run below is actually worth, stated at its real size:** the mismatch refusal is
**re-confirmed against this phase's code and this phase's wheel**, twenty-eight commits and
one new landing mode later, and it produced one corroboration F1.4b's §12 did not — see the
retry note at the end.

Task 5 left the workspace in the state that arms it: the deployed wheel was built from
`6cfe0f0`, and the evidence commits moved HEAD to `ac379c9`. Launching
`opl-gold-fact-payment` at HEAD against that wheel is the guard's own scenario, costs one
task, and **writes nothing** — the refusal is the point.

Job run **`972804892628743`**, guard task run **`522276384734989`**, job state `TERMINATED` /
`RUN_EXECUTION_ERROR`, and the `fact_payment` task **`SKIPPED`**:

```
WrongRevision: refusing to run: the deployed wheel was built from
6cfe0f00c6f62720feb711a9478c005a33b3b7e7, and this run was launched for
ac379c9797c5c3713b9433389e3a998ecc3bf6f9. Different commits -- so every task after this one
would execute code that is not the code you are reading.
The likeliest cause is the simplest: nobody ran `databricks bundle deploy -t free` since
ac379c9797c5c3713b9433389e3a998ecc3bf6f9. CI validates the repository and never what is
deployed, so nothing else would have gone red. It can also be a stale wheel inside a fresh
deploy -- the jobs receive the wheel through the glob `dependencies: ["../../dist/*.whl"]`
over a dist/ that nothing cleans.
Deploy, then launch again with the same revision.
```

**What it adds, now that the novelty claim is withdrawn.** The mismatch shape still refuses
under code twenty-eight commits newer, with a fourth landing mode and a fifth profile in the
tree — a regression check rather than a discovery. And **the guard task ran TWICE**: attempt 0
(`522276384734989`) and attempt 1 (`414272406289427`), both `RUN_EXECUTION_ERROR`. That is
master route §4 standing decision 4 — *"`max_retries: 0` does not prevent a retry"* — caught
in the wild a second time, and it is why the guard task must stay side-effect-free. This
section first said the experiment "costs one task"; it cost two attempts, and the retry is
the more useful half.

**The `+dirty` shape is NOT covered by this and remains open.** ADR 0009 separates the two
deliberately: reproducing it means deploying an artefact known to be built from a dirty tree
and cleaning up behind it. §3 keeps that entry rather than striking it.

### 2.11 What this phase made FALSE, re-published in one place

Protocol §9 condition 5 asks a phase to delete what it falsified rather than leave two
answers in the repository. Every row below was a published number in
`docs/f3-workspace-run-evidence.md` or the phase handoff; **every replacement is marked above
with its provenance** — controller-verified where a statement id is given, *Reported* where
the loader's own log is the source, and one row below is explicitly a derivation with its
arithmetic shown rather than either.

| published at F3's close | **now** | where marked |
|---|---|---|
| `bronze_payments` 30,150 rows / 30,000 ids / 3 batches | **40,150 / 40,000 / 4** | §2.4 |
| `fact_payment` 30,000 rows | **40,000** | §2.8 |
| distinct business tuples 27,600 = 3 × 9,200 | **36,800 = 4 × 9,200** | §2.8 |
| legitimate repeats 2,400 = 3 × 800 | **3,200 = 4 × 800** | §2.8 |
| channels / currencies reached 5 / 1 | **5 / 2** | §2.8 |
| distinct `event_date_key` 2 | **3** — 20260620, **20260622**, 20260801 | §2.8 |
| `dim_currency` 1 member, fact-side cardinality 1 | **2 members, fact-side cardinality 2** | §2.6 |
| §9.4's "`dim_currency` at fact-side cardinality 1 … **cannot be wrong**" | **retired.** T1 existed to do exactly this | §2.6 |
| §6's payment legs for `47070968`: **18** / 38 | **39 / 38** | §2.8 |
| `dim_date` **50** members | **50** — unchanged, and predicted so | §2.6 |
| the guard's **eleven** accepts, zero refusals | **fifteen** accepts, zero refusals | §2.10 |
| the handoff's "100% of the 30,000 fact rows fall on days with no quote, and the path that goes unexercised is the DIRECT lookup" | **false twice over.** 2,041 rows resolve a quote same-day (measured); and under an instant rule 6,480 of the original 30,000 already sat in an earlier BRT day — **derived, not measured**: each F1b stream opens at 00:00Z = 21:00 BRT the previous day, so 3 h ÷ 5,000 ms = 2,160 events per stream × 3 promoted streams | §2.8, ADR 0016 |
| `fact_payment`'s 10 columns | **13** — `fx_rate`, `amount_brl`, `fx_rate_date_key` | §2.7 |

**And numbers this document made false about ITSELF**, kept out of the table above because
that table is for *inherited* claims and a phase must mark its own separately. **This list
said THREE and there are SIX**, which is the smallest thing on it and the most on-brand:

| this document said | **is** | where | **found by** |
|---|---|---|---|
| §1.3: **35,095** rows where the two date keys agree | **37,136** | §2.8 — the one falsified §1 prediction | the **SQL that tested it** (`01f19831-bc82-1694-b89b-b83e3f1db092`), in `fc8dd8d` |
| §0.4: the extraction is **42 requests** of ~220 bytes | **60** requests, 42 quotes + 18 empty | §2.5, and now marked at §0.4 | **re-reading the run log**, in `fc8dd8d` |
| §2.8's own first draft: the ratio route is **"3 rows short"** | **2**, against the column it was written against | §2.8 | the **independent audit**, in `ac379c9` |
| §1.3: `fx_rate_date` is the **second** deviation from §4.3's column list | the **third of three** | §2.9 | reading the rebuilt table's schema |
| §0.4 and §3.1: the fractional-second width is **1, 3 or 6** digits | **every width from 1 to 6**, five of them in this window | §3.1 | the **whole-branch docs review**, then measured twice |
| §1.3: two quote dates sharing one publication instant, witnessed by 2001-12-21 and 1984-12-03/04/05 | **neither is that case**, and the series has no witness | §3 | the **whole-branch docs review**, then measured over 10,447 rows |

**The first three are one defect**: a number correct about one population, published as the
answer about another — the keys-agree row counted BRL and was asked about same-day
resolutions, "42" counted quotes and was asked about calls, "3" counted unrecovered rows and
was asked about one of the two buckets they fall in. **The last three are a second defect
this phase should name as squarely**: an ENUMERATION or a CITATION offered where the argument
needed neither — two deviations where the criterion yields three, three fractional widths
where the source uses six, two witnesses for a case the series does not exhibit. In every one
the operative conclusion survived and the specifics beside it did not, which is precisely why
nobody re-derived them.

**Attribution, from the git history rather than from memory** — the column above. This
paragraph read "**Two were found by an independent audit and one by re-reading the run log**",
which is wrong on the first two rows: `fc8dd8d` introduced both the 37,136 and the 60, one
from the statement that tested the prediction and one from the run log, and the audit
(`ac379c9`) came afterwards and found the third. **None of the six was found by the author of
the sentence carrying it.**

### 2.11.1 A CORRECTION THAT LANDS IN THE DOCUMENT AND NOT IN THE SOURCE IS HALF A CORRECTION

**Two of this phase's own retractions were published here and left standing in code**, found
by the consolidation pass sweeping for the shape rather than for the sentences the reviewers
named. Both are the phase's signature defect turned on its own repairs: the copy a reader
*quotes* was fixed and the copies a reader *runs* were not.

| retracted in | the claim | still live in, until the sweep |
|---|---|---|
| §1.1, and `opl.gold.fx` | "the **415 µs** remainder puts the boundary strictly between two events" — it is the **.750 s**; 29,179,750 mod 5,000 = 4,750, so publication sits **249.585 ms** before index 5,836 and **250 ms** before it with the microseconds dropped entirely | `src/opl/generator/profiles.py`, `tests/test_payment_profiles.py` |
| ADR 0016 Decision 4 | "`amount_brl` **0.08% wrong on every USD row**" — it is **0.0816%** on the 2,864 rows at 5.14420, and **0.0097%** the other way on the 2,041 at 5.13950 | `src/opl/bronze/rules.py`, `src/opl/gold/fx.py`, `src/opl/gold/registry.py`, `tests/gold/test_fact_payment_fx.py` |

§1.1's own retraction says it was corrected "here rather than in the three source comments
alone, because this is the copy a reader quotes" — and then two of the three were not
corrected at all. **Neither whole-branch reviewer named either family**, which is the useful
part: a review reads what it is pointed at, and a retraction is only closed by a `grep` for
the retracted string across every tree that ships.

---

## 3. What ships UNEXERCISED

**Standing decision §4.6: a path that ran zero rows through it is not a path that works.**
Accumulated as the phase runs rather than reconstructed at its end.

- **The duplicate-quote disagreement branch.** One duplicate pair in 3.6 years, and it agrees
  (§0.7). The refusal has no witness. *(Task 5 note: `src/opl/gold/fx.py:51-52` calls
  2001-12-21 "a second [pair], identical stamps" while `fx.py`'s `rate_intervals` and §1.3
  describe that same date as **two quote dates sharing one publication instant** — the
  `orderBy` tie-break case, a different phenomenon — and it falls 22 years outside the 903-row
  window this count is taken over. **The measured count stays ONE**; the repository does not
  agree with itself about a second and neither reading is quoted here as settled.)*

  > **SETTLED IN THE CONSOLIDATION PASS, AND `fx.py:51-52` IS RIGHT.** Single-day requests
  > through `scripts/probe_ptax.py`'s own `fetch_window`: **2001-12-21 returns TWO rows for
  > THAT ONE quote date**, both stamped `2001-12-21 23:55:00.0`, both `2.33030 / 2.33110`. It
  > is a duplicate pair, it agrees, and it is outside the walked window — so the measured count
  > inside the walk stays ONE and the disagreement refusal still has no witness either way.

- **THE `orderBy` TIE-BREAK, WHICH HAS NO MEASURED WITNESS AT ALL.** `fx.rate_intervals`
  orders by publication instant **and then** by quote date, and §1.3 and the source justified
  the second key with two witnesses. **Neither is that phenomenon**: 2001-12-21 is one quote
  date with two rows (above), and 1984-12-03 / 04 / 05 publish on 1984-12-05 at **11:31**,
  **12:40** and **18:50** — one publication DATE, three distinct INSTANTS.

  **Controller-verified**, one range request over the whole series through
  `scripts/probe_ptax.py`'s `fetch_window`:

  | | |
  |---|---|
  | series walked | 1984-11-28 .. 2026-08-13 |
  | rows | **10,447** |
  | distinct publication stamps | **10,446** |
  | stamps carried by more than one row | **1** — `2001-12-21 23:55:00.0`, both rows one quote date |
  | tightest gap between two DISTINCT instants | 27 ms — 2025-04-23, **within** one quote date |
  | tightest gap between two distinct QUOTE DATES | **6 minutes** — 1996-04-10 published `1996-04-11 18:36`, 1996-04-11 published `18:30`, the later quote date first |

  **One repeated stamp, and it is within a single quote date, is a complete answer**: two
  distinct quote dates sharing an instant would necessarily produce a second repeated stamp,
  and there is none in 42 years. `ptax_source.MAX_PUBLICATION_SPREAD`'s own walk had recorded
  the six minutes and was never quoted against the tie-break's claim.

  **The second key stays** and is not removed by this finding. `bronze_ptax` is written
  `mode("append")`, so two quote dates under one stamp is reachable from a hand-repaired file,
  a revised window or a second fetch — a landing, not a publication — and without the key the
  surviving rate is arbitrary while every count stays right. It is exercised by
  `test_two_quote_dates_sharing_one_publication_instant_do_not_depend_on_the_row_order` and by
  nothing else, which is what this ledger is for.
- **The holiday crossing, on fact rows.** No Brazilian national holiday falls in this phase's
  payment range (§0.6), so it is exercised over the series in the unit suite only.
- **The below-the-series refusal.** Nothing in this phase's range sits below 2026-06-03.
- **The provenance guard's `+dirty` REFUSAL, in the workspace — STILL OPEN.** The
  **mismatch** shape is not open and never was in this phase: `docs/f1.4b-pr-b-run-evidence.md`
  §12 proved it on 2026-08-03 and §2.10.1 re-confirmed it here. What has never been reproduced
  against the workspace is a deploy built from a **dirty tree**, because doing so means
  publishing an artefact known to be built from uncommitted work and cleaning up after it —
  [ADR 0009](adr/0009-deployed-revision-provenance.md) states that separation explicitly and
  keeps it.

  > **This entry was struck as "CLOSED — it refused" and is restored.** The strike collapsed
  > two shapes ADR 0009 keeps apart, so it removed a genuinely unexercised path from the
  > ledger whose governing rule is standing decision §4.6. **Authored by this phase's
  > controller**, in the same commit as the retracted novelty claim above. `.plans/HANDOFF.md`
  > had it right and was not the source of the error.

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
  **It had NO witness when this entry was first written** — every other "suite-only" line in
  this ledger means a fixture reaches the branch, and `grep` found only the fact-side analogues.
  Closed in the fix pass by one `venda="abc"` row
  (`test_a_rate_that_cannot_be_read_is_refused_and_the_distinct_count_cannot_see_it`), which
  also exercises the reason the NULL count sits beside `count_distinct`: the cast NULLs, the
  distinct count ignores it, so `rates` is 0 and only the second branch can fire.
- **The empty-series refusal** (`quotes == 0`), added in the fix pass. A `bronze_ptax` that
  reduces to no quotes at all cannot happen behind a successful ingest of that window, so
  the only population that reaches it is a fixture. Suite-only.
- **`FactRole`'s reader-versus-source refusal.** `READS_DATE` on a contract-sourced role, or a
  derived role read as ISO text or as a member. Like
  `_refuse_a_derived_role_this_loader_cannot_produce` it cannot fire on the live registry — it
  fires on an EDIT, at import, where the alternative is a `date_format` over raw ISO text that
  casts in the session zone and keys every midnight-UTC payment to the previous day.
- **The high-end coverage report is not a refusal and must not be read as one.** `fx_beyond_series`
  is predicted **0** on this data, which means the series reached past every conversion — not
  that a path was exercised. The state it exists to make visible (a truncated extraction) is
  reachable only from a fixture here.
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

**Added by Task 5, from what the four runs did and did not touch.** Everything above was
written before a run existed; these entries are the ones only a run could produce, and they
are the *residue* of four SUCCESS results rather than a list of things that went wrong.

- **CONFIRMED UNEXERCISED BY THE RUNS, not merely predicted so.** Four entries above stopped
  being forecasts and became measurements: the **ghost on both role keys** is `0 / 0` over
  80,000 (row, role) references (§2.8), the **four orphan counters** are all 0, **`fx_beyond_series`
  is 0**, and the **one-rate branch** did not fire because `fx_rates_used` came back 3. Each is
  a zero, and **a zero is not coverage** — the state each exists to make visible was not
  reachable from this data, which is what §4.6 means.
- **THE DQ GATE'S FAILING ROUTE, IN THIS PHASE.** `fail_on_dq` was **SKIPPED** on both bronze
  runs (`good=10000 bad=0`, `good=42 bad=0`), so the ten PTAX rules and the payments rules were
  all exercised in the *accept* direction only. **No PTAX row has ever been rejected by the gate
  in the workspace.** `drifting` — the one profile that makes the gate fire — was deliberately
  not run in this phase, so the quarantine path carries no F-API witness. `bronze_ptax_quarantine`
  exists and holds **zero** rows (controller-verified, `01f19832-8f32-1a3e-b056-222f99f62c67`,
  `from_cache: None`).
- **NO LONGER UNEXERCISED: the no-quote-for-this-day branch.** 18 of the fetch's **60**
  requests answered HTTP 200 with `"value":[]` and were read as absences (§2.5). Moved off
  this list because a run put real bodies through it. **The refusal of a wholly-empty span
  stays on it** — 42 of the 60 answered, so `_refuse_a_span_with_no_quote_at_all` did not
  fire, and the two are different branches.
- **`reclaim_landing`, for PTAX.** This table deliberately has none (its way back is the
  request), which means the landing directory is a permanently re-ingestible surface. §3.1's
  residual — a file written by another wheel, hand-repaired, or copied in, judged by the gate
  alone — is not merely open but now has a real file sitting in it:
  `/Volumes/workspace/default/landing/api/2026-08/ptax/usd-2026-06-03_2026-08-01.jsonl`.
- **The refuse-a-different-file-under-one-name branch, for PTAX** — still. It fires when BCB
  *revises* a rate for a window already landed, or when a second fetch derives different bytes
  for the same filename. This run was a **first** landing, so the branch was reached with
  nothing to compare against. A second `opl-bronze-ptax` run over the same window is the
  cheapest way to exercise it and was not made, because it would also append 42 duplicate rows
  to `bronze_ptax` and put the gold-side reduce to work — which is a different experiment.
- **THE GOLD-SIDE REDUCE RAN OVER A POPULATION THAT NEEDED NO REDUCING.** `rate_intervals` read
  42 rows and reduced them to 42 `(currency, quote_date)` pairs — one to one. Its
  *agreeing*-duplicate path, which §3 above calls "exercised" on the grounds that a re-run is an
  ordinary event, **has not been exercised in the workspace**: no re-run has happened. The
  disagreement refusal remains without a witness at either layer. Corrected here rather than
  left as the stronger claim it was written as.
- **The empty-series refusal and the below-the-series refusal**, both still fixture-only after
  the runs, for the reasons already stated — the 60-day window landed its 42 quotes successfully and nothing
  in the payment range sits below 2026-06-03.
- **The holiday crossing, on fact rows** — closed as unexercised by measurement rather than by
  argument. §2.5's anti-join found exactly one missing business day, **2026-06-04**, and the
  earliest date any fact row resolves to is **2026-06-19** (§2.8). So the one holiday in the
  landed series is 15 days below the closest fact row could reach it. The witness for ADR 0016's
  central argument is a **series** row, and no fact row crosses a holiday in this lakehouse.
- **THE TWO-RATE PROPERTY RESTS ON ONE STREAM AND ONE DAY.** 4,905 of 40,000 fact rows are
  converted at all; every one of them falls on 2026-06-22; every one resolves to one of two
  quotes. The mechanism is proven — and it is proven **once**. The other **40** landed quotes are
  reachable by no payment in this lakehouse, and `fx_rate_date_key` takes 4 of the 51 values
  `dim_date` holds. A reader must not read "42 quotes landed" as "42 quotes exercised".
- **The `assert_deployed_revision` guard cost 21.5% of this phase's task time — ~113 s of
  serverless start-up across four accepts** — and refused nothing *during the phase's own five
  runs*. It was then made to refuse deliberately, off the critical path (§2.10.1), so the cost
  is now a measured premium on a mechanism this phase watched work rather than on one it only
  assumed. **This bullet first read "…and refused nothing … a string equality that has never
  once been unequal in the workspace"**, which was false in two directions at once: the
  deliberate refusal is recorded forty lines above it, and the mismatch had already been proven
  in F1.4b. Left corrected rather than deleted, because the *cost* observation survives and a
  reader meeting only the retraction would think the 21.5% was withdrawn too.

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
survives — **strengthened by the measurement that falsified the enumeration it rested on.**

> **RETRACTED: "1, 3 or 6 digits".** §0.4 published the fractional-second width as `{1, 3, 6}`
> — one digit in 1984, three in 2025, six in 2026 — and so did ADR 0016, `fx.py`,
> `ptax_source.py` and `tests/bronze/test_ptax_rules.py`. **The series uses every width from 1
> to 6, and five of the six are inside this phase's own extraction window.**
>
> **Controller-verified over the 42 quotes of 2026-06-03 .. 2026-08-01 as landed in
> `bronze_ptax`** — `SELECT length(data_hora_cotacao), count(*), min(...) GROUP BY 1`,
> statement **`01f19844-283f-1218-b6e5-73a3b1a3f342`**, `from_cache: None` — and
> **independently re-derived from the LIVE endpoint**, one range request through
> `scripts/probe_ptax.py`'s own `fetch_window`. The two agree row for row, and they share no
> layer: one reads the table this repository wrote, the other reads BCB.
>
> *(An earlier statement id for the same measurement, `01f19839-6c59-1be2-bf65-485856e3bdb8`,
> **resolves** — `GET /api/2.0/sql/statements/…` returns it in state `CLOSED` — so it is a real
> execution and not the "resolves to nothing" species this repository has struck twice. But a
> `CLOSED` statement's rows can no longer be read, so the numbers above are published against
> the id a reader can still fetch. Checking that a published id resolves costs one API call and
> is the cheapest guard this project has.)*
>
> | `length(data_hora_cotacao)` | fractional digits | rows | a real example |
> |---|---|---|---|
> | 22 | **2** | 2 | `2026-06-03 13:06:26.54` |
> | 23 | 3 | 2 | `2026-06-05 13:03:38.306` |
> | 24 | **4** | 1 | `2026-07-23 13:11:15.6614` |
> | 25 | **5** | 2 | `2026-06-09 13:12:05.30888` |
> | 26 | 6 | 35 | `2026-06-19 13:03:25.555497` |
>
> Over the whole 2023-01-01 .. 2026-08-05 walk the counts are 1:11, 2:66, 3:785, 4:1, 5:2,
> 6:38, and the 1984 rows are all width 1. **The conclusion survives and is worth more:** a
> fixed-width slice does not merely break "on the series", it breaks on **7 of this phase's
> own 42 landed quotes**, and the retired enumeration would have licensed a `.SSSSSS` pattern
> that this window itself falsifies. `{1,6}` in the shape regex is `%f`'s range and not a
> tally of observed widths, which is why it was never at risk.
>
> **AND THE RENDERING HAZARD THIS DOCUMENT NAMES TWICE ELSEWHERE IS WHY NOBODY SAW IT.** The
> Task 0 run log printed 2026-06-03's stamp as `13:06:26.540000`, because Python's `%f` pads
> to six — so the width-2 row was read off a rendering that had already normalised the one
> property being enumerated. Same species as `str(5.0773)` dropping the trailing zero (§0.2)
> and `to_date()` rendering an instant in the session zone (§1.3).

So any single pattern rejects real rows — `1984-12-03 11:29:00.0`, `2026-06-03 13:06:26.54`
and `2026-07-23 13:11:15.6614` among them.

> **AND ONE STAMP IN THE TABLE ABOVE THIS PARAGRAPH IS NOT A SERIES ROW.** The retraction's
> own `to_timestamp` table gives `2025-04-23 13:02:31.416` as a real row; **the API returns
> `13:06:30.416`** for that date — the earlier of §0.7's duplicate pair, which §0.7 spells
> correctly. The *measurement* stands (`13:02:31.416` parses to `2025-04-23 10:02:31.416000`,
> which is what the row demonstrates); only its billing as a series row was wrong.
> `tests/bronze/test_ptax_rules.py` carried the same transposition and now carries the real
> stamp.

---

## 4. How this phase ends, against protocol §9's six conditions

**F3's evidence walked these six in public and this document did not**, which left a reader
of `docs/` unable to see the one thing about F-API that most needs seeing: **CI has never run
on this branch.** The walk is the mechanism that makes a phase's own incompleteness legible
from outside `.plans/`, and omitting it is the same failure as F3's empty §0.4 placeholder —
a public reader following a claim to nothing.

| # | condition | status |
|---|---|---|
| 1 | every artefact the phase promised exists, built by its own code | ✅ `bronze_ptax` (42 rows), `bronze_payments` at 40,150, `dim_currency` at 2 members, `fact_payment` rebuilt at 40,000 × 13 — all four jobs SUCCESS at `6cfe0f0` (§2.1) |
| 2 | every prediction marked, the falsified ones kept | ✅ §2 marks every §1 row; **one falsified** (35,095 → 37,136, §2.8) and kept unadjusted, plus six numbers this document made false about itself (§2.11) |
| 3 | **CI green on the MERGED PR** | ❌ **OPEN, and it is not close.** Nothing is pushed, so no PR exists, so **CI has never run on this branch at all** — `.github/workflows/ci.yml` triggers on `push: [main]` and `pull_request:` only. There is no green whole-suite verdict for F-API from any process |
| 4 | `docs/<phase>-run-evidence.md` exists, controller-verified separated from reported | ✅ this file — one document rather than F3's two, and every claim labelled |
| 5 | `.plans/HANDOFF.md` updated, including deleting what the phase made false | ✅ §2.11 is the public half; the handoff carries the rest |
| 6 | what remains unexercised is listed as unexercised | ✅ §3, accumulated as the phase ran rather than reconstructed at its end |

**Condition 3 is the only one open, and the local number that stands in for it is not a
substitute.** `uv run pytest --collect-only -q` selects **2,106 of 2,112 collected, 6
deselected, no collection errors** at this revision. **That is a COLLECTION and not a run.**
The whole suite has never executed on this branch: it does not fit this Windows box in one
command (`tests/gold` alone exceeds the 600 s local tool cap, and two local Spark suites must
never run concurrently — `.plans/HANDOFF.md` measures why), and CI is the only place it runs
in one process. Individual files have been run and pass; **nobody may quote that as "the
suite passes".**

**On review.** The review of record is the **split two-reviewer whole-branch pass**, code and
docs as disjoint packages — the shape F2 established because one reviewer does not read that
volume carefully. **The code reviewer would merge. The docs reviewer would not**, until four
blockers cleared; three were closed by the controller and the fourth (§0.4 unmarked while
§2.8 claimed it corrected) by the consolidation pass that wrote this section. Protocol §A5:
a CodeRabbit `pass` under "Review rate limited" is **absence, not approval**, and would not
have counted here either way.
