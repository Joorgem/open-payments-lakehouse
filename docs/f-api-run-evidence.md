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
- **Every PTAX DQ rule except `bad_quote_date_shape` is a near-tautology against the live
  source.** The landed record is built by this repository from an already-validated
  response, so the gate re-asserts at the table what the extraction refused at the row.
  Each rule's docstring says so. **A green gate here is not evidence about BCB** — it is
  evidence that our own fetch did not drift, which is a smaller claim wearing the same
  colour.

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
carried by this lakehouse at all. The rule is tightened, and the test asserts the **value**
rather than only the verdict.

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
