# ADR 0006 — bronze DQ gate: keep fail-closed on any reject, add a triage path, and — measured — keep it

## Status
Accepted. Decision 3 (rate-based gating), deferred when this ADR was written and
left unplaced in F1.4a, was **resolved on 2026-08-03 against adopting a
threshold** once two months × three tables existed to decide from. See
[Decision 3, resolved](#decision-3-resolved-2026-08-03-f14b-pr-b--no-rate-threshold)
below; that section, not Decision 3's own text, is the operative policy.

## Context
F1.3 replaced F1.2's whole-table gate with a **batch-scoped** one: the gate and
the promote both filter on `_batch_id == run_id`, so a bad batch no longer
wedges every later clean batch. Running it against the real 2026-06
Estabelecimentos snapshot exposed what that still does not solve.

Measured on real data across four batches. The staging counts below total
42,780,919; the 42,780,915 rows promoted differ by exactly the four rejects:

| batch | staging rows | rejected | reason |
|---|---|---|---|
| parts 1–2 | 9,506,870 | 0 | — |
| parts 3–5 | 14,260,305 | 0 | — |
| parts 6–7 | 9,506,871 | 1 | `bad_cnpj_basico_length` |
| parts 8–9 | 9,506,873 | 3 | `bad_cnpj_basico_length` |

A reject rate of 4 in 42.8M — about 1 in 10.7 million — recurring in half the
batches. Two consequences followed:

1. **Good rows had nowhere to go.** With zero tolerance, one rejected row
   blocked promotion of 9,506,870 good ones; and because a later run ingests new
   files under a new `_batch_id`, the blocked batch is never revisited. The rows
   sat in staging, unreachable by any automated path.
2. **Zero tolerance means a human in every run.** At this reject rate, a
   scheduled bronze ingestion would stop and wait for triage on most executions.
   A pipeline that requires manual intervention on most runs is not automated;
   it is manual work wearing a scheduler.

The rejects themselves were not noise. They were fragments of records split by
the CSV reader (ADR 0005) — the gate was catching the *shadow* of a parse
defect, while the actually-corrupted parent row passed every rule and was
promoted. That reframes what the gate is for: it caught a real problem, but not
the row that was really wrong.

## Decision
Three parts, deliberately separated by risk:

1. **Keep fail-closed on any reject for now.** Changing the gate's semantics at
   the close of a phase, without time to validate the new behavior against real
   data, would trade a known and documented problem for an unvalidated one.
2. **Ship the missing triage path**: `repromote_triaged_batch`, a separate
   operator job that runs the promote step alone against an explicitly named
   `batch_id`, for use *after* a human has read the quarantine and accepted the
   rejects. It is not a bypass — it reuses `promote_batch`, which re-applies the
   same rules and appends only passing rows, so rejects stay out of bronze and
   stay in quarantine. Its two safety properties sit in two different places:
   *isolation* comes from living outside the ingestion flow, which is what keeps
   anything automated from reaching it; *validation* comes from `promote_batch`,
   which refuses the sentinel its `batch_id` parameter defaults to, and any
   `batch_id` that names nothing in either staging or bronze. (The ingestion
   flow's own promote passes an `--in-flow` token declaring that its `batch_id`
   is the run executing it, which is what lets a scheduled run with no new files
   succeed as a no-op; the operator job omits it, so a mistyped id there is
   still refused.) The default itself guards nothing — before
   that check existed, running the job with no `--params` matched no rows,
   appended nothing and exited 0, reporting SUCCESS for a batch it never
   promoted. Both stranded batches above were recovered with it (9,506,870 rows
   each).
3. **Defer rate-based gating — to no phase yet** (this said "to F1.4" when the
   ADR was accepted; corrected in F1.4a, see below), specified here rather than left as a
   vague intention: block promotion when the reject *rate* exceeds a threshold,
   otherwise quarantine and continue. The threshold must be chosen against
   measured history, not guessed — the observed baseline is ~1e-7. [**That number
   is wrong, and the withdrawal below is partly why**: the six cells measured in
   F1.4b PR B put socios at 6.4e-5–6.5e-5, roughly 640× higher, and empresas at
   1.4e-8. There is no single baseline to threshold against — the spread is per
   table. Flagged inside the withdrawn specification rather than corrected, because
   what is on the page has to be the reasoning the resolution actually answered.]
   This keeps
   the failure modes that matter fail-closed, because none of them produce four
   bad rows: a wrong encoding, a schema shift, a changed delimiter or a
   truncated file all move the rate by orders of magnitude.

   **Which phase, corrected in F1.4a.** This item named F1.4, and F1.4 has since
   been scoped without it: the F1.4a design excludes it by name as carry-forward
   #3 (§4.8, "does not resolve the carry-forwards that are not this phase's"), and
   the F1.4b outline (§5) does not pick it up either. So the specification above
   stands and the schedule attached to it does not — it is **unplaced**, and will
   stay unplaced until a phase claims it. Corrected here rather than left standing
   because a deferral that names a phase reads as planned work to anyone who does
   not go and check whether that phase took it, which is the same failure mode as
   a docstring that promises a guard nobody wrote.

   **Resolved in F1.4b PR B, 2026-08-03 — and resolved AGAINST.** F1.4b PR B
   claimed it, supplied the measured history this item demanded, and the answer
   is no threshold. The specification above is therefore **withdrawn as an
   intention**, not carried further: it is kept in place because the reasoning
   that produced it is what the resolution had to answer, and a decision record
   that quietly deletes the option it rejected cannot be checked. Read
   [Decision 3, resolved](#decision-3-resolved-2026-08-03-f14b-pr-b--no-rate-threshold)
   for the numbers, the options rejected, and what would reverse it.

## Consequences
- Every run with a reject fails and needs a human to read the quarantine and
  re-promote. That is a bounded, documented operation rather than a dead end,
  but it is manual — and as of the resolution below it is **permanent** rather
  than pending, so it is a cost this ADR accepts and not one it is waiting out.
  Measured shape of that cost: two repromotes per month (empresas and socios
  both reject persistent source dirt every month), one CLI command each.
- Re-running the triage job is safe, which matters because "I am not sure the
  first invocation took" is the expected operator state: `promote_batch` decides
  from row COUNTS, not from "any row present". It skips the append when bronze
  holds *all* of the batch's promotable rows, recognises an already-promoted batch
  whose staging rows are gone, and **refuses on a partial**, printing both counts
  — because bronze is append-only with no per-row identity, so "append the
  missing ones" is indistinguishable from "append everything twice". The gate's
  quarantine append follows the same rule, so a repaired gate task cannot double
  the rejects a human is triaging.
- Rate-based gating trades a hard stop for a monitoring obligation: rejects
  would accumulate silently unless someone watches them. The alert must be on
  the **trend** of the quarantine, not on the presence of rows in it — otherwise
  the threshold just relocates the noise.

  > **Stale in tone, 2026-08-03.** This is written as a live design consideration
  > for something now **decided against** (Decision 3, resolved below). It is
  > conditional, so it is not false — the obligation it describes is exactly what
  > adopting a threshold would still cost — but it is no longer a trade-off this
  > repository is weighing. Read it as part of the record of why the answer was no,
  > not as pending design work.
- Two known gaps stay open and are carried into F1.4b — unlike Decision 3 these
  two are placed: the F1.4a design (§4.8) moves carry-forwards #4 and #5 to F1.4b,
  where new rule sets for `empresas` and `socios` are being authored anyway and the
  checks are cheap. Both are cases where bronze accepts damage without a signal:

  > **Status as of 2026-08-03 (F1.4b PR B, the phase this list was carried into).**
  > "Two known gaps stay open" is no longer accurate and neither bullet below should
  > be read in the present tense. The encoding gap is **closed** on the half that
  > was a coverage claim and open only on the metric half; the completeness gap is
  > **narrower** than its wording says. Each bullet carries its own annotation
  > below, and this line is kept rather than rewritten because F1.4b is what
  > discharged it and a consequences list that silently updates itself cannot be
  > checked against the phase that acted on it.
  - **No completeness rule.** A row whose entire trailing tail is NULL passes
    all key rules. ADR 0005's fix removes the known cause of that shape, not the
    class. A field-count or trailing-NULL check would have made the original
    incident fail-closed instead of silent.

    > **Narrowed, 2026-08-03 (F1.4b PR B Task 7).** The wording above overstates
    > the gap, and the narrowing is recorded here as well as in ADR 0005 because
    > the pointer between the two was one-directional: a reader who opens this ADR
    > alone got the overstated version with nothing telling them so. `caed88e`
    > made `municipio` a required field for estabelecimentos, and `municipio` is
    > ordinal 21 of 30 — while the parent row ADR 0005 describes lost 18 of 30,
    > i.e. everything from ordinal 13 on. **That exact row would be rejected
    > today**, by `null_or_empty_municipio`. What remains true is the narrower
    > statement: **no *completeness* rule exists**, so a break losing only the
    > last few ordinals (22–30) still passes. See
    > [ADR 0005](0005-csv-multiline-parallelism-ceiling.md) for the measurement
    > that made the widening safe to add live.
  - **The encoding check covers 2 of 30 columns.** `rules_for("estabelecimentos")`
    looks for the Unicode replacement character only in `nome_fantasia` and
    `logradouro`. One record in `Estabelecimentos8` carries a byte (`0x8f`) that
    windows-1252 cannot decode at all — Python raises on it, while Java's
    decoder substitutes U+FFFD **silently**, which makes that character the only
    in-band evidence that a byte was lost. Which column holds it is not yet
    known, so whether the current rule catches this specific record is
    undetermined. The fix is to fold the check over every string column of the
    contract rather than a hand-picked pair, and to count occurrences per column
    as a metric instead of only as a reject reason. Repairing the byte is
    explicitly rejected: `0x8f` is undefined in windows-1252, so the source has
    non-cp1252 contamination and guessing a codepage per record would be
    inventing data.

    > **Closed on the coverage half by `caed88e`, 2026-07-31** (recorded here
    > 2026-08-03). The headline above is false in the present tense and has been
    > since before this branch. `opl.bronze.rules._encoding_check` now folds the
    > U+FFFD test over **every column of the contract**, derived from `TABLES` so a
    > contract gaining a column gains the check with it, and it is wired for all
    > four contracts — not for `estabelecimentos` alone. The "2 of 30" figure and
    > the `nome_fantasia`/`logradouro` pair describe the rule as it stood **up to
    > `caed88e`**, which is exactly why they must stay on the page: the 2026-06
    > estabelecimentos run measured 0 rejects under that rule and the 2026-07 run
    > measured 4 under this one, and the difference is the rule, not the data. That
    > is the fact Decision 3's `†` footnote, and §20.3, §23.1 and §25.2 of
    > `docs/f1.4b-pr-b-run-evidence.md`, all rest on.
    >
    > **What is still open is the metric half only.** This bullet states the fix as
    > two things — fold the check over every column, *and* count occurrences per
    > column as a metric rather than only as a reject reason. The first is done; the
    > second is not, and nothing counts U+FFFD per column today. §25.6 of the run
    > evidence already calls this gap "half closed"; that is the accurate reading.

    **Answered 2026-08-03.** `correio_eletronico`. Four estabelecimentos rows
    in the 2026-07 ingest were rejected for `encoding_replacement_char`, all
    four in that column, and in each the field's entire content is the
    undecodable byte(s) — no truncated address, no real email lost. All four
    also have `_rescued_data` NULL, which is the doctrine consequence this
    observation adds: `_rescued_data` NULL tests CSV field-splitting, not
    character decoding, so for this reject reason it cannot tell source dirt
    from a lossy parse on its own — the reject *reason* has to be read before
    `_rescued_data` is trusted to classify a row. Full mechanism, the per-row
    detail and the cross-month consequence are in
    `docs/f1.4b-pr-b-run-evidence.md` §20.2 and §20.3. This does not by itself
    resolve the rate-gating question in Decision 3 — that is resolved
    separately, below, and this answer is one of its inputs.

---

## Decision 3, resolved 2026-08-03 (F1.4b PR B) — no rate threshold

Decision 3 deferred rate-based gating on the explicit ground that "the threshold
must be chosen against measured history, not guessed", and F1.4a corrected the
schedule attached to it without touching the specification. F1.4b PR B supplied
the history. **The answer is no threshold: any reject continues to stop the
batch.**

This is a decision, not a renewed deferral, and the difference is that the
numbers are on this page and the conditions that would reverse it are stated so
they can be checked. A deferral renewed without saying what the measurement
showed is indistinguishable from one nobody looked at.

### The six cells

Two months × three tables. Every cell reconciles exactly (staging = bronze +
quarantine) in `docs/f1.4b-pr-b-run-evidence.md` §17, §18, §20 and §21.3.
**Re-queried against the SQL warehouse on 2026-08-03 for this ADR**, separately
from that document, and every figure below agreed with it:

| table | month | rejects | staged | rate | reason |
|---|---|---|---|---|---|
| empresas | 2026-06 | 1 | 68,629,148 | 1.5e-8 | `null_or_empty_razao_social` |
| empresas | 2026-07 | 1 | 69,062,850 | 1.4e-8 | `null_or_empty_razao_social` |
| socios | 2026-06 | 1,797 | 27,838,448 | 6.5e-5 | `null_or_empty_nome_socio_razao_social` |
| socios | 2026-07 | 1,786 | 27,992,378 | 6.4e-5 | `null_or_empty_nome_socio_razao_social` |
| estabelecimentos | 2026-06 | 0 | 71,874,448 | 0 † | — |
| estabelecimentos | 2026-07 | 4 | 72,318,968 | 5.5e-8 | `encoding_replacement_char` |

† **Not a measurement of the month.** The encoding check covered 2 of 30 columns
until `caed88e` (2026-07-31), which is *after* the 2026-06 run. The re-query
counted U+FFFD across every contract column of both staging months directly,
independent of what either run's gate recorded: **4 rows in 2026-06 and 4 in
2026-07**. The 2026-06 four are the same four records (§20.3), sitting in
2026-06's bronze, promoted un-flagged. So 2026-06's zero measures the rule that
was deployed, not the data that arrived, and the two estab cells are not
comparable with each other.

That fact is load-bearing for everything below, so it is stated once here rather
than as a footnote: **a reject rate in this repository is a property of a
(month, rule set) pair, never of a month alone.**

### Two reject families, and why they differ in kind

- **`null_or_empty_*` is source dirt.** The parse is correct; the source field is
  empty. empresas' single reject is `cnpj_basico` **08314885 in both months** —
  one company the RFB has not fixed between snapshots. socios moved 1,797 →
  1,786, stable to two significant figures. Nothing was lost in transit; the RFB
  shipped a blank and bronze declined to store it.
- **`encoding_replacement_char` is data loss.** cp1252 leaves five byte values
  undefined (`0x81 0x8D 0x8F 0x90 0x9D`). Java's decoder substitutes U+FFFD for
  them **silently**, where Python raises, and the source byte is gone. The parse
  "succeeded" — `_rescued_data` is NULL for all four rows — while destroying
  content. The column is `correio_eletronico` (answered above).

One is a fact about the world that no engineering removes. The other is a defect
in how bytes were read, and tolerating it at *any* rate is accepting silent
corruption. The asymmetry is real, and it is what a per-reason threshold would
have exploited. It turns out not to be sufficient — see below.

### Rejected: a single rate threshold with an absolute floor

The **same reason family** measures 6.4e-5 on socios and 1.4e-8 on empresas — a
factor of about **4,400**, three and a half orders of magnitude. A single number
across tables is therefore inert on one end or useless on the other. Set at
socios' 6.4e-5 so socios stops firing, it tolerates `6.4e-5 × 69,062,850 ≈
4,400` rejects on an empresas batch — 4,400× that table's entire observed defect
load of one row, and enough headroom for a real parse defect to walk through
green. Set anywhere below that, socios turns red every month, which is the state
being fixed. (The 4,400 is the rate ratio itself, not a second coincidence.)
Set at empresas' 1.4e-8, socios' floor is 0.4 rows and the threshold is absolute
gating with extra arithmetic. `docs/f1.4b-pr-a-run-evidence.md:344-345` had
already set the bar — "per table, and more than one month per table" — and this
spread is the reason it set it.

### Rejected: a per-table rate threshold

Per-table clears that bar arithmetically. It does not clear it statistically.
Of the three tables, exactly **one** has a rate worth calibrating against:

- **empresas** — numerator 1 in both months, and the *same record* both times. A
  rate whose numerator is one carries ~100% Poisson uncertainty; this one is not
  a sample at all, it is a single persistent row observed twice. Its "rate"
  moved from 1.5e-8 to 1.4e-8 because the *denominator* grew.
- **estabelecimentos** — numerator 0 for the tolerable family in both months.
  A threshold cannot be calibrated from an absence.
- **socios** — 1,797 and 1,786, agreeing to 1.2%. Genuinely usable. One table of
  three.

`rules.py` already refuses to declare `empresas.capital_social` a required field
because it is "plausibly always filled" and "'plausibly' is not the standard a
hard gate is held to." A threshold derived from n=1 and n=0 is that same
standard, applied to the gate's release valve instead of to one of its rules —
and the release valve is the side where being wrong fails open.

### Rejected: a per-reason threshold — and this one fails structurally

Tolerating `null_or_empty_*` by rate while never tolerating
`encoding_replacement_char` and `rescued_data_present` was the recommended
option. It fails on a property of the gate rather than on the evidence:

**`_dq_reject_reason` names the first rule that fired, not everything wrong with
the row.** `dq._reject_reason` builds a first-match-wins `when` chain, and
`rules_for` ranks every `null_or_empty_*` rule *above* `encoding_replacement_char`
in all four contracts. A row that is both blank in a required column and carries
a lost byte elsewhere reports the **tolerated** reason. A "never tolerated"
reason that a co-occurring tolerated reason can hide is not a never.

This is not an accident of the current ordering that could be swapped away
cheaply — the ordering is deliberate and documented (`rules.py`'s comment block
records that grouping the required-field rules already changed which reason a
doubly-defective estab row reports, and that a row should be "judged by what is
wrong with IT" before where it came from). Reordering to protect the never-list
would trade one first-match artefact for another.

**Measured, so this is not an argument from possibility.** Across all six cells
the overlap is **zero** — no row in any month is both required-blank and
U+FFFD-carrying. Queried 2026-08-03 over both staging months of all three
tables (`fffd_any` / `req_blank` / `both`): estabelecimentos 4/0/0 and 4/0/0,
empresas 0/1/0 twice, socios 0/1797/0 and 0/1786/0.

So the hole is latent, not open. What disqualifies the option anyway is that
**adopting it is what would stop anyone measuring it.** Once `null_or_empty_*`
no longer fires the gate, that overlap count is a number nobody has a reason to
run, and the first month it goes non-zero is the month the never-tolerated check
silently stops applying — with no run turning red to say so. Pinned as a live
property by `tests/bronze/test_rules.py::test_a_blank_required_column_hides_the_lost_byte_behind_it`,
which asserts the shadowing exists *today*: whoever removes it must change that
test, which is the point of writing it.

The precondition for this option is therefore structural rather than
evidentiary: per-reason gating needs per-reason counts derived from **all**
matching rules, not from the first. That is a change to what `evaluate` reports
(or a second aggregate beside it), and it must ship *before* any per-reason
tolerance, never with it.

### Accepted: absolute gating stays — with its cost stated

Empresas and socios fire the gate **every month, indefinitely**, because both
reject persistent source dirt. That is the price, and it is now measured rather
than projected: **two repromotes per month**, one CLI command each.

That is materially smaller than the cost this ADR booked in F1.3. The original
"a human in every run" figure was measured on a batch-per-part flow where a
single estabelecimentos ingest was four batches; it is now one batch per table
per month. The cost did not go away, but the arithmetic behind the original
complaint no longer holds, and the decision should not inherit it uncorrected.

Two things bought with that price, both measured rather than asserted, because
"a human looks" is only worth something if looking has produced anything:

- The **triage-doctrine correction** (§20.2) — `_rescued_data` NULL does not
  mean source dirt — exists because four rows stopped a batch and someone had to
  read them. Applying the standing instruction literally would have repromoted
  four rows with lost bytes into bronze.
- The **cross-month asymmetry and its F2 modelling hazard** (§20.3) — a
  satellite cannot distinguish "unchanged" from "not observed" — was found in
  the same sitting, from the same four rows.

Both of PR B's most consequential findings came from the gate firing on a batch
whose reject rate (5.5e-8) is below any threshold this ADR would plausibly have
set. That is not decisive on its own; a control cannot be justified solely by
what it has incidentally surfaced. It is recorded because the *rate* of those
four rows is exactly the argument a threshold would have made for waving them
through.

### The defect this decision must not leave standing: reclaim is unreachable

`reclaim_landing` has **never deleted a byte through the wired path** — eight
task instances hung off the ingestion jobs, two executions, zero bytes
(`docs/f1.4b-pr-b-run-evidence.md` §16). It has deleted bytes exactly once, and
not through that path: F1.4a invoked it through a temporary job resource on
2026-07-31 and it reclaimed **16,743,815,717 B** correctly
(`docs/f1.4a-migration-evidence.md:467-485`). An earlier revision of this
section said "never deleted a byte in this project's history", full stop; that
was false and is corrected here — see `docs/f1.4b-pr-b-run-evidence.md` §26.1.
The correction does not weaken the argument below and sharpens it: **the defect
is wiring, and `retention.py` is not merely believed correct but measured
correct on a real Volume, on this exact table.**

1. `reclaim_landing` depends on `promote`; `promote` depends on
   `check_bad_rows outcome: "true"`. **A batch with one reject never reaches it.**
2. `repromote_triaged_batch` — the job that actually promotes a gated batch — is
   `assert_deployed_revision` → `promote`, with **no reclaim task at all**
   (confirmed in `databricks/resources/repromote_batch_job.yml`).
3. A later clean run cannot compensate: its own batch ingested nothing, because
   the checkpoint already consumed the files, so its reclaim correctly refuses.

So a gate that always fires has silently disabled a *different* control for the
life of the project. Residue today: 8.21 GB of 2026-06 CSVs, deliberately left
in the Volume so the debt stays visible. Projected floor: ~48 GB peak per month,
2.18× this Volume's demonstrated high, against no published quota.

**A threshold is not the fix for this, and adopting one to fix it would be the
worse outcome.** A threshold only lowers how often step 1 diverts the flow. It
leaves steps 1 and 2 exactly as they are — so every batch that *does* exceed the
threshold still strands, and those are precisely the batches whose landing files
sit longest, because a human is sitting on them. That is a control that works
until it matters, which is worse than one visibly broken.

**The fix is to give the triage path the reclaim it lacks.** `reclaim_landing`
is already guarded independently of the gate: it deletes only the inner files
bronze proves it holds rows of, for that batch, and refuses with a message
naming the three reasons it might not. Its real precondition is "these rows are
persisted", and `repromote_triaged_batch` satisfies that at exactly the point
the in-flow promote does. Wiring it there restores the control on both paths
without loosening the gate on either.

**Carried past F1.4b PR B as its own change**, not done here: it alters the
storage projection for every month added and belongs in a change that says so.
`retention.py` deleting correctly once reached no longer needs proving — F1.4a
already ran it decoupled from the gate, through a temporary job resource against
an already-promoted batch, and it deleted 16.74 GB with zero refusals and zero
failures. **What remains unproven is only that the new wiring reaches it.**

### What would reverse this decision

Not "more months". Three conditions, all three required, each independently
checkable:

1. **Per-reason counts that are not first-match-wins**, so a tolerated reason
   cannot shadow a never-tolerated one. Structural; no quantity of months
   substitutes for it.
2. **A non-degenerate numerator per table** for the family being tolerated: at
   least six monthly observations per table with a reject count ≥ 10, so the
   Poisson relative error falls under ~30%. empresas (numerator 1) and
   estabelecimentos (numerator 0 for that family) satisfy neither today, and
   more months of "1" and "0" will not change that. What would change it is the
   source getting dirtier — which is the event the gate exists to catch, so the
   condition is self-consistent rather than circular.
3. **The reclaim decoupling shipped first**, so that a threshold, if it is ever
   adopted, is adopted for what it does and not as a workaround for a wiring
   defect.

### The warning for whoever applies this to a month nobody here has seen

**Every rate on this page is a rate of the rule set deployed when it was
measured.** The estabelecimentos pair proves the hazard rather than raising it:
2026-06 measured 0 and 2026-07 measured 4 for the *same four records*, because
`caed88e` widened the encoding check between the runs. Nothing about the data
changed.

So: a threshold carried forward past a change to `rules_for` is calibrated
against a gate that no longer exists, and its failure mode is fail-open. **Any
number derived from this section must be re-derived after every change to
`rules_for`, or discarded.** The same applies in the other direction to this
decision itself — if a future rule set produces reject volumes an order of
magnitude above these, "keep absolute gating" stops meaning "two repromotes a
month" and the cost side of this trade has to be re-argued, not assumed.

One consequence already open, recorded here because it follows directly and this
decision does not settle it: 2026-06's bronze holds four rows the current rules
would reject. **Whether the system of record is re-gated when a rule widens is a
policy question F1.4b PR B surfaced and did not answer** — absolute gating is
what makes it visible (the batch stopped, a human read it), and it does not by
itself say what to do about rows already promoted under a narrower gate.

### Where a threshold would live, if a later phase adopts one

Two corrections to the map, recorded so a future implementer inherits the code
rather than the plan:

1. **The gate is not in `rules.py`.** It is a `condition_task` in each job YAML
   (`bronze_empresas_job.yml`: `EQUAL_TO` over
   `{{tasks.dq_gate_batch.values.bad_row_count}}` against `"0"`), fed by
   `dq_gate_batch.py`'s `_publish`. `rules.py` holds only row-level predicates,
   and a rate is an aggregate that cannot be expressed there. A `condition_task`
   does string comparison with **no arithmetic**, so the rate would have to be
   computed in `databricks/src/dq_gate_batch.py` — which already holds both
   counts from `promote.tally` — and published as a second task value for the
   condition to read.
2. **`rescued_data_present` is not in `rules.py` either.** It is a literal in
   `src/opl/bronze/dq.py`, applied above every per-table rule, and `rules.py`'s
   own docstring says so. The "never tolerated" half of the per-reason option
   could not have been implemented where the plan located it.
