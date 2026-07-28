# ADR 0006 — bronze DQ gate: keep fail-closed on any reject, add a triage path, defer rate-based gating

## Status
Accepted (with a proposed evolution, deliberately not implemented in F1.3)

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
   which refuses the sentinel its `batch_id` parameter defaults to and any
   `batch_id` matching no staging row. The default itself guards nothing — before
   that check existed, running the job with no `--params` matched no rows,
   appended nothing and exited 0, reporting SUCCESS for a batch it never
   promoted. Both stranded batches above were recovered with it (9,506,870 rows
   each).
3. **Defer rate-based gating to F1.4**, specified here rather than left as a
   vague intention: block promotion when the reject *rate* exceeds a threshold,
   otherwise quarantine and continue. The threshold must be chosen against
   measured history, not guessed — the observed baseline is ~1e-7. This keeps
   the failure modes that matter fail-closed, because none of them produce four
   bad rows: a wrong encoding, a schema shift, a changed delimiter or a
   truncated file all move the rate by orders of magnitude.

## Consequences
- Until F1.4, every run with a reject fails and needs a human to read the
  quarantine and re-promote. That is now a bounded, documented operation rather
  than a dead end, but it is still manual.
- Re-running the triage job is safe, which matters because "I am not sure the
  first invocation took" is the expected operator state: `promote_batch` is
  idempotent per `_batch_id` — it skips the append when bronze already holds that
  batch — so a second invocation cannot double-count the batch.
- Rate-based gating trades a hard stop for a monitoring obligation: rejects
  would accumulate silently unless someone watches them. The alert must be on
  the **trend** of the quarantine, not on the presence of rows in it — otherwise
  the threshold just relocates the noise.
- Two known gaps stay open and are carried into F1.4, both of them cases where
  bronze accepts damage without a signal:
  - **No completeness rule.** A row whose entire trailing tail is NULL passes
    all key rules. ADR 0005's fix removes the known cause of that shape, not the
    class. A field-count or trailing-NULL check would have made the original
    incident fail-closed instead of silent.
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
