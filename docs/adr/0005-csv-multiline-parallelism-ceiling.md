# ADR 0005 — read RFB CSVs with multiLine, accepting a file-level parallelism ceiling

## Status
Accepted

## Context
The F1.3 Estabelecimentos runs promoted silently damaged records into bronze,
and finding out why produced a measurement worth keeping.

RFB ships records with literal newlines **inside quoted fields**. That is valid
CSV (RFC 4180 §2.6), and Python's `csv` module reads the files as exactly
4,753,435 records per part with zero field-count deviations. Spark, whose CSV
reader defaults to `multiLine=false`, splits every such record in two:

- a **parent** carrying the leading fields with the whole tail NULL — measured
  at 18 of 30 columns lost — which satisfies every key DQ rule
  (`cnpj_basico` present and 8 chars, `cnpj_ordem`, `cnpj_dv` present) and was
  therefore **promoted**;
- a **fragment** starting mid-field, whose `cnpj_basico` is garbage
  (`'";"02"'`), which the DQ gate correctly rejected.

Incidence in the real 2026-06 snapshot: 1 record in `Estabelecimentos6`,
3 in `Estabelecimentos8`, of 4,753,435 each. The quarantine counts matched the
scan exactly (1 and 3), and so did the count of promoted rows missing their
trailing fields (1 and 3).

The trap worth naming: because the split adds one row and the gate removes one
row, bronze ended up with exactly 4,753,435 rows per part — the *correct*
count — with a damaged record inside it. No row-count reconciliation could
have caught this.

## Decision
Set `multiLine=true` in the shared `csv_read_options()`, so it applies to the
Auto Loader streams (lookup and estabelecimentos when this was written; empresas
and socios joined them in F1.4b PR A, so it is **four** streams today) and to the
local batch reader
that exists to be their testable twin. Scoping it to one call site was
considered and rejected: the defect is latent in the lookup path by the same
mechanism (only its file sizes and current content make it lucky), and
splitting the options would break the one invariant `reader.py` exists to hold —
that both paths parse the RFB files byte-identically.

## Consequences
- **Correctness first, parallelism second.** With `multiLine=true` Spark cannot
  split a single file across tasks: the unit of parallelism becomes the file,
  not the ~128 MB block. Estabelecimentos ships as **ten parts in total** — nine
  of ~320–370 MB plus part 0 at 2,128,818,559 B — and for the nine this is a
  non-issue: there are enough files to keep the cluster busy, and the lookup
  files were already one task each.
- **Part 0 is the case this ADR exists to flag, and it has now been run.** Part 0
  is 2,128,818,559 B compressed, 6,780,467,695 B of CSV (recorded here as
  "roughly 14 GB" when this ADR was written; re-measured in F1.4 against the
  Volume — the conclusion below is unaffected, because the ~9 min single-task
  read that justifies the ceiling was really measured and only the size
  attributed to that file was wrong), so under `multiLine=true`
  it was read, parsed and written by a **single task**: 29,093,533 rows in a run
  of about nine minutes, taking bronze to 71,874,448 rows. Wall clocks in
  [`docs/f1.3-estabelecimentos-run-evidence.md`](../f1.3-estabelecimentos-run-evidence.md).

  > **Dated, 2026-08-03 (F1.4b PR B Task 7).** Every byte count in this bullet and
  > the one above is **keyed to the 2026-06 object** and moves every month:
  > 2026-07's `Estabelecimentos0.zip` is 2,164,567,397 B, not 2,128,818,559 B, and
  > took 42 upload parts rather than 41 (ADR 0007 carries the same correction).
  > "Ten parts in total" and "~320–370 MB" both held for 2026-07. The 71,874,448
  > figure is the 2026-06 table; `bronze_cnpj_estabelecimentos` now holds
  > **144,193,412** rows over two months, at Delta version 17, 59 files,
  > 6,886,570,119 B. Part 0 has now been read under `multiLine=true` **twice**,
  > the second time in the 2026-07 ingest — but PR B published no per-task
  > timings, so **the ~9 min single-task read is still one observation**, neither
  > reproduced nor challenged, and the ceiling is still uncompared against
  > `multiLine=false`.
  That is one run with nothing isolated — not a benchmark, and no comparison
  against `multiLine=false` exists — so what it settles is that the ceiling is
  livable at this scale, not what it costs. No pre-splitting was needed. Should a
  later stage find it unacceptable, the fix is still upstream of the reader —
  split the giant inner CSV on newline-safe boundaries during the in-Volume unzip
  so the file count, not the reader mode, restores parallelism.
- `multiLine` is not part of Auto Loader's schema state, so flipping it neither
  invalidates the schema location nor triggers reprocessing. Records already
  ingested under `multiLine=false` are **not** repaired by this change: the
  checkpoint has consumed those files, so correcting already-landed rows
  requires a deliberate re-ingestion.
- A regression lock (`test_options_are_cp1252_semicolon_quoted_headerless`)
  now asserts the option, so it cannot be dropped silently.
- The defect *class* is not closed, only its known cause. No rule detects a row
  whose entire trailing tail is NULL, so a future parse break of a different
  shape would again pass fail-closed gating. See ADR 0006.

  > **Narrowed, 2026-08-03 (F1.4b PR B Task 7).** As written this overstates the
  > gap. `caed88e` made `municipio` a required field for estabelecimentos, and
  > `municipio` is ordinal 21 of 30 — while the parent row this ADR describes lost
  > 18 of 30 columns, i.e. everything from ordinal 13 on. **That exact row would
  > be rejected today**, by `null_or_empty_municipio`, and the widening was safe
  > to add live because blanks in `municipio` measured 0 over the table
  > (F1.4b PR A §8). What remains true is the narrower statement: **no
  > *completeness* rule exists**, so a break losing only the last few ordinals
  > (22–30) still passes.
  >
  > **The cross-reference, corrected 2026-08-04 (CodeRabbit on PR #7).** This note
  > used to end "ADR 0006 repeats the original wording; both should be read with
  > this narrowing", and that sentence was true when it was written and false by
  > the time the branch was pushed: the same fix wave added the identical narrowing
  > to [ADR 0006 §Consequences](0006-bronze-dq-gate-policy.md), under its **No
  > completeness rule** bullet, precisely because the pointer between the two was
  > one-directional. Both ADRs now carry it, and a reader arriving at either one
  > alone gets the narrowed statement. The correction is recorded rather than
  > silently deleted because the stale sentence is what a reviewer read, and an ADR
  > that quietly repairs its own claims cannot be checked against the review that
  > found them.
