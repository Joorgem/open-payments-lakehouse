# scripts/backfill_snapshot_columns.py
"""One-off: add the two snapshot columns to an existing table and fill them.

Run ON Databricks (it needs the Delta table), once per table that predates the
columns. A third argument names WHICH of a contract's three tables to write --
`--bronze` (the default), `--staging` or `--quarantine`; the two-argument form is
what F1.4a ran against `bronze_cnpj_estabelecimentos` and still means `--bronze`.
The lookup tables acquire the columns through their controlled reload, which is
what `scripts/migrate_lookups_to_subdir.py` sets up.

THREE TABLES AND NOT ONE, because the gap is per TABLE and not per contract. F1.4a
added the columns to `bronze_cnpj_estabelecimentos` alone, and F1.4b measured what
that left behind: `bronze_cnpj_estab_staging` (35 columns) and
`bronze_cnpj_estab_quarantine` (36) carry neither. Nothing in this repo writes with
`mergeSchema` -- deliberately; a schema is a contract, and a write that widens it
without being asked is a drift you find out about later -- so the next estab ingest
sends a 37-column stream into the 35-column staging table and fails at the Delta
write, and the DQ gate then appends a 38-column reject frame into the 36-column
quarantine and fails there too. The second failure is the worse one: it needs a
dirty row to appear, so it can stay latent for months after the expensive ingest
that unblocked it.

A QUARANTINE THAT HOLDS ROWS IS REFUSED -- see `refuse_non_empty_quarantine`.
Adding the columns to an empty one is a schema migration; filling them on rows
already there stamps a month onto somebody else's rejects that nothing here proves
they belong to.

REVERSIBILITY: the Delta version before the write is printed BEFORE anything is
written, so `RESTORE TABLE <t> TO VERSION AS OF <n>` is available if the
verification below fails. Print first, write second: a version number printed
after a failed write is a version number nobody has. The version is taken before
the `ALTER TABLE ADD COLUMNS` too, so restoring to it undoes the columns as well
as their values -- the column addition is itself a Delta commit.

NOT FOR ADDING A NEW MONTH, and it enforces that rather than asking. A table
already stamped with a different month is refused BEFORE the write, because the
write is what destroys the evidence: restamping 2026-06 rows as 2026-07 sends
every `_snapshot_ref_date` to NULL and then passes every post-write check. A new
month's rows are stamped at ingestion by `add_audit_columns`; run the job.

REFUSES TO CLAIM SUCCESS IT HAS NOT VERIFIED. The write is an overwrite of a
71.9M-row table, and the three ways it can go wrong that still return without
error are all checked afterwards, with the numbers printed either way: the row
count changing, a NULL landing in `_snapshot_month`, and a pre-existing column
disappearing. Any of them raises, and the message carries the RESTORE statement
plus the one instruction that failure needs: restore before re-running, because a
second run baselines itself on whatever this one left behind.

RAISES rather than exiting. `databricks/src/fail_on_dq.py` records the reason:
an exception "carries the reason into the run" output, where a SystemExit's
message can be reduced to an exit code by the task wrapper. This script's whole
value is the numbers in its failure message.

usage: backfill_snapshot_columns.py <table> <month> [--bronze|--staging|--quarantine]
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.registry import BronzeTable, table_spec
from opl.bronze.snapshot import (
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    ref_date_column,
)
from opl.config import DEFAULT, require_month

USAGE = (
    "usage: backfill_snapshot_columns.py <table> <month> [--bronze|--staging|--quarantine]"
    "   (e.g. estabelecimentos 2026-06 --staging; the role defaults to --bronze)"
)

# The optional third argument, and the `BronzeTable` field each flag selects. The
# VALUES are field names, resolved by `getattr` in `resolve_target`, so the flag and
# the field are ONE spelling instead of two. Written this way rather than as an
# if/elif chain because such a chain needs a final branch for the value that matched
# nothing, and a fall-through in a target resolver is how a run overwrites a table
# nobody asked for -- membership is checked here, and an unmatched flag never reaches
# the lookup.
_ROLE_FLAGS: dict[str, str] = {
    "--bronze": "bronze",
    "--staging": "staging",
    "--quarantine": "quarantine",
}

# Omitting the flag means bronze. NOT a stylistic default: the two-argument form is
# what F1.4a ran against a 71.9M-row table and what `docs/f1.4a-migration-evidence.md`
# tells an operator to re-run, so it has to keep meaning exactly what it meant.
_DEFAULT_ROLE_FLAG = "--bronze"

# The two columns and their Delta types, in the order `ALTER TABLE ADD COLUMNS`
# takes them. The NAMES come from `opl.bronze.snapshot`, which is where the
# stream that writes them for every new batch gets them: a literal here would be
# a second spelling, and a backfill that filled a differently-named column would
# leave the real one NULL on 71.9M rows while reporting success.
_NEW_COLUMNS: tuple[tuple[str, str], ...] = (
    (SNAPSHOT_MONTH_COLUMN, "STRING"),
    (SNAPSHOT_REF_DATE_COLUMN, "DATE"),
)


@dataclass(frozen=True)
class BackfillCheck:
    """What the table looks like after the write. Frozen: it is evidence, not state.

    Every field comes from ONE aggregate pass (see `_check`), so the numbers are
    mutually consistent by construction rather than by five scans of a table
    happening to agree."""

    rows: int
    null_month: int
    null_ref_date: int
    months: tuple[str, ...]
    ref_dates: tuple[str, ...]
    # Columns that were present before the write and are not present after it.
    # Empty is the only acceptable value; see `_verify_or_raise`.
    lost_columns: tuple[str, ...]


def resolve_target(args: list[str]) -> tuple[str, str]:
    """(qualified table, month) for `args`, or refuse. NO SESSION, on purpose.

    OWNS ARITY, because nothing else does any more: `main` checked `len(args) != 2`
    and this file parsed no flags at all. Pulled out of `main` so that every way the
    third argument can resolve to the WRONG TABLE is checkable without a session --
    and the wrong table here is not a diagnostic inconvenience, it is an overwrite of
    something nobody named. Arity, table, month and role are all refused before Spark
    for the same reason the rest of this repo does it: nothing about a mistyped
    argument needs a serverless start to be told about.

    TABLE AND MONTH TOGETHER, and nothing else in the return. The two-argument form's
    meaning is what this change most endangers -- it is already deployed, already run
    against 71.9M rows, and already written into `docs/f1.4a-migration-evidence.md` as
    the thing to re-run -- so the pair it produces is pinned literally by test, with no
    third element a later reader could add to and quietly redefine. `main` asks the
    registry for the spec itself.
    """
    if not 2 <= len(args) <= 3:
        raise ValueError(USAGE)
    spec = table_spec(args[0])
    # `require_month` also refuses ABSENCE, which matters more here than anywhere:
    # this month is written into every row of a 71.9M-row table, so a guessed one is
    # a wrong answer in the data rather than a missing one, and the config's pinned
    # month equals the job YAMLs' own default so the substitution would never look
    # wrong. It is also what catches `<table> --staging` -- two arguments, so arity
    # cannot see it -- rather than letting a flag be read as a month.
    month = require_month(args[1], action=f"backfill the snapshot columns of {args[0]}")
    flag = args[2] if len(args) == 3 else _DEFAULT_ROLE_FLAG
    if flag not in _ROLE_FLAGS:
        raise ValueError(
            f"{flag!r} is not a backfill target. The third argument names WHICH of "
            f"{args[0]}'s three tables this run writes -- one of "
            f"{', '.join(sorted(_ROLE_FLAGS))} -- and omitting it means "
            f"{_DEFAULT_ROLE_FLAG}, the two-argument form F1.4a ran. NOTHING WAS "
            "WRITTEN and no session was started. It is refused rather than ignored "
            "because ignoring it would silently write the BRONZE table while the "
            "operator watched for the one they asked for, and this script's write is "
            "an overwrite -- the wrong target is not a no-op. " + USAGE
        )
    return DEFAULT.table(getattr(spec, _ROLE_FLAGS[flag])), month


def latest_version(spark: SparkSession, tbl: str) -> int:
    """The table's current Delta version -- the RESTORE target.

    `max(version)` and not `DESCRIBE HISTORY ... LIMIT 1`: a LIMIT with no ORDER
    BY returns whichever row the plan emits first, so the number an operator
    would undo the write with would depend on Delta's history ordering staying an
    implementation detail it does not promise. Of everything this script prints,
    this is the one value that must not be approximately right.
    """
    history = spark.sql(f"DESCRIBE HISTORY {tbl}")
    version = history.agg(F.max("version")).collect()[0][0]
    if version is None:
        raise RuntimeError(
            f"{tbl} has no Delta history, so there is no version to RESTORE to and "
            "this backfill would not be reversible. Nothing was written."
        )
    return int(version)


def missing_columns(existing: frozenset[str]) -> tuple[tuple[str, str], ...]:
    """The snapshot columns `existing` does not already have.

    Per-column rather than "does the month column exist?": a run interrupted
    between the ALTER and the write leaves the table with both columns and no
    values, but a future third column added to `_NEW_COLUMNS` would leave the
    table with two of three, and `ADD COLUMNS` errors on a column that is already
    there. Checking each one makes a re-run idempotent whatever it interrupted.
    """
    return tuple((name, sql_type) for name, sql_type in _NEW_COLUMNS if name not in existing)


def pre_write_scan(before: DataFrame) -> tuple[int, tuple[str, ...]]:
    """(row count, distinct non-NULL `_snapshot_month` values), in ONE pass.

    BEFORE the write, and that ordering is the whole point rather than an
    optimisation: the write overwrites `_snapshot_month`, so a month already in
    the table is evidence that stops existing the moment it runs. `_check`'s
    post-write `collect_set` cannot see it -- after a restamp it reports exactly
    the month that was passed in, which is how a contradicting month satisfied
    every post-write invariant and exited 0.

    One pass, so the row count the verification compares against and the months
    it is refused on come from the same scan of the same 71.9M rows.
    """
    aggregates = [F.count(F.lit(1)).alias("rows")]
    # Conditional because the column is exactly what may not exist yet -- that is
    # the case this script was written for.
    has_month = SNAPSHOT_MONTH_COLUMN in before.columns
    if has_month:
        aggregates.append(F.collect_set(SNAPSHOT_MONTH_COLUMN).alias("months"))
    row = before.agg(*aggregates).collect()[0]
    months = tuple(sorted(str(m) for m in row["months"])) if has_month else ()
    return int(row["rows"]), months


def refuse_contradicting_month(existing: tuple[str, ...], *, month: str, tbl: str) -> None:
    """Refuse when the table already carries a DIFFERENT snapshot month.

    THE ONE STATE THIS SCRIPT MUST NOT PROCEED ON, and the only one that gets
    past every check downstream of the write. Run with `2026-07` against a table
    already stamped `2026-06` and all 71.9M rows are restamped, every
    `_snapshot_ref_date` goes NULL -- `ref_date_column` requires the filename's
    token to AGREE with the month, and it no longer does -- and the verification
    PASSES: the row count is unchanged, `_snapshot_month` holds no NULLs because
    it holds the new month everywhere, and no column was lost. Exit 0, with the
    previous month's evidence overwritten and only a NULL-count warning to show
    for it.

    Post-write checking cannot close this, which is why the refusal is here.

    This file's own docstring already scopes the script to tables that PREDATE
    the columns; this makes that scope enforced rather than merely stated. The
    invitation is near rather than hypothetical: F1.4b lands a second month, and
    "backfill the new month" is exactly what gets typed. A second month's rows
    are stamped at INGESTION by `add_audit_columns`, which is why it takes
    `snapshot_month` with no default -- they are never backfilled.

    Re-running with the SAME month is allowed: that is an idempotent repeat, and
    refusing it would make a script whose write can be interrupted unrepeatable.
    """
    contradicting = tuple(m for m in existing if m != month)
    if not contradicting:
        return
    raise RuntimeError(
        f"refusing to backfill {tbl}: it already carries {SNAPSHOT_MONTH_COLUMN}="
        f"{', '.join(repr(m) for m in contradicting)}, and this run was given "
        f"month={month!r}. NOTHING WAS WRITTEN. Proceeding would restamp every row "
        f"with {month!r} and send every {SNAPSHOT_REF_DATE_COLUMN} to NULL, because "
        "the reference date is only derived when the filename's own token agrees "
        "with the month -- and it would then pass this script's verification, "
        "because the row count would be unchanged and the column would have no "
        "NULLs. This script backfills tables that PREDATE the snapshot columns; it "
        "is not how a new month is added. A new month's rows are stamped at "
        "ingestion (add_audit_columns), so run the ingestion job for that month "
        f"instead. If you really do mean to re-stamp {tbl}, say so by passing the "
        "month it already carries, or clear the column first -- deliberately, and "
        "with the reason written down."
    )


def refuse_non_empty_quarantine(rows: int, *, month: str, tbl: str, spec: BronzeTable) -> None:
    """Refuse to fill the snapshot columns on a quarantine table that HOLDS ROWS.

    ADDING the columns to an EMPTY quarantine is the reason `--quarantine` exists.
    `databricks/src/dq_gate_batch.py:85` appends its reject frame with
    `mode("append").saveAsTable(...)` and no `mergeSchema`, so the moment staging
    carries the two snapshot columns the gate's frame is two columns wider than the
    quarantine it appends into and the append fails -- probed, and it fails against a
    0-row target too (`_LEGACY_ERROR_TEMP_DELTA_0007`). On zero rows the overwrite
    below fills nothing, so such a run is `ALTER TABLE ... ADD COLUMNS` and no more.

    FILLING them on rows already there is a different act, and it is the one refused.
    A quarantine row is a REJECT: it is in that table because the gate could not prove
    it good. `_fill` stamps `_snapshot_month = <month>` on every row it rewrites, and
    nothing here establishes that a reject belongs to the month this run was given --
    `_source_file` is the only thing that could say, and the fill consults it for the
    DATE, never for the month. So the write would assert a fact about somebody else's
    rejects that nobody measured, into the table ADR 0006 makes the measured reject
    history a later DQ threshold is set against.

    THIS IS WHERE THE PRINCIPLE IS LOAD-BEARING, and it is strictly stronger than
    refusing the flag outright. A blanket refusal would protect `estab_quarantine`,
    which holds 0 rows and needs the migration, while protecting
    `empresas_quarantine` (1 row) and `socios_quarantine` (1,797) only by accident of
    nobody having passed the flag. A row count refuses on the fact that makes it
    wrong.

    KEYED ON THE RESOLVED TABLE NAME, not on the flag the operator typed: were
    `resolve_target` ever to map a flag to the wrong field, a flag-keyed guard would
    stay silent on the very table it was protecting. This asks the only question that
    matters -- is the table I am about to overwrite THIS contract's quarantine? -- and
    takes `spec` rather than a name so the caller cannot hand it somebody else's.

    NEEDS THE COUNT, so it cannot live in `resolve_target` beside the other
    argument refusals. It is fed the count `pre_write_scan` already took in one pass
    for the verification, so it costs no extra scan and cannot disagree with it.
    """
    if tbl != DEFAULT.table(spec.quarantine) or not rows:
        return
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in _NEW_COLUMNS)
    raise RuntimeError(
        f"refusing to backfill {tbl}: it holds {rows} row(s), and every one of them "
        f"is a REJECT. NOTHING WAS WRITTEN. This script fills {SNAPSHOT_MONTH_COLUMN} "
        f"on every row it rewrites, so proceeding would stamp those rejects with "
        f"{SNAPSHOT_MONTH_COLUMN}={month!r}, a month nothing here proves they "
        "belong to -- and this is the table ADR 0006 makes the measured reject "
        "history, so a month invented in it is a month a later DQ threshold gets set "
        "against. WHAT THIS TABLE ACTUALLY NEEDS IS THE COLUMNS, NOT THE VALUES: the "
        "DQ gate appends its reject frame with no mergeSchema, so it fails while the "
        "quarantine is narrower than staging. Add them and leave the rows that are "
        "already there NULL -- the truthful value for a row whose month was never "
        "measured:\n"
        f"  ALTER TABLE {tbl} ADD COLUMNS ({columns});\n"
        "That is the entire migration this run would have performed on an empty "
        "table; only the fill is being refused. If one of the two columns is already "
        "there, add just the other -- ADD COLUMNS errors on a column that exists. An "
        "EMPTY quarantine is accepted, which is the case this target was added for."
    )


def _check(after: DataFrame, columns_before: frozenset[str]) -> BackfillCheck:
    """Every post-write fact, in ONE aggregate pass over the table.

    One pass and not five `.count()` calls: at 71.9M rows each scan is real
    money, and separate scans of the same table are separate answers that only
    happen to agree. `collect_set` skips NULLs, which is why the NULL counts are
    taken alongside it rather than inferred from the distinct values.
    """
    row = after.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count(F.when(F.col(SNAPSHOT_MONTH_COLUMN).isNull(), F.lit(1))).alias("null_month"),
        F.count(F.when(F.col(SNAPSHOT_REF_DATE_COLUMN).isNull(), F.lit(1))).alias("null_ref"),
        F.collect_set(SNAPSHOT_MONTH_COLUMN).alias("months"),
        F.collect_set(SNAPSHOT_REF_DATE_COLUMN).alias("ref_dates"),
    ).collect()[0]
    return BackfillCheck(
        rows=int(row["rows"]),
        null_month=int(row["null_month"]),
        null_ref_date=int(row["null_ref"]),
        months=tuple(sorted(str(m) for m in row["months"])),
        ref_dates=tuple(sorted(str(d) for d in row["ref_dates"])),
        lost_columns=tuple(sorted(columns_before - frozenset(after.columns))),
    )


def verify_or_raise(check: BackfillCheck, *, rows_before: int, tbl: str, version: int) -> None:
    """Refuse to report success unless all three invariants hold.

    Pure, so every reachable verdict is covered by a test that needs no session.
    The message carries BOTH row counts and the RESTORE statement because the
    operator reading it has to decide between undoing and investigating, and
    neither number is recoverable from a log that only said "failed".

    A NULL `_snapshot_ref_date` is deliberately NOT one of the three. It is a
    legitimate value -- `ref_date_column` returns NULL rather than guessing when
    the filename token and the folder's month disagree -- so raising on it would
    make a designed outcome an error. It is reported loudly instead, by
    `_warn_on_null_ref_dates`, because on a table whose filenames all carry the
    same token it should still be zero.
    """
    problems = []
    if check.rows != rows_before:
        problems.append(f"the row count changed: {check.rows} rows, expected {rows_before}")
    if check.null_month:
        problems.append(f"{check.null_month} row(s) have a NULL {SNAPSHOT_MONTH_COLUMN}")
    if check.lost_columns:
        problems.append(f"these columns are GONE: {', '.join(check.lost_columns)}")
    if not problems:
        return
    raise RuntimeError(
        f"backfill REFUSED to report success on {tbl} -- " + "; ".join(problems) + ". "
        f"Undo it with: RESTORE TABLE {tbl} TO VERSION AS OF {version} -- then "
        "investigate before re-running. Nothing downstream has read this table yet, "
        "so restoring costs nothing but the rewrite. "
        # The trap is the RE-RUN, not this failure, and it is silent. So the
        # warning goes in the message an operator is already reading, beside the
        # statement it tells them to run -- not in a report they may never open.
        "RESTORE FIRST, AND KEEP THIS OUTPUT: a second run of this script measures "
        "its row count off the table this run just left behind, verifies against "
        "THAT, exits 0, and prints its own version as the way back. Once it has, "
        f"version {version} survives only in this output and in Delta's history."
    )


def _warn_on_null_ref_dates(check: BackfillCheck, *, tbl: str, version: int) -> None:
    """Say loudly that some rows have no reference date, without failing.

    DO NOT LOOSEN THE PARSE in response to this. A NULL means the token in
    `_source_file` and the month this ran with disagreed, or the filename carried
    no token or two -- `opl.bronze.snapshot` refuses all three rather than
    stamping a date it cannot prove, because a wrong `applied_date` orders the
    vault's history incorrectly with nothing to show for it. Reconcile the
    offending filenames against the folder they are in first.
    """
    if not check.null_ref_date:
        return
    print(
        f"backfill: WARNING -- {check.null_ref_date} row(s) in {tbl} have a NULL "
        f"{SNAPSHOT_REF_DATE_COLUMN}. The values are written and the table is "
        f"consistent (RESTORE TABLE {tbl} TO VERSION AS OF {version} still undoes it), "
        "but do NOT loosen the filename parse. Find which files they came from:\n"
        f"  SELECT {SOURCE_FILE_COLUMN}, count(*) FROM {tbl} "
        f"WHERE {SNAPSHOT_REF_DATE_COLUMN} IS NULL GROUP BY 1 ORDER BY 2 DESC;"
    )


def _assert_constraints(spark: SparkSession, spec: BronzeTable, tbl: str) -> None:
    """Re-assert the registry's constraint DDL after the write -- ONLY on bronze.

    NOT ON STAGING OR QUARANTINE, and that is a decision with a mechanism, not the
    part nobody got to. `spec.constraints` is BRONZE DDL: the only other thing that
    issues it is `databricks/src/promote_batch.py:_assert_constraints`, against
    `DEFAULT.table(spec.bronze)`, and `databricks/src/ensure_masked_table.py` records
    that a quarantine table "has never carried a constraint". So on either of the
    other two tables these statements would not RE-assert anything the overwrite
    dropped; they would ADD a constraint the table never had.

    What that would cost, concretely, on estabelecimentos -- whose set is
    `cnpj_basico SET NOT NULL` plus `CHECK (length(trim(cnpj_basico)) = 8)`.
    `opl.bronze.rules` declares `null_or_empty_cnpj_basico` and
    `bad_cnpj_basico_length` for that contract BECAUSE ROWS LIKE THAT ARRIVE, and they
    arrive into STAGING. On staging the constraint fails the ingest's Delta write for
    a row the DQ gate exists to route into quarantine; on quarantine it fails the
    gate's append of that very reject. Either turns a designed, recoverable outcome
    into a broken pipeline -- and it would be planted by a run whose own last line
    said DONE, on a table where today's data happens to satisfy the constraint, to be
    discovered by whichever month first has a dirty row.

    WHY AT ALL, on the table this does run against.
    `mode("overwrite").saveAsTable` on an existing table is planned by
    Spark either as an overwrite of the table's DATA (`OverwriteByExpression`,
    which keeps the table's metadata) or as a replace of the TABLE
    (`AtomicReplaceTableAsSelect`, which does not, and drops CHECK constraints and
    NOT NULL with it). Both plans were observed for this exact write against a
    local Delta table while building this script, so the choice is real and not
    hypothetical -- and which one Unity Catalog picks for a 71.9M-row managed
    table is not observable from here without running it against the workspace.
    So this removes the question rather than depending on the answer: re-running
    the DDL leaves the constraints asserted either way. `cnpj_basico_len8`
    silently disappearing off a 71.9M-row table would be invisible until the next
    promote happened to re-add it.

    The DDL itself is NOT restated -- it comes from `spec.constraints`, the same
    field `databricks/src/promote_batch.py:_assert_constraints` applies after
    every append. Only the application is duplicated, not the statements, so
    there is nothing here to drift from them.

    Constraints are the only declarative metadata this repo sets on a bronze
    table today (`delta.dataSkippingStatsColumns` is deferred to F1.4b). If that
    changes -- or if the registry ever grows metadata scoped to staging or
    quarantine -- this function has to grow with it, and the skip below is the line
    that would then be wrong.
    """
    bronze = DEFAULT.table(spec.bronze)
    if tbl != bronze:
        # Said out loud rather than silently skipped: an operator comparing this run's
        # output against F1.4a's -- which ends with a "re-asserted 3 constraint
        # statement(s)" line -- has to be able to see that its absence was a decision.
        print(
            f"backfill: {tbl} is not {bronze}, so the registry's "
            f"{len(spec.constraints)} constraint statement(s) were NOT issued. They "
            "are bronze DDL (see promote_batch), and a staging or quarantine table "
            "has never carried one, so the overwrite dropped nothing here to "
            "re-assert. Asserting them would reject AT THE WRITE the rows the DQ "
            "gate exists to route into quarantine."
        )
        return
    for statement in spec.constraints:
        spark.sql(statement.format(table=tbl))
    print(f"backfill: re-asserted {len(spec.constraints)} constraint statement(s) on {tbl}")


def _fill(source: DataFrame, month: str) -> DataFrame:
    """`source` with both snapshot columns stamped.

    A DataFrame overwrite, not an `UPDATE ... SET`: the reference date is a
    Column expression over `_source_file`, and expressing it twice -- once in
    Python for the stream, once as a SQL string here -- is two things that can
    drift. `ref_date_column` is the one implementation, and it is the same one
    `add_audit_columns` uses for every new batch.
    """
    return source.withColumn(SNAPSHOT_MONTH_COLUMN, F.lit(month)).withColumn(
        SNAPSHOT_REF_DATE_COLUMN,
        ref_date_column(F.col(SOURCE_FILE_COLUMN), month),
    )


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    # Every argument refused BEFORE Spark, the order every job task in this repo
    # uses: nothing about a mistyped table, a malformed month or a misspelled target
    # needs a serverless session to diagnose. `resolve_target` owns all of it,
    # including the arity this line used to check itself.
    tbl, month = resolve_target(args)
    # The spec, separately, and safe to index for it: `resolve_target` has already
    # refused a bad arity and an unregistered name, so this is the same registered
    # table it just resolved. Asked for again rather than returned alongside `tbl`
    # because `resolve_target` answers WHICH TABLE and nothing else, while the two
    # role-dependent decisions below need the registry entry itself -- and it is a
    # lookup in a frozen dict, so there is no work to save by widening that return.
    spec = table_spec(args[0])

    spark = SparkSession.builder.getOrCreate()
    before = spark.read.table(tbl)
    columns_before = frozenset(before.columns)
    if SOURCE_FILE_COLUMN not in columns_before:
        raise RuntimeError(
            f"{tbl} has no {SOURCE_FILE_COLUMN} column, so {SNAPSHOT_REF_DATE_COLUMN} "
            "cannot be derived and this backfill would fill it with NULL on every row. "
            "Nothing was written."
        )

    # The version, then its RESTORE statement, then the row count -- in that
    # order and each printed as it is known. The count is a full scan of 71.9M
    # rows and can fail or be killed; a version obtained but never printed is a
    # version nobody has.
    version = latest_version(spark, tbl)
    print(f"backfill: {tbl} at version {version}")
    print(f"backfill: to undo -> RESTORE TABLE {tbl} TO VERSION AS OF {version}")
    rows_before, months_before = pre_write_scan(before)
    print(f"backfill: {rows_before} rows, month={month}, already carries {list(months_before)}")
    # Both refused HERE, before the ALTER and before the write, because the write
    # destroys the evidence they are refused on -- see `refuse_contradicting_month`.
    # Neither is scoped to a target: a staging table stamped with the wrong month is
    # the same defect as a bronze one, and the quarantine guard decides from the name
    # it was handed rather than from the flag that produced it.
    refuse_contradicting_month(months_before, month=month, tbl=tbl)
    refuse_non_empty_quarantine(rows_before, month=month, tbl=tbl, spec=spec)
    if months_before:
        print(f"backfill: {tbl} is already stamped {month} -- re-running is idempotent")

    to_add = missing_columns(columns_before)
    if to_add:
        spark.sql(
            f"ALTER TABLE {tbl} ADD COLUMNS "
            f"({', '.join(f'{name} {sql_type}' for name, sql_type in to_add)})"
        )
        print(f"backfill: added {', '.join(name for name, _ in to_add)} (metadata-only)")

    # Re-read: the DataFrame above was resolved against the pre-ALTER schema.
    _fill(spark.read.table(tbl), month).write.format("delta").mode("overwrite").saveAsTable(tbl)

    after = spark.read.table(tbl)
    check = _check(after, columns_before)
    print(
        f"backfill: rows={check.rows} (was {rows_before}) null_month={check.null_month} "
        f"null_ref_date={check.null_ref_date} months={list(check.months)} "
        f"ref_dates={list(check.ref_dates)}"
    )
    # Verify BEFORE the constraint DDL: that statement re-validates a CHECK over
    # the whole table, and a table this script is about to tell the operator to
    # RESTORE should not be paid for twice.
    verify_or_raise(check, rows_before=rows_before, tbl=tbl, version=version)
    _assert_constraints(spark, spec, tbl)
    _warn_on_null_ref_dates(check, tbl=tbl, version=version)
    print(f"backfill: DONE. {tbl} is at a new version; version {version} is the way back")
    return 0


if __name__ == "__main__":
    # BARE CALL, NOT `raise SystemExit(main())`, and the difference is not stylistic
    # -- it decides whether a SUCCESSFUL run is reported as a success.
    #
    # This script runs as a Databricks `spark_python_task` (that is what the module
    # docstring's "Run ON Databricks" means), and serverless executes the file
    # inside an IPython shell. There, SystemExit is an uncaught exception rather
    # than a process exit: on 2026-07-31 this exact line took a run that had
    # printed every expected number and its own "DONE" line and reported it as
    #     Task backfill failed ... SystemExit: 0
    # with life_cycle_state INTERNAL_ERROR.
    #
    # It got worse than a misleading status. INTERNAL_ERROR is a harness-level
    # error, not a clean task failure, so Databricks RETRIED the task -- past
    # `max_retries: 0`, which does not apply to it -- and the 71.9M-row overwrite
    # ran a second time. That was harmless BY DESIGN, not by luck:
    # `refuse_contradicting_month` deliberately permits a repeat with the SAME
    # month, and the write is idempotent, so the retry took the documented
    # re-run path. What luck decided was only that the retry inherited the same
    # month -- a retry is not a place to be depending on that. For a script whose
    # failure message warns that a second run baselines its row count off whatever
    # the first left behind, an exit convention that can silently trigger that
    # second run is a defect regardless of how the second run turns out.
    #
    # Every task in `databricks/src/` already calls `main()` bare, which is why
    # none of them has ever shown this. `scripts/extract_cnpj.py` keeps
    # `raise SystemExit(main())` correctly: it is a LOCAL CLI, where SystemExit is
    # the exit code. This file is the one script in `scripts/` that runs on the
    # workspace, so it follows the workspace's convention instead.
    #
    # Errors are unaffected: `main` RAISES (ValueError/RuntimeError) rather than
    # exiting, which is what carries the numbers into the run output -- see the
    # module docstring and `databricks/src/fail_on_dq.py`.
    main()
