# src/opl/bronze/backfill_prewrite.py
"""What the snapshot-column backfill decides BEFORE it writes anything.

The columns its `ALTER TABLE` has to add, the one pre-write scan every refusal is
made from, and the three refusals themselves. Its only caller is
`scripts/backfill_snapshot_columns.py`, which keeps the CLI, the write, and every
post-write check -- so a backticked name below that is not defined here (`_fill`,
`_check`, `verify_or_raise`, `fill_and_overwrite`, `resolve_target`,
`_warn_on_null_ref_dates`, `_assert_constraints`) is that script's.

WHY IT WAS EXTRACTED AT ALL. The script reached 1,048 lines against this repo's
800-line ceiling -- the same ceiling that, in this same phase, kept a correctness
guard out of `src/opl/bronze/registry.py` because that file sits at 798 of 800. A
cap that is enforced where it is cheap and waived where it is expensive is worse
than no cap, because every later "it does not fit" becomes negotiable. This is the
expensive one being paid.

THIS IS ONE-OFF MIGRATION LOGIC, AND IT LIVES IN THE LIBRARY ANYWAY. Nothing in
the pipeline imports it and nothing should: a new month's rows are stamped at
INGESTION by `add_audit_columns`, which is exactly what the refusals below tell an
operator to use instead.

THE DEPLOYMENT CONSEQUENCE, WHICH IS WHY THE LIBRARY AND NOT A SIBLING UNDER
`scripts/`. Read this before moving it:

  * The script runs on Databricks as a `spark_python_task`, and `scripts/` is
    OUTSIDE the bundle sync root, so deploying it needs a temporary
    `sync: paths: [".", "../scripts"]` entry -- see the NOTE in
    `databricks/databricks.yml`. That is unchanged by this extraction. What IS
    changed is that nothing further is needed: the script already imports `opl.*`,
    so the WHEEL is already on that job's path and carries this module for free.
  * A sibling module under `scripts/` would instead have to be found on
    `sys.path`, and the one platform this script actually runs on does not execute
    it the way CPython does -- serverless runs the file inside an IPython shell,
    which is how a run that printed every expected number once reported
    `SystemExit: 0` as INTERNAL_ERROR and got itself retried (see the script's
    `__main__` block). Betting a module-level import on `sys.path[0]` being the
    script's directory there is the same class of bet, and it would fail at import
    time, on the workspace, after a deploy.
  * The test suite could not have caught that bet either. It loads scripts by file
    location with no `sys.path` edit -- `tests/test_revision_stamp.py` says so
    outright -- and `importlib.util.spec_from_file_location` does not put
    `scripts/` on the path.

WHAT THAT COSTS, said plainly rather than left to be discovered: the wheel now
ships migration code no job task imports, and `opl.bronze` carries a module whose
whole reasoning is about one script. That is the price of the three points above.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from opl.bronze.autoloader import SOURCE_FILE_COLUMN
from opl.bronze.registry import BronzeTable
from opl.bronze.snapshot import (
    SNAPSHOT_MONTH_COLUMN,
    SNAPSHOT_REF_DATE_COLUMN,
    token_month_column,
    token_month_key,
)
from opl.config import DEFAULT

# The two columns and their Delta types, in the order `ALTER TABLE ADD COLUMNS`
# takes them. The NAMES come from `opl.bronze.snapshot`, which is where the
# stream that writes them for every new batch gets them: a literal here would be
# a second spelling, and a backfill that filled a differently-named column would
# leave the real one NULL on 71.9M rows while reporting success.
_NEW_COLUMNS: tuple[tuple[str, str], ...] = (
    (SNAPSHOT_MONTH_COLUMN, "STRING"),
    (SNAPSHOT_REF_DATE_COLUMN, "DATE"),
)


def missing_columns(existing: frozenset[str]) -> tuple[tuple[str, str], ...]:
    """The snapshot columns `existing` does not already have.

    Per-column rather than "does the month column exist?": a run interrupted
    between the ALTER and the write leaves the table with both columns and no
    values, but a future third column added to `_NEW_COLUMNS` would leave the
    table with two of three, and `ADD COLUMNS` errors on a column that is already
    there. Checking each one makes a re-run idempotent whatever it interrupted.
    """
    return tuple((name, sql_type) for name, sql_type in _NEW_COLUMNS if name not in existing)


@dataclass(frozen=True)
class PreWriteScan:
    """What the table looked like BEFORE anything was written. Evidence, not state.

    Every field comes from ONE aggregate pass (see `pre_write_scan`), so the row
    count the verification compares against, the months the run is refused on and
    the source months it is refused on cannot disagree with each other.
    """

    rows: int
    # Distinct non-NULL `_snapshot_month` values already in the table. Empty when
    # the column does not exist yet -- the case this script was written for.
    months: tuple[str, ...]
    # Distinct `token_month_key`-shaped months the FILENAMES assert ('6-07', ...).
    # NULLs are dropped by `collect_set`, so this is what the files that DO carry
    # exactly one token say, and nothing about the ones that do not.
    source_months: tuple[str, ...]
    # Rows whose `_source_file` carries no token, or two. Not a refusal -- their
    # reference date is legitimately NULL -- but reported before the commit rather
    # than only after it.
    rows_without_source_month: int


def pre_write_scan(before: DataFrame) -> PreWriteScan:
    """Everything the refusals need, in ONE pass over the table.

    BEFORE the write, and that ordering is the whole point rather than an
    optimisation: the write overwrites `_snapshot_month`, so a month already in
    the table is evidence that stops existing the moment it runs. `_check`'s
    post-write `collect_set` cannot see it -- after a restamp it reports exactly
    the month that was passed in, which is how a contradicting month satisfied
    every post-write invariant and exited 0.

    THE SOURCE MONTHS ARE READ HERE FOR A DIFFERENT REASON, and it is the one that
    survives a table with no `_snapshot_month` at all. `_source_file` is NOT
    overwritten by the fill, so unlike the months above it does not stop existing
    -- but the post-write check that would notice a disagreement does not exist:
    `_check` counts NULL `_snapshot_ref_date` and `verify_or_raise` deliberately
    does not raise on it. So the disagreement is refused here, before the ALTER
    and before the overwrite, rather than reported after the commit. See
    `refuse_contradicting_source_month`.

    One pass, so all four numbers come from the same scan of the same 71.9M rows.
    """
    aggregates = [
        F.count(F.lit(1)).alias("rows"),
        F.collect_set(token_month_column(F.col(SOURCE_FILE_COLUMN))).alias("source_months"),
        F.count(
            F.when(token_month_column(F.col(SOURCE_FILE_COLUMN)).isNull(), F.lit(1))
        ).alias("no_source_month"),
    ]
    # Conditional because the column is exactly what may not exist yet -- that is
    # the case this script was written for. `_source_file` is not conditional:
    # `main` refuses a table without it before reaching here, because the whole
    # backfill would fill `_snapshot_ref_date` with NULL on every row.
    has_month = SNAPSHOT_MONTH_COLUMN in before.columns
    if has_month:
        aggregates.append(F.collect_set(SNAPSHOT_MONTH_COLUMN).alias("months"))
    row = before.agg(*aggregates).collect()[0]
    return PreWriteScan(
        rows=int(row["rows"]),
        months=tuple(sorted(str(m) for m in row["months"])) if has_month else (),
        source_months=tuple(sorted(str(m) for m in row["source_months"])),
        rows_without_source_month=int(row["no_source_month"]),
    )


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

    The script's own module docstring already scopes it to tables that PREDATE
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


def refuse_contradicting_source_month(
    source_months: tuple[str, ...], *, month: str, tbl: str
) -> None:
    """Refuse when the FILENAMES disagree with the month, or with each other.

    THE HALF `refuse_contradicting_month` STRUCTURALLY CANNOT SEE. A legacy table
    with no `_snapshot_month` column passes that guard on an empty tuple -- by
    design; it is the case this script exists for. `_fill` then stamps the
    requested month on every row, and `ref_date_column` returns NULL for
    `_snapshot_ref_date` on every row whose `_source_file` token disagrees with
    it. The post-write verification then PASSES: the row count is unchanged and
    `_snapshot_month` holds no NULLs, because it holds the month this run just
    wrote. A verification that reads the value it just wrote cannot fail for the
    right reason, so the disagreement only ever surfaced as a NULL reference date
    AFTER the overwrite was committed, in a warning.

    So it is refused BEFORE the ALTER and before the overwrite, on the one piece of
    evidence a legacy table does carry: the filenames. Both live runs of this phase
    were unaffected -- estab staging held exactly one month and the run measured
    `null_ref = 0` -- which is the data being safe, not the guard being there.

    MIXED IS REFUSED TOO, and not only mismatched. A table whose files carry two
    months has no single month to be stamped with: whichever one is passed, the
    other month's rows get a `_snapshot_month` that contradicts their own filename
    and a NULL reference date. Bronze is multi-month from F1.4b onward, so such a
    table is a normal object now -- it is simply not a thing this script may write,
    and the rows it would silently mislabel are the reason.

    NOT SCOPED TO LEGACY TABLES, deliberately. A table that already carries the
    month is refused by `refuse_contradicting_month` when the month differs and
    permitted when it matches; on that permitted repeat this guard still has to
    hold, because an idempotent re-run of a stamp that was wrong is still wrong.

    A ROW WITH NO TOKEN IS NOT REFUSED. `token_month_column` returns NULL for a
    filename carrying no token or two, and `collect_set` drops those, so they are
    absent from `source_months` rather than disagreeing with it. Their reference
    date is legitimately NULL -- see `_warn_on_null_ref_dates`, which is the
    control for absence -- and refusing them here would refuse the lookup tables
    and any future source whose naming this module has not seen. The count is
    printed before the write instead; `PreWriteScan.rows_without_source_month`
    carries it.
    """
    key = token_month_key(month)
    contradicting = tuple(m for m in source_months if m != key)
    if not contradicting:
        return
    raise RuntimeError(
        f"refusing to backfill {tbl}: this run was given month={month!r}, whose "
        f"filename token is {key!r}, and {SOURCE_FILE_COLUMN} says its rows came "
        f"from {', '.join(repr(m) for m in contradicting)}"
        + (f" as well as {key!r}" if key in source_months else "")
        + ". NOTHING WAS WRITTEN -- no ALTER and no overwrite. Proceeding would "
        f"stamp {SNAPSHOT_MONTH_COLUMN}={month!r} on rows whose own filename "
        f"contradicts it and send their {SNAPSHOT_REF_DATE_COLUMN} to NULL, "
        "because the reference date is only derived when the filename's token "
        "agrees with the month. THAT WOULD THEN PASS THIS SCRIPT'S VERIFICATION: "
        "the row count would be unchanged and the month column would have no "
        "NULLs, because this run had just written it -- the disagreement would "
        "reach you as a warning after the commit, if at all. The token carries a "
        "single year digit, so 'Y-MM' is all the filename can prove and the decade "
        "is this run's parameter. If the table really does hold more than one "
        "month it is not a backfill target at all: a month's rows are stamped at "
        "ingestion (add_audit_columns). If it holds exactly one and this run named "
        "the other, re-run with the month the files actually carry."
    )


def refuse_non_empty_quarantine(rows: int, *, month: str, tbl: str, spec: BronzeTable) -> None:
    """Refuse to fill the snapshot columns on a quarantine table that HOLDS ROWS.

    ADDING the columns to an EMPTY quarantine is the reason `--quarantine` exists.
    `databricks/src/dq_gate_batch.py:85` appends its reject frame with
    `mode("append").saveAsTable(...)` and no `mergeSchema`, so the moment staging
    carries the two snapshot columns the gate's frame is two columns wider than the
    quarantine it appends into and the append fails -- probed, and it fails against a
    0-row target too (`_LEGACY_ERROR_TEMP_DELTA_0007`). On zero rows the script's
    overwrite fills nothing, so such a run is `ALTER TABLE ... ADD COLUMNS` and no more.

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

    THIS GUARD IS ABOUT BACK-STAMPING REJECTS AND NOTHING ELSE. It is NOT what keeps
    the overwrite away from a masked quarantine table -- `fill_and_overwrite` is, by
    not issuing a write when there is nothing to fill. Read that before relaxing this
    one: permitting a quarantine backfill whose rows already carry this month (the
    idempotent repeat `refuse_contradicting_month` allows) is a change to THIS
    predicate that must not be made on the assumption that the write is otherwise
    harmless.
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
