# tests/test_backfill_prewrite.py
"""Unit tests for opl.bronze.backfill_prewrite.

Everything the snapshot-column backfill decides BEFORE it writes: which columns
its ALTER has to add, the one pass it reads its evidence in, and the three
refusals made from that evidence. Pure except for the scan, which takes the
shared Spark session because its aggregate has to be the real aggregate over real
filenames rather than a hand-written tuple.

These were `scripts/backfill_snapshot_columns.py`'s until that file went 248 lines
over this repo's 800-line ceiling. They are asserted against the module directly
rather than through the script's re-export, because the module is where the
functions live now; that the script actually REACHES them is a separate claim, and
it is pinned end-to-end through `main` in
`tests/test_backfill_snapshot_columns_script.py`.

Backticked names in the reasoning below that are not defined here (`_fill`,
`_check`, `verify_or_raise`, `_warn_on_null_ref_dates`) are that script's -- the
post-write half, which stayed with it.
"""
from __future__ import annotations

import pytest

from opl.bronze.backfill_prewrite import (
    missing_columns,
    pre_write_scan,
    refuse_contradicting_month,
    refuse_contradicting_source_month,
    refuse_non_empty_quarantine,
)
from opl.bronze.registry import table_spec
from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN

# --- refusing a contradicting month, BEFORE the write --------------------------


def test_a_table_carrying_a_different_month_is_refused_with_both_values_named():
    """The one state that gets past every post-write check.

    Restamping 2026-06 rows as 2026-07 leaves the row count unchanged, leaves
    ``_snapshot_month`` with no NULLs, and loses no column -- so the verification
    passes and the run exits 0, having sent every ``_snapshot_ref_date`` to NULL
    because the filename token no longer agrees with the month.
    """
    with pytest.raises(RuntimeError) as exc:
        refuse_contradicting_month(("2026-06",), month="2026-07", tbl="wh.d.b")
    message = str(exc.value)
    assert "'2026-06'" in message and "'2026-07'" in message
    assert "NOTHING WAS WRITTEN" in message
    # It must say what to do instead, or it is a wall.
    assert "ingestion" in message


def test_a_table_with_no_month_yet_is_the_case_this_script_is_for():
    refuse_contradicting_month((), month="2026-06", tbl="wh.d.b")


def test_re_running_with_the_same_month_is_allowed():
    """Idempotent repeat. Refusing it would make a script whose write can be
    interrupted unrepeatable."""
    refuse_contradicting_month(("2026-06",), month="2026-06", tbl="wh.d.b")


def test_a_partly_stamped_table_is_refused_on_the_month_that_disagrees():
    with pytest.raises(RuntimeError, match="'2026-05'"):
        refuse_contradicting_month(("2026-05", "2026-06"), month="2026-06", tbl="wh.d.b")


# --- refusing a month the FILENAMES contradict, BEFORE the write ---------------
#
# The half ``refuse_contradicting_month`` structurally cannot see. A legacy table
# with no ``_snapshot_month`` passes that guard on an empty tuple -- by design --
# and then ``_fill`` stamps the requested month, ``ref_date_column`` returns NULL
# for every row whose filename token disagrees, and the post-write verification
# PASSES: the row count is unchanged and the month column has no NULLs, because
# this run just wrote it. A check that reads the value it wrote cannot fail for
# the right reason.

_LANDING = "/Volumes/workspace/default/landing/cnpj"
_JUNE_FILE = f"{_LANDING}/2026-06/estabelecimentos/K3241.K03200Y0.D60613.ESTABELE"
_JULY_FILE = f"{_LANDING}/2026-07/estabelecimentos/K3241.K03200Y0.D60711.ESTABELE"


def test_filenames_agreeing_with_the_month_are_the_normal_case():
    """Both live runs of this phase were exactly this: one month in the table, and
    the run named it. That is what must stay unrefused."""
    refuse_contradicting_source_month(("6-06",), month="2026-06", tbl="wh.d.b")
    refuse_contradicting_source_month((), month="2026-06", tbl="wh.d.b")


def test_a_legacy_table_whose_files_carry_another_month_is_refused():
    """The state neither internal review caught. Nothing here has a
    ``_snapshot_month`` to contradict, so this is the only evidence there is."""
    with pytest.raises(RuntimeError) as exc:
        refuse_contradicting_source_month(("6-06",), month="2026-07", tbl="wh.d.b")
    message = str(exc.value)
    # Both sides of the disagreement, in the spelling the filename actually uses.
    assert "'2026-07'" in message and "'6-07'" in message and "'6-06'" in message
    assert "NOTHING WAS WRITTEN" in message and "no ALTER" in message
    # And WHY the post-write check would not have caught it -- otherwise an operator
    # reading this concludes the verification is enough.
    assert "PASS THIS SCRIPT'S VERIFICATION" in message
    # A refusal that names no alternative is a wall.
    assert "re-run with the month the files actually carry" in message


def test_a_table_holding_two_months_of_files_is_refused_whichever_month_is_named():
    """Mixed, not merely mismatched. Bronze is multi-month from F1.4b onward, so
    such a table is a normal object -- it is simply not a thing this script may
    write, because whichever month is passed, the other month's rows get a
    ``_snapshot_month`` contradicting their own filename and a NULL reference date."""
    for month in ("2026-06", "2026-07"):
        with pytest.raises(RuntimeError, match="NOTHING WAS WRITTEN"):
            refuse_contradicting_source_month(
                ("6-06", "6-07"), month=month, tbl="wh.d.b"
            )


def test_the_mixed_refusal_says_the_named_month_was_present_too():
    """``6-06`` and ``6-07`` with ``2026-06`` requested: listing only the
    contradicting one would read as 'no file agrees', which is a different
    incident from 'this table holds two months'."""
    with pytest.raises(RuntimeError) as exc:
        refuse_contradicting_source_month(
            ("6-06", "6-07"), month="2026-06", tbl="wh.d.b"
        )
    assert "as well as '6-06'" in str(exc.value)


def test_the_decade_cannot_be_used_to_smuggle_a_disagreement_past_the_guard():
    """The token carries one year digit, so ``2016-06`` and ``2026-06`` produce the
    same key. That is a property of the filename, not a hole: ``_snapshot_month``
    sits beside the date carrying the operator's own decade, which is the
    ``ref_date_column`` bargain, and this guard must not pretend to close it."""
    refuse_contradicting_source_month(("6-06",), month="2016-06", tbl="wh.d.b")


def test_the_guard_still_holds_on_the_idempotent_repeat():
    """``refuse_contradicting_month`` deliberately permits re-running with the month
    the table already carries. An idempotent repeat of a stamp that was WRONG is
    still wrong, so this guard is not scoped to legacy tables."""
    with pytest.raises(RuntimeError, match="NOTHING WAS WRITTEN"):
        refuse_contradicting_source_month(("6-06",), month="2026-07", tbl="wh.d.b")


def test_a_malformed_month_is_refused_by_the_key_rather_than_matching_nothing():
    """Reached only by a direct caller -- ``resolve_target`` has already been through
    ``require_month`` -- but a key built from a malformed month would silently equal
    no filename at all, and the refusal would then name the wrong cause."""
    with pytest.raises(ValueError, match="YYYY-MM"):
        refuse_contradicting_source_month(("6-06",), month="2026-6", tbl="wh.d.b")


# --- refusing to back-stamp rejects that are already in quarantine -------------

_ESTAB = "workspace.default.bronze_cnpj_estab_quarantine"


def test_a_quarantine_holding_rows_is_refused():
    """Adding the columns to an EMPTY quarantine is a schema migration. Filling them
    on rows that are already there asserts a month those rejects were never proven
    to belong to. Empresas (1 row) and socios (1,797) are the live cases."""
    with pytest.raises(RuntimeError) as exc:
        refuse_non_empty_quarantine(
            1797, month="2026-06", tbl=_ESTAB, spec=table_spec("estabelecimentos")
        )
    message = str(exc.value)
    # The count it found, or the operator cannot tell a stray row from a full month.
    assert "1797 row(s)" in message
    # And the month it would have stamped, which is the fact being asserted.
    assert "'2026-06'" in message
    assert "NOTHING WAS WRITTEN" in message
    # What to do instead: the columns without the fill. A refusal that names no
    # alternative is a wall.
    assert f"ALTER TABLE {_ESTAB} ADD COLUMNS" in message
    assert SNAPSHOT_MONTH_COLUMN in message and SNAPSHOT_REF_DATE_COLUMN in message


def test_an_empty_quarantine_is_the_migration_this_target_exists_for():
    """0 rows is estab's live case: the DQ gate's reject append fails against a
    quarantine narrower than staging even at 0 rows, and on 0 rows the fill this
    guard is protecting has nothing to stamp."""
    refuse_non_empty_quarantine(
        0, month="2026-06", tbl=_ESTAB, spec=table_spec("estabelecimentos")
    )


@pytest.mark.parametrize(
    "tbl",
    [
        "workspace.default.bronze_cnpj_estabelecimentos",
        "workspace.default.bronze_cnpj_estab_staging",
    ],
)
def test_a_populated_bronze_or_staging_is_not_refused(tbl):
    """71.9M rows in the table this script was written for is the normal case, not
    the refused one -- the guard must be about the quarantine and nothing else."""
    refuse_non_empty_quarantine(
        71_874_448, month="2026-06", tbl=tbl, spec=table_spec("estabelecimentos")
    )


def test_the_guard_is_about_the_quarantine_of_the_table_it_was_given():
    """Keyed on the resolved name against THIS spec's quarantine, so estab's
    quarantine is not refused while socios' is being backfilled, and vice versa."""
    socios = table_spec("socios")
    # estab's quarantine, socios' spec: not this run's quarantine, so not refused.
    refuse_non_empty_quarantine(1797, month="2026-06", tbl=_ESTAB, spec=socios)
    with pytest.raises(RuntimeError, match="1797 row\\(s\\)"):
        refuse_non_empty_quarantine(
            1797,
            month="2026-06",
            tbl="workspace.default.bronze_cnpj_socios_quarantine",
            spec=socios,
        )


# --- the ALTER is idempotent per column ---------------------------------------


def test_missing_columns_lists_both_when_neither_is_present():
    assert missing_columns(frozenset({"cnpj_basico", "_source_file"})) == (
        (SNAPSHOT_MONTH_COLUMN, "STRING"),
        (SNAPSHOT_REF_DATE_COLUMN, "DATE"),
    )


def test_missing_columns_is_empty_on_a_re_run():
    existing = frozenset({SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN, "_source_file"})
    assert missing_columns(existing) == ()


def test_missing_columns_lists_only_the_absent_one():
    """A run interrupted after a partial ALTER must still be re-runnable:
    ``ADD COLUMNS`` errors on a column that is already there."""
    existing = frozenset({SNAPSHOT_MONTH_COLUMN, "_source_file"})
    assert missing_columns(existing) == ((SNAPSHOT_REF_DATE_COLUMN, "DATE"),)


def test_pre_write_scan_returns_no_months_when_the_column_does_not_exist_yet(spark):
    """The case this script was written for. The aggregate has to be conditional:
    ``collect_set`` on a column that is not there fails analysis."""
    df = spark.createDataFrame([("a",), ("b",)], ["_source_file"])
    scan = pre_write_scan(df)
    assert scan.rows == 2 and scan.months == ()
    # Neither filename carries a token, so neither asserts a month -- and both are
    # counted, so the operator sees before the commit how many rows can only get a
    # NULL reference date.
    assert scan.source_months == () and scan.rows_without_source_month == 2


def test_pre_write_scan_collects_the_months_a_table_already_carries(spark):
    """Read BEFORE the write, because the write is what destroys this evidence --
    ``_check``'s post-write ``collect_set`` reports whatever was just stamped."""
    df = spark.createDataFrame(
        [("a", "2026-06"), ("b", "2026-06"), ("c", None)],
        ["_source_file", SNAPSHOT_MONTH_COLUMN],
    )
    scan = pre_write_scan(df)
    assert scan.rows == 3 and scan.months == ("2026-06",)


def test_pre_write_scan_reads_the_months_the_FILENAMES_assert(spark):
    """The evidence a LEGACY table does carry. It has no ``_snapshot_month`` for
    ``refuse_contradicting_month`` to read, and ``_source_file`` is the only thing
    that can say which month its rows belong to -- so the scan has to collect it in
    the same pass, or the two refusals are made from two different reads of a
    71.9M-row table."""
    df = spark.createDataFrame(
        [
            (f"{_LANDING}/2026-06/estabelecimentos/K3241.K03200Y0.D60613.ESTABELE",),
            (f"{_LANDING}/2026-07/estabelecimentos/K3241.K03200Y0.D60711.ESTABELE",),
            (f"{_LANDING}/2026-06/lookups/Cnaes.csv",),
        ],
        ["_source_file"],
    )
    scan = pre_write_scan(df)
    assert scan.rows == 3
    assert scan.source_months == ("6-06", "6-07")
    # The untokenised file is absent from the set rather than disagreeing with it,
    # and counted separately.
    assert scan.rows_without_source_month == 1
