# tests/test_backfill_snapshot_columns_script.py
"""Unit tests for scripts/backfill_snapshot_columns.py.

The verification logic is pure and tested without a session: what the backfill
has to get right is REFUSING, and every refusal is a decision made from counts.
Two tests take the shared Spark session because they pin assumptions about Delta
that no amount of pure Python can check -- that ``DESCRIBE HISTORY`` yields a
``version`` column, and that the one-pass aggregate reports what it claims to.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from opl.bronze.snapshot import SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN

_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backfill_snapshot_columns.py"
_spec = importlib.util.spec_from_file_location("backfill_snapshot_cli", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
cli = importlib.util.module_from_spec(_spec)
# REGISTERED BEFORE exec_module, unlike `tests/test_extract_cnpj_cli.py`'s loader:
# `@dataclass` resolves its own module out of `sys.modules` to inspect annotations,
# so a module loaded from a file path but never registered dies on the decorator
# with `AttributeError: 'NoneType' object has no attribute '__dict__'` -- an error
# naming neither dataclasses nor the loader. That script has no dataclass, so it
# never needed this.
sys.modules[_spec.name] = cli
_spec.loader.exec_module(cli)


def _check(**overrides) -> cli.BackfillCheck:
    defaults = {
        "rows": 100,
        "null_month": 0,
        "null_ref_date": 0,
        "months": ("2026-06",),
        "ref_dates": ("2026-06-13",),
        "lost_columns": (),
    }
    return cli.BackfillCheck(**{**defaults, **overrides})


# --- refusing to claim success it has not verified -----------------------------


def test_verify_accepts_an_unchanged_row_count_with_no_null_month():
    cli.verify_or_raise(_check(), rows_before=100, tbl="t", version=7)


@pytest.mark.parametrize(
    ("check", "expected"),
    [
        (_check(rows=99), "99 rows, expected 100"),
        (_check(rows=200), "200 rows, expected 100"),
        (_check(null_month=3), f"3 row\\(s\\) have a NULL {SNAPSHOT_MONTH_COLUMN}"),
        (_check(lost_columns=("cnpj_basico",)), "these columns are GONE: cnpj_basico"),
    ],
)
def test_verify_raises_with_the_numbers_on_every_mismatch(check, expected):
    with pytest.raises(RuntimeError, match=expected):
        cli.verify_or_raise(check, rows_before=100, tbl="wh.d.bronze", version=7)


def test_every_refusal_carries_the_restore_statement():
    """The operator reading this has to choose between undoing and investigating,
    and the version is not recoverable from a log that only said 'failed'."""
    with pytest.raises(RuntimeError) as exc:
        cli.verify_or_raise(_check(null_month=1), rows_before=100, tbl="wh.d.b", version=42)
    assert "RESTORE TABLE wh.d.b TO VERSION AS OF 42" in str(exc.value)


def test_the_failure_message_names_the_unsafe_re_run():
    """The trap is the RE-RUN, not the failure, and it is silent.

    A second run measures ``rows_before`` off the table the first one corrupted,
    verifies against THAT, exits 0, and prints its own version as the way back --
    after which the original survives only in the first run's output. That has to
    be in the message the operator is already reading, not only in a report.
    """
    with pytest.raises(RuntimeError) as exc:
        cli.verify_or_raise(_check(rows=99), rows_before=100, tbl="wh.d.b", version=42)
    message = str(exc.value)
    assert "RESTORE FIRST, AND KEEP THIS OUTPUT" in message
    assert "version 42 survives only in this output" in message


# --- refusing a contradicting month, BEFORE the write --------------------------


def test_a_table_carrying_a_different_month_is_refused_with_both_values_named():
    """The one state that gets past every post-write check.

    Restamping 2026-06 rows as 2026-07 leaves the row count unchanged, leaves
    ``_snapshot_month`` with no NULLs, and loses no column -- so the verification
    passes and the run exits 0, having sent every ``_snapshot_ref_date`` to NULL
    because the filename token no longer agrees with the month.
    """
    with pytest.raises(RuntimeError) as exc:
        cli.refuse_contradicting_month(("2026-06",), month="2026-07", tbl="wh.d.b")
    message = str(exc.value)
    assert "'2026-06'" in message and "'2026-07'" in message
    assert "NOTHING WAS WRITTEN" in message
    # It must say what to do instead, or it is a wall.
    assert "ingestion" in message


def test_a_table_with_no_month_yet_is_the_case_this_script_is_for():
    cli.refuse_contradicting_month((), month="2026-06", tbl="wh.d.b")


def test_re_running_with_the_same_month_is_allowed():
    """Idempotent repeat. Refusing it would make a script whose write can be
    interrupted unrepeatable."""
    cli.refuse_contradicting_month(("2026-06",), month="2026-06", tbl="wh.d.b")


def test_a_partly_stamped_table_is_refused_on_the_month_that_disagrees():
    with pytest.raises(RuntimeError, match="'2026-05'"):
        cli.refuse_contradicting_month(("2026-05", "2026-06"), month="2026-06", tbl="wh.d.b")


def test_a_null_ref_date_alone_is_reported_and_not_raised(capsys):
    """``ref_date_column`` returns NULL rather than guessing when the filename
    token and the folder's month disagree, so a NULL is a designed outcome. It is
    warned about loudly -- with the diagnostic query -- and does not fail the run."""
    check = _check(null_ref_date=5, ref_dates=())
    cli.verify_or_raise(check, rows_before=100, tbl="wh.d.b", version=9)
    cli._warn_on_null_ref_dates(check, tbl="wh.d.b", version=9)
    out = capsys.readouterr().out
    assert "WARNING" in out and "5 row(s)" in out
    assert "do NOT loosen the filename parse" in out
    assert SNAPSHOT_REF_DATE_COLUMN in out and "IS NULL" in out


def test_no_warning_when_every_row_has_a_reference_date(capsys):
    cli._warn_on_null_ref_dates(_check(), tbl="wh.d.b", version=9)
    assert capsys.readouterr().out == ""


# --- the ALTER is idempotent per column ---------------------------------------


def test_missing_columns_lists_both_when_neither_is_present():
    assert cli.missing_columns(frozenset({"cnpj_basico", "_source_file"})) == (
        (SNAPSHOT_MONTH_COLUMN, "STRING"),
        (SNAPSHOT_REF_DATE_COLUMN, "DATE"),
    )


def test_missing_columns_is_empty_on_a_re_run():
    existing = frozenset({SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN, "_source_file"})
    assert cli.missing_columns(existing) == ()


def test_missing_columns_lists_only_the_absent_one():
    """A run interrupted after a partial ALTER must still be re-runnable:
    ``ADD COLUMNS`` errors on a column that is already there."""
    existing = frozenset({SNAPSHOT_MONTH_COLUMN, "_source_file"})
    assert cli.missing_columns(existing) == ((SNAPSHOT_REF_DATE_COLUMN, "DATE"),)


# --- arguments are refused before Spark --------------------------------------


@pytest.mark.parametrize("argv", [[], ["estabelecimentos"], ["a", "b", "c"]])
def test_main_refuses_the_wrong_number_of_arguments(argv):
    with pytest.raises(ValueError, match="usage:"):
        cli.main(argv)


def test_main_refuses_an_unknown_table_before_building_a_session():
    from opl.bronze.registry import UnknownTable

    with pytest.raises(UnknownTable, match="unknown bronze table 'estab'"):
        cli.main(["estab", "2026-06"])


@pytest.mark.parametrize("month", ["", "2026-6", "2026-06/zips"])
def test_main_refuses_an_absent_or_malformed_month_before_building_a_session(month):
    with pytest.raises(ValueError, match="refusing to backfill the snapshot columns"):
        cli.main(["estabelecimentos", month])


# --- the two Delta/Spark assumptions -----------------------------------------


def test_latest_version_reads_the_newest_delta_version(spark, tmp_path):
    """``max(version)`` and not ``LIMIT 1``: the RESTORE target must not depend on
    ``DESCRIBE HISTORY``'s row order, which Delta does not promise.

    A path-based table under ``tmp_path`` rather than a named one: the shared
    local warehouse and its Derby metastore outlive the process, so a named probe
    table leaves a directory behind that makes the NEXT run fail with
    ``DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`` -- observed, not theorised.
    ``latest_version`` only interpolates the name into SQL, so ``delta.`<path>```
    exercises the identical statement.
    """
    location = (tmp_path / "history_probe").as_posix()
    spark.range(2).write.format("delta").save(location)
    tbl = f"delta.`{location}`"
    first = cli.latest_version(spark, tbl)
    spark.range(2).write.format("delta").mode("append").save(location)
    assert cli.latest_version(spark, tbl) == first + 1


def test_pre_write_scan_returns_no_months_when_the_column_does_not_exist_yet(spark):
    """The case this script was written for. The aggregate has to be conditional:
    ``collect_set`` on a column that is not there fails analysis."""
    df = spark.createDataFrame([("a",), ("b",)], ["_source_file"])
    assert cli.pre_write_scan(df) == (2, ())


def test_pre_write_scan_collects_the_months_a_table_already_carries(spark):
    """Read BEFORE the write, because the write is what destroys this evidence --
    ``_check``'s post-write ``collect_set`` reports whatever was just stamped."""
    df = spark.createDataFrame(
        [("a", "2026-06"), ("b", "2026-06"), ("c", None)],
        ["_source_file", SNAPSHOT_MONTH_COLUMN],
    )
    assert cli.pre_write_scan(df) == (3, ("2026-06",))


def test_check_reports_counts_distincts_and_lost_columns_in_one_pass(spark):
    """Pins the aggregate: NULLs counted (``collect_set`` drops them) and a column
    that vanished from the write reported rather than assumed absent."""
    rows = [
        ("f.D60613.CNAECSV", "2026-06", "2026-06-13"),
        ("f.D60613.PAISCSV", "2026-06", "2026-06-13"),
        ("f.D99999.XCSV", None, None),
    ]
    df = spark.createDataFrame(
        rows, ["_source_file", SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN]
    )
    check = cli._check(df, frozenset({"_source_file", "dropped_by_the_write"}))
    assert check.rows == 3
    assert check.null_month == 1
    assert check.null_ref_date == 1
    assert check.months == ("2026-06",)
    assert check.ref_dates == ("2026-06-13",)
    assert check.lost_columns == ("dropped_by_the_write",)
