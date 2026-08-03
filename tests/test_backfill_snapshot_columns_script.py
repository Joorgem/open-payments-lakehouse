# tests/test_backfill_snapshot_columns_script.py
"""Unit tests for scripts/backfill_snapshot_columns.py.

The verification logic is pure and tested without a session: what the backfill
has to get right is REFUSING, and every refusal is a decision made from counts.
Two tests take the shared Spark session because they pin assumptions about Delta
that no amount of pure Python can check -- that ``DESCRIBE HISTORY`` yields a
``version`` column, and that the one-pass aggregate reports what it claims to.

``_assert_constraints`` gets a recording stand-in rather than either, because its
load-bearing assertion is a NEGATIVE one -- that no constraint DDL reaches a
staging or quarantine table -- and a session cannot state that: the statements are
``ALTER TABLE workspace.default.*``, which exists only in the workspace.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from opl.bronze.registry import table_spec
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


# --- refusing to back-stamp rejects that are already in quarantine -------------

_ESTAB = "workspace.default.bronze_cnpj_estab_quarantine"


def test_a_quarantine_holding_rows_is_refused():
    """Adding the columns to an EMPTY quarantine is a schema migration. Filling them
    on rows that are already there asserts a month those rejects were never proven
    to belong to. Empresas (1 row) and socios (1,797) are the live cases."""
    with pytest.raises(RuntimeError) as exc:
        cli.refuse_non_empty_quarantine(
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
    cli.refuse_non_empty_quarantine(
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
    cli.refuse_non_empty_quarantine(
        71_874_448, month="2026-06", tbl=tbl, spec=table_spec("estabelecimentos")
    )


def test_the_guard_is_about_the_quarantine_of_the_table_it_was_given():
    """Keyed on the resolved name against THIS spec's quarantine, so estab's
    quarantine is not refused while socios' is being backfilled, and vice versa."""
    socios = table_spec("socios")
    # estab's quarantine, socios' spec: not this run's quarantine, so not refused.
    cli.refuse_non_empty_quarantine(1797, month="2026-06", tbl=_ESTAB, spec=socios)
    with pytest.raises(RuntimeError, match="1797 row\\(s\\)"):
        cli.refuse_non_empty_quarantine(
            1797,
            month="2026-06",
            tbl="workspace.default.bronze_cnpj_socios_quarantine",
            spec=socios,
        )


# --- constraint DDL is bronze DDL, and only bronze gets it ---------------------


class _RecordingSpark:
    """A ``SparkSession`` stand-in that records the SQL it is asked to run.

    Recording and not a no-op: the assertion that matters is that the list is
    EMPTY for a staging or quarantine target, and a no-op double cannot tell an
    unissued statement from an issued one."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_constraints_are_re_asserted_on_bronze_with_the_table_filled_in():
    """The reason this function exists: ``mode('overwrite')`` may be planned as a
    table REPLACE, which drops CHECK constraints and NOT NULL with it."""
    spec = table_spec("estabelecimentos")
    # Or the two assertions below are `0 == 0` and `all([])`, which is the one shape
    # that would let a "never assert on bronze" regression through green.
    assert spec.constraints
    spark = _RecordingSpark()
    tbl = "workspace.default.bronze_cnpj_estabelecimentos"
    cli._assert_constraints(spark, spec, tbl)
    assert len(spark.statements) == len(spec.constraints)
    # Formatted, and against the table that was passed -- an unformatted `{table}`
    # would be a syntax error and a table name from anywhere else would be a
    # constraint asserted on something nobody named.
    assert all(tbl in statement and "{table}" not in statement for statement in spark.statements)


@pytest.mark.parametrize(
    "tbl",
    [
        "workspace.default.bronze_cnpj_estab_staging",
        "workspace.default.bronze_cnpj_estab_quarantine",
    ],
)
def test_no_constraint_ddl_is_issued_against_staging_or_quarantine(tbl, capsys):
    """``spec.constraints`` is BRONZE DDL and asserting it here would plant a
    landmine that this run's own output would report as success.

    estab's set is ``cnpj_basico SET NOT NULL`` plus
    ``CHECK (length(trim(cnpj_basico)) = 8)``, and ``opl.bronze.rules`` has a
    ``null_or_empty_cnpj_basico`` and a ``bad_cnpj_basico_length`` rule *because
    rows like that arrive* -- into staging. On staging the constraint fails the
    ingest write for a row the gate exists to reject; on quarantine it fails the
    gate's append of that very reject. Neither table has ever carried a constraint,
    so the overwrite dropped nothing there to re-assert."""
    spark = _RecordingSpark()
    cli._assert_constraints(spark, table_spec("estabelecimentos"), tbl)
    assert spark.statements == []
    out = capsys.readouterr().out
    # Silence would leave an operator comparing this run against F1.4a's output
    # unable to see that the missing line was a decision.
    assert "NOT issued" in out and tbl in out


# --- no write where there is nothing to write ---------------------------------


class _RecordingFrame:
    """A ``DataFrame`` stand-in that records the ``_fill``-then-write chain.

    The local Delta session CANNOT execute ``mode("overwrite").saveAsTable`` at all
    -- it raises ``AnalysisException: Table X does not support truncate in batch
    mode`` -- so a real table cannot pin either side of this decision. What has to
    be pinned is that the 0-row case issues NO write, and a recorder states that
    directly. It still takes the ``spark`` fixture, because ``_fill`` builds real
    Column expressions and ``F.lit`` needs the JVM."""

    def __init__(self) -> None:
        self.stamped: list[str] = []
        self.saved: list[str] = []
        self.formats: list[str] = []
        self.modes: list[str] = []

    def withColumn(self, name: str, _column) -> _RecordingFrame:  # noqa: N802
        self.stamped.append(name)
        return self

    @property
    def write(self) -> _RecordingFrame:
        return self

    def format(self, value: str) -> _RecordingFrame:
        self.formats.append(value)
        return self

    def mode(self, value: str) -> _RecordingFrame:
        self.modes.append(value)
        return self

    def saveAsTable(self, name: str) -> None:  # noqa: N802
        self.saved.append(name)


def test_no_write_is_issued_when_there_are_no_rows_to_fill(spark, capsys):
    """At 0 rows the ALTER is the whole migration, and issuing the overwrite anyway
    risks the table's METADATA for no gain: this file's own ``_assert_constraints``
    records that ``mode('overwrite')`` may be planned as ``AtomicReplaceTableAsSelect``,
    which drops CHECK constraints, NOT NULL -- and a UC column mask on the masked
    contracts. ``_assert_constraints`` now returns early for a non-bronze target, so
    the script's only metadata re-assertion does not cover the one target where a
    0-row write is the normal case."""
    frame = _RecordingFrame()
    assert (
        cli.fill_and_overwrite(frame, month="2026-06", tbl="wh.d.q", rows=0) is False
    )
    assert frame.saved == [] and frame.stamped == []
    out = capsys.readouterr().out
    assert "no write was issued" in out and "wh.d.q" in out


def test_the_overwrite_runs_when_there_are_rows_to_fill(spark):
    """The other direction, or the skip above is a script that never writes."""
    frame = _RecordingFrame()
    assert cli.fill_and_overwrite(frame, month="2026-06", tbl="wh.d.b", rows=71_874_448) is True
    assert frame.saved == ["wh.d.b"]
    # Both columns stamped, and the write shape unchanged from what F1.4a ran.
    assert frame.stamped == [SNAPSHOT_MONTH_COLUMN, SNAPSHOT_REF_DATE_COLUMN]
    assert frame.formats == ["delta"] and frame.modes == ["overwrite"]


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


# --- which of the table's three roles this run writes -------------------------


def test_the_target_defaults_to_bronze_so_existing_invocations_are_unchanged():
    """The two-argument form is what F1.4a ran against a 71.9M-row table and what
    the migration doc tells an operator to re-run. It must not change meaning."""
    assert cli.resolve_target(["estabelecimentos", "2026-06"]) == (
        "workspace.default.bronze_cnpj_estabelecimentos", "2026-06")


def test_bronze_can_be_named_explicitly():
    """The refusal message offers ``--bronze`` as one of the three, so it has to be
    accepted -- a message naming a flag that is then refused sends the operator
    reading it somewhere wrong."""
    assert cli.resolve_target(["estabelecimentos", "2026-06", "--bronze"]) == (
        "workspace.default.bronze_cnpj_estabelecimentos", "2026-06")


def test_staging_can_be_named_explicitly():
    assert cli.resolve_target(["estabelecimentos", "2026-06", "--staging"]) == (
        "workspace.default.bronze_cnpj_estab_staging", "2026-06")


def test_quarantine_can_be_named_explicitly():
    """A valid target: estab's quarantine is 36 columns with neither snapshot column
    and 0 rows, and the DQ gate's reject append (38 cols into 36) fails on it --
    proven to raise ``_LEGACY_ERROR_TEMP_DELTA_0007`` even against a 0-row table."""
    assert cli.resolve_target(["estabelecimentos", "2026-06", "--quarantine"]) == (
        "workspace.default.bronze_cnpj_estab_quarantine", "2026-06")


def test_the_role_is_resolved_against_the_table_that_was_named():
    """Not against estabelecimentos, and not against a fixed month: a resolver that
    ignored either argument would satisfy every test above."""
    assert cli.resolve_target(["socios", "2026-07", "--quarantine"]) == (
        "workspace.default.bronze_cnpj_socios_quarantine", "2026-07")


def test_an_unknown_role_is_refused_before_spark():
    """A role that is not one of the three is refused rather than ignored: ignoring
    it would overwrite bronze while the operator watched for the table they asked
    for, and this write is an overwrite, so the wrong target is not a no-op."""
    with pytest.raises(ValueError, match="not a backfill target"):
        cli.resolve_target(["estabelecimentos", "2026-06", "--bronze-ish"])


def test_a_role_flag_where_the_month_should_be_is_refused_as_a_month():
    """``estabelecimentos --staging`` is two arguments, so arity cannot catch it.
    It must not be read as a month, and the message has to name the value it
    rejected -- an operator who typed the flag one position early has to be able to
    see WHICH argument was read as a month."""
    with pytest.raises(ValueError, match="refusing to backfill the snapshot columns") as exc:
        cli.resolve_target(["estabelecimentos", "--staging"])
    assert "'--staging'" in str(exc.value)


@pytest.mark.parametrize("argv", [[], ["estabelecimentos"], ["a", "b", "c", "d"]])
def test_resolve_target_refuses_the_wrong_number_of_arguments(argv):
    with pytest.raises(ValueError, match="usage:"):
        cli.resolve_target(argv)


# --- arguments are refused before Spark --------------------------------------


# Three arguments is no longer wrong -- the third names the target role -- so the
# too-many case has to be four.
@pytest.mark.parametrize("argv", [[], ["estabelecimentos"], ["a", "b", "c", "d"]])
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
