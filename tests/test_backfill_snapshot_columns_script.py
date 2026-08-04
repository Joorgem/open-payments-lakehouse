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

WHAT IS NO LONGER HERE. The three pre-write refusals and the mask restoration left
this script for ``opl.bronze.backfill_prewrite`` and ``opl.bronze.backfill_masks``
when it went 248 lines over the 800-line ceiling, and their tests went with them,
to ``tests/test_backfill_prewrite.py`` and ``tests/test_backfill_masks.py``. What
stays here is the script: its arguments, its write, its post-write checks -- and
the end-to-end runs through ``main`` at the bottom, which are what prove the
refusals are wired into it at all rather than merely correct in isolation.
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


# --- a NULL reference date is reported, and not raised ------------------------


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


# --- constraint DDL is bronze DDL, and only bronze gets it ---------------------


class _CatalogRows:
    """What ``system.information_schema.column_masks`` hands back."""

    def __init__(self, columns: tuple[str, ...]) -> None:
        self._columns = columns

    def collect(self) -> list[dict[str, str]]:
        return [{"column_name": column} for column in self._columns]


class _RecordingSpark:
    """A ``SparkSession`` stand-in that records the SQL it is asked to run.

    Recording and not a no-op: the assertion that matters is that the list is
    EMPTY for a staging or quarantine target, and a no-op double cannot tell an
    unissued statement from an issued one.

    ``masked`` is what the CATALOG will report when asked, INDEPENDENTLY of the
    statements this session was handed. That independence is the whole of the mask
    regression test: a stand-in that answered the probe out of the ``SET MASK``
    statements it had just recorded could never express the failure being guarded
    against -- an ALTER that returns without error against a table whose metadata
    does not end up carrying the mask."""

    def __init__(self, masked: tuple[str, ...] = ()) -> None:
        self.statements: list[str] = []
        self._masked = masked

    def sql(self, statement: str) -> _CatalogRows | None:
        self.statements.append(statement)
        if "column_masks" in statement:
            return _CatalogRows(self._masked)
        return None


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


# --- the last line has to be true of the run that just happened ---------------


@pytest.mark.parametrize(
    ("wrote", "to_add"),
    [
        (True, ()),
        (True, ((SNAPSHOT_MONTH_COLUMN, "STRING"),)),
        (False, ((SNAPSHOT_MONTH_COLUMN, "STRING"),)),
    ],
)
def test_the_done_line_offers_the_restore_target_whenever_something_was_committed(
    wrote, to_add
):
    """Three of the four reachable ``(wrote, to_add)`` combinations committed
    something -- the overwrite, the ALTER, or both -- so a new version exists and
    ``version`` is genuinely the way back from it.

    Byte-identical to what F1.4a printed and what
    ``docs/f1.4a-migration-evidence.md`` quotes twice; an operator diffing this run
    against that record must not see a wording change."""
    assert cli._done_line(wrote=wrote, to_add=to_add, tbl="wh.d.b", version=2) == (
        "backfill: DONE. wh.d.b is at a new version; version 2 is the way back"
    )


def test_the_done_line_refuses_to_claim_a_version_that_was_never_committed():
    """The fourth combination: a REPEAT over an already-migrated 0-row target. No
    ALTER (``missing_columns`` is empty) and no write (nothing to fill), so nothing
    was committed at all -- and the other wording would name a version that does not
    exist AND offer ``version`` as the way back from nothing, both wrong at once, on
    the path a Databricks INTERNAL_ERROR retry takes."""
    line = cli._done_line(wrote=False, to_add=(), tbl="wh.d.q", version=9)
    assert "is at a new version" not in line
    assert "NOTHING was committed" in line
    # It still has to say where the table IS, or the operator is left guessing.
    assert "still at version 9" in line


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


# --- neither the ALTER nor the overwrite happens on a contradicting table ------
#
# The refusals themselves are pinned in ``tests/test_backfill_prewrite.py``, against
# the module they now live in. What these three prove is the WIRING: that ``main``
# reaches them BEFORE it issues the ALTER, and that a run whose month agrees still
# gets there. The filenames are restated here for that reason -- they are this
# file's input, not a copy of that file's fixtures.

_LANDING = "/Volumes/workspace/default/landing/cnpj"
_JUNE_FILE = f"{_LANDING}/2026-06/estabelecimentos/K3241.K03200Y0.D60613.ESTABELE"
_JULY_FILE = f"{_LANDING}/2026-07/estabelecimentos/K3241.K03200Y0.D60711.ESTABELE"


class _StoppedAfterTheAlter(Exception):
    """Raised by the stub the SECOND time ``spark.read.table`` is called.

    ``main`` reads the table once up front and once more to build the fill, so the
    second read is the first thing that happens AFTER the ALTER. Stopping there is
    what lets one stub express both directions: a run that reaches it got past
    every refusal, and a run that does not never issued the ALTER either.
    """


class _StubSession:
    """A ``SparkSession`` stand-in for ``main``: records SQL, hands out one frame.

    The frame is a REAL local DataFrame, so ``pre_write_scan``'s aggregate is the
    real aggregate over real filenames -- the refusal under test is made from the
    same expression the workspace would evaluate, not from a hand-written tuple.
    Only the session is faked, because ``main`` builds one and reads a Unity
    Catalog table by name.
    """

    def __init__(self, frame, history) -> None:
        self._frame = frame
        self._history = history
        self.statements: list[str] = []
        self.reads = 0

    @property
    def read(self) -> _StubSession:
        return self

    def table(self, _name: str):
        self.reads += 1
        if self.reads > 1:
            raise _StoppedAfterTheAlter
        return self._frame

    def sql(self, statement: str):
        self.statements.append(statement)
        return self._history if statement.startswith("DESCRIBE HISTORY") else None


def _run_main(monkeypatch, spark, paths: list[str], month: str) -> _StubSession:
    """``main`` against a LEGACY table -- ``_source_file`` and nothing else."""
    frame = spark.createDataFrame([(p,) for p in paths], ["_source_file"])
    session = _StubSession(frame, spark.createDataFrame([(3,)], ["version"]))
    monkeypatch.setattr(
        cli, "SparkSession", type("_B", (), {"builder": type("_G", (), {
            "getOrCreate": staticmethod(lambda: session)})()})
    )
    cli.main(["estabelecimentos", month, "--staging"])
    return session


def test_a_legacy_table_whose_files_are_another_month_reaches_no_ALTER_and_no_write(
    monkeypatch, spark
):
    """THE END-TO-END REGRESSION. A legacy table -- no ``_snapshot_month`` at all,
    which is the case this script exists for -- whose ``_source_file`` values carry
    2026-06, asked to stamp 2026-07.

    Before this guard the run proceeded: the ALTER added the columns, the overwrite
    stamped 2026-07 on 71.9M rows, every ``_snapshot_ref_date`` went NULL, and
    ``verify_or_raise`` PASSED, because the row count was unchanged and the month
    column it had just written held no NULLs. The only symptom was a warning after
    the commit.

    ``session.reads == 1`` is the overwrite half: ``main`` re-reads the table to
    build the fill, so a second read is the write path having been entered."""
    session = _run_main_expecting_refusal(monkeypatch, spark, [_JUNE_FILE] * 3, "2026-07")
    assert not any(s.startswith("ALTER TABLE") for s in session.statements)
    assert session.reads == 1


def test_a_table_holding_two_months_of_files_reaches_no_ALTER_either(monkeypatch, spark):
    """Mixed rather than mismatched, and the month asked for is one the files DO
    carry -- so the naive 'does any file agree?' test would have let this through."""
    session = _run_main_expecting_refusal(
        monkeypatch, spark, [_JUNE_FILE, _JULY_FILE], "2026-06"
    )
    assert not any(s.startswith("ALTER TABLE") for s in session.statements)
    assert session.reads == 1


def _run_main_expecting_refusal(monkeypatch, spark, paths, month) -> _StubSession:
    frame = spark.createDataFrame([(p,) for p in paths], ["_source_file"])
    session = _StubSession(frame, spark.createDataFrame([(3,)], ["version"]))
    monkeypatch.setattr(
        cli, "SparkSession", type("_B", (), {"builder": type("_G", (), {
            "getOrCreate": staticmethod(lambda: session)})()})
    )
    with pytest.raises(RuntimeError, match="NOTHING WAS WRITTEN"):
        cli.main(["estabelecimentos", month, "--staging"])
    return session


def test_the_same_table_with_an_agreeing_month_DOES_reach_the_ALTER(monkeypatch, spark):
    """THE OTHER DIRECTION, or the two tests above pass against a script that
    refuses everything. Identical table, identical stub, the month its filenames
    actually carry: the run gets past every refusal, issues the ALTER for both
    snapshot columns, and is stopped only by the stub at the fill's re-read."""
    with pytest.raises(_StoppedAfterTheAlter):
        _run_main(monkeypatch, spark, [_JUNE_FILE] * 3, "2026-06")
