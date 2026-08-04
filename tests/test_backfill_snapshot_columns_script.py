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
    cli.refuse_contradicting_source_month(("6-06",), month="2026-06", tbl="wh.d.b")
    cli.refuse_contradicting_source_month((), month="2026-06", tbl="wh.d.b")


def test_a_legacy_table_whose_files_carry_another_month_is_refused():
    """The state neither internal review caught. Nothing here has a
    ``_snapshot_month`` to contradict, so this is the only evidence there is."""
    with pytest.raises(RuntimeError) as exc:
        cli.refuse_contradicting_source_month(("6-06",), month="2026-07", tbl="wh.d.b")
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
            cli.refuse_contradicting_source_month(
                ("6-06", "6-07"), month=month, tbl="wh.d.b"
            )


def test_the_mixed_refusal_says_the_named_month_was_present_too():
    """``6-06`` and ``6-07`` with ``2026-06`` requested: listing only the
    contradicting one would read as 'no file agrees', which is a different
    incident from 'this table holds two months'."""
    with pytest.raises(RuntimeError) as exc:
        cli.refuse_contradicting_source_month(
            ("6-06", "6-07"), month="2026-06", tbl="wh.d.b"
        )
    assert "as well as '6-06'" in str(exc.value)


def test_the_decade_cannot_be_used_to_smuggle_a_disagreement_past_the_guard():
    """The token carries one year digit, so ``2016-06`` and ``2026-06`` produce the
    same key. That is a property of the filename, not a hole: ``_snapshot_month``
    sits beside the date carrying the operator's own decade, which is the
    ``ref_date_column`` bargain, and this guard must not pretend to close it."""
    cli.refuse_contradicting_source_month(("6-06",), month="2016-06", tbl="wh.d.b")


def test_the_guard_still_holds_on_the_idempotent_repeat():
    """``refuse_contradicting_month`` deliberately permits re-running with the month
    the table already carries. An idempotent repeat of a stamp that was WRONG is
    still wrong, so this guard is not scoped to legacy tables."""
    with pytest.raises(RuntimeError, match="NOTHING WAS WRITTEN"):
        cli.refuse_contradicting_source_month(("6-06",), month="2026-07", tbl="wh.d.b")


def test_a_malformed_month_is_refused_by_the_key_rather_than_matching_nothing():
    """Reached only by a direct caller -- ``resolve_target`` has already been through
    ``require_month`` -- but a key built from a malformed month would silently equal
    no filename at all, and the refusal would then name the wrong cause."""
    with pytest.raises(ValueError, match="YYYY-MM"):
        cli.refuse_contradicting_source_month(("6-06",), month="2026-6", tbl="wh.d.b")


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


# --- the PII mask the overwrite may have dropped ------------------------------
#
# ``mode("overwrite").saveAsTable`` is planned either as an overwrite of the DATA
# or as a replace of the TABLE, and a replace drops CHECK constraints, NOT NULL
# *and a UC column mask*. ``_assert_constraints`` put the first two back and
# nothing put the third back -- which left the PII control as the one piece of
# metadata resting on Unity Catalog choosing the other plan.

_SOCIOS = table_spec("socios")
_SOCIOS_BRONZE = "workspace.default.bronze_cnpj_socios"
_SOCIOS_STAGING = "workspace.default.bronze_cnpj_socios_staging"
_SOCIOS_QUARANTINE = "workspace.default.bronze_cnpj_socios_quarantine"
_BOTH_NAMES = ("nome_socio_razao_social", "nome_do_representante")


@pytest.mark.parametrize("tbl", [_SOCIOS_BRONZE, _SOCIOS_QUARANTINE])
def test_both_masked_tables_get_their_masks_back_after_a_populated_overwrite(tbl):
    """Bronze AND quarantine, which is where this differs from
    ``_assert_constraints``: those statements are bronze DDL and return early off
    bronze, so a quarantine backfill had nothing at all restoring its metadata."""
    spark = _RecordingSpark()
    assert cli.restore_masks(spark, _SOCIOS, tbl, wrote=True) == _BOTH_NAMES
    # The function FIRST -- a ``SET MASK`` naming a routine that does not exist
    # fails the ALTER, which is ``ensure_masked_table``'s own ordering.
    assert spark.statements[0].startswith("CREATE OR REPLACE FUNCTION")
    masks = [s for s in spark.statements if "SET MASK" in s]
    assert len(masks) == len(_BOTH_NAMES)
    for column in _BOTH_NAMES:
        assert any(f"ALTER TABLE {tbl} ALTER COLUMN `{column}` SET MASK" in s for s in masks)


def test_the_ddl_is_the_masking_modules_and_not_a_second_spelling():
    """Byte-identical to what ``ensure_masked_table`` issues, because both ask
    ``opl.bronze.masking``. Two spellings of a mask statement is two things that
    can drift, on the one control whose drift is personal names in the clear."""
    from opl.bronze.masking import MASK_FUNCTION, mask_function_ddl, set_mask_ddl
    from opl.config import DEFAULT

    spark = _RecordingSpark()
    cli.restore_masks(spark, _SOCIOS, _SOCIOS_BRONZE, wrote=True)
    function = DEFAULT.table(MASK_FUNCTION)
    assert spark.statements == [mask_function_ddl(function)] + [
        set_mask_ddl(_SOCIOS_BRONZE, column, function) for column in _BOTH_NAMES
    ]


def test_staging_is_never_masked_by_this_script(capsys):
    """THE DIRECTION THAT MATTERS MOST, and it is not the one CodeRabbit raised.
    ``promote_batch`` READS staging and writes what it read into bronze, so a mask
    there would put ``***`` into bronze permanently and would make the DQ gate stop
    rejecting rows with a missing name -- the 1,797 rows that rule caught in the
    live run would have landed. ``required_masks`` asks ``masked_table_ddls``
    which tables are covered rather than listing them again here, so a restoration
    that grew a third table would have to be argued for in the module that argues
    about it."""
    spark = _RecordingSpark()
    assert cli.required_masks(_SOCIOS, _SOCIOS_STAGING) == ()
    assert cli.restore_masks(spark, _SOCIOS, _SOCIOS_STAGING, wrote=True) == ()
    assert spark.statements == []
    assert "no column mask to restore" in capsys.readouterr().out


def test_an_unmasked_contract_issues_no_mask_ddl_at_all(capsys):
    """estabelecimentos is every live run this script has had. Restoration must be
    a no-op there, and must SAY so -- an operator has to be able to tell 'this
    table carries no mask' from 'the restoration did not run'."""
    spark = _RecordingSpark()
    estab = "workspace.default.bronze_cnpj_estabelecimentos"
    assert cli.restore_masks(spark, table_spec("estabelecimentos"), estab, wrote=True) == ()
    assert spark.statements == []
    assert "declares no masked column" in capsys.readouterr().out


def test_nothing_is_re_applied_when_no_overwrite_was_issued(capsys):
    """The ``ALTER TABLE ADD COLUMNS`` is a metadata commit that does not replace
    the table, so ``wrote=False`` is not a state in which a mask can have been
    dropped -- and it is the 0-row quarantine path ``fill_and_overwrite`` skips."""
    spark = _RecordingSpark()
    assert cli.restore_masks(spark, _SOCIOS, _SOCIOS_BRONZE, wrote=False) == ()
    assert spark.statements == []
    assert "cannot have been replaced" in capsys.readouterr().out


def test_the_catalog_is_asked_about_this_table_only():
    """Verification, not the fix. A ``SET MASK`` returning without error proves the
    ALTER was accepted; it does not prove the catalog reports a mask on the column
    an operator will read from."""
    sql = cli.mask_probe_sql(_SOCIOS_BRONZE)
    assert "system.information_schema.column_masks" in sql
    assert "table_catalog = 'workspace'" in sql
    assert "table_schema = 'default'" in sql
    assert "table_name = 'bronze_cnpj_socios'" in sql
    # Not the staging table, whose masks would be a defect rather than a match.
    assert "staging" not in sql


def test_observed_masks_reads_the_column_names_the_catalog_returned():
    spark = _RecordingSpark(masked=_BOTH_NAMES)
    assert cli.observed_masks(spark, _SOCIOS_BRONZE) == frozenset(_BOTH_NAMES)


def test_a_catalog_that_agrees_is_accepted():
    cli.verify_masks_or_raise(
        frozenset(_BOTH_NAMES), _BOTH_NAMES, tbl=_SOCIOS_BRONZE, version=7
    )


def test_the_run_REFUSES_to_report_success_when_a_mask_did_not_come_back():
    """THE REGRESSION CodeRabbit asked for, exercised through the real path: the
    statements are issued and the CATALOG still does not carry one of them.

    ``_RecordingSpark`` answers the probe from its own ``masked`` argument and not
    from the statements it recorded, so this states exactly the failure being
    guarded against -- an ALTER that returns without error against a table whose
    metadata does not end up carrying the mask. A stand-in that echoed the
    statements back could not express it at all."""
    spark = _RecordingSpark(masked=("nome_socio_razao_social",))
    restored = cli.restore_masks(spark, _SOCIOS, _SOCIOS_BRONZE, wrote=True)
    with pytest.raises(RuntimeError) as exc:
        cli.verify_masks_or_raise(
            cli.observed_masks(spark, _SOCIOS_BRONZE),
            restored,
            tbl=_SOCIOS_BRONZE,
            version=42,
        )
    message = str(exc.value)
    # The column that is unmasked, and not the one that is.
    assert "nome_do_representante" in message
    # What the state actually IS, in the words an operator needs to act on.
    assert "READABLE IN" in message and "THE CLEAR" in message
    # The supported repair, and what RESTORE does and does not fix.
    assert "ensure_masked_table.py" in message
    assert f"RESTORE TABLE {_SOCIOS_BRONZE} TO VERSION AS OF 42" in message
    assert "RESTORE moves the DATA and does not put a mask back" in message
    # And what the catalog does hold, so the operator is not left guessing.
    assert "nome_socio_razao_social" in message


def test_an_empty_catalog_answer_fails_every_required_column():
    """A table replace that dropped both, and a re-application that did not take."""
    with pytest.raises(RuntimeError) as exc:
        cli.verify_masks_or_raise(
            frozenset(), _BOTH_NAMES, tbl=_SOCIOS_BRONZE, version=1
        )
    assert all(column in str(exc.value) for column in _BOTH_NAMES)
    assert "(none)" in str(exc.value)


def test_a_masked_contract_declares_no_check_constraint_so_the_order_is_safe():
    """Why the mask goes on BEFORE ``_assert_constraints`` rather than after. UC
    refuses a CHECK on a table carrying a column mask, so masking first would break
    the constraint DDL for any masked contract that declared one -- and the
    registry refuses that combination at import. Restated from this file because it
    is what makes the ordering chosen here safe, and the ordering is what keeps the
    PII window from sitting behind a statement that re-validates metadata over the
    whole table."""
    from opl.bronze.masking import MASKED_COLUMNS

    assert MASKED_COLUMNS.get(_SOCIOS.contract)
    assert not any("CHECK" in statement.upper() for statement in _SOCIOS.constraints)


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
    scan = cli.pre_write_scan(df)
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
    scan = cli.pre_write_scan(df)
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
    scan = cli.pre_write_scan(df)
    assert scan.rows == 3
    assert scan.source_months == ("6-06", "6-07")
    # The untokenised file is absent from the set rather than disagreeing with it,
    # and counted separately.
    assert scan.rows_without_source_month == 1


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
