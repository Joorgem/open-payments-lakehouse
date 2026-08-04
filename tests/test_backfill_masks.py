# tests/test_backfill_masks.py
"""Unit tests for opl.bronze.backfill_masks.

``mode("overwrite").saveAsTable`` is planned either as an overwrite of the DATA or
as a replace of the TABLE, and a replace drops CHECK constraints, NOT NULL *and a
UC column mask*. The script's ``_assert_constraints`` puts the first two back;
this module is what puts the third back and then refuses to report success while
the catalog disagrees.

No workspace and no session: ``_RecordingSpark`` records the SQL it is handed and
answers the catalog probe from its own ``masked`` argument, INDEPENDENTLY of those
statements -- which is the only way to state the failure being guarded against, an
ALTER that returns without error against a table whose metadata does not end up
carrying the mask.

``_CatalogRows`` and ``_RecordingSpark`` are restated here rather than shared:
``tests/test_backfill_snapshot_columns_script.py`` still needs them for
``_assert_constraints``, which stayed in the script, and a doubles module imported
across two test files by filename would be the first ``sys.path`` assumption in a
suite that deliberately has none.
"""
from __future__ import annotations

import pytest

from opl.bronze.backfill_masks import (
    mask_probe_sql,
    observed_masks,
    required_masks,
    restore_masks,
    verify_masks_or_raise,
)
from opl.bronze.registry import table_spec


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
    assert restore_masks(spark, _SOCIOS, tbl, wrote=True) == _BOTH_NAMES
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
    restore_masks(spark, _SOCIOS, _SOCIOS_BRONZE, wrote=True)
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
    assert required_masks(_SOCIOS, _SOCIOS_STAGING) == ()
    assert restore_masks(spark, _SOCIOS, _SOCIOS_STAGING, wrote=True) == ()
    assert spark.statements == []
    assert "no column mask to restore" in capsys.readouterr().out


def test_an_unmasked_contract_issues_no_mask_ddl_at_all(capsys):
    """estabelecimentos is every live run this script has had. Restoration must be
    a no-op there, and must SAY so -- an operator has to be able to tell 'this
    table carries no mask' from 'the restoration did not run'."""
    spark = _RecordingSpark()
    estab = "workspace.default.bronze_cnpj_estabelecimentos"
    assert restore_masks(spark, table_spec("estabelecimentos"), estab, wrote=True) == ()
    assert spark.statements == []
    assert "declares no masked column" in capsys.readouterr().out


def test_nothing_is_re_applied_when_no_overwrite_was_issued(capsys):
    """The ``ALTER TABLE ADD COLUMNS`` is a metadata commit that does not replace
    the table, so ``wrote=False`` is not a state in which a mask can have been
    dropped -- and it is the 0-row quarantine path ``fill_and_overwrite`` skips."""
    spark = _RecordingSpark()
    assert restore_masks(spark, _SOCIOS, _SOCIOS_BRONZE, wrote=False) == ()
    assert spark.statements == []
    assert "cannot have been replaced" in capsys.readouterr().out


def test_the_catalog_is_asked_about_this_table_only():
    """Verification, not the fix. A ``SET MASK`` returning without error proves the
    ALTER was accepted; it does not prove the catalog reports a mask on the column
    an operator will read from."""
    sql = mask_probe_sql(_SOCIOS_BRONZE)
    assert "system.information_schema.column_masks" in sql
    assert "table_catalog = 'workspace'" in sql
    assert "table_schema = 'default'" in sql
    assert "table_name = 'bronze_cnpj_socios'" in sql
    # Not the staging table, whose masks would be a defect rather than a match.
    assert "staging" not in sql


def test_observed_masks_reads_the_column_names_the_catalog_returned():
    spark = _RecordingSpark(masked=_BOTH_NAMES)
    assert observed_masks(spark, _SOCIOS_BRONZE) == frozenset(_BOTH_NAMES)


def test_a_catalog_that_agrees_is_accepted():
    verify_masks_or_raise(
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
    restored = restore_masks(spark, _SOCIOS, _SOCIOS_BRONZE, wrote=True)
    with pytest.raises(RuntimeError) as exc:
        verify_masks_or_raise(
            observed_masks(spark, _SOCIOS_BRONZE),
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
        verify_masks_or_raise(
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
