# tests/bronze/test_promote.py
"""Promotion of one batch into bronze: idempotence and the operator guards.

The Spark tests here write real local Delta tables rather than using doubles,
because what they assert is a property of the Delta write itself: whether a
second append duplicates a batch. They are slow (~40 s each on a local session),
so there are as few as the properties allow; everything that can be proven
without a session is proven without one below.

The whole promote POLICY -- which of the reachable (bronze count, staging count)
states appends, which skips, which refuses -- is a pure function of two counts
(`plan_promotion`), so every state is covered here without a session. Only the
states whose reachability depends on real Delta behaviour get a Spark test.

Both defects were verified on the live workspace before this module existed:
promoting the same `_batch_id` twice doubled the rows and exited 0 both times,
and running the operator job with no `--params` matched no staging row, appended
nothing and also exited 0 -- SUCCESS for a batch it never promoted.
"""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from opl.bronze.promote import (
    SENTINEL_BATCH_ID,
    PromoteOutcome,
    PromoteRefused,
    plan_promotion,
    promote_batch,
    require_batch_id,
)
from opl.bronze.rules import rules_for
from opl.contracts.cnpj_schemas import columns_for

_ESTAB_RULES = rules_for("estabelecimentos")

# THE FULL 30-COLUMN CONTRACT, not the five columns the old rules happened to
# name. F1.4b folded the U+FFFD check over every column of the contract instead
# of a hand-picked pair, so `_ESTAB_RULES` now resolves all thirty and a five-
# column projection fails to analyse. That is the intended coupling and not an
# accident: a staging table missing a contract column is a broken ingest, and a
# rule set that silently skipped the columns it could not find would report a
# clean gate over data it never looked at. Derived from `columns_for` rather
# than restated, so the contract cannot drift away from the fixture.
_ESTAB_COLUMNS = columns_for("estabelecimentos")

_STAGING_SCHEMA = StructType(
    [StructField(name, StringType()) for name in (*_ESTAB_COLUMNS, "_batch_id")]
)


def _estab_row(cnpj_basico: str, batch_id: str) -> tuple[str, ...]:
    """One full-contract staging row. Every column non-blank so that the ONLY
    thing distinguishing `_good` from `_rejected` is `cnpj_basico`'s length --
    `municipio` in particular is now a required field, so leaving it empty would
    reject every row here for a reason these tests are not about."""
    values = {name: "X" for name in _ESTAB_COLUMNS}
    values.update(
        cnpj_basico=cnpj_basico, cnpj_ordem="0001", cnpj_dv="95",
        nome_fantasia="PADARIA AÇAÍ", logradouro="RUA A", municipio="7107",
    )
    return (*(values[name] for name in _ESTAB_COLUMNS), batch_id)


def _good(cnpj_basico: str, batch_id: str) -> tuple[str, ...]:
    return _estab_row(cnpj_basico, batch_id)


def _rejected(batch_id: str) -> tuple[str, ...]:
    # 7 chars -> bad_cnpj_basico_length, the reason both real F1.3 rejects had.
    return _estab_row("1234567", batch_id)


class _ExplodingSpark:
    """Any attribute access fails: a refusal must reach no table at all."""

    def __getattr__(self, name: str):
        raise AssertionError(f"a refused promote must not touch Spark (read .{name})")


@pytest.mark.parametrize("batch_id", [SENTINEL_BATCH_ID, "", "   ", None])
def test_a_batch_id_that_names_no_batch_is_refused_before_spark(batch_id):
    """A job-parameter default is not validation. The sentinel used to be treated
    as any other id: it matched no staging row, appended nothing and exited 0."""
    with pytest.raises(PromoteRefused, match="batch_id"):
        promote_batch(
            _ExplodingSpark(), batch_id,
            staging_table="s", bronze_table="b", rules=_ESTAB_RULES,
        )


def test_the_refusal_tells_the_operator_how_to_find_the_batch_id():
    """...and how to pass it, WITH its table.

    The `table=` half was added in F1.4: promote_batch now takes the table first,
    and the operator job's `table` parameter has a real default, so a copy-pasted
    `--params batch_id=<id>` silently repromotes against whatever that default
    names. This is the message a triager reads mid-incident, so an invocation it
    can paste has to be the CURRENT one -- a stale one here is the same class of
    defect as the hardcoded quarantine name that sent estab triagers to the
    lookup table."""
    with pytest.raises(PromoteRefused) as excinfo:
        require_batch_id(SENTINEL_BATCH_ID)
    message = str(excinfo.value)
    assert "dq_gate_batch" in message
    assert "--params table=estabelecimentos,batch_id=" in message


def test_a_real_batch_id_is_accepted_and_stripped():
    assert require_batch_id(" 315230730740144 ") == "315230730740144"


def _plan(*, bronze_rows=0, promotable=0, rejected=0, in_flow=False):
    return plan_promotion(
        "b1",
        bronze_rows=bronze_rows,
        staged_promotable=promotable,
        staged_rejected=rejected,
        in_flow=in_flow,
        staging_table="stg",
        bronze_table="bronze",
    )


def test_bronze_holding_every_promotable_row_of_the_batch_is_already_promoted():
    result = _plan(bronze_rows=2, promotable=2, rejected=1)

    assert result.outcome is PromoteOutcome.ALREADY_PROMOTED
    assert (result.appended_rows, result.rejected_rows, result.bronze_rows) == (0, 1, 2)


def test_bronze_holding_only_part_of_the_batch_is_refused_with_both_counts():
    """The presence check used to collapse this count into a boolean: bronze
    holding a strict SUBSET of the batch reported "already promoted", the missing
    rows were never appended and the task exited 0. Reachable: the documented
    rebuild drops bronze and leaves staging, so bronze's count for a
    pre-rebuild `_batch_id` can legitimately differ from staging's."""
    with pytest.raises(PromoteRefused) as excinfo:
        _plan(bronze_rows=1, promotable=2)

    message = str(excinfo.value)
    assert "1 row" in message and "2 promotable" in message


def test_bronze_holding_more_rows_than_the_batch_can_promote_is_refused():
    """The other side of the same mismatch: an older build that appended twice,
    or a DQ rule tightened since the batch was promoted."""
    with pytest.raises(PromoteRefused, match="4 row"):
        _plan(bronze_rows=4, promotable=2)


def test_an_already_promoted_batch_is_recognised_when_staging_no_longer_holds_it():
    """Staging-based refusals used to run first, so a batch whose staging rows
    are gone (truncated, or the rebuild that drops staging) could never reach the
    idempotent skip: the promote task was permanently un-repairable."""
    result = _plan(bronze_rows=2)

    assert result.outcome is PromoteOutcome.ALREADY_PROMOTED_STAGING_GONE
    assert (result.appended_rows, result.bronze_rows) == (0, 2)


def test_a_reject_count_that_cannot_be_re_derived_is_unknown_and_not_zero():
    """The reject count comes from staging, and this outcome is the state where
    staging no longer holds the batch -- so there is no number to report. It used to
    be filled in with 0, a placeholder for "unknown" that only the docstring
    distinguished from a count: `promote_batch.py` printed it as "0 rejected row(s)
    ... stay in quarantine", telling an operator the batch had no rejects when the
    quarantine may well hold some. `None` makes the difference part of the type, so
    the next caller has to decide what to do with it instead of doing arithmetic on a
    zero that means nothing."""
    assert _plan(bronze_rows=2).rejected_rows is None
    # Contrast: every other outcome reports a count it actually derived.
    assert _plan(bronze_rows=2, promotable=2, rejected=1).rejected_rows == 1


def test_an_in_flow_batch_that_ingested_nothing_is_a_successful_no_op():
    """A scheduled run with no new files ingests nothing, and that empty batch
    still reaches promote. It used to be refused -- the pipeline's only
    legitimate no-op path ended FAILED."""
    result = _plan(in_flow=True)

    assert result.outcome is PromoteOutcome.NOTHING_INGESTED
    assert (result.appended_rows, result.rejected_rows) == (0, 0)


def test_an_operator_batch_id_that_names_nothing_is_still_refused():
    with pytest.raises(PromoteRefused, match="no row"):
        _plan(in_flow=False)


def test_a_batch_whose_every_row_is_rejected_is_refused_even_in_flow():
    with pytest.raises(PromoteRefused, match="rejected"):
        _plan(promotable=0, rejected=3, in_flow=True)


def test_a_batch_absent_from_bronze_is_planned_for_append():
    result = _plan(promotable=2, rejected=1)

    assert result.outcome is PromoteOutcome.APPENDED
    assert (result.appended_rows, result.rejected_rows) == (2, 1)


@pytest.fixture
def tables(spark, tmp_path):
    """A throwaway Delta database per test, so `saveAsTable` writes real managed
    tables (with a real transaction log) under tmp_path instead of into the
    repo's spark-warehouse."""
    db = f"promote_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE {db} LOCATION '{tmp_path.as_uri()}'")
    yield SimpleNamespace(staging=f"{db}.staging", bronze=f"{db}.bronze")
    spark.sql(f"DROP DATABASE {db} CASCADE")


def _stage(spark, table: str, rows: list[tuple[str, ...]]) -> None:
    (spark.createDataFrame(rows, _STAGING_SCHEMA)
     .write.format("delta").mode("append").saveAsTable(table))


def _promote(spark, tables, batch_id):
    return promote_batch(
        spark, batch_id,
        staging_table=tables.staging, bronze_table=tables.bronze, rules=_ESTAB_RULES,
    )


def _bronze_batches(spark, tables) -> list[str]:
    return sorted(r._batch_id for r in spark.read.table(tables.bronze).collect())


def test_promoting_the_same_batch_twice_appends_it_once(spark, tables):
    """The repair-run scenario: `promote` commits its append, the constraint DDL
    after it fails, the task is marked FAILED with the rows already in bronze,
    and a Databricks repair run re-executes it with the same {{job.run_id}}.
    Without a guard the batch lands twice and the run reports SUCCESS."""
    _stage(spark, tables.staging,
           [_good("12345678", "b1"), _good("87654321", "b1"), _rejected("b1")])

    first = _promote(spark, tables, "b1")
    second = _promote(spark, tables, "b1")

    # The reject count is reported on both passes: the operator job promotes a
    # batch whose rejects a human agreed to accept, so the number is log-worthy.
    assert (first.appended_rows, first.rejected_rows, first.outcome) \
        == (2, 1, PromoteOutcome.APPENDED)
    assert (second.appended_rows, second.rejected_rows, second.outcome) \
        == (0, 1, PromoteOutcome.ALREADY_PROMOTED)
    assert _bronze_batches(spark, tables) == ["b1", "b1"]  # not four rows


def test_idempotence_is_per_batch_and_a_refusal_leaves_bronze_alone(spark, tables):
    """Two distinct properties of the same table state: the guard keys on
    `_batch_id`, not on "bronze already has rows"; and a refused promote must
    change nothing -- a parameterless accident used to re-validate the CHECK
    constraint over all 71.9M bronze rows, with a window holding no constraint."""
    _stage(spark, tables.staging, [_good("12345678", "b1"), _good("87654321", "b2")])

    _promote(spark, tables, "b1")
    _promote(spark, tables, "b2")
    with pytest.raises(PromoteRefused):
        _promote(spark, tables, SENTINEL_BATCH_ID)

    assert _bronze_batches(spark, tables) == ["b1", "b2"]


def test_a_batch_id_matching_no_staging_row_is_refused(spark, tables):
    _stage(spark, tables.staging, [_good("12345678", "b1")])

    with pytest.raises(PromoteRefused, match="b-typo"):
        _promote(spark, tables, "b-typo")

    assert not spark.catalog.tableExists(tables.bronze)


def test_a_batch_whose_every_row_is_rejected_is_refused(spark, tables):
    """Nothing to promote is not success either: no path may end green having
    appended zero rows, except the idempotent re-run above."""
    _stage(spark, tables.staging, [_rejected("b1"), _rejected("b1")])

    with pytest.raises(PromoteRefused, match="rejected"):
        _promote(spark, tables, "b1")

    assert not spark.catalog.tableExists(tables.bronze)


def test_a_partial_bronze_is_refused_and_a_promoted_batch_survives_staging_loss(spark, tables):
    """Two states the old boolean presence check got wrong, against real tables,
    proving the counts are read from the tables the caller named:

    * bronze holds ONE of batch b1's two promotable rows -- reported "already
      promoted", so the missing row was silently never appended;
    * batch b2 is fully in bronze but no longer in staging -- the staging-based
      refusals ran first, so the skip was unreachable and the task un-repairable.
    """
    _stage(spark, tables.staging, [_good("12345678", "b1"), _good("87654321", "b1"),
                                   _good("11111111", "b2")])
    _stage(spark, tables.bronze, [_good("12345678", "b1")])

    with pytest.raises(PromoteRefused, match="1 row"):
        _promote(spark, tables, "b1")
    assert _bronze_batches(spark, tables) == ["b1"]  # refused: nothing appended

    _promote(spark, tables, "b2")
    spark.sql(f"DELETE FROM {tables.staging} WHERE _batch_id = 'b2'")
    repromoted = _promote(spark, tables, "b2")

    assert repromoted.outcome is PromoteOutcome.ALREADY_PROMOTED_STAGING_GONE
    # Against real tables: the rejects were counted from staging, staging no longer
    # holds b2, so there is no count -- not a count of zero.
    assert repromoted.rejected_rows is None
    assert _bronze_batches(spark, tables) == ["b1", "b2"]
