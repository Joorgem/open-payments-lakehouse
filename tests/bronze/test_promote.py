# tests/bronze/test_promote.py
"""Promotion of one batch into bronze: idempotence and the operator guards.

The Spark tests here write real local Delta tables rather than using doubles,
because what they assert is a property of the Delta write itself: whether a
second append duplicates a batch. They are slow (~40 s each on a local session),
so there are as few as the properties allow; everything that can be proven
without a session is proven without one below.

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
    PromoteRefused,
    promote_batch,
    require_batch_id,
)
from opl.bronze.rules import rules_for
from opl.spark import local_session

_ESTAB_RULES = rules_for("estabelecimentos")

_STAGING_SCHEMA = StructType([
    StructField("cnpj_basico", StringType()),
    StructField("cnpj_ordem", StringType()),
    StructField("cnpj_dv", StringType()),
    StructField("nome_fantasia", StringType()),
    StructField("logradouro", StringType()),
    StructField("_batch_id", StringType()),
])


def _good(cnpj_basico: str, batch_id: str) -> tuple[str, ...]:
    return (cnpj_basico, "0001", "95", "PADARIA AÇAÍ", "RUA A", batch_id)


def _rejected(batch_id: str) -> tuple[str, ...]:
    # 7 chars -> bad_cnpj_basico_length, the reason both real F1.3 rejects had.
    return ("1234567", "0001", "95", "X", "Y", batch_id)


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
    with pytest.raises(PromoteRefused) as excinfo:
        require_batch_id(SENTINEL_BATCH_ID)
    message = str(excinfo.value)
    assert "dq_gate_batch" in message and "--params batch_id=" in message


def test_a_real_batch_id_is_accepted_and_stripped():
    assert require_batch_id(" 315230730740144 ") == "315230730740144"


@pytest.fixture(scope="module")
def spark():
    session = local_session("test-promote")
    yield session
    session.stop()


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
    assert (first.appended_rows, first.rejected_rows, first.already_promoted) \
        == (2, 1, False)
    assert (second.appended_rows, second.rejected_rows, second.already_promoted) \
        == (0, 1, True)
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
