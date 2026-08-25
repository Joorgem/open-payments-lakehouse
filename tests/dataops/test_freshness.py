"""The freshness view, and every arm of its status ladder reached from real tables.

WHY THE ARMS ARE DRIVEN AND NOT MATCHED. `opl.bronze.reconcile`'s reason, unchanged: an arm
nothing can enter is a status that will never be wrong, and a metric whose "not a fault"
arm is unreachable reports a fault for every table that is deliberately not being
ingested. Three of the seven statuses cannot be produced by anything in the live workspace
today -- `overdue`, `source_date_missing` and `never_ingested` -- so this file is the only
place they are ever seen before an operator sees them.

THE DATES ARE RELATIVE, NEVER LITERAL. `date_sub(current_date(), 400)` is overdue on any
day this suite runs; a literal would be overdue until the cadence changed and then quietly
stop testing the arm.

`payments` AND `ptax` ARE BUILT WITHOUT A `_snapshot_ref_date` COLUMN AT ALL, which is the
state they are in, and it makes the derivation load-bearing rather than decorative: if
`declares_source_date` ever said yes for them, this fixture would not parse."""
from __future__ import annotations

import pytest

from opl.config import OplConfig
from opl.dataops.freshness import (
    NEVER_INGESTED,
    NO_DECLARED_CADENCE,
    NO_SOURCE_DATE_COLUMN_STATUS,
    OVERDUE,
    PAUSED_BY_DECISION,
    SOURCE_DATE_MISSING,
    WITHIN_CADENCE,
    freshness_sql,
)

_SCHEMA = "opl_freshness_probe"
_EMPTY_SCHEMA = "opl_freshness_empty_probe"
_CONFIG = OplConfig(catalog="spark_catalog", schema=_SCHEMA)
_EMPTY_CONFIG = OplConfig(catalog="spark_catalog", schema=_EMPTY_SCHEMA)

# table key -> (snapshot_month, the `_snapshot_ref_date` expression or None for the two
# tables that carry no such column). One row each; the view only ever takes maxima.
_ROWS: dict[str, tuple[str, str | None]] = {
    # Paused, and stale by any measure anyone could invent. Still not a fault.
    "lookup": ("2020-01", "date_sub(current_date(), 900)"),
    # 400 days OLD, which is 355 days past a 45-day expectation. The two are different
    # numbers and this comment used to print the age as the overshoot.
    "empresas": ("2026-07", "date_sub(current_date(), 400)"),
    "estabelecimentos": ("2026-07", "current_date()"),
    # Rows, a declared cadence, and no observable source date on any of them.
    "socios": ("2026-07", "CAST(NULL AS DATE)"),
    "merchant": ("2026-08", "current_date()"),
    "payments": ("2026-08", None),
    "ptax": ("2026-08", None),
}


def _bronze_view_sql(month: str, ref_date: str | None, *, empty: bool) -> str:
    """One bronze table, as the three columns the freshness view reads off it."""
    ref = "" if ref_date is None else f", {ref_date} AS _snapshot_ref_date"
    where = " WHERE false" if empty else ""
    return (
        f"SELECT current_timestamp() AS _ingested_at, '{month}' AS _snapshot_month{ref}"
        f"{where}"
    )


def _build(spark, config: OplConfig, *, empty: bool) -> None:
    from opl.bronze.registry import REGISTRY

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.schema}")
    for spec in REGISTRY.values():
        month, ref_date = _ROWS[spec.name]
        body = _bronze_view_sql(month, ref_date, empty=empty)
        spark.sql(f"CREATE OR REPLACE VIEW {config.table(spec.bronze)} AS {body}")


@pytest.fixture(scope="module")
def probe(spark):
    """Seven bronze tables with rows, and seven with none, in two schemas this module owns.

    The second schema exists for exactly one arm: `never_ingested` needs a table that is
    EMPTY and not paused, and `merchant` is the only registered table declaring a cadence
    kind that would otherwise let the arm through -- so it cannot be both in one build."""
    _build(spark, _CONFIG, empty=False)
    _build(spark, _EMPTY_CONFIG, empty=True)
    yield spark
    for schema in (_SCHEMA, _EMPTY_SCHEMA):
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")


def _by_source(spark, config: OplConfig = _CONFIG) -> dict:
    return {row["source"]: row for row in spark.sql(freshness_sql(config)).collect()}


def test_every_status_but_one_is_reachable_in_a_single_build(probe):
    """Six of the seven arms, produced by the shipped SQL over tables built to reach them."""
    rows = _by_source(probe)
    assert rows["lookup"]["source_freshness_status"] == PAUSED_BY_DECISION
    assert rows["empresas"]["source_freshness_status"] == OVERDUE
    assert rows["estabelecimentos"]["source_freshness_status"] == WITHIN_CADENCE
    assert rows["socios"]["source_freshness_status"] == SOURCE_DATE_MISSING
    assert rows["merchant"]["source_freshness_status"] == NO_DECLARED_CADENCE
    assert rows["payments"]["source_freshness_status"] == NO_SOURCE_DATE_COLUMN_STATUS
    assert rows["ptax"]["source_freshness_status"] == NO_SOURCE_DATE_COLUMN_STATUS


def test_a_table_that_was_never_ingested_is_not_reported_as_fresh(probe):
    """The seventh arm, and it is the only one that is about the TABLE rather than the
    declaration. An empty table has a NULL age, and `NULL > 45` is not false -- it is
    unknown, which a CASE treats as "not this arm" and falls through.

    MEASURED BY DELETING THE ARM: a merchant table with no rows at all then reports
    `no_declared_cadence`, its cadence kind, exactly as a fully-loaded one does. Not
    `within_cadence` in this instance -- an arm above catches it first -- which is the
    sharper version of the same defect: with no arm for emptiness, every table is
    described by what was DECLARED about it and nothing describes what is in it."""
    rows = _by_source(probe, _EMPTY_CONFIG)
    assert rows["merchant"]["source_freshness_status"] == NEVER_INGESTED
    assert rows["merchant"]["bronze_rows"] == 0
    assert rows["merchant"]["last_ingested_at"] is None


def test_a_paused_table_is_never_a_fault_and_prints_the_decision_that_paused_it(probe):
    """THE ACCEPTANCE CASE. `lookup` is 900 days behind here and 66 in the workspace, two
    snapshot months behind its three CNPJ siblings either way. It is not overdue: F1.4b PR B
    recorded that 2026-07 lookups were out of scope and their zips were never on disk. A
    metric that cannot say that is the alert an operator mutes in week one."""
    row = _by_source(probe)["lookup"]
    assert row["source_freshness_status"] == PAUSED_BY_DECISION
    assert row["source_age_days"] == 900
    assert row["expected_every_days"] is None
    assert "recorded scope decision" in row["cadence_note"]
    assert "f1.4b-pr-b-run-evidence.md 25.5" in row["cadence_note"]


def test_dropping_the_missing_source_date_arm_makes_a_dateless_table_read_as_fresh(probe):
    """The mutation the ladder refuses, executed, so its fifth arm is shown to do work.

    `socios` here holds rows, declares a 45-day cadence, and carries no observable
    `_snapshot_ref_date` on any of them. Without the arm, `source_age_days >
    expected_every_days` compares NULL and falls to the ELSE."""
    mutated = freshness_sql(_CONFIG).replace(
        f"WHEN last_source_date IS NULL THEN '{SOURCE_DATE_MISSING}'\n    ", ""
    )
    assert SOURCE_DATE_MISSING not in mutated
    rows = {row["source"]: row for row in probe.sql(mutated).collect()}
    assert rows["socios"]["source_freshness_status"] == WITHIN_CADENCE


def test_the_two_metrics_are_reported_apart_and_one_of_them_carries_no_verdict(probe):
    """Pipeline freshness is defined for all seven; source freshness for five.

    Collapsed into one number the metric is NULL for two of seven and invites the reading
    that they are broken. And `pipeline_age_days` gets no status of its own, deliberately:
    nothing in this bundle schedules an ingest, so a threshold on it would be measuring
    when an operator last typed a command."""
    rows = _by_source(probe)
    assert all(row["pipeline_age_days"] == 0 for row in rows.values())
    assert rows["payments"]["last_source_date"] is None
    assert rows["payments"]["source_age_days"] is None
    assert rows["estabelecimentos"]["source_age_days"] == 0
    columns = set(probe.sql(freshness_sql(_CONFIG)).columns)
    assert "source_freshness_status" in columns
    assert not any(name.startswith("pipeline") and name.endswith("status") for name in columns)


def test_the_snapshot_month_shows_the_tiers_a_single_freshness_number_would_hide(probe):
    """Three tiers in the live workspace -- 2026-06 lookup, 2026-07 the CNPJ giants,
    2026-08 merchant/payments/ptax -- and the column that carries them is the operator's
    own vocabulary for "which month am I looking at", not a derived age."""
    rows = _by_source(probe)
    assert rows["lookup"]["last_snapshot_month"] == "2020-01"
    assert rows["empresas"]["last_snapshot_month"] == "2026-07"
    assert rows["ptax"]["last_snapshot_month"] == "2026-08"


def test_a_cadence_note_containing_an_apostrophe_survives_into_the_view(probe, monkeypatch):
    r"""The escaping, executed -- and it caught the obvious spelling being silently wrong.

    These notes are English prose an operator reads, so an apostrophe is a matter of time.
    This test was written expecting a CREATE that fails loudly if the escaping were wrong;
    what it found is worse. `sql_string_literal` doubled the apostrophe -- the SQL-standard
    escape -- and Spark returned `dont`: the lexer ends the literal and starts another,
    adjacent literals concatenate, and the character is DELETED with nothing failing
    anywhere. Same on the `opl-free` SQL warehouse. The backslash form is what Spark
    implements, and the backslash in the note is here so the escape of the escape is
    exercised too."""
    from opl.dataops import cadence as cadence_module
    from opl.dataops.cadence import Cadence

    note = "the RFB's own rhythm; don't invent one (not a C:\\path)"
    patched = dict(cadence_module.CADENCE)
    patched["ptax"] = Cadence(kind=patched["ptax"].kind, every_days=None, why=note)
    monkeypatch.setattr("opl.dataops.freshness.CADENCE", patched)
    rows = _by_source(probe)
    assert rows["ptax"]["cadence_note"] == note


def test_the_ingest_stamp_writes_the_column_this_view_reads(probe):
    """`_ingested_at` is a literal in `withColumn` and a second literal in `masking.py`'s
    DDL, so `freshness` naming it a third time would be an unchecked spelling. The stamp is
    RUN here and the column it produces compared, which makes the third one a cross-check.

    The other two columns this view reads are module constants and are imported."""
    from opl.bronze.autoloader import add_common_audit_columns
    from opl.dataops.freshness import INGESTED_AT_COLUMN

    stamped = add_common_audit_columns(
        probe.sql("SELECT 1 AS x"),
        batch_id="b",
        snapshot_month="2026-08",
        record_source="probe",
    )
    assert INGESTED_AT_COLUMN in stamped.columns
