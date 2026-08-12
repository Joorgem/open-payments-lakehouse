# tests/bronze/test_payment_rules.py
"""The DQ gate's verdict on the generated payment stream.

ITS OWN FILE, not an addition to `test_rules.py`. That file is 745 lines against an
800 cap and its fixtures are built on `cnpj_schemas.TABLES` -- `_row(contract, ...)`
indexes it directly -- so every payments fixture would have had to route around the
one helper the file is organised on. The seam is the one this suite already uses
between `test_registry.py` and `test_registry_guards.py`: what changes here is a
SOURCE, not the gate's machinery.

WHAT IS ASSERTED ELSEWHERE AND NOT REPEATED: that a registered table has a rule set
at all (`test_rules.py::test_every_registered_table_has_a_rule_set`, which sweeps the
registry and therefore already covers payments), and that every predicate is a
zero-argument factory (same file, now derived from `REQUIRED_FIELDS`).

THE PHASE CLAIM THIS FILE CARRIES is the drift one. `opl.contracts.payments` refuses
to declare the drift column so that Auto Loader rescues it; `dq._reject_reason` ranks
`rescued_data_present` above every per-table rule; and the consequence -- a drifted
row is QUARANTINED rather than absorbed -- is what
`test_a_drifted_row_is_rejected_as_rescued_data_and_not_as_something_narrower`
states, on a frame shaped the way the ingest will shape it."""
from __future__ import annotations

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql.types import StringType, StructField, StructType

from opl.bronze.dq import REJECT_COLUMN, RESCUED_DATA_COLUMN, evaluate, split
from opl.bronze.rules import rules_for
from opl.bronze.schema import struct_for
from opl.contracts.payments import (
    COLUMNS,
    CONTRACT,
    COUNTERPARTY_COLUMNS,
    DRIFT_COLUMN,
    REQUIRED_COLUMNS,
)

_REPLACEMENT_CHAR = "�"

# A row that passes every rule, so a test states only the one field it is about. The
# counterparties are eight characters because the two width rules are shape-sensitive:
# a test about a blank `currency` that silently tripped a length rule instead would
# assert the wrong thing while staying green -- `test_rules._ROW_DEFAULTS` records the
# same trap on the CNPJ side.
_CLEAN = {
    "transaction_id": "a" * 64,
    "event_time": "2026-08-01T00:00:00.000Z",
    "emitted_at": "2026-08-01T00:00:01.500Z",
    "payer_cnpj_basico": "00000004",
    "payee_cnpj_basico": "00000015",
    "amount": "23815.80",
    "currency": "BRL",
    "payment_method": "PIX",
}


def _row(**overrides: str | None) -> tuple[str | None, ...]:
    """One all-string payment row in contract order.

    Refuses an override that is not a contract column, for `test_rules._row`'s
    reason: `_row(payer_cnpj="")` (typo) would otherwise build a perfectly CLEAN row
    and then have a reject reason asserted against it -- failing for a reason that
    has nothing to do with the typo, or passing because something else was dirty."""
    unknown = sorted(set(overrides) - set(COLUMNS))
    if unknown:
        raise AssertionError(f"{unknown} is not a payments column -- {', '.join(COLUMNS)}")
    return tuple(overrides.get(column, _CLEAN[column]) for column in COLUMNS)


def _frame(spark, rows, *, rescued: list[str | None] | None = None):
    """A staging-shaped frame: the contract's columns, optionally plus `_rescued_data`.

    Explicit schema, never inference: `_rescued_data` is all-NULL in a clean row and
    Spark cannot determine an all-null column's type."""
    schema = struct_for(CONTRACT)
    if rescued is None:
        return spark.createDataFrame(list(rows), schema)
    widened = StructType([*schema.fields, StructField(RESCUED_DATA_COLUMN, StringType())])
    return spark.createDataFrame(
        [(*row, value) for row, value in zip(rows, rescued, strict=True)], widened
    )


def test_the_payments_rule_order_is_pinned():
    """First-match-wins makes order part of the contract, and this set will decide
    what the first rows of a live quarantine table say about themselves.

    The shape is the same argument the CNPJ sets make: what is MISSING, then what is
    the wrong SHAPE, then what is damaged in its BYTES. `unprovable_snapshot_ref_date`
    is absent because the COLUMN is -- a generated stream declares no reference date
    in its filename, so `add_common_audit_columns` does not stamp one and there is
    nothing for the rule to refuse. That is the one omission in this list that is a
    consequence rather than a choice."""
    assert [name for name, _ in rules_for(CONTRACT)] == [
        "null_or_empty_transaction_id",
        "null_or_empty_event_time",
        "null_or_empty_emitted_at",
        "null_or_empty_payer_cnpj_basico",
        "null_or_empty_payee_cnpj_basico",
        "null_or_empty_amount",
        "null_or_empty_currency",
        "null_or_empty_payment_method",
        "bad_payer_cnpj_basico_length",
        "bad_payee_cnpj_basico_length",
        "encoding_replacement_char",
    ]


def test_every_required_column_has_its_own_rule_and_the_names_say_which():
    """The gate is exactly as strict as the contract, asserted as a set equality.

    A subset would be a gate looser than the generator -- the difference being the
    columns a bug between the two could empty unnoticed. A superset would be a rule
    for a column that does not exist. And one rule per column rather than a collapsed
    `missing_required_field`, so the quarantine row names WHICH column was empty; the
    reason strings are the same vocabulary the CNPJ quarantines already use."""
    produced = {name for name, _ in rules_for(CONTRACT)}
    assert {f"null_or_empty_{column}" for column in REQUIRED_COLUMNS} <= produced
    assert len([n for n in produced if n.startswith("null_or_empty_")]) == len(
        REQUIRED_COLUMNS
    )


def test_a_clean_row_passes_and_each_blank_column_names_itself(spark):
    """Every required column, dirtied one at a time, over one frame.

    Parameterised inside the test rather than by pytest, so the whole set costs one
    Spark action instead of eight -- the fixture is session-scoped but a `collect` per
    column is not free."""
    dirty = ["", "   ", None]
    rows = [_row()] + [
        _row(**{column: dirty[i % len(dirty)]})
        for i, column in enumerate(REQUIRED_COLUMNS)
    ]
    evaluated = evaluate(_frame(spark, rows), rules_for(CONTRACT))
    reasons = [r[REJECT_COLUMN] for r in evaluated.collect()]
    assert reasons[0] is None, "the clean row must survive every rule"
    assert reasons[1:] == [f"null_or_empty_{column}" for column in REQUIRED_COLUMNS]


@pytest.mark.parametrize("column", COUNTERPARTY_COLUMNS)
def test_a_counterparty_that_lost_a_leading_zero_is_rejected(spark, column):
    """THE INTEGRATION CLAIM, REFUSED AT THE GATE RATHER THAN MEASURED AFTERWARDS.

    `'00000004'` becoming `'4'` is what a numeric round trip does, and it is the exact
    failure F1b Task 4's "100% resolution against hub_empresa" exists to catch -- a
    key that is still a key, still non-blank, and joins to nothing. Catching it here
    means bronze never comes to hold it.

    Both counterparties, because a rule written for one and pasted for the other with
    the column name left stale would leave the payee unchecked and pass every other
    test in this file: the reason strings differ, so a paste is visible only to a
    test that dirties each column separately."""
    rows = [_row(**{column: "4"}), _row(**{column: "000000041"}), _row()]
    evaluated = evaluate(_frame(spark, rows), rules_for(CONTRACT))
    reasons = [r[REJECT_COLUMN] for r in evaluated.collect()]
    assert reasons == [f"bad_{column}_length", f"bad_{column}_length", None]


def test_the_replacement_char_is_caught_in_any_column(spark):
    """A LIVE CONTROL ON THIS PHASE'S CENTRAL RISK, not inherited boilerplate.

    U+FFFD is what Java's decoder substitutes, SILENTLY, for bytes it cannot map --
    which is what a stream written in some other encoding, or read as some other
    encoding, arrives as. `opl.bronze.generated_landing` writes UTF-8 explicitly and
    `reader.jsonl_read_options` declares UTF-8 explicitly; this is what fires if
    either of those is ever undone, and `amount` is dirtied because it is the column
    a mojibake would be least expected in and most consequential."""
    rows = [
        _row(amount=f"238{_REPLACEMENT_CHAR}15.80"),
        _row(payment_method=f"PI{_REPLACEMENT_CHAR}"),
    ]
    evaluated = evaluate(_frame(spark, rows), rules_for(CONTRACT))
    reasons = [r[REJECT_COLUMN] for r in evaluated.collect()]
    assert reasons == ["encoding_replacement_char", "encoding_replacement_char"]


def test_a_drifted_row_is_rejected_as_rescued_data_and_not_as_something_narrower(spark):
    """THE DRIFT VERDICT, WHICH IS THE CLAIM THE PHASE IS BUILT ON.

    The frame is shaped the way the ingest shapes it: contract columns plus
    `_rescued_data`, which Auto Loader populates with any JSON key the supplied schema
    does not declare. `opl.contracts.payments` refuses to declare `payment_channel`
    precisely so that it lands there, and `dq._reject_reason` puts
    `rescued_data_present` ABOVE every per-table rule -- so a drifted row reports the
    parse being suspect rather than whatever narrower thing is also true of it. A
    narrower reason would send a triager looking at one column when the whole record's
    shape is what changed.

    The second row carries drift AND a blank column, which is the case that
    distinguishes precedence from mere presence: a gate that applied the per-table
    rules first would report `null_or_empty_currency` and the drift would go
    unremarked in the quarantine that is meant to record it."""
    rows = [_row(), _row(), _row(currency="")]
    rescued = [None, f'{{"{DRIFT_COLUMN}":"MOBILE"}}', f'{{"{DRIFT_COLUMN}":"TERMINAL"}}']
    frame = _frame(spark, rows, rescued=rescued)
    reasons = [r[REJECT_COLUMN] for r in evaluate(frame, rules_for(CONTRACT)).collect()]
    assert reasons == [None, "rescued_data_present", "rescued_data_present"]

    good, bad = split(frame, rules_for(CONTRACT))
    assert (good.count(), bad.count()) == (1, 2), (
        "the drifted rows must go to quarantine and the clean one must promote -- the "
        "gate is all-or-nothing, so this batch stops the promote entirely"
    )


def test_the_gate_refuses_a_frame_missing_a_contract_column(spark):
    """The coupling the encoding check introduces, stated for this source too.

    `rules_for("payments")` folds U+FFFD over all eight columns, so it can only be
    applied to a frame carrying all eight: a projection is an AnalysisException rather
    than a narrower gate. That is the choice, not a side effect -- a staging table
    missing a contract column is a broken ingest, and a gate that quietly narrowed
    itself to the columns it could find would report a CLEAN run over data it never
    looked at."""
    projection = spark.createDataFrame(
        [("a" * 64, "00000004")], ["transaction_id", "payer_cnpj_basico"]
    )
    with pytest.raises(AnalysisException):
        evaluate(projection, rules_for(CONTRACT)).collect()


def test_the_read_schema_does_not_declare_the_drift_column(spark):
    """WHERE THE DRIFT VERDICT ACTUALLY COMES FROM, asserted at its source.

    Every assertion above rests on `_rescued_data` being populated, and what populates
    it is that `struct_for("payments")` has nowhere else to put an undeclared key.
    A read schema carrying `payment_channel` would absorb the drift into a plain
    STRING column that is merely NULL before the drift point: nothing rescued, the
    gate green, and the phase requirement -- caught rather than absorbed -- silently
    unmet. `spark` is taken as a fixture because `struct_for` builds Spark types."""
    fields = [field.name for field in struct_for(CONTRACT).fields]
    assert fields == list(COLUMNS)
    assert DRIFT_COLUMN not in fields
    assert all(str(field.dataType) == "StringType()" for field in struct_for(CONTRACT).fields)
