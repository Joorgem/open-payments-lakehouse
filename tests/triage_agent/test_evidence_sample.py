"""WHAT A ROW LOOKS LIKE, AND WHAT MAY LEAVE: the state vocabulary and the taint sweep.

SPLIT FROM `test_evidence.py` AT A SUBJECT SEAM when that file reached the 800-line cap.
This half is the STATES -- the words `row_shapes_sql` puts where a value would be, and the
proof that no publishable statement puts a value there instead; `test_evidence_census.py`
is the COUNTS. The corpus both read is in `conftest.py`.

THE THING THIS FILE HAS TO PROVE, and it is a pair rather than a value. It is numbered as
it was when both halves were one file, where it was the third of five:

  3. A masked column reports `masked` and an unmasked EMPTY column reports `empty`, BOTH
     arms over the same five rows. One arm proves nothing: a sampler answering `masked`
     everywhere passes the first, one ignoring the declaration passes the second.

AND ONE MORE, WHICH MAKES THIS REPOSITORY'S PUBLICNESS SAFE RATHER THAN CAREFUL: every fixture
value carries a sentinel EXCEPT where a state word needs it not to, and each personal column a
token of its own, so "did a value escape" and "did a PERSONAL value escape" are both askable of
the OUTPUT -- of the batches `_TAINT_SWEEP` walks, which are not `_INCIDENTS`. Both arms keep
their control in the same test, because a taint check whose reader is broken -- or whose fixture
never planted what it looks for -- reports clean over everything.

AND WHY THE CORPUS CANNOT SUPPLY THAT SWEEP ON ITS OWN, which `evidence.py`'s header states
and the previous pass could not fit under the cap: `nome_socio_razao_social` is `''` in both
socios batches BECAUSE that emptiness is what the gate rejected them for, which is why
`_TAINT_SWEEP` walks an invented batch as well as the eleven.
"""
from __future__ import annotations

from opl.bronze.dq import RESCUED_DATA_COLUMN
from opl.bronze.promote import BATCH_COLUMN
from opl.bronze.registry import table_spec
from opl.contracts.catalogue import columns_for
from opl.triage_agent.evidence import (
    EMPTY,
    MASKED,
    NULL_VALUE,
    PRESENT,
    REPLACEMENT_CHAR,
    VALUE_STATES,
    evidence_sql,
    row_sample_sql,
    row_shapes_sql,
    value_state_sql,
)
from opl.triage_agent.severity import severity_sql

from .conftest import (
    _CONFIG,
    _INCIDENTS,
    _MASKED_SOCIOS_COLUMN,
    _MATRIX_BATCH,
    _MATRIX_OVERRIDES,
    _MATRIX_STATES,
    _PAYMENTS_BATCH,
    _PERSONAL_TOKENS,
    _REPLACEMENT,
    _SENTINEL,
    _SOCIOS_BATCHES,
    _TAINT_SWEEP,
    _UNMASKED_SOCIOS_COLUMN,
    _counts_by_reason,
    _run,
)

# ----------------------------------------------------------------------------------
# 3. The sample's vocabulary: masked is not empty, and neither can borrow the other.
# ----------------------------------------------------------------------------------


def _states(spark, source: str, batch: str) -> list[dict[str, str]]:
    rows = _run(spark, row_shapes_sql(table_spec(source), _CONFIG), batch)
    return [row["value_states"] for row in rows]


def test_a_masked_column_reports_masked_and_an_unmasked_empty_one_reports_empty(probe):
    """BOTH ARMS, over five rows holding the same five SHAPES in two columns.

    They hold the same states row for row and differ in one thing: row 0's token, which is
    unique to the personal column. That is what lets the taint test at the bottom of this
    file ask whether a PERSONAL value escaped rather than whether any value did.

    The rejection these socios rows carry IS that the name column is null or empty, and
    that same column is masked -- so a sampler that read it would show `***` and could not
    tell "masked from me" from "empty, which is why this row is here". `masked` is emitted
    without reading the column, so it says which of those it is: neither. The value is not
    evidence in this output, for any reader.

    Row by row, the unmasked column moves through `present`, `empty`, `present`, `null`,
    `replacement_char` while the masked one never moves at all. The `***` row is the sharp
    one: that is exactly what a UC mask emits, and the unmasked column still calls it
    `present` -- so no value can manufacture the word `masked`."""
    states = _states(probe, "socios", _MATRIX_BATCH)
    assert len(states) == 5

    unmasked = [row[_UNMASKED_SOCIOS_COLUMN] for row in states]
    masked = [row[_MASKED_SOCIOS_COLUMN] for row in states]

    assert unmasked == list(_MATRIX_STATES)
    assert masked == [MASKED] * 5
    assert "'***'" in _MATRIX_OVERRIDES[_UNMASKED_SOCIOS_COLUMN], "the sharp row, still planted"


def test_every_declared_personal_column_is_masked_and_no_other_column_is(probe):
    """The redaction is exactly the declaration, over a whole sampled row.

    Not only the two columns the test above looks at: the socios contract has eleven, two
    of them declared personal, and a redaction that covered one or spread to all of them
    would be a different control. `_rescued_data` is profiled too and is NOT redacted --
    its state is the evidence for `rescued_data_present`, the largest incident here."""
    row = _states(probe, "socios", _SOCIOS_BATCHES[0])[0]
    masked = {column for column, state in row.items() if state == MASKED}

    assert masked == {_MASKED_SOCIOS_COLUMN, "nome_do_representante"}
    assert set(row) == {*columns_for("socios"), RESCUED_DATA_COLUMN}
    assert row[RESCUED_DATA_COLUMN] == NULL_VALUE

    payments = _states(probe, "payments", _PAYMENTS_BATCH)[0]
    assert MASKED not in payments.values(), "no payments column is declared personal"
    assert payments[RESCUED_DATA_COLUMN] == PRESENT, "the reason this incident exists"


def test_the_empty_and_null_states_are_the_gates_own_predicate_and_not_a_second_spelling(
    probe,
):
    """`null_or_empty_*` is `isNull() | trim(...) == ''`, and this file must agree with it.

    Two spellings of one rule is what this repository keeps paying for, and here the drift
    would be invisible: a state expression testing `= ''` rather than `TRIM(...) = ''`
    reports a whitespace-only value as `present` while the gate that rejected the row calls
    it blank -- so the sample would contradict the reject reason beside it and neither
    would look wrong. The gate's own predicate is executed here and the two are compared
    row for row, rather than the SQL being read."""
    from opl.bronze.rule_predicates import _null_or_blank

    values = [("a",), ("",), ("   ",), (None,), (f"x{_REPLACEMENT}y",)]
    frame = probe.createDataFrame(values, "c STRING")
    frame.createOrReplaceTempView("gate_probe")
    blank = [row["blank"] for row in frame.withColumn("blank", _null_or_blank("c")()).collect()]

    states = [
        row["state"]
        for row in probe.sql(
            f"SELECT {value_state_sql('c', masked=False)} AS state FROM gate_probe"
        ).collect()
    ]

    try:
        assert states == [PRESENT, EMPTY, EMPTY, NULL_VALUE, REPLACEMENT_CHAR]
        assert blank == [False, True, True, True, False]
        assert [state in (EMPTY, NULL_VALUE) for state in states] == blank
    finally:
        # `probe` drops a DATABASE; a temp view is session-scoped and outlives that.
        probe.catalog.dropTempView("gate_probe")


def test_every_state_this_module_reports_is_one_of_the_closed_vocabulary(probe):
    """A word outside `VALUE_STATES` would be a value leaking through the state column.

    Swept over every incident that has rows rather than over one, because the state
    expression is generated per column and a contract with a column the generator handled
    differently would only show up on that contract.

    EQUALITY IN BOTH DIRECTIONS, WHICH IS THE POINT OF A CLOSED VOCABULARY. The first
    version required four of the five, leaving `replacement_char` unrequired in the test
    whose subject is that every arm is reached -- though the sweep reaches it, on
    `estabelecimentos.correio_eletronico`, ADR 0006's four lost-byte rows."""
    seen = {
        state
        for source, batch in _INCIDENTS[:6]
        for row in _states(probe, source, batch)
        for state in row.values()
    }

    assert seen, "the sweep found no sampled rows at all"
    assert seen == set(VALUE_STATES), (
        f"{sorted(seen - set(VALUE_STATES))} is not a state and "
        f"{sorted(set(VALUE_STATES) - seen)} was reached by nothing"
    )


def test_the_sample_is_bounded_even_where_the_incident_is_two_thousand_rows(probe):
    """A sample is bounded and a census is not, which is why they are two statements."""
    assert len(_states(probe, "payments", _PAYMENTS_BATCH)) == 20
    assert _counts_by_reason(probe, "payments", _PAYMENTS_BATCH) == {"rescued_data_present": 2000}
    narrower = row_shapes_sql(table_spec("payments"), _CONFIG, limit=3)
    assert len(_run(probe, narrower, _PAYMENTS_BATCH)) == 3


# ----------------------------------------------------------------------------------
# The publishable statements emit no row value, asserted against the OUTPUT.
# ----------------------------------------------------------------------------------


def _planted(spark, source: str, column: str, batch: str) -> str:
    """What the FIXTURE holds in one column of one batch, read straight off the table.

    The control for every "this token did not escape" assertion: an absence check over a
    string the fixture never planted is green for a reason nothing to do with the statement
    under test, which is what the shipped personal-column assertion was."""
    rows = spark.sql(
        f"SELECT `{column}` AS planted FROM {_CONFIG.table(table_spec(source).quarantine)} "
        f"WHERE {BATCH_COLUMN} = '{batch}'"
    ).collect()
    return str([row["planted"] for row in rows])


_GRADE = "severity"


def _publishable(source: str) -> dict[str, str]:
    """EVERY statement this project would put in front of a stranger, for one source.

    FOUR AND NOT THREE, AND THE FOURTH IS T3's. `severity.severity_sql` composes the census
    and the reconciliation, and T6 puts the row it returns into a PUBLIC GitHub issue -- so
    it is publishable by the same definition as `evidence_sql`'s three, and it belongs in
    the sweep that reads OUTPUT rather than in a hand check.

    ITS OTHER ARM IS A NAME COUNT AND IS BLIND TO THE LEAK THIS ONE CATCHES, which is the
    pairing `test_evidence_contract.py`'s header states for the first three and the reason
    the fourth cannot be left with one arm. `test_severity.py::test_the_graded_statement_
    READS_no_declared_personal_column` counts declared-personal column NAMES in the
    generated SQL, and `severity_sql` emits three `SELECT *`, so a leak into it need never
    spell one. MEASURED 2026-08-24: adding `leak AS (SELECT * FROM <this source's
    quarantine> WHERE _batch_id = :batch_id LIMIT 1)` to `severity_sql`, joined through
    `LEFT JOIN` and projected as `l.*`, leaves `test_severity.py` GREEN -- the colour is the
    claim and no total is quoted for it -- and turns the sweep below RED on
    `payments/592660596679630`, the first swept incident whose quarantine still holds rows.
    The same leak on `socios`/`_MATRIX_BATCH` puts BOTH `_PERSONAL_TOKENS` values into the
    graded row, measured on its own, so what this arm catches there is the PERSONAL half and
    not merely some value."""
    return {**evidence_sql(source, _CONFIG), _GRADE: severity_sql(source, _CONFIG)}


def test_no_publishable_statement_emits_a_row_value_and_the_sample_proves_the_reader_works(
    probe,
):
    """THE PROPERTY THIS REPOSITORY'S PUBLICNESS RESTS ON, checked where it can be checked.

    Every fixture value carries `_SENTINEL`, so any statement that projects a column value
    puts one in its output. The three statements `evidence_sql` returns must emit none.

    THE SWEEP IS `_TAINT_SWEEP` AND NOT `_INCIDENTS`, which is the difference between this
    check and the one that shipped: the corpus's socios rows hold `''` in
    `nome_socio_razao_social`, so the most sensitive column here carries no findable value
    anywhere in the eleven and projecting it in the clear swept clean.

    BOTH CONTROLS ARE IN THIS TEST. A taint check whose reader is broken reports clean over
    everything, so the same reader is pointed at `row_sample_sql` -- NOT publishable -- and
    required to find sentinels. And `assert "MARIA" not in leaked` ran against a batch
    where no column held that string, so deleting `row_sample_sql`'s mask filter outright
    left it green; the tokens are now read out of the fixture first and required to be
    there, making the absence below an absence from the OUTPUT and not from the input.

    THE SWEEP IS OVER `_publishable`, WHICH IS FOUR STATEMENTS AND NOT THREE, and that
    helper's docstring carries why the fourth is here and what its other arm cannot see."""
    for source, batch in _TAINT_SWEEP:
        for name, sql in _publishable(source).items():
            rows = _run(probe, sql, batch)
            assert _SENTINEL not in str(rows), f"{name} on {source}/{batch} emitted a value"
            assert name != _GRADE or len(rows) == 1, (
                f"{source}/{batch} graded to {len(rows)} rows, so the line above is an "
                "absence check over an empty result and green for the wrong reason"
            )

    leaked = str(_run(probe, row_sample_sql(table_spec("socios"), _CONFIG), _MATRIX_BATCH))
    assert _SENTINEL in leaked, "the sentinel reader found nothing where values ARE projected"

    for column, token in _PERSONAL_TOKENS.items():
        assert token in _planted(probe, "socios", column, _MATRIX_BATCH), (
            f"{token} is not in {column} of this batch, so the assertion below is an "
            "absence check over a string the fixture never planted"
        )
        assert token not in leaked, f"{column}'s value reached the NOT-publishable sample"
