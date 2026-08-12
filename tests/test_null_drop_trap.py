# tests/test_null_drop_trap.py
"""The defect this phase can now commit against itself, measured and then made unreachable.

THE MECHANISM, IN ONE PARAGRAPH. SQL's `COUNT(DISTINCT a, b, c)` drops every row that is
NULL in any of a, b, c -- not the missing value, the whole ROW -- and returns a smaller,
confident number with no warning attached. This repository has already lost 8,761 rows to
exactly that. Through F1b Task 1 the payment stream could not reproduce it: every contract
column is a non-empty string on every row, so there was no NULL for the behaviour to bite
on and the defect was INERT. THE DRIFT COLUMN ARMS IT. Every record emitted before the
drift point lacks that key, so any distinct count that included it would silently exclude
the entire pre-drift population -- on this phase's own evidence, in the phase that exists
to make defects measurable.

WHAT THIS FILE DOES ABOUT IT, in four layers, because a comment is not a mechanism:

  1. MEASURES it. The numbers below are what the naive count actually returns on the
     pinned fixture -- 60 where the honest answer is 168, over 120 dropped rows out of 180.
     A number written down before the workspace run is a prediction; the same number
     discovered afterwards is a coincidence.
  2. REFUSES it AT IMPORT. `opl.contracts.payments` will not load if a drift column has
     been added to `COLUMNS`, `REQUIRED_COLUMNS` or `BUSINESS_ATTRIBUTE_COLUMNS`. A test
     that fails is the minimum; a module that does not import is what stops a broken
     declaration reaching a workspace between a green suite and a deploy.
  3. GENERALISES it. The guard's fourth check is `BUSINESS_ATTRIBUTE_COLUMNS ⊆
     REQUIRED_COLUMNS` -- it is NULLABILITY, not this one column name, that makes a column
     unusable in a distinct count -- so a second optional column added in a later phase is
     refused without anyone editing the guard.
  4. NAMES it at every call site. `opl.generator.measures` exposes
     `distinct_business_attributes` (cannot drop a row) and `distinct_over_dropping_nulls`
     (drops rows, and says so in its own name) as two functions rather than one with a
     column argument, so the dangerous count cannot be reached by passing a longer list to
     the safe one. The last test sweeps every source file that ships or runs for a
     `COUNT(DISTINCT ...)` naming the drift column, and proves the sweep can fail.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from opl.contracts import payments
from opl.contracts.payments import (
    BUSINESS_ATTRIBUTE_COLUMNS,
    COLUMNS,
    DRIFT_COLUMN,
    DRIFT_COLUMNS,
    REQUIRED_COLUMNS,
)
from opl.generator import measures
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import DefectSpec, delivered_records
from opl.generator.stream import StreamSpec

_REPO = Path(__file__).resolve().parents[1]

# The same fixture `tests/test_payment_defects.py` pins, restated rather than imported: a
# number this file quotes must be reconstructable from what this file declares, and a
# cross-file import would make the pinned counts below depend on an edit nobody reading
# them would look at.
_SPEC = StreamSpec(
    seed=20260812,
    stream_id="F1B-DEFECTS",
    event_count=180,
    repeat_count=12,
    window_start="2026-06-01T00:00:00.000Z",
    event_interval_ms=60_000,
    emission_lag_ms=1_500,
    cnpj_pool=validated_pool([f"{n:08d}" for n in range(1, 21)]),
)
_DRIFT = DefectSpec(drift_from_index=120)

# The tuple a reviewer would write without thinking about it: "the business attributes,
# plus the new column". It is spelled here, once, and nowhere in `src/`.
_NAIVE = (*BUSINESS_ATTRIBUTE_COLUMNS, DRIFT_COLUMN)


# --- the defect, measured --------------------------------------------------------------


def test_a_distinct_count_including_the_drift_column_drops_the_whole_pre_drift_population():
    """THE NUMBER, BEFORE ANYONE RUNS THE QUERY THAT PRODUCES IT.

    168 distinct attribute tuples over 180 rows is the honest answer, and it does not move
    when the stream drifts. Add the drift column to the tuple and the answer is 60 -- one
    per drifted row -- because the 120 rows that lack the key were never looked at. The
    count does not fail, does not warn, and is off by more than half the stream."""
    records = delivered_records(_SPEC, _DRIFT)
    assert len(records) == 180
    assert measures.distinct_business_attributes(records) == 168
    assert measures.distinct_over_dropping_nulls(records, BUSINESS_ATTRIBUTE_COLUMNS) == 168
    assert measures.distinct_over_dropping_nulls(records, _NAIVE) == 60
    assert measures.rows_dropped_by_null(records, _NAIVE) == 120
    assert measures.rows_dropped_by_null(records, BUSINESS_ATTRIBUTE_COLUMNS) == 0


def test_on_the_clean_stream_the_same_mistake_returns_zero_and_still_does_not_fail():
    """WHY THIS WAS NOT CATCHABLE BEFORE THE DRIFT COLUMN EXISTED, and why it is worse
    than it looks.

    Over a stream where NO row carries the column, the naive count drops every row and
    returns 0 -- a query that answered "there are no distinct payments in this stream" and
    raised nothing. The clean stream could not exhibit the defect because it had no NULL;
    it also could not warn about it."""
    clean = delivered_records(_SPEC, DefectSpec())
    assert measures.distinct_business_attributes(clean) == 168
    assert measures.distinct_over_dropping_nulls(clean, _NAIVE) == 0
    assert measures.rows_dropped_by_null(clean, _NAIVE) == 180


def test_the_safe_count_is_unmoved_by_the_drift_and_that_is_the_point():
    """A count that changes when a column NOTHING IN IT was added is a count of something
    else. This is the number the duplicate/repeat arithmetic subtracts from, so it has to
    mean the same thing in a drifted stream and a clean one."""
    clean = delivered_records(_SPEC, DefectSpec())
    drifted = delivered_records(_SPEC, _DRIFT)
    assert measures.distinct_business_attributes(clean) == 168
    assert measures.distinct_business_attributes(drifted) == 168


def test_the_safe_count_raises_rather_than_drops_when_a_required_column_is_missing():
    """The opposite of SQL's behaviour, deliberately. A stream that lost a required column
    is a finding; a smaller count is what that finding looks like when it is swallowed."""
    records = list(delivered_records(_SPEC, DefectSpec()))
    truncated = [{k: v for k, v in records[0].items() if k != "amount"}, *records[1:]]
    with pytest.raises(KeyError):
        measures.distinct_business_attributes(truncated)
    assert measures.distinct_over_dropping_nulls(truncated, BUSINESS_ATTRIBUTE_COLUMNS) == 167


# --- the defect, refused at import -------------------------------------------------------


def test_the_declared_tuples_do_not_carry_a_drift_column():
    """The live declaration, checked directly. Every refusal below is monkeypatched, so
    without this they would all pass over a contract that was already broken."""
    assert DRIFT_COLUMNS == (DRIFT_COLUMN,)
    for declared in (COLUMNS, REQUIRED_COLUMNS, BUSINESS_ATTRIBUTE_COLUMNS):
        assert DRIFT_COLUMN not in declared
    payments._assert_no_drifting_column_is_declared_by_v1()


@pytest.mark.parametrize(
    "tuple_name", ["COLUMNS", "REQUIRED_COLUMNS", "BUSINESS_ATTRIBUTE_COLUMNS"]
)
def test_a_drift_column_added_to_a_declared_tuple_is_refused(monkeypatch, tuple_name):
    """THE TEST THE RULING ASKED FOR, over all three tuples rather than the one it named.

    Each has its own silent failure. In `COLUMNS` the serialiser emits the column on every
    record and there is no drift left to catch. In `REQUIRED_COLUMNS` a read schema built
    from the contract absorbs it, nothing lands in `_rescued_data`, and the DQ gate's
    highest-precedence rule never fires. In `BUSINESS_ATTRIBUTE_COLUMNS` every distinct
    count over the attributes drops the pre-drift population."""
    monkeypatch.setattr(payments, tuple_name, (*getattr(payments, tuple_name), DRIFT_COLUMN))
    with pytest.raises(ValueError, match="are drift columns"):
        payments._assert_no_drifting_column_is_declared_by_v1()


def test_a_nullable_business_attribute_is_refused_whatever_it_is_called(monkeypatch):
    """THE GENERAL RULE, which is what makes this unreachable rather than merely tested: a
    column some rows do not carry may never be a business attribute, whatever it is
    called. Exercised on a column that is NOT the drift column, so the guard is shown to
    enforce NULLABILITY rather than one name it happens to know -- a second optional
    column added in a later phase is refused without an edit here."""
    monkeypatch.setattr(
        payments, "BUSINESS_ATTRIBUTE_COLUMNS", (*BUSINESS_ATTRIBUTE_COLUMNS, "settlement_id")
    )
    with pytest.raises(ValueError, match="not required"):
        payments._assert_no_drifting_column_is_declared_by_v1()


# --- the defect, swept for in every file that ships or runs -------------------------------


def _string_literals(source: str) -> list[str]:
    """Every string literal in `source` that is not a docstring, f-strings reassembled.

    DOCSTRINGS ARE EXCLUDED for the reason the generator's purity sweep excludes them:
    the modules in this repository EXPLAIN the NULL-dropping defect, quoting the very
    statement shape a text search looks for, so a sweep that read them would go red on the
    documentation of the property it guards. Comments never reach the AST at all.

    An f-string arrives as a `JoinedStr` whose literal parts are reassembled here, because
    every SQL statement in this repository is built that way -- `opl.generator.cnpj_pool.
    pool_query` interpolates a table name into a `SELECT` -- and a sweep that only read
    plain `Constant` nodes would miss all of them.

    `+`-CONCATENATION OF LITERALS IS FOLDED HERE, AND IT WAS A REAL HOLE. An independent
    review of F1b Tasks 0-2 defeated the first spelling of this sweep with
    `"SELECT COUNT(DISTINCT " + "amount, payment_channel" + ")"`: two `Constant` nodes, each
    harmless alone, joined by an operator the sweep never looked at. Implicit adjacent
    concatenation (`"a" "b"`, no `+`) was already caught only because CPython folds it into
    one `Constant` before the AST exists -- which is luck, not coverage. `_folded` below
    walks `BinOp(Add)` trees so the two spellings are treated alike.

    WHAT IS STILL NOT COVERED, stated because the docstring this replaces understated it:
    `%`-formatting, `.format()`, and any statement assembled from run-time values. This is
    a floor, not a proof, and the layer that actually closes the defect is the import-time
    guard in `opl.contracts.payments` -- which refuses the drift column in every declared
    tuple and generalises to `BUSINESS_ATTRIBUTE_COLUMNS <= REQUIRED_COLUMNS`. That guard
    resisted every bypass the same review attempted."""
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                docstrings.add(id(body[0].value))
    def _folded(node: ast.AST) -> str | None:
        """`node` as one string when it is built only from string literals, else None.

        Recurses through `BinOp(Add)` so `"a" + "b"` reads as `"ab"`, which is the hole the
        review found. A branch that is not a literal makes the whole expression unreadable
        here -- returning the readable half would invent a statement nobody wrote."""
        if isinstance(node, ast.Constant):
            return node.value if isinstance(node.value, str) else None
        if isinstance(node, ast.JoinedStr):
            parts = [v.value for v in node.values if isinstance(v, ast.Constant)]
            return "".join(part for part in parts if isinstance(part, str))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = _folded(node.left), _folded(node.right)
            return None if left is None or right is None else left + right
        return None

    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.BinOp | ast.JoinedStr):
            text = _folded(node)
            if text is not None:
                found.append(text)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) not in docstrings:
                found.append(node.value)
    return found


def _counts_distinct_over_the_drift_column(text: str) -> bool:
    """Whether `text` is a statement taking a distinct count that names the drift column."""
    folded = "".join(text.upper().split())
    return "COUNT(DISTINCT" in folded and DRIFT_COLUMN.upper() in folded


# Every file that ships in the wheel or runs as a job task, which is where a query like
# this would do damage. `docs/` is deliberately out of scope: the evidence files ARGUE
# about this defect in prose and quoting it there is the point.
#
# DELIBERATELY NOT PARAMETRISED, unlike the two whole-tree sweeps in
# `tests/test_revision_stamp.py` and `tests/test_task_wiring.py`. Those give each source
# file its own test id, so every new module under `src/opl/**` adds two tests to the
# suite -- a number three implementers have under-predicted, and one that whoever writes
# the next plan has to carry. A third sweep on the same shape would make it three, for a
# diagnostic gain of nothing: the assertion below names the file and the offending literal
# in its own message.
_SWEPT = sorted(
    [*(_REPO / "src" / "opl").rglob("*.py"), *(_REPO / "databricks" / "src").glob("*.py")]
    + sorted((_REPO / "scripts").glob("*.py"))
)


def test_no_shipped_source_takes_a_distinct_count_over_the_drift_column():
    """THE LAYER THAT REACHES INTO CODE NOBODY HAS WRITTEN YET.

    The guards above refuse a broken DECLARATION. This refuses a broken QUERY: any SQL
    literal in the wheel or in a job entry point that takes a `COUNT(DISTINCT ...)` naming
    the drift column. F1b Task 3's ingest and Task 4's evidence are the code this is aimed
    at, and it is in place before either of them exists.

    ITS LIMIT, STATED RATHER THAN LEFT TO BE DISCOVERED: it reads one literal at a time, so
    a statement assembled from two variables at run time slips through. That is the shape
    no SQL in this repository is written in today, and the check is a floor rather than a
    proof."""
    assert _SWEPT, "the sweep found no source files; the paths are wrong, not the code"
    offenders = {
        source.name: [
            text
            for text in _string_literals(source.read_text(encoding="utf-8"))
            if _counts_distinct_over_the_drift_column(text)
        ]
        for source in _SWEPT
    }
    named = {name: texts for name, texts in offenders.items() if texts}
    assert not named, (
        f"{named} takes a distinct count naming {DRIFT_COLUMN!r}. Every row written before "
        "the drift point is NULL in that column, so the count silently excludes the whole "
        "pre-drift population -- this repository's own 8,761-row defect, on its own "
        "evidence."
    )


def test_the_sweep_catches_the_statement_it_is_looking_for():
    """A GUARD IS ONLY CLOSED BY THE PROBE THAT CLOSES IT. Every file above passes, and a
    sweep that could not fail would pass identically -- so the detector is run against the
    statement a future task would plausibly write, in both its plain and f-string forms,
    and against two near-misses it must NOT flag."""
    caught = (
        'sql = "SELECT COUNT(DISTINCT payer_cnpj_basico, payment_channel) FROM bronze"',
        'sql = f"SELECT COUNT(DISTINCT amount, {X}, payment_channel) FROM {table}"',
        'sql = ("SELECT COUNT(DISTINCT\\n  amount,\\n  payment_channel\\n) FROM t")',
    )
    for source in caught:
        assert any(_counts_distinct_over_the_drift_column(t) for t in _string_literals(source))
    ignored = (
        'sql = "SELECT COUNT(DISTINCT transaction_id) FROM bronze"',
        'sql = "SELECT payment_channel FROM bronze WHERE payment_channel IS NOT NULL"',
        '"""A COUNT(DISTINCT a, payment_channel) drops the pre-drift rows."""',
    )
    for source in ignored:
        assert not any(_counts_distinct_over_the_drift_column(t) for t in _string_literals(source))


def test_the_measurement_cannot_import_the_injection():
    """`opl.generator.measures` must not import `opl.generator.defects`, at any depth.

    THIS CLAIM WAS LOAD-BEARING AND UNGUARDED, which an independent review of F1b Tasks
    0-2 found and this closes. `measures.py`'s own docstring and `opl.generator.__init__`
    both rest on it: a measurement that could see the injection would confirm itself, and
    every count this phase publishes -- 168 honest against 60 naive, five injected late
    positions, seven duplicates -- would be the generator agreeing with the generator.

    Every other invariant of comparable weight in this package is enforced rather than
    stated: the purity sweep walks the AST, `_assert_no_drifting_column_is_declared_by_v1`
    refuses at import, `_assert_every_column_has_a_renderer` refuses in both directions.
    This one was prose. Now it is a test.

    THE CHECK IS OVER THE IMPORTED MODULE'S OWN AST, not over `sys.modules`: importing
    `measures` inside a test session where something else already imported `defects` would
    make a `sys.modules` check pass while the source said otherwise."""
    source = (_REPO / "src" / "opl" / "generator" / "measures.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
            imported.extend(f"{node.module}.{alias.name}" for alias in node.names)
    offenders = sorted({name for name in imported if "defects" in name})
    assert not offenders, (
        f"opl.generator.measures imports {offenders} -- the measurement can now see the "
        "injection, so every count it produces is the generator confirming itself"
    )
