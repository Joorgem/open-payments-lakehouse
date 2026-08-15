# tests/test_payment_generator.py
"""The clean stream, and the three tensions F1b exists to resolve.

Each section below names the tension it closes. The tests ARE the deliverable -- a
generator whose determinism, whose repeat/duplicate distinction and whose real-CNPJ
claim are asserted in prose rather than in a query is exactly what this phase was
called to stop producing.

WHAT IS DELIBERATELY ABSENT: any assertion about duplicates, late arrivals or drift.
This stream has none. Task 2 adds them, and the tests here are what its additions must
keep true -- `test_the_clean_stream_contains_no_late_arrival` is the property a late
arrival negates, and it has to hold FIRST or a Task 2 failure has two candidate causes.
"""
from __future__ import annotations

import ast
import hashlib
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from opl.contracts.payments import COLUMNS, CURRENCIES, PAYMENT_METHODS
from opl.generator import events as events_module
from opl.generator import stream as stream_module
from opl.generator.cnpj_pool import validated_pool
from opl.generator.derivation import draw_below, pick
from opl.generator.events import (
    LINE_SEPARATOR,
    PaymentEvent,
    business_attributes_of,
    format_amount,
    record_of,
    to_jsonl,
)
from opl.generator.instants import from_text, to_text
from opl.generator.measures import late_arrivals
from opl.generator.stream import DEFAULT_CURRENCIES, StreamSpec, generate, records_for

_GENERATOR = Path(stream_module.__file__).parent

# THE PINNED SPEC. Everything golden in this file is a function of exactly these
# values, so a change to any of them is a change to every pinned number below -- which
# is the point: the derivation is not allowed to move quietly.
_POOL = validated_pool([f"{n:08d}" for n in range(1, 21)])
_SPEC = StreamSpec(
    seed=20260809,
    stream_id="F1B-CLEAN",
    event_count=24,
    repeat_count=4,
    window_start="2026-06-01T00:00:00.000Z",
    event_interval_ms=250,
    emission_lag_ms=1_500,
    cnpj_pool=_POOL,
)


def _spec(**overrides) -> StreamSpec:
    """`_SPEC` with fields replaced. Written out rather than `dataclasses.replace`d so
    that `__post_init__` -- which is where every refusal in this file lives -- runs on
    the result."""
    fields = {
        "seed": _SPEC.seed,
        "stream_id": _SPEC.stream_id,
        "event_count": _SPEC.event_count,
        "repeat_count": _SPEC.repeat_count,
        "window_start": _SPEC.window_start,
        "event_interval_ms": _SPEC.event_interval_ms,
        "emission_lag_ms": _SPEC.emission_lag_ms,
        "cnpj_pool": _SPEC.cnpj_pool,
        "currencies": _SPEC.currencies,
    }
    return StreamSpec(**{**fields, **overrides})


# --- T1: determinism -------------------------------------------------------------------


def test_the_same_seed_produces_byte_identical_output():
    """T1's first half, over BYTES rather than over objects.

    Equality of two `tuple[PaymentEvent, ...]` would pass on a generator whose
    rendering had changed; the property the phase needs is that the FILE reproduces,
    and the file is text. Generated twice from two independently constructed specs, so
    the comparison cannot be satisfied by returning the same cached object."""
    first = to_jsonl(records_for(_spec()))
    second = to_jsonl(records_for(_spec()))
    assert first == second
    assert first.encode("utf-8") == second.encode("utf-8")


def test_the_pinned_stream_is_this_one_and_no_other():
    """THE GOLDEN PIN, in the shape `tests/vault/test_hashing.py` pins its digests.

    A digest of the whole file plus the first line in full: the digest catches any
    change anywhere, and the line is what makes the failure readable -- a bare digest
    mismatch tells nobody which field moved. Both are functions of `_SPEC`, so this
    goes red for a changed derivation, a changed column order, a changed timestamp
    format, a changed amount range and a changed serialiser alike. That breadth is
    deliberate: every one of those is a re-generation of every stream ever produced."""
    text = to_jsonl(records_for(_SPEC))
    assert hashlib.sha256(text.encode("utf-8")).hexdigest() == (
        "b45f1dc7d507da08bcc3a5d2586317ca589aeda29187f68f5039ff4ba7ad9ac9"
    )
    assert len(text.encode("utf-8")) == 7028
    assert text.splitlines()[0] == (
        '{"transaction_id":"859151c368c7ceba40e1d2fe883329dfb3d86f87f258fd01d5731'
        '4b56160bcf0","event_time":"2026-06-01T00:00:00.000Z","emitted_at":'
        '"2026-06-01T00:00:01.500Z","payer_cnpj_basico":"00000004",'
        '"payee_cnpj_basico":"00000015","amount":"23815.80","currency":"BRL",'
        '"payment_method":"PIX"}'
    )


def test_a_one_tuple_draw_is_index_zero_for_every_seed_and_position():
    """THE MECHANISM T1's BYTE-IDENTITY CLAIM RESTS ON, MEASURED RATHER THAN ARGUED.

    `opl.contracts.payments.CURRENCIES` gained a second member and not one byte of the
    four landed streams moved. The reason is arithmetic and not luck: `pick` is
    `items[draw_below(..., bound=len(items))]` and `draw_below` reduces a 256-bit digest
    modulo the bound, so `bound = 1` gives 0 for every seed, purpose, index and salt --
    `n % 1 == 0` for every n. A one-tuple therefore yields its single member forever, and
    the DOMAIN's length never enters a draw at all.

    ASSERTED OVER A SPREAD RATHER THAN OVER ONE CASE, because the claim quantifies over
    seeds and positions and the failure it guards against would be an implementation of
    `draw_below` that stopped being a plain modulo -- a rejection-sampling rewrite, say,
    which the module docstring records as considered and refused. Four seeds, four
    purposes, sixteen positions and two salts is 512 draws in well under a second.

    AND THE CONVERSE, because "always 0" is trivially satisfiable by a bug: the same
    positions against `bound = 2` must NOT be constant, or the mechanism above would be
    describing a `draw_below` that returns zero unconditionally."""
    seeds = (0, 1, 20260813, 20260817)
    purposes = ("currency", "F1B-CLEAN-2026-08/currency", "amount", "z")
    ones, twos = set(), set()
    for seed in seeds:
        for purpose in purposes:
            for index in range(16):
                for salt in (0, 1):
                    kwargs = {"seed": seed, "purpose": purpose, "index": index, "salt": salt}
                    ones.add(draw_below(bound=1, **kwargs))
                    twos.add(draw_below(bound=2, **kwargs))
                    assert pick(("BRL",), **kwargs) == "BRL"
    assert ones == {0}
    assert twos == {0, 1}, "a bound of 2 that draws one value would make the above vacuous"


@pytest.mark.parametrize(
    ("field", "value"),
    [("seed", 20260810), ("stream_id", "F1B-OTHER"), ("window_start", "2026-07-01T00:00:00.000Z")],
)
def test_a_different_seed_or_stream_produces_a_different_stream(field, value):
    """The other half of T1: reproducibility is worthless if everything reproduces.

    `stream_id` is in here because it is folded into every derivation purpose -- two
    emission runs of one seed must not be the same payments -- and `window_start`
    because a stream generated for another month must not be the same payments with
    the dates rewritten."""
    assert to_jsonl(records_for(_spec(**{field: value}))) != to_jsonl(records_for(_SPEC))


def test_the_serialised_stream_carries_no_carriage_return():
    """The Windows trap, closed at the only place this repository can close it.

    `to_jsonl` joins on a literal `"\\n"`. An `os.linesep` join -- or a text-mode
    write with default newline translation -- yields `\\r\\n` here and `\\n` on
    Databricks, so "the same seed produces the same bytes" would be true only within
    one operating system. This repo has already paid for that once, in a file-rewrite
    probe that "restored" a file and left it byte-changed."""
    text = to_jsonl(records_for(_SPEC))
    assert "\r" not in text
    assert LINE_SEPARATOR == "\n"
    assert text.endswith(LINE_SEPARATOR), "lines are TERMINATED, so chunks concatenate"
    assert len(text.splitlines()) == _SPEC.event_count


def test_the_generator_reads_no_clock_and_no_file():
    """PURITY AS A PROPERTY OF THE SOURCE, swept over the whole package.

    Over the AST and not over the text, for the reason
    `test_no_module_that_runs_on_databricks_asks_the_engine_to_cache` gives: the
    modules' docstrings EXPLAIN that there is no `now()` and no `random`, so a
    substring check would go red on the documentation of the very property it guards.

    `datetime` itself is not banned -- `opl.generator.instants` needs it to turn an
    integer into text -- so what is banned is the ABILITY to acquire an ambient value:
    the constructors of now, the `random` module, the environment, and `open`."""
    banned_calls = {"now", "utcnow", "today", "time", "monotonic", "open"}
    banned_modules = {"random", "os", "time", "secrets", "uuid"}
    for path in sorted(_GENERATOR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = [alias.name.split(".")[0] for alias in node.names]
                found = [root for root in roots if root in banned_modules]
                assert not found, f"{path.name} imports {found}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in banned_modules, (
                    f"{path.name} imports from {node.module}"
                )
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in banned_calls, (
                    f"{path.name}:{node.lineno} calls {node.func.attr}() -- the "
                    "generator's output must be a function of its arguments alone"
                )


def test_the_generator_runs_where_pyspark_is_not_installed():
    """Modelled on `tests/vault/test_hashing.py::test_the_module_is_pure_and_spark_
    free`: a subprocess with a meta-path blocker, so this catches pyspark arriving
    through ANY future import rather than the literal one grepped for today.

    In-process would prove nothing -- pyspark is installed in this environment and
    already in `sys.modules` before any test runs."""
    probe = textwrap.dedent(
        """
        import sys

        class _NoPyspark:
            def find_spec(self, name, path=None, target=None):
                if name == "pyspark" or name.startswith("pyspark."):
                    raise ImportError("pyspark is not installed here")
                return None

        sys.meta_path.insert(0, _NoPyspark())
        from opl.generator.cnpj_pool import validated_pool
        from opl.generator.defects import DefectSpec, delivered_records
        from opl.generator.events import to_jsonl
        from opl.generator.measures import duplicate_row_count
        from opl.generator.stream import StreamSpec, records_for

        pool = validated_pool([f"{n:08d}" for n in range(1, 21)])
        spec = StreamSpec(
            seed=20260809, stream_id="F1B-CLEAN", event_count=24, repeat_count=4,
            window_start="2026-06-01T00:00:00.000Z", event_interval_ms=250,
            emission_lag_ms=1500, cnpj_pool=pool,
        )
        text = to_jsonl(records_for(spec))
        assert len(text.splitlines()) == 24
        defective = delivered_records(spec, DefectSpec(duplicate_count=2))
        assert duplicate_row_count(defective) == 2
        assert "pyspark" not in sys.modules
        print("ok")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


# --- T1: the two timestamps, and the order they agree on -------------------------------


def test_event_times_advance_by_whole_intervals_from_the_declared_window():
    """No clock is read, so every `event_time` is arithmetic on the spec. Asserted as
    the exact value rather than as "increasing", because "increasing" would also hold
    for a stream that read the clock once and added to it."""
    start = from_text(_SPEC.window_start)
    for index, record in enumerate(records_for(_SPEC)):
        assert record["event_time"] == to_text(start + index * _SPEC.event_interval_ms)


def test_nothing_is_emitted_before_it_happened():
    """`emitted_at >= event_time` for every record, in every stream. The invariant a
    late arrival does NOT break: a late record is emitted after its neighbours, not
    before itself."""
    for record in records_for(_SPEC):
        assert from_text(record["emitted_at"]) >= from_text(record["event_time"])


def test_the_clean_stream_contains_no_late_arrival():
    """THE PROPERTY TASK 2 NEGATES, pinned before it is negated.

    A late arrival is a record emitted AFTER records whose `event_time` is newer than
    its own. So in a stream with none, sorting by `emitted_at` and sorting by
    `event_time` give the same order -- asserted here as both sequences being strictly
    increasing in emission order, which is the same statement without a sort to get
    wrong.

    AND IT IS NOW A MEASUREMENT RATHER THAN AN ARITHMETIC IDENTITY. When this was
    written, `emitted_at = event_time + constant` meant the assertion could not have
    failed for any stream this module could produce, and its author said so. The last
    line runs `opl.generator.measures.late_arrivals` -- the same function that returns
    five over `tests/test_payment_defects.py`'s late-arrival stream, and the same
    definition Task 4's query mirrors -- so the zero here is a fact about this stream
    rather than about the formula."""
    records = records_for(_SPEC)
    assert late_arrivals(records) == ()
    events = [from_text(r["event_time"]) for r in records]
    emissions = [from_text(r["emitted_at"]) for r in records]
    assert events == sorted(events) and len(set(events)) == len(events)
    assert emissions == sorted(emissions) and len(set(emissions)) == len(emissions)
    assert [r["transaction_id"] for r in sorted(records, key=lambda r: r["emitted_at"])] == [
        r["transaction_id"] for r in sorted(records, key=lambda r: r["event_time"])
    ]


# --- T2: a duplicate is not a repeat ---------------------------------------------------


def test_distinct_identities_and_distinct_business_attributes_disagree():
    """THE TEST THAT CLOSES T2, in the exact shape the plan asks for.

    If these two counts agreed, the fixture would contain no legitimate repeat and a
    dedup that collapsed repeats would pass every test in this file. The numbers are
    EXACT rather than an inequality because the generator guarantees distinct
    attributes for base events -- see `stream._distinct_attributes`, which refuses
    rather than emitting a collision, which is what makes 20 predictable instead of
    measured."""
    records = records_for(_SPEC)
    assert len({r["transaction_id"] for r in records}) == _SPEC.event_count == 24
    assert len({business_attributes_of(r) for r in records}) == _SPEC.base_count == 20
    assert _SPEC.event_count - _SPEC.repeat_count == 20


def test_a_legitimate_repeat_shares_every_attribute_and_no_identity():
    """What a repeat IS, asserted row by row: same payer, payee, amount, currency and
    method as some earlier event; its own `transaction_id`; its own timestamps.

    That last clause matters. A repeat sharing the identity would be a DUPLICATE, and
    a repeat sharing the timestamps would be indistinguishable from one."""
    records = records_for(_SPEC)
    by_attributes: dict[tuple[str, ...], list[dict[str, str]]] = {}
    for record in records:
        by_attributes.setdefault(business_attributes_of(record), []).append(record)
    repeated = [group for group in by_attributes.values() if len(group) > 1]
    assert sum(len(group) - 1 for group in repeated) == _SPEC.repeat_count == 4
    for group in repeated:
        identities = {r["transaction_id"] for r in group}
        assert len(identities) == len(group), "a repeat must not share an identity"
        assert len({r["event_time"] for r in group}) == len(group)


def test_the_identity_is_not_a_function_of_the_business_attributes():
    """The failure mode `opl.contracts.payments` forbids in prose, refused in data.

    An id derived from the attributes would make every legitimate repeat collide onto
    one id -- and then "same id twice" would mean either a redelivery or ordinary
    business, and no downstream dedup claim could be falsified."""
    records = records_for(_SPEC)
    groups: dict[tuple[str, ...], set[str]] = {}
    for record in records:
        groups.setdefault(business_attributes_of(record), set()).add(record["transaction_id"])
    assert any(len(identities) > 1 for identities in groups.values())


# --- T3: the counterparties -------------------------------------------------------------


def test_every_counterparty_comes_from_the_pool_it_was_handed():
    """T3, at the boundary where it is checkable.

    The generator draws only from `spec.cnpj_pool`, so the resolution rate against
    `hub_empresa` is 100% exactly when the pool came from `hub_empresa` -- and that is
    a property of ONE extraction, measurable once, rather than a claim smeared through
    the generator. Task 4 measures the other half against the real hub."""
    pool = set(_SPEC.cnpj_pool)
    for record in records_for(_SPEC):
        assert record["payer_cnpj_basico"] in pool
        assert record["payee_cnpj_basico"] in pool


def test_a_company_never_pays_itself():
    """Excluded by construction (`stream._counterparties` steps the payee over the
    payer rather than redrawing), so every counterparty-pair count downstream can be
    read without asking whether it means intra-group settlement."""
    for record in records_for(_SPEC):
        assert record["payer_cnpj_basico"] != record["payee_cnpj_basico"]


# --- the typed / string boundary --------------------------------------------------------


def test_nothing_but_a_non_empty_string_leaves_the_generator():
    """HOW TYPED EMISSION AND STRING BRONZE ARE RECONCILED, asserted over a whole
    stream rather than argued.

    `record_of` is annotated `dict[str, str]` and this is the run-time half: no `int`,
    no `float`, no `bool`, no `None` and no `""` in any contract column. `bool` is
    checked explicitly because it is an `int` subclass and `isinstance(True, str)` is
    the only check that would not have caught it.

    The consequence is the one that makes Task 2 measurable: a NULL in the payments
    bronze table can only mean the JSON did not carry the field."""
    for record in records_for(_SPEC):
        assert tuple(record) == COLUMNS, "key order is the emitted byte order"
        for column, value in record.items():
            assert type(value) is str, f"{column} left the generator as {type(value)}"
            assert value != "" and value.strip() == value and value.strip() != ""


def test_the_amount_is_rendered_from_integer_cents_with_a_fixed_scale():
    """No float exists anywhere in the path. `format_amount` is integer division, so
    there is no rounding mode to get wrong and no shortest-round-trip repr to move
    between platforms."""
    assert format_amount(12345) == "123.45"
    assert format_amount(5) == "0.05"
    assert format_amount(100) == "1.00"
    assert format_amount(5_000_000) == "50000.00"
    for record in records_for(_SPEC):
        whole, _, fraction = record["amount"].partition(".")
        assert len(fraction) == 2 and whole and "e" not in record["amount"].lower()


@pytest.mark.parametrize("cents", [0, -1, 1.5, True, "100"])
def test_an_amount_that_is_not_a_positive_whole_number_of_cents_is_refused(cents):
    """Zero and negatives are refunds and reversals -- a different event this contract
    does not model -- not payments with an unusual sign."""
    with pytest.raises((TypeError, ValueError)):
        format_amount(cents)


def test_the_currency_and_method_come_from_the_declared_domains():
    """AND THE CURRENCY COMES FROM THE SPEC, NOT FROM THE CONTRACT'S DOMAIN.

    This test asserted `record["currency"] in CURRENCIES` until F-API, and that
    assertion survives the widening while saying strictly less than it used to: with two
    members in the domain it passes for a stream that draws either one. The property that
    is load-bearing after T1 is that a spec's stream draws only what THAT SPEC declares,
    which is what keeps a one-tuple spec byte-identical to the day it was written."""
    assert _SPEC.currencies == DEFAULT_CURRENCIES == ("BRL",)
    assert set(_SPEC.currencies) < set(CURRENCIES), "the domain is wider than this draw"
    for record in records_for(_SPEC):
        assert record["currency"] in _SPEC.currencies
        assert record["payment_method"] in PAYMENT_METHODS


def test_a_stream_drawing_two_currencies_draws_both_and_only_from_its_own_tuple():
    """THE OTHER HALF, because a one-currency stream cannot tell a working draw from a
    constant. With `("BRL", "USD")` the same code has to produce both values -- and only
    those two -- at indices decided by the same purpose, index and salt as before."""
    mixed = _spec(currencies=("BRL", "USD"))
    drawn = [record["currency"] for record in records_for(mixed)]
    assert set(drawn) == {"BRL", "USD"}, "a two-tuple that draws one value is a constant"
    assert set(drawn) <= set(CURRENCIES)


def test_a_contract_column_with_no_renderer_is_refused(monkeypatch):
    """Both directions, because neither fails at run time: a missing renderer drops a
    column from every record, and a surplus one injects a key the contract does not
    declare -- which the DQ gate would read as drift a clean stream never meant to
    emit."""
    monkeypatch.setattr(events_module, "COLUMNS", COLUMNS + ("settlement_id",))
    with pytest.raises(ValueError, match="no renderer"):
        events_module._assert_every_column_has_a_renderer()
    monkeypatch.setattr(events_module, "COLUMNS", COLUMNS[:-1])
    with pytest.raises(ValueError, match="does not declare"):
        events_module._assert_every_column_has_a_renderer()


# --- the instant format -----------------------------------------------------------------


def test_an_instant_round_trips_through_its_one_text_form():
    for milliseconds in (0, 1, 999, 1_000, 1_780_000_000_123):
        assert from_text(to_text(milliseconds)) == milliseconds
    assert to_text(0) == "1970-01-01T00:00:00.000Z"
    assert to_text(1_780_000_000_123) == "2026-05-28T20:26:40.123Z"


@pytest.mark.parametrize(
    "text",
    [
        "2026-06-01T00:00:00Z",           # no fractional part
        "2026-06-01T00:00:00.000000Z",    # six fractional digits
        "2026-06-01T00:00:00.000+00:00",  # a second spelling of UTC
        "2026-06-01 00:00:00.000Z",       # a space instead of T
        "2026-06-01T00:00:00.5Z",         # one fractional digit: strptime WOULD take it
    ],
)
def test_every_other_iso_spelling_is_refused(text):
    """All five are legal ISO-8601 and all five are refused, because `to_text` emits
    exactly one of them and the two functions have to be inverses. The last one is the
    reason the regex runs BEFORE `strptime`: `%f` accepts one to six digits, so
    `strptime` alone would read it as 500 ms and render it back differently."""
    with pytest.raises(ValueError, match="is not an instant"):
        from_text(text)


# --- what the spec refuses ---------------------------------------------------------------


def test_a_repeat_count_that_leaves_no_base_event_is_refused():
    """A repeat copies an EARLIER base event, so position 0 is always a base and at
    most `event_count - 1` positions can repeat."""
    with pytest.raises(ValueError, match="leaves no room"):
        _spec(event_count=4, repeat_count=4)


def test_a_pool_that_is_not_canonical_is_refused_rather_than_sorted():
    """The pool's ORDER decides which company gets which payment. A spec that quietly
    sorted its input would generate a different stream from the one its literal
    argument describes -- and the argument is what a reader reconstructs a run from."""
    with pytest.raises(ValueError, match="not canonical"):
        _spec(cnpj_pool=tuple(reversed(_POOL)))


@pytest.mark.parametrize(
    ("currencies", "expected"),
    [
        (("BRL", "EUR"), "not a subset"),
        (("USD", "BRL"), "reorders the contract"),
        (("BRL", "BRL"), "repeats a member"),
        ((), "non-empty tuple"),
        ("BRL", "non-empty tuple"),
        (["BRL"], "non-empty tuple"),
    ],
)
def test_a_currency_tuple_that_is_not_a_subsequence_of_the_domain_is_refused(
    currencies, expected
):
    """FOUR DISTINCT SILENT FAILURES BEHIND ONE GUARD, and none of them raises anywhere
    else.

    OUTSIDE THE DOMAIN lands rows `dim_currency` has no member for: they resolve to the
    ghost key, which is the one population that dimension's own tests assert cannot exist.

    REORDERED is the one that moves landed bytes, and it is why the rule is a SUBSEQUENCE
    and not a subset. `pick` is positional, so `("USD", "BRL")` makes index 0 mean USD and
    flips the currency of every row in the stream -- with the same declared members, the
    same seed and the same purpose.

    REPEATED gives one currency a doubled share while reading as a tuple that mentions
    each once.

    A BARE `str` IS THE FOURTH, and it is refused here rather than three frames down in
    `pick`: `"BRL"` satisfies `Sequence[str]`, so the draw would return one CHARACTER and
    the stream's currency column would hold `B`, `R` or `L`. A `list` is refused with it,
    because the field is part of a frozen spec that has to stay hashable."""
    with pytest.raises((TypeError, ValueError), match=expected):
        _spec(currencies=currencies)


def test_a_lower_case_stream_id_is_refused_because_the_hash_folds_case():
    """`hash_key` upper-cases every component, so `"f1b"` and `"F1B"` would derive the
    SAME stream while reading as two. Refusing lower case makes the folding a no-op
    instead of a trap."""
    with pytest.raises(ValueError, match="carries"):
        _spec(stream_id="f1b-clean")


@pytest.mark.parametrize(
    ("field", "value"),
    [("event_count", 0), ("event_interval_ms", 0), ("seed", -1), ("emission_lag_ms", -1)],
)
def test_a_degenerate_spec_field_is_refused(field, value):
    """`event_interval_ms=0` is the interesting one: it does not fail, it produces a
    stream where every event shares one `event_time`, which makes the emission-order
    property vacuously true and the late-arrival test unable to fail."""
    with pytest.raises((TypeError, ValueError)):
        _spec(**{field: value})


def test_two_derivation_purposes_differing_only_in_case_are_refused(monkeypatch):
    """Two fields on one purpose draw the SAME 256-bit value at every index, so they
    would be perfectly correlated forever in a stream that reproduces perfectly."""
    monkeypatch.setattr(stream_module, "PURPOSES", ("payer", "PAYER"))
    with pytest.raises(ValueError, match="collide after upper-casing"):
        stream_module._assert_purposes_are_distinct()


def test_the_live_purposes_pass_their_own_guard():
    """Guard the guard: the refusal above is monkeypatched, so this is what says the
    eight real purposes are distinct."""
    stream_module._assert_purposes_are_distinct()


def test_the_generator_refuses_rather_than_emitting_a_colliding_base_event(monkeypatch):
    """The claim `_distinct_attributes` makes, exercised by shrinking the attribute
    space until it cannot be met.

    With one amount, one currency, one method and a two-company pool there are exactly
    two distinct attribute tuples (A->B and B->A), so a third base event has nowhere to
    go. Emitting the collision instead would make the stream's distinct-attribute count
    unpredictable -- and Task 2's duplicate count subtracts from it."""
    monkeypatch.setattr(stream_module, "MIN_AMOUNT_CENTS", 100)
    monkeypatch.setattr(stream_module, "MAX_AMOUNT_CENTS", 100)
    monkeypatch.setattr(stream_module, "PAYMENT_METHODS", ("PIX",))
    tiny = validated_pool(["00000001", "00000002"])
    with pytest.raises(ValueError, match="could not be given business attributes"):
        generate(_spec(cnpj_pool=tiny, event_count=3, repeat_count=0))


def test_an_event_is_frozen_and_keyword_only():
    """`payer_cnpj_basico` and `payee_cnpj_basico` are adjacent, both `str` and both
    eight characters, so a positional construction that swapped them would type-check
    and reverse the direction of every payment in the stream."""
    event = generate(_SPEC)[0]
    with pytest.raises(TypeError):
        PaymentEvent("x", 0, 0, "a", "b", 1, "BRL", "PIX")  # type: ignore[misc]
    with pytest.raises(Exception):  # noqa: B017 -- FrozenInstanceError is not exported here
        event.amount_cents = 1  # type: ignore[misc]
    assert record_of(event)["transaction_id"] == event.transaction_id
