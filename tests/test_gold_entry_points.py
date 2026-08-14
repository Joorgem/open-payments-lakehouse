"""What each gold JOB ENTRY POINT does before Spark exists, and what its run log says --
the script side of the seam `tests/test_gold_job_wiring.py` holds the YAML side of.

SPLIT OUT OF THAT FILE BY F3 TASK 4, AT THE CAP AND NOT PAST IT. It stood at 805 lines
before this task -- already over the project's 800 (master protocol section 4.12: whoever
touches a file at the cap splits it FIRST) -- and a fourth entry point took it to 946. The
seam is the one the file's own section comment already drew: everything above `# --- the
entry point itself ---` is about a YAML declaring a TASK, and everything below it is about
a PYTHON MODULE under `databricks/src`. They share no fixture and no assertion.

WHAT IS DUPLICATED ACROSS THE SPLIT, and it is three lines: `_REPO` and `_SRC`. Everything
else moved wholesale. `_TASK_PARAMETERS` did NOT come with it, which is the one thing worth
saying: the wrong-kind sweep below used it to slice argv down to each task's arity, and it
now passes the FULL argv instead -- every entry point refuses the kind before it reads its
second argument, so a task handed one argument too many is refused for the reason under
test rather than for the extra argument. A second copy of that mapping would have been the
"one rule, two spellings" defect this repository keeps paying for.

WHAT A GOLD ENTRY POINT CAN GET WRONG THAT NO OTHER LAYER'S CAN. It is handed ONE registry
key and resolves everything else -- a satellite, that satellite's parent hub, a dimension,
three conformed dimensions -- from a registry. So the paste it meets is a name from ANOTHER
gold job: registered, spelled correctly, and of the wrong KIND. Every one of those must be
refused on the driver, before `getOrCreate()` starts a serverless session that costs money.

Nothing here starts Spark. The entry point is loaded by path with the importlib pattern the
other task tests use -- `databricks/src` scripts are job entry points, not part of the opl
wheel."""
from __future__ import annotations

import ast
import importlib.util
from datetime import date
from pathlib import Path

import pytest

from opl.config import (
    DEFAULT,
    SENTINEL_MONTH,
    SESSION_TIMEZONE,
    SESSION_TIMEZONE_CONFIG,
)
from opl.gold.columns import GHOST_ROWS
from opl.gold.dimensions import DimensionLoadResult
from opl.gold.facts import FactLoadResult
from opl.gold.registry import REGISTRY as GOLD_REGISTRY
from opl.gold.specs import (
    ConformedDimension,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
)

_REPO = Path(__file__).resolve().parents[1]
_SRC = _REPO / "databricks" / "src"

_ENTRY_POINTS = (
    "gold_load_dimension",
    "gold_load_conformed_dimension",
    "gold_load_pit",
    "gold_load_fact",
)

# The two entry points whose RUN LOG prints the ghost in an arithmetic line, and so the two
# that must read `GHOST_ROWS` from the module that declares it. Enumerated rather than
# derived from `_ENTRY_POINTS`: the PIT task has no ghost of its own -- it writes pointers
# into satellites, and the ghost belongs to the dimension the pointers are read beside --
# and the fact task counts ghost REFERENCES per role, which is a measurement over the
# written table rather than a row count it has to reconcile.
_GHOST_PRINTING_ENTRY_POINTS = ("gold_load_dimension", "gold_load_conformed_dimension")

# How many names each entry point qualifies through `opl.config.DEFAULT.table`. Three of
# the four qualify three, for three different threes: the SCD2 task qualifies the
# dimension, its source satellite and that satellite's parent hub; the conformed task
# qualifies the dimension, the fact source whose members it measures against, and the
# satellite a calendar spans; the PIT task qualifies the table, its hub, and its
# satellites -- the last of those in ONE call inside a comprehension over the declared
# list, which is why the number is three and not four.
#
# THE FACT TASK QUALIFIES FIVE, and the fourth and fifth are the point of the count: it is
# the only task in this repository that reads OTHER GOLD TABLES, and since F-API the only one
# that reads ANOTHER LAYER'S. The fact, the bronze payments it is grained on, `bronze_ptax`,
# `dim_company`, and the three conformed dimensions -- the last again in ONE call inside a
# comprehension, which is why it is five and not seven.
#
# IT READ FOUR UNTIL THE FX COLUMNS LANDED, and the number is enumerated rather than derived
# for exactly that reason: a task that gained an input without gaining a line here would be a
# task reading a table nothing in this file knows it reads.
_QUALIFIED_NAMES = {
    "gold_load_dimension": 3,
    "gold_load_conformed_dimension": 3,
    "gold_load_pit": 3,
    "gold_load_fact": 5,
}

# The two `opl.config` names every session pin is written from. Spelled as the IDENTIFIER
# and not as the value, because that is what the AST locks below look for: a task that
# hardcoded `"UTC"` would satisfy a check on the value and would be the second spelling.
SESSION_TIMEZONE_CONFIG_NAME = "SESSION_TIMEZONE_CONFIG"
SESSION_TIMEZONE_NAME = "SESSION_TIMEZONE"

# WHICH KIND EACH ENTRY POINT BUILDS -- the one declaration the wrong-kind sweep below is
# derived from, so that adding a task or a kind cannot leave a pairing untested. The
# conformed entry is the UNION and not either half, because that task genuinely builds
# both and `isinstance` accepts a `X | Y` as a class tuple would.
_BUILDS = {
    "gold_load_dimension": Scd2Dimension,
    "gold_load_conformed_dimension": ConformedDimension,
    "gold_load_pit": PointInTimeTable,
    "gold_load_fact": PaymentFact,
}

# EVERY (entry point, registered table it must refuse) pairing, derived rather than
# listed -- see the test's own docstring for what the hand-written list of six could not
# keep saying once a fourth task existed.
_WRONG_KINDS = tuple(
    (script, name)
    for script, kind in _BUILDS.items()
    for name, spec in sorted(GOLD_REGISTRY.items())
    if not isinstance(spec, kind)
)

# WHAT EACH TASK'S KIND REFUSAL SAYS, AND THE SWEEP BELOW IS RED WITHOUT IT. Measured, by
# stubbing all four guards to the identity function and re-running: `pytest.raises(
# ValueError)` alone kept 13 of the 18 pairings GREEN, because argv[1] is a months window
# and the three tasks that read it as a `load_date` raise their own ValueError -- "is not
# an ISO-8601 timestamp" -- from the very next line. An unfalsifiable assertion is worth
# less than no assertion, because it is counted.
#
# A FRAGMENT PER SCRIPT AND NOT ONE SHARED WORD. "is not" would match the ISO-8601 message
# too and re-open the hole at the first re-wording; each of these names the KIND the task
# builds, which is the thing under test and the one sentence only that guard can produce.
_REFUSED_AS = {
    "gold_load_dimension": "is not an SCD2 dimension",
    "gold_load_conformed_dimension": "is not a conformed dimension",
    "gold_load_pit": "is not a point-in-time table",
    "gold_load_fact": "is not the payment fact",
}


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"{name}_task", _SRC / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tree(script: str) -> ast.Module:
    return ast.parse((_SRC / f"{script}.py").read_text(encoding="utf-8"), filename=script)


def _non_docstring_strings(tree: ast.Module) -> list[str]:
    """Every string literal in `tree` that is not a docstring -- this repository's prose
    names the thing a module must NOT do in order to explain why, so a check over the raw
    text would refuse the explanation along with the thing."""
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.FunctionDef | ast.ClassDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_gold_entry_point_spells_a_catalog_or_a_schema(script):
    """The qualification comes from `opl.config.DEFAULT.table` and from nowhere else.

    `opl.gold.registry` states the division this task is the other half of: a spec
    carries an unqualified name and the loader takes qualified tables as arguments. Gold
    is where that division is most easily broken, because a gold task qualifies THREE
    names -- and Free Edition's single catalog is what would make a hardcoded one
    invisible."""
    qualification = f"{DEFAULT.catalog}.{DEFAULT.schema}."
    spelled = [
        value for value in _non_docstring_strings(_tree(script)) if qualification in value
    ]
    assert not spelled, (
        f"{script}.py spells {qualification!r} in {spelled}. Catalog and schema come from "
        "opl.config.DEFAULT.table(name); a second spelling is a coordinate that drifts "
        "the day this project is on a workspace with more than one catalog"
    )
    qualifies = [
        node
        for node in ast.walk(_tree(script))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "table"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DEFAULT"
    ]
    assert len(qualifies) == _QUALIFIED_NAMES[script], (
        f"{script}.py calls DEFAULT.table(...) {len(qualifies)} times, not "
        f"{_QUALIFIED_NAMES[script]}. The SCD2 task qualifies the dimension, its source "
        "satellite and that satellite's parent hub; the conformed task qualifies the "
        "dimension, the fact source it measures against and the satellite a calendar "
        "spans"
    )


def _timezone_pins(tree: ast.Module) -> list[tuple[str, ...]]:
    """Every `<something>.conf.set(A, B)` in `tree`, as the pair of NAMES it was handed.

    Names and not values, because the point of the lock is that neither half is spelled
    here: a task that wrote `"UTC"` inline would pass a check on the value and would be
    the second spelling this repository keeps paying for."""
    return [
        tuple(argument.id for argument in node.args if isinstance(argument, ast.Name))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "conf"
    ]


def test_the_pinned_session_timezone_is_utc_and_is_spelled_once():
    """The two values every pin below is made of, asserted where they are DECLARED.

    A `TIMESTAMP` is UTC micros and every conversion into or out of one resolves through
    `spark.sql.session.timeZone`; local Spark inherits the OS zone and serverless
    defaults to UTC, so an unpinned gold build writes `dim_company`'s version boundaries
    three hours apart in the two places while the fact asks as of an `event_time` the
    payment contract stamps in UTC. `opl.config` argues it at length."""
    assert (SESSION_TIMEZONE_CONFIG, SESSION_TIMEZONE) == ("spark.sql.session.timeZone", "UTC")


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_every_gold_entry_point_pins_the_session_timezone_after_it_gets_the_session(script):
    """THE PIN, ON THE ONE OBJECT THAT CAN CARRY IT.

    Serverless hands the task a session that already exists, so this cannot be a builder
    option the way `opl.spark.local_session` spells it -- it has to be set on the session
    the task was given, and it has to be set BEFORE anything reads a table, because the
    setting decides what `CAST(applied_date AS TIMESTAMP)` means.

    ASSERTED AS THE TWO IMPORTED NAMES rather than as a string: `opl.config` owns both
    halves for the reason it owns the catalog and the schema, and a task spelling `"UTC"`
    for itself is a coordinate that drifts."""
    tree = _tree(script)
    pins = _timezone_pins(tree)
    assert (SESSION_TIMEZONE_CONFIG_NAME, SESSION_TIMEZONE_NAME) in pins, (
        f"{script}.py does not call spark.conf.set({SESSION_TIMEZONE_CONFIG_NAME}, "
        f"{SESSION_TIMEZONE_NAME}); it found {pins}. Without it the build inherits "
        "whatever zone the cluster defaults to, and every interval bound it writes moves "
        "with it"
    )
    def line_of(matches) -> int:
        found = [node.lineno for node in ast.walk(tree) if matches(node)]
        assert len(found) == 1, f"{script}.py has {len(found)} of the call, expected 1"
        return found[0]

    got_session = line_of(
        lambda node: isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "getOrCreate"
    )
    pinned = line_of(
        lambda node: isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "conf"
    )
    assert got_session < pinned, (
        f"{script}.py pins the session timezone on line {pinned}, before it has a "
        f"session on line {got_session}"
    )


def test_the_local_session_factory_pins_the_same_timezone_the_entry_points_do():
    """The other half, and it must be the SAME two names.

    `opl.spark.local_session` is what every Spark test in this repository runs against,
    so a local session left on the operating system's zone means the suite asserts one
    set of instants and the workspace writes another -- which is exactly the gap this
    lock exists to close. It is a builder `.config(...)` there rather than a `conf.set`
    because that factory CREATES the session; the values are the same."""
    source = (_REPO / "src" / "opl" / "spark.py").read_text(encoding="utf-8")
    tree = ast.parse(source, filename="spark.py")
    configured = [
        tuple(argument.id for argument in node.args if isinstance(argument, ast.Name))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "config"
    ]
    assert (SESSION_TIMEZONE_CONFIG_NAME, SESSION_TIMEZONE_NAME) in configured, (
        "opl/spark.py does not pass "
        f"({SESSION_TIMEZONE_CONFIG_NAME}, {SESSION_TIMEZONE_NAME}) to .config(...); it "
        f"passes {configured}"
    )


@pytest.mark.parametrize("script", _ENTRY_POINTS)
def test_no_gold_entry_point_raises_system_exit(script):
    """Serverless runs these under IPython, where an uncaught `SystemExit` reports a
    SUCCESSFUL run as FAILED. Every task under databricks/src calls a bare `main()`."""
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "SystemExit" not in source
    assert source.rstrip().endswith('if __name__ == "__main__":\n    main()')


def test_the_entry_point_refuses_a_table_no_gold_registry_registers_before_spark():
    """`opl.gold.registry.table_spec` refuses before a session, naming the alternatives
    -- an operator who mistyped a table should not wait for a serverless start to be
    told so."""
    task = _load("gold_load_dimension")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["dim_compnay", "2026-06+2026-07", "2026-08-12T12:00:00"])


def test_the_entry_point_refuses_a_vault_table_handed_where_the_dimension_belongs():
    """The paste this layer actually produces. `sat_empresa_dados` is a real table, in a
    real registry, and it is the very table `dim_company` derives from -- so it is the
    single most plausible string to end up in that parameter. It is not a gold table, and
    the refusal has to say so before a session."""
    task = _load("gold_load_dimension")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["sat_empresa_dados", "2026-06+2026-07", "2026-08-12T12:00:00"])


@pytest.mark.parametrize(("script", "wrong_kind"), _WRONG_KINDS)
def test_each_gold_entry_point_refuses_the_kind_the_other_one_builds(script, wrong_kind):
    """THE PASTE THE SECOND GOLD JOB MADE POSSIBLE, now over EVERY (entry point, wrong
    table) pairing the registry can produce. Every name here is registered, every one is
    spelled correctly, `table` is the only parameter three of the four tasks take, and each
    would be refused only after a serverless session had started if the kind were not
    checked on the driver.

    DERIVED FROM `_BUILDS` AND THE REGISTRY, WHICH IS THE CHANGE F3 TASK 4 MADE. It was a
    hand-written list of six pairs -- correct, and one edit away from being a totality
    claim that quietly stopped being total: a fourth entry point would have added its own
    refusals to the list and left the OTHER three untested against the fifth kind, with
    nothing red. `_WRONG_KINDS` is now every pairing that exists, so a kind or a task added
    later acquires its pairings without anybody remembering to.

    IT IS NOT A THEORETICAL SWAP. `gold_load_dimension` resolves its source through
    `spec.source_satellite`; `CalendarDimension` deliberately spells its own satellite
    field `applied_date_source`, `PointInTimeTable` spells its own `satellites` and
    `PaymentFact` spells its own `company_dimension`, so the wrong kind cannot walk far
    enough into any of these tasks to build something plausible out of a spec that is not
    one.

    BOTH DIRECTIONS FOR EVERY KIND, WHICH IS THE PROPERTY THIS PARAMETRISATION EXISTS FOR.
    A kind added to the registry acquires the OTHER tasks' refusals for free only if those
    refusals are inclusions (`isinstance(spec, Scd2Dimension)`) rather than exclusions --
    and this sweep is what proves they are, on the one file where an exclusion was actually
    found (see `opl.gold.registry
    ._assert_no_two_dimensions_draw_from_one_payment_column`).

    THE FULL ARGV IS PASSED TO EVERY TASK, INCLUDING THE THREE THAT TAKE TWO ARGUMENTS,
    and that is what let `_TASK_PARAMETERS` stay in the YAML half of the split rather than
    be spelled twice.

    AND THE MESSAGE IS MATCHED, WHICH IS THE CORRECTION THIS DOCSTRING IS. It used to claim
    that a task handed one argument too many "is refused for the reason under test", as a
    statement about where the guard stands. The claim about `main` is true -- every one of
    the four refuses the kind before reading argv[1] -- but the ASSERTION could not see it:
    argv[1] is a months window, and for the three tasks that read argv[1] as a `load_date`
    it is itself a ValueError. Stubbing all four guards to the identity function left 13 of
    these 18 pairings GREEN. `match=` is what closes that: with the guard gone the error is
    an ISO-8601 ValueError (three tasks) or an `AttributeError` (the SCD2 one), and neither
    carries `_REFUSED_AS`'s fragment, so all 18 go red.

    IT NOW ASSERTS THE ORDER AS WELL AS THE REFUSAL, and that is why argv[1] stays a months
    window rather than being made a valid timestamp. A guard moved below `required_load_date`
    would still raise ValueError on this argv, but it would raise the WRONG one -- so the
    ordering claim above is now the thing the match is measuring rather than a remark
    beside it."""
    task = _load(script)
    with pytest.raises(ValueError, match=_REFUSED_AS[script]):
        task.main([wrong_kind, "2026-06+2026-07", "2026-08-12T12:00:00"])


def test_the_conformed_entry_point_refuses_a_table_no_gold_registry_registers():
    """Refused before Spark, naming the alternatives, exactly as its sibling is."""
    task = _load("gold_load_conformed_dimension")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["dim_dat", "2026-08-12T12:00:00"])


def test_the_conformed_entry_point_validates_its_timestamp_before_the_session():
    """The only argument it takes besides the table, and it is checked ahead of
    `SparkSession.builder.getOrCreate()` -- a guard after the session would start one to
    reject an argument."""
    task = _load("gold_load_conformed_dimension")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["dim_channel", ""])


def test_the_pit_entry_point_refuses_a_table_no_gold_registry_registers():
    """Refused before Spark, naming the alternatives, exactly as its two siblings are."""
    task = _load("gold_load_pit")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["pit_estabeleciment", "2026-08-12T12:00:00"])


def test_the_pit_entry_point_validates_its_timestamp_before_the_session():
    """The only argument it takes besides the table, checked ahead of
    `SparkSession.builder.getOrCreate()` -- a guard after the session would start one to
    reject an argument, and this job's session is the most expensive in the repository."""
    task = _load("gold_load_pit")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["pit_estabelecimento", ""])


def test_the_fact_entry_point_refuses_a_table_no_gold_registry_registers():
    """Refused before Spark, naming the alternatives, exactly as its three siblings are."""
    task = _load("gold_load_fact")
    with pytest.raises(ValueError, match="unknown gold table"):
        task.main(["fact_paymnet", "2026-08-12T12:00:00"])


def test_the_fact_entry_point_validates_its_timestamp_before_the_session():
    """The only argument it takes besides the table, checked ahead of
    `SparkSession.builder.getOrCreate()` -- a guard after the session would start one to
    reject an argument."""
    task = _load("gold_load_fact")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["fact_payment", ""])


def _fact_result(**overrides):
    """A `FactLoadResult` with today's predicted numbers, one field replaced.

    Hand-built in `_dimension_result`'s shape and for its reason: the notes are pure
    functions of a frozen dataclass, and driving them through a real load to read one
    sentence would test the loader instead. The defaults are the job YAML's own
    predictions against today's bronze, so this fixture also pins them.

    THE NUMBERS MOVED WITH THE FOURTH AND FIFTH STREAMS, and the YAML moved with them: it
    read 20,150 / 20,000 / 18,400 / 1,600 for two promoted streams. `orphaned` is now keyed
    per FACT KEY and not per dimension, because `dim_date` answers two of them since F-API
    T4b -- an event day outside the calendar and an FX quote date outside it are different
    defects with different repairs."""
    fields = {
        "table": "workspace.default.fact_payment",
        "appended": 40_000,
        "already_present": 0,
        "source_rows": 40_150,
        "source_identities": 40_000,
        "source_tuples": 36_800,
        "retained_tuples": 36_800,
        "legitimate_repeats": 3_200,
        "unresolved": (("payer_company_sk", 0), ("payee_company_sk", 0)),
        "orphaned": (
            ("event_date_key", 0), ("fx_rate_date_key", 0), ("channel_key", 0),
            ("currency_key", 0),
        ),
        "fx_quotes": 42,
        "fx_first_published": "2026-06-03 16:03:00",
        "fx_last_published": "2026-07-31 16:10:31.061071",
        "fx_rates_used": 3,
    }
    fields.update(overrides)
    return FactLoadResult(**fields)


def test_the_resolution_note_reports_zero_as_unexercised_and_not_as_success():
    """MASTER PROTOCOL SECTION 4.6, in the one place this phase can actually reach it.
    `COALESCE(<as-of lookup>, GHOST)` cannot fire on this data -- 1,024 of 1,024
    counterparties resolve to `hub_empresa` and `dim_company` covers every hub key -- so
    the number will be 0 and the run log must not let that read as a proven path.

    AND THE DENOMINATOR IS (ROW, ROLE) AND NOT ROW. Two roles over 40,000 rows is 80,000
    references, which is the grain the acceptance has to be measured at: a fact resolving
    payers and not payees would otherwise report a 50% rate that reads as a data problem."""
    task = _load("gold_load_fact")
    unexercised = task._resolution_note(_fact_result(), 2)
    assert "80000 (row, role) references" in unexercised
    assert "UNEXERCISED" in unexercised
    exercised = task._resolution_note(
        _fact_result(unresolved=(("payer_company_sk", 3), ("payee_company_sk", 0))), 2
    )
    assert "UNEXERCISED" not in exercised
    assert "payer_company_sk: 3" in exercised and "payee_company_sk: 0" in exercised, (
        "the note sums the roles, so a fact that resolved one role and not the other "
        "reads as a data-quality rate rather than as a missing foreign key"
    )


def test_the_repeat_note_says_zero_legitimate_repeats_is_a_deletion_and_not_a_clean_stream():
    """THE ONE NUMBER IN THIS TASK THAT IS NOT A TAUTOLOGY. `COUNT(*) - COUNT(DISTINCT
    transaction_id)` re-measures bronze's own arithmetic and comes out at 150 whichever
    column the deduplication was taken over; the legitimate repeats do not. A fact
    deduplicated on the business attributes reports 0 here and 36,800 rows, and every other
    number it prints still looks plausible."""
    task = _load("gold_load_fact")
    survived = task._repeat_note(_fact_result())
    assert "3200 legitimate repeats survived" in survived
    deleted = task._repeat_note(
        _fact_result(appended=36_800, legitimate_repeats=0)
    )
    assert "NOT a clean stream" in deleted and "deleted real payments" in deleted


def test_the_integrity_note_tells_a_clean_star_from_a_stale_conformed_dimension():
    """THE PRICE OF DERIVING THE FOUR CONFORMED KEYS WITHOUT A JOIN, and the two states
    must not read alike. There is no ghost to fall back on for a derived key, so a payment
    whose day `dim_date` does not hold joins to nothing and vanishes from every report
    grouped by date -- which a row count cannot see. It is reported rather than refused
    because the repair is upstream and additive.

    NAMED PER FACT KEY, WHICH IS WHAT THE SECOND DATE ROLE FORCED. A note labelled `dim_date`
    would be true of both an event day and an FX quote date falling outside the calendar, and
    those have the same repair but not the same cause -- the second one means the extraction
    window reached back further than the calendar does."""
    task = _load("gold_load_fact")
    clean = task._integrity_note(_fact_result())
    assert "names a member that exists" in clean
    stale = task._integrity_note(
        _fact_result(
            orphaned=(
                ("event_date_key", 0), ("fx_rate_date_key", 10_000), ("channel_key", 0),
                ("currency_key", 0),
            )
        )
    )
    assert "fx_rate_date_key: 10000 rows" in stale and "Re-run the conformed build" in stale
    assert "event_date_key" not in stale, "a key with no orphan is named as if it had one"


def test_the_fx_note_reports_one_rate_as_the_state_the_phase_exists_to_end():
    """`fx_rates_used` IS THE NUMBER THIS SENTENCE EXISTS FOR. With a single-currency domain
    the rate was 1.00000 on every row, `amount_brl` equalled `amount` everywhere, and no test
    over either column could fail -- so ONE is reported as the state F-API set out to leave
    behind rather than as a clean conversion, exactly as zero unresolved references are
    reported as an unexercised path.

    THE SERIES' THREE NUMBERS RIDE ALONG BECAUSE GAPLESSNESS IS NOT REFUSED. `opl.gold.fx`
    declines to assert it -- any bound is either a Brazilian holiday calendar, which T3
    refuses, or a number drawn from one extraction window -- so a bounded extraction has to be
    visible in the log instead, as a quote count and a last publication instant that do not
    reach the days the fact needed."""
    task = _load("gold_load_fact")
    spec = GOLD_REGISTRY["fact_payment"]
    mixed = task._fx_note(_fact_result(), spec)
    assert "3 distinct amount_brl conversion rates" in mixed
    assert "42 reduced (currency, quote_date) quotes" in mixed
    assert "does NOT equal SUM(amount) x rate to the cent" in mixed
    single = task._fx_note(_fact_result(fx_rates_used=1), spec)
    assert "ONE rate" in single
    assert "the state a mixed-currency stream exists to end" in single


@pytest.mark.parametrize(
    ("appended", "already_present", "last_layer", "hub_keys", "expected"),
    [
        (144_193_416, 0, 72_318_968, 72_318_968, "which reconciles"),
        (144_193_416, 0, 72_318_960, 72_318_968, "does NOT reconcile"),
        (0, 144_193_416, 72_318_968, 72_318_968, "the re-run is a no-op"),
    ],
    ids=["reconciles", "hub keys with no history", "idempotent re-run"],
)
def test_the_pit_reconciliation_note_tells_three_states_apart(
    appended, already_present, last_layer, hub_keys, expected
):
    """THE THREE STATES MUST NOT READ ALIKE, which is why the note is a function.

    A shortfall means hub keys for which NO satellite ever wrote a row: they are absent
    from every layer of this table, and a run that printed only its own row count would
    leave that invisible -- the failure `gold_load_dimension.py` records for the mirror
    case, a satellite version whose hash key matched no hub row. The idempotent re-run is
    the third state and must not report a shortfall of everything.

    The numbers are Task 0's published ones, so this test also pins the reconciliation
    the job YAML predicts (`docs/f3-run-evidence.md` §0.5)."""
    task = _load("gold_load_pit")
    result = task.PitLoadResult(
        table="workspace.default.pit_estabelecimento",
        appended=appended,
        already_present=already_present,
        as_of_dates=(date(2026, 6, 13), date(2026, 7, 11)),
        hub_keys=hub_keys,
        rows_per_as_of=((date(2026, 6, 13), 71_874_448), (date(2026, 7, 11), last_layer)),
    )
    assert expected in task._reconciliation_note(result)


def test_the_window_and_the_timestamp_are_validated_before_the_session():
    """BOTH HALVES IN ONE RUN, and neither is reachable any other way without Spark: a
    good window and a load date that cannot parse must fail on the LOAD DATE, which
    proves the window was accepted -- and that both guards stand ahead of
    `SparkSession.builder.getOrCreate()`, because a guard after the session would start
    one to reject an argument."""
    task = _load("gold_load_dimension")
    with pytest.raises(ValueError, match="ISO-8601"):
        task.main(["dim_company", "2026-06+2026-07", ""])
    with pytest.raises(ValueError, match="no months were given"):
        task.main(["dim_company", SENTINEL_MONTH, "2026-08-12T12:00:00"])


# --- the line the run log actually carries -------------------------------------------


def _dimension_result(*, appended: int, already_present: int, versions: int = 4):
    """A `DimensionLoadResult` with the two counts a caller controls.

    Hand-built, in `tests/vault/test_satellite_diagnostics.py::_result`'s shape and for
    its reason: the note is a pure function of a frozen dataclass, and driving it through
    a real 69.2M-row load to see one sentence would test the loader instead."""
    return DimensionLoadResult(
        table="workspace.default.dim_company",
        appended=appended,
        already_present=already_present,
        source_versions=versions,
        distinct_keys=appended + already_present,
    )


def test_the_idempotent_re_run_line_cannot_be_printed_over_a_short_dimension():
    """THE DEFECT IN ITS EXACT SHAPE. `_reconciliation_note`'s re-run branch fired on
    `appended == 0 and already_present` and compared `already_present` to NOTHING -- so a
    dimension permanently short of a dangling satellite version printed a clean "the
    re-run is a no-op" on every later run, and the shortfall the function exists to make
    visible was invisible on the one branch a retry lands in.

    THE TWO INPUTS DIFFER BY ONE ROW AND MUST NOT READ ALIKE: four versions plus the
    ghost is five, and a target holding four is the state the old branch called a no-op.

    `main` IS ASSERTED TO PRINT THROUGH IT, which is what keeps this from being
    self-referential -- a test of the note alone stays green while `main` prints the old
    sentence beside it. Same closing assertion as the vault's."""
    task = _load("gold_load_dimension")
    expected = 4 + GHOST_ROWS

    short_rerun = task._reconciliation_note(
        _dimension_result(appended=0, already_present=4)
    )
    whole_rerun = task._reconciliation_note(
        _dimension_result(appended=0, already_present=expected)
    )
    fresh = task._reconciliation_note(
        _dimension_result(appended=expected, already_present=0)
    )
    short_fresh = task._reconciliation_note(
        _dimension_result(appended=4, already_present=0)
    )

    assert "does NOT reconcile" in short_rerun and "no-op" not in short_rerun
    assert "does NOT reconcile" in short_fresh
    assert "no-op" in whole_rerun and "does NOT reconcile" not in whole_rerun
    assert str(expected) in whole_rerun, (
        "the re-run line names no total, so it reconciles against nothing -- which is "
        "the defect this test exists for"
    )
    assert "which reconciles" in fresh and "no-op" not in fresh
    assert "_reconciliation_note(result)" in Path(task.__file__).read_text(
        encoding="utf-8"
    ), "main no longer prints through _reconciliation_note, so this test measures nothing"


@pytest.mark.parametrize("script", _GHOST_PRINTING_ENTRY_POINTS)
def test_the_ghost_row_count_is_one_constant_across_the_loader_and_the_task(script):
    """`GHOST_ROWS` is `opl.gold.columns`', and both gold entry points and both loaders
    read it. It was spelled twice -- once in `opl.gold.conformed`, once in this task --
    while `load_dimension` enforced nothing, so the two could disagree and the run log
    would print the arithmetic that agreed with itself. The loader now REFUSES a target
    that is not `source_versions + GHOST_ROWS`, which makes a second spelling a way to
    print a reconciliation against a total the load rejected.

    THE IMPORT PATH IS ASSERTED AND NOT ONLY THE VALUE, WHICH IS WHAT THIS TEST GAINED.
    It checked `gold_load_dimension` alone, and `gold_load_conformed_dimension` -- the
    other task whose run log prints a ghost -- took the constant from `opl.gold.conformed`
    instead. That module re-exports it only incidentally: `GHOST_ROWS` is absent from its
    `__all__`, so an `is` comparison passed while the task depended on an implementation
    detail of a module whose subject is not this constant. Both are checked now, and both
    are checked for WHERE they read it from, because one object reached by two paths stops
    being one object the moment either path is re-exported by something else."""
    task = _load(script)
    assert task.GHOST_ROWS is GHOST_ROWS == 1
    source = (_SRC / f"{script}.py").read_text(encoding="utf-8")
    assert "GHOST_ROWS = " not in source, "the task declares a GHOST_ROWS of its own"
    assert "from opl.gold.columns import GHOST_ROWS" in source, (
        f"{script} reads GHOST_ROWS through another module's incidental re-export rather "
        "than from opl.gold.columns, which is the one module that declares it"
    )


