# tests/test_streaming_delta_ingest.py
"""The half of T2 that needs no broker: the session pre-decision, and the refusals
`opl.streaming.ingest` makes before a query exists.

RUNS IN CI'S DEFAULT INVOCATION, which is the point of splitting it out. The Kafka read
itself is `tests/integration/test_payment_stream_ingest.py` and is deselected without a
Redpanda container; everything here is either pure or runs on the ordinary local session,
so the pre-decision below is guarded on every commit rather than only on the days a broker
is running.
"""
from __future__ import annotations

import ast
import inspect
import textwrap

import pytest

from opl.contracts.payments import COLUMNS
from opl.spark import KAFKA_CONNECTOR_PACKAGE, PACKAGES_CONFIG, local_session
from opl.streaming import ingest
from opl.streaming.ingest import (
    DECIDED_READER_OPTIONS,
    KAFKA_COLUMNS,
    LATEST,
    PROCESSING_IDENTITY,
    VALUE_COLUMN,
    payment_stream,
    write_payment_stream,
)

# The Spark option `_progress_of` reads the ring buffer's size off. Spelled here so the
# fake session below answers the key the SHIPPED code asks for and the default for every
# other -- a refusal reading a misspelled key would see 100, never fire, and pass a test
# whose fake answered whatever it was asked.
_PROGRESS_CAP_CONFIG = "spark.sql.streaming.numRecentProgressUpdates"


class _FakeConf:
    """`session.conf`, reduced to the one `get(key, default)` `_progress_of` calls."""

    def __init__(self, values: dict[str, str]):
        self._values = values

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._values.get(key, default)


class _FakeSession:
    def __init__(self, cap: str | None = None):
        self.conf = _FakeConf({} if cap is None else {_PROGRESS_CAP_CONFIG: cap})


class _FakeQuery:
    """A `StreamingQuery` reduced to `recentProgress`, which is all `_progress_of` reads.

    A dict per progress rather than a Mock: `p["numInputRows"]` on a Mock returns another
    Mock, and `sum()` over those would fail somewhere other than where the meaning is."""

    def __init__(self, progresses: list[dict[str, int]]):
        self.recentProgress = progresses


def _progresses(count: int, rows: int = 1) -> list[dict[str, int]]:
    return [{"batchId": i, "numInputRows": rows} for i in range(count)]


def test_the_default_local_session_carries_no_kafka_connector(spark):
    """THE PRE-DECISION, GUARDED. `opl.spark.local_session` builds the session all 2,683
    tests share, and the Kafka connector resolves 11 artifacts and 57 MB from Maven at
    SparkContext creation -- so it is an opt-in that defaults off, and a commit that
    flipped the default would put a network dependency on every one of them.

    Read off the SESSION rather than off the source, because that is where the cost would
    actually be paid: a default that changed via `configure_spark_with_delta_pip`, via a
    conftest, or via a `SPARK_SUBMIT_ARGS` in the environment is invisible to a source
    check and visible here."""
    resolved = spark.sparkContext.getConf().get(PACKAGES_CONFIG, "")
    assert "kafka" not in resolved, (
        f"the shared local session resolves {resolved!r}. The Kafka connector belongs on "
        "`local_session(kafka=True)` only -- see `opl.spark.KAFKA_CONNECTOR_PACKAGE`"
    )
    assert "io.delta" in resolved, (
        "and this is the floor on the assertion above: a session resolving NO packages at "
        "all would satisfy it for the wrong reason"
    )


def test_the_connector_coordinate_follows_the_running_pyspark():
    """A hardcoded `3.5.9` would survive a pyspark bump by resolving the WRONG connector,
    whose symptom is a NoSuchMethodError naming neither version."""
    import pyspark

    assert KAFKA_CONNECTOR_PACKAGE.endswith(f":{pyspark.__version__}")
    assert KAFKA_CONNECTOR_PACKAGE.startswith("org.apache.spark:spark-sql-kafka-0-10_2.12:")


def test_a_read_that_would_start_after_the_corpus_is_refused_before_a_session_is_touched():
    """`startingOffsets: latest` reads zero records from an already-published topic, and
    "no duplicates appeared" is then true and worthless.

    `None` IS PASSED AS THE SESSION ON PURPOSE: it is what makes this test say that the
    refusal happens BEFORE the reader is built, rather than merely that it happens. A
    refusal placed after `spark.readStream` would raise AttributeError here."""
    with pytest.raises(ValueError, match=f"refusing startingOffsets={LATEST!r}"):
        payment_stream(None, topic="t", bootstrap="localhost:9092", starting_offsets=LATEST)


def test_a_kafka_column_that_shadowed_a_contract_column_is_refused(monkeypatch):
    """The import-time guard, shown firing.

    A collision raises nowhere on its own: `select` keeps one of the two and the landed
    table carries a payment field under a name the exactly-once counts read as an offset.
    Monkeypatched rather than edited, so the shipped constants stay what they are while
    the guard is asked the question it exists for."""
    monkeypatch.setattr(ingest, "KAFKA_COLUMNS", (COLUMNS[0], *KAFKA_COLUMNS))
    with pytest.raises(AssertionError, match="both a Kafka metadata column and"):
        ingest._assert_the_kafka_columns_do_not_shadow_the_contract()


def test_a_kafka_session_taken_after_a_kafka_less_one_is_refused_by_name(spark):
    """THE GUARD THE WHOLE MARKER SEPARATION RESTS ON, shown firing.

    `spark.jars.packages` is applied when the SparkContext is created and ignored by every
    later builder, and the JVM here is process-wide -- so once this suite's ordinary
    session exists, `local_session(kafka=True)` gets THAT session back from `getOrCreate`,
    without the connector and without a word. Failing later inside `readStream` names a
    missing data source and not the session that was already open.

    THE `spark` FIXTURE IS THE PRECONDITION, NOT DECORATION: the Kafka-less session has to
    already exist for `getOrCreate` to hand one back, and requesting the fixture is what
    makes that true here regardless of which file pytest reaches first. `local_session("x")`
    is called anyway, and its identity asserted, because "the ordinary spelling returns the
    session that is already open" IS the hazard -- stated as a measurement rather than in
    the prose of `opl.spark`.

    WHAT THIS TEST LEAVES BEHIND, RECORDED BECAUSE IT CANNOT BE PUT BACK. `getOrCreate` on
    an existing session applies the builder's options to that session's RUNTIME conf, so
    after the refusal below `spark.conf.get("spark.jars.packages")` reports the delta
    coordinate with `,org.apache.spark:spark-sql-kafka-0-10_2.12:<pyspark version>` appended,
    for the rest of the process. Measured, on this session: the SparkConf is UNCHANGED, which
    is why nothing here is affected -- `_refuse_a_session_that_cannot_read_kafka` and
    `test_the_default_local_session_carries_no_kafka_connector` both read
    `sparkContext.getConf()`, not the runtime conf. It is not restored because the public API
    refuses to: `spark.conf.set` and `spark.conf.unset` on that key both raise
    `[CANNOT_MODIFY_CONFIG]`, a core Spark config being unsettable from user code even though
    `getOrCreate` has just set it. A later test that reads its packages off the RUNTIME conf
    would be reading this one's leftovers."""
    assert local_session("x") is spark
    with pytest.raises(RuntimeError, match="does not carry"):
        local_session("y", kafka=True)


def test_a_truncated_progress_ring_is_refused_rather_than_summed_over():
    """`recentProgress` IS A RING BUFFER, and a total taken over a full one is an
    UNDERCOUNT -- the same short read this phase refuses everywhere else, arriving through
    Spark's own instrument instead of through the broker.

    The refusal is unreachable at this phase's scale -- the shipped runs measure 1 and 3
    CONSUMING batches (`tests/integration/test_payment_stream_ingest.py` asserts
    `batch_ids == (0,)` and `== (0, 1, 2)`) against a default cap of 100 -- which is
    exactly why it needs a test rather than a comment: nothing else in the suite can tell a
    working guard from `if False`. Both arms here, and the cap comes from the FAKE SESSION's
    conf, so the test is over the comparison and not over the default.

    NO SPARK AND NO BROKER: a fake query holding a list of progress dicts and a fake
    session holding one conf entry are the whole of what `_progress_of` reads, so the arm
    that is unreachable in a real run is reachable here for free."""
    under = ingest._progress_of(_FakeQuery(_progresses(2, rows=5)), _FakeSession("3"))
    assert under == ((0, 1), 10)

    at_the_cap = _FakeQuery(_progresses(3, rows=5))
    with pytest.raises(RuntimeError, match="3 progress updates against a"):
        ingest._progress_of(at_the_cap, _FakeSession("3"))
    # ...and the SAME query is fine once the buffer is bigger than the run, which is what
    # says the refusal is about the ring filling and not about the number of batches.
    assert ingest._progress_of(at_the_cap, _FakeSession("4")) == ((0, 1, 2), 15)


def test_the_progress_cap_key_is_one_spark_actually_knows(spark):
    """THE FLOOR UNDER THE FAKE ABOVE, and the one thing that fake cannot supply.

    `_progress_of` reads the cap with a DEFAULT, and `spark.conf.get` answers a key it does
    not know by returning that default rather than by raising -- measured on this session: a
    misspelled key returns '100' when a default is passed and raises `SQL_CONF_NOT_FOUND`
    when one is not.

    THE TWO SPELLINGS, MUTATED ONE AT A TIME AND RUN. Misspell the shipped literal alone and
    the fake above goes red (it reads the fake's default 100, and the refusal does not fire
    on 3 progresses) while this test stays green -- so the fake is what pins the two
    spellings to EACH OTHER. Misspell BOTH the same way and the fake goes green again, this
    test being the only one in the file that fails; without it, `_progress_of` would read the
    caller's 100 forever on a real session and nothing here would say so.

    So: asked WITHOUT a default, on a real session, where a key Spark does not know raises.
    The value is asserted too, because `_progress_of`'s own fallback and the "(100 by
    default)" in its docstring are claims about Spark's default and not about this project's
    choice."""
    assert spark.conf.get(_PROGRESS_CAP_CONFIG) == "100"

    # ...and the floor on that: a key Spark does not know is silent under a default and
    # raises without one, which is the whole reason the assertion above omits it.
    misspelled = _PROGRESS_CAP_CONFIG + "s"
    assert spark.conf.get(misspelled, "100") == "100"
    with pytest.raises(Exception, match="SQL_CONF_NOT_FOUND"):
        spark.conf.get(misspelled)


def test_a_batch_that_consumed_no_rows_is_not_reported_as_a_batch():
    """`availableNow` ends with a progress update carrying 0 input rows, and counting it
    would make `batch_ids` disagree with the batch split the rate limit predicts -- which
    is the number T3 chooses its fault's batch id from."""
    mixed = _FakeQuery([{"batchId": 0, "numInputRows": 7}, {"batchId": 1, "numInputRows": 0}])
    assert ingest._progress_of(mixed, _FakeSession("100")) == ((0,), 7)


def test_the_default_floor_refuses_zero_and_a_short_read_is_not_its_job():
    """WHAT `minimum_rows=1` ACTUALLY DEFENDS, measured rather than described.

    The default is a floor against ZERO -- the drained checkpoint, the recreated topic,
    the spec that read from the wrong end -- and it accepts a run that consumed 1 record of
    29. "Every assertion carries a non-zero floor" is the stronger reading and this default
    does not give it; a SHORT read is caught by callers that state an exact count, or by
    callers that pass a real floor instead of the default. Read off the signature rather
    than off a literal typed twice, so a changed default lands here."""
    default = inspect.signature(write_payment_stream).parameters["minimum_rows"].default
    assert default == 1

    with pytest.raises(RuntimeError, match="consumed 0 records"):
        ingest._refuse_a_run_that_processed_nothing(0, default, "t")
    ingest._refuse_a_run_that_processed_nothing(1, default, "t")  # 1 of 29 -- accepted
    ingest._refuse_a_run_that_processed_nothing(29, default, "t")

    # ...and only an explicit floor sees the short read the default let through.
    with pytest.raises(RuntimeError, match="consumed 1 records"):
        ingest._refuse_a_run_that_processed_nothing(1, 29, "t")


class _FakeWriter:
    """A `DataStreamWriter` reduced to its two terminal methods, each recording the
    destination it was given. Neither returns a query, because `_start_at_the_one_
    destination` is the only thing under test and it does nothing with the return."""

    def __init__(self) -> None:
        self.started: list[tuple[str, str | None]] = []

    def start(self, path):
        self.started.append(("start", path))

    def toTable(self, table):  # noqa: N802 -- Spark's spelling, not this project's
        self.started.append(("toTable", table))


def _spelled_option_names() -> set[str]:
    """Every literal `payment_stream` hands to `.option(...)`, read off its own source."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(payment_stream)))
    return {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "option"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }


def test_the_refused_option_names_are_exactly_the_ones_the_reader_spells_itself():
    """THE LOCK THAT KEEPS THE REFUSAL LIST FROM ROTTING, and it is not bookkeeping.

    `broker_options` was added so a SASL_SSL broker's settings could reach the reader; the
    price is that a caller can now hand in ANY reader option. `DECIDED_READER_OPTIONS` is
    what stops one of them re-deciding something this function decided -- and a list that
    is maintained by hand goes stale in the direction that reports green: an option added
    to the reader and not to the frozenset becomes overridable by a caller, silently.

    Derived from the FUNCTION'S OWN SOURCE, so the two can only agree by being the same
    thing. `startingOffsets` is why the property matters: a caller-supplied `latest` would
    walk around the refusal in shipped code, through the parameter added to support SASL."""
    assert _spelled_option_names() == set(DECIDED_READER_OPTIONS)
    assert "startingOffsets" in DECIDED_READER_OPTIONS


@pytest.mark.parametrize("name", sorted(DECIDED_READER_OPTIONS))
def test_a_broker_option_that_re_decides_a_reader_decision_is_refused(name):
    """Every one of them, and BEFORE the session is touched -- `None` is passed as the
    session for `test_a_read_that_would_start_after_the_corpus_is_refused`'s reason: a
    refusal placed after `spark.readStream` would raise AttributeError here instead."""
    with pytest.raises(ValueError, match="which this function decides for itself"):
        payment_stream(
            None, topic="t", bootstrap="localhost:9092", broker_options={name: "x"}
        )


@pytest.mark.parametrize(
    "spelling",
    ["startingOffsets", "STARTINGOFFSETS", "startingoffsets", "StartingOffsets"],
)
def test_the_refusal_folds_case_because_the_reader_it_protects_folds_case(spelling):
    """FOUR SPELLINGS OF ONE OPTION, and the reader reads them as one.

    Measured on a `readStream` in this project's own session: `.option("header", "true")`
    followed by `.option("HEADER", "false")` returns the header row AS DATA, and the
    reverse order does not -- so the name is folded and the LAST spelling wins.
    `payment_stream` applies the caller's options AFTER its own, which makes the caller's
    the later one. An exact-match refusal would have refused `startingOffsets` and accepted
    `STARTINGOFFSETS`, which reaches the same option and overrides the same decision -- and
    `latest` over an already-published topic reads zero records, the one outcome no count
    downstream can tell from success."""
    with pytest.raises(ValueError, match="which this function decides for itself"):
        payment_stream(
            None, topic="t", bootstrap="localhost:9092", broker_options={spelling: LATEST}
        )


def test_the_security_options_a_managed_broker_needs_pass_the_refusal():
    """THE FLOOR UNDER THE SWEEP ABOVE: a refusal that rejected everything would satisfy it
    and would make the SASL door unusable. These three are what
    `opl.streaming.managed_broker.sasl_reader_options` returns."""
    ingest._refuse_options_that_reopen_a_decision(
        {
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-256",
            "kafka.sasl.jaas.config": "<a jaas string>",
        }
    )
    ingest._refuse_options_that_reopen_a_decision({})


@pytest.mark.parametrize(
    ("path", "table"), [(None, None), ("/tmp/sink", "workspace.default.t")]
)
def test_a_sink_with_two_destinations_or_none_is_refused(path, table):
    """Neither is no destination at all; both is a run whose counts describe nothing in
    particular. Refused rather than defaulted, because on the deploy target the two are not
    interchangeable -- Unity Catalog will not create a Delta table inside a Volume, so a
    serverless run has a NAME and no writable path, and a local test has the reverse."""
    with pytest.raises(ValueError, match="exactly one of `path`"):
        ingest._start_at_the_one_destination(_FakeWriter(), path, table)


@pytest.mark.parametrize(
    ("path", "table", "expected"),
    [
        ("/tmp/sink", None, ("start", "/tmp/sink")),
        (None, "workspace.default.t", ("toTable", "workspace.default.t")),
    ],
)
def test_each_destination_reaches_the_writer_method_that_can_take_it(path, table, expected):
    """`start(path)` and `toTable(name)` are different methods, and handing a catalog name
    to `start` creates a Delta table in a DIRECTORY LITERALLY CALLED
    `workspace.default.t` -- which succeeds, locally, and is discovered by a reader who
    cannot find the table."""
    writer = _FakeWriter()
    ingest._start_at_the_one_destination(writer, path, table)
    assert writer.started == [expected]


def test_the_kafka_columns_are_the_processing_identity_plus_the_record():
    """The landed row's three non-contract columns, stated once. The pair is what T3 counts
    duplicates over and the value is what the golden digest is rebuilt from -- so a column
    quietly dropped from this tuple would take one of those two claims with it."""
    assert KAFKA_COLUMNS == (*PROCESSING_IDENTITY, VALUE_COLUMN)
    assert len(set(KAFKA_COLUMNS)) == 3
    assert not set(KAFKA_COLUMNS) & set(COLUMNS)
