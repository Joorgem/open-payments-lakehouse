# tests/test_payment_streaming.py
"""ONE CORPUS, TWO TRANSPORTS: what the Kafka producer publishes, against what the Volume
lands, against the digest F1b pinned before Kafka was in this repository at all.

THE ANCHOR IS A LITERAL AND NOT A HELPER, which is the whole reason this file can say
anything. `tests/test_payment_generator.py::test_the_pinned_stream_is_this_one_and_no_other`
pins sha256 `b45f1dc7...` over 7,028 bytes of serialised text for one declared spec. Both
transports are compared against THAT 64-character constant below -- not against each other,
and not against a number this file computes twice. A digest test whose two sides are both
produced by the code under test reports equality under every outcome including the one
where nothing was published. The 10,000-record path has the SECOND such anchor,
`a527b61c...` from the committed `scripts/probe_byte_identity.py`, and it is there for the
same reason -- with one more. `opl.streaming.messages`' own rebuild refusal cannot see a
change to the SHARED serialiser at all, because both of its sides call `serialised_bytes`
and a change moves them together; its docstring says so. A digest taken outside the code
under test is what does see one, so both record counts here carry one.

THE SPEC AND THE PIN ARE COPIED FROM THAT FILE ON PURPOSE, AND THE COPY CANNOT ROT. A copied
fixture that drifted from its original would ordinarily be a silent divergence -- this file
green while describing a stream nothing produces. It cannot be, because
`test_the_kafka_frames_rebuild_the_pinned_file_byte_for_byte` asserts the FILE side of the
copy against the literal first: a `_SPEC` that no longer matches the one F1b pinned stops
producing `b45f1dc7...` and this file goes red before it ever reaches the Kafka side. The
alternative -- importing a local from another test module by path -- would tie the two files
together at collection time for no property the assertion above does not already give. The
`scripts/probe_byte_identity.py` import below is the opposite case, for the opposite reason:
nothing here re-derives the probe's numbers, so without the import no assertion in this file
can see them move -- measured, and it is what `test_the_pool_and_the_baseline_this_file_copies
_are_the_probes_own` exists for.

EVERY ASSERTION OVER A RECORD SET CARRIES A NON-ZERO FLOOR ON THE RECORDS ACTUALLY COMPARED,
and that is F4's standing instruction in its streaming shape. An empty record set satisfies
"every published value parses", "the multisets are equal" and "no duplicate appeared"
simultaneously and for free. So each such test states the count it is comparing over, and
the two counts it states -- 24 for the pinned spec, 10,000 for the declared profile -- are
the ones the declarations predict. Two exceptions, both deliberate. The tests over a broker
CONFIGURATION carry no floor because there is no record set for them to be vacuous over.
And the three staged delivery failures state their count AFTER the refusal rather than
before, because the number that matters there is how many records reached `produce()`, which
is a fact about the double and readable only once the call has returned.

NOTHING HERE OPENS A SOCKET. The producer takes an injected client, so what the digests are
taken over is the exact `value=` bytes handed to `produce()`. The same properties through a
real Redpanda are `tests/integration/test_payment_streaming_live.py`, which needs the
container and is deselected by default; this file runs in CI's default invocation today.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import textwrap
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from opl.bronze.generated_landing import serialised_bytes
from opl.contracts.payments import IDENTITY_COLUMN
from opl.generator import events as events_module
from opl.generator.cnpj_pool import validated_pool
from opl.generator.defects import DefectSpec, delivered_records
from opl.generator.profiles import CROSS_CURRENCY, POOL_SIZE, PROFILES
from opl.generator.stream import StreamSpec, records_for
from opl.streaming.messages import (
    _refuse_frames_that_do_not_rebuild_the_file,
    message_key,
    message_values,
)
from opl.streaming.producer import (
    BOOTSTRAP_ENV_VAR,
    PASSWORD_ENV_VAR,
    SASL_MECHANISM,
    USERNAME_ENV_VAR,
    BrokerConfig,
    broker_from_environment,
    publish_records,
)

# `scripts/` on the path so this file can IMPORT the probe whose baseline it copies, the way
# `tests/test_merchant_population.py` imports `merchant_population`. The alternative -- a
# comment naming the probe -- is what was here, and a reviewer falsified it: see
# `test_the_pool_and_the_baseline_this_file_copies_are_the_probes_own`.
_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import probe_byte_identity as probe  # noqa: E402

# THE PINNED SPEC AND THE PINNED DIGEST, copied verbatim from
# `tests/test_payment_generator.py`. See this module's docstring for why the copy is safe:
# the first test asserts the file side of it against the literal, so a drifted copy is a
# red test rather than a quiet divergence.
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
_PINNED_SHA = "b45f1dc7d507da08bcc3a5d2586317ca589aeda29187f68f5039ff4ba7ad9ac9"
_PINNED_BYTES = 7_028
_PINNED_ROWS = 24

# The declared profile this file replays, and the number that anchors it. 2,926,588 is
# `tests/test_payment_profiles.py`'s own pin for `cross-currency`, and it is POOL-INDEPENDENT
# by that file's stated argument -- every `cnpj_basico` is eight characters and both currency
# codes are three -- which is what lets a synthetic pool here reach the number a workspace run
# lands from `hub_empresa`. It is also the number that DISCRIMINATES the framing decision: a
# producer that stripped the `\n` from each value would publish 2,916,588 bytes, one per
# record short, and every other assertion in this file would still pass.
_CROSS_CURRENCY_BYTES = 2_926_588
_CROSS_CURRENCY_ROWS = 10_000

# AND THE DIGEST FOR THE SAME 10,000 RECORDS, copied from
# `scripts/probe_byte_identity.py`'s `_BASELINE["cross-currency"]` -- the committed artefact
# whose five numbers `docs/f-api-run-evidence.md` section 1 published. It reproduces here
# because the fixture below builds from the probe's OWN pool object rather than from a second
# spelling of it, and it gives the 10,000-record path the external anchor `b45f1dc7...` gives
# the 24-record one: a constant this file did not compute.
# `uv run python scripts/probe_byte_identity.py` re-derives it in ~70 s.
_CROSS_CURRENCY_SHA = "a527b61c19ec7933dd15bc7896ca4e34efeaa1b90d2bab768c1f0f80336b37dc"

_TOPIC = "opl-payments-test"


class _Message:
    """The delivery report's message object: the two accessors librdkafka's callback gets."""

    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class RecordingProducer:
    """Stands in for `confluent_kafka.Producer`: records every (topic, key, value) and
    acknowledges each one through the delivery callback the way the real client does when
    `flush` drains the queue.

    NOT A `Mock`, for `tests/conftest.py::FakeFilesApi`'s reason: a Mock grows whatever
    attribute it is asked for, so a test built on one keeps passing after the producer stops
    counting deliveries. The three injected failures below are the three the producer refuses
    -- a broker error, a report that never arrives, and a queue that does not drain -- and
    each exists so that a passing refusal test cannot be a refusal that never fired.

    PARTITIONS ARE A FUNCTION OF THE KEY, as librdkafka's `murmur2 % partitions` is. Not the
    same hash and it does not need to be: what the double has to preserve is the PROPERTY the
    key was chosen for -- equal keys land in one partition -- because that is what
    `test_a_redelivery_shares_its_originals_key_and_therefore_its_partition` measures."""

    def __init__(
        self,
        *,
        partitions: int = 1,
        error_at: int | None = None,
        silent_at: int | None = None,
        left_queued: int = 0,
    ) -> None:
        self.partitions = partitions
        self.error_at = error_at
        self.silent_at = silent_at
        self.left_queued = left_queued
        self.published: list[tuple[str, bytes, bytes]] = []
        self.coordinates: list[tuple[int, int, int]] = []
        self.polls = 0
        self._pending: list[Callable[[Any, Any], None]] = []
        self._offsets: dict[int, int] = {}

    def produce(
        self, topic: str, *, value: bytes, key: bytes, on_delivery: Callable[[Any, Any], None]
    ) -> None:
        self.published.append((topic, key, value))
        self._pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float) -> int:
        for index, callback in enumerate(self._pending):
            if index == self.silent_at:
                continue
            if index == self.error_at:
                callback(f"Broker: message {index} was not persisted", None)
                continue
            callback(None, _Message(*self._coordinates(index)))
        self._pending.clear()
        return self.left_queued

    def _coordinates(self, index: int) -> tuple[int, int]:
        """(partition, offset) for the record at `index`, keyed the way a partitioner is."""
        partition = sum(self.published[index][1]) % self.partitions
        offset = self._offsets.get(partition, 0)
        self._offsets[partition] = offset + 1
        self.coordinates.append((index, partition, offset))
        return partition, offset

    def values(self) -> list[bytes]:
        """Exactly the bytes handed to `produce()`, in the order they were handed over."""
        return [value for _, _, value in self.published]


@pytest.fixture(scope="module")
def cross_currency_records() -> tuple[dict[str, str], ...]:
    """A DECLARED PROFILE at its real size, from a synthetic pool.

    MODULE-SCOPED because generating 10,000 events is ~10 s and two tests read it -- the same
    shape and the same stated assumption as `tests/test_payment_profiles.py`'s fixture, whose
    comment block carries the argument for why a synthetic pool is sound here.

    THE POOL IS THE PROBE'S POOL, and that is now literal rather than a promise: it is
    `probe._POOL` itself, the object `scripts/probe_byte_identity.py` declares, imported.
    `_CROSS_CURRENCY_SHA` is only a comparable anchor while this corpus and the probe's are
    one, and building from the probe's own object is what makes a change to it arrive here --
    the digest assertion in `test_a_declared_profile_publishes_the_byte_count_a_workspace_run
    _lands` is then taken over a different corpus. It used to be a SECOND declaration of the
    same expression with a comment claiming they were coupled, and they were not; see
    `test_the_pool_and_the_baseline_this_file_copies_are_the_probes_own` for what a reviewer
    measured and for the other direction, the probe's baseline numbers."""
    profile = PROFILES[CROSS_CURRENCY]
    return delivered_records(profile.stream_spec(probe._POOL), profile.defects)


# --- THE EQUIVALENCE, AGAINST THE GOLDEN PIN ------------------------------------------


def test_the_kafka_frames_rebuild_the_pinned_file_byte_for_byte():
    """THE ANCHOR, and the test that makes the copied fixture above safe.

    Three claims in one direction: the spec this file declares still produces F1b's pinned
    FILE, the Kafka frames concatenate back to exactly those bytes, and the concatenation
    therefore digests to the same literal. If the copy had drifted, the first assertion --
    which touches no streaming code at all -- is what goes red, so a green run here cannot
    mean "the two transports agree about a corpus nobody pinned"."""
    records = records_for(_SPEC)
    landed = serialised_bytes(records)
    assert len(records) == _PINNED_ROWS
    assert hashlib.sha256(landed).hexdigest() == _PINNED_SHA
    assert len(landed) == _PINNED_BYTES

    values = message_values(records)
    assert len(values) == _PINNED_ROWS
    assert b"".join(values) == landed
    assert hashlib.sha256(b"".join(values)).hexdigest() == _PINNED_SHA


def test_what_the_producer_publishes_digests_to_the_golden_pin():
    """THE DEMANDED TEST: the digest is taken over what `produce()` was actually handed.

    Not over `message_values`, which the test above already covers, and not over anything
    the producer reports about itself: `double.values()` is the list of `value=` arguments
    the client received, so an ordering change, a dropped record or a re-serialisation
    between framing and publishing all move it. Three partitions in the double, so the
    equality is not an artefact of a single-partition arrangement.

    `published.byte_count` and `published.sha256` are asserted against the same literal
    BESIDE the captured bytes, which is what ties the producer's own evidence line to the
    pin -- an operator quoting a run log and a reader quoting this pin compare directly."""
    records = records_for(_SPEC)
    double = RecordingProducer(partitions=3)
    published = publish_records(records, topic=_TOPIC, producer=double)
    captured = double.values()

    assert len(captured) == _PINNED_ROWS == published.message_count
    assert hashlib.sha256(b"".join(captured)).hexdigest() == _PINNED_SHA
    assert sum(len(value) for value in captured) == _PINNED_BYTES
    assert (published.sha256, published.byte_count) == (_PINNED_SHA, _PINNED_BYTES)
    assert {topic for topic, _, _ in double.published} == {_TOPIC}


def test_the_multiset_of_published_values_is_the_multiset_of_the_files_lines():
    """THE FORM THAT SURVIVES MORE THAN ONE PARTITION, stated over the file's own lines.

    The file side is derived by CUTTING the landed bytes -- `splitlines(keepends=True)` --
    which is deliberately the route `opl.streaming.messages` refuses to take when it builds
    frames. That is what makes this a comparison rather than a restatement: one side cuts
    the file up, the other builds the pieces independently, and the multiset equality is
    the claim a consumer reading three partitions in an arbitrary interleaving can check.

    `keepends=True` is safe here for the reason `serialised_bytes` guarantees and this test
    re-asserts: no carriage return can reach these bytes, so the only boundary to split on
    is the `\\n` the terminator put there."""
    records = records_for(_SPEC)
    landed = serialised_bytes(records)
    assert b"\r" not in landed
    lines = landed.splitlines(keepends=True)
    assert len(lines) == _PINNED_ROWS

    double = RecordingProducer(partitions=3)
    publish_records(records, topic=_TOPIC, producer=double)
    assert Counter(double.values()) == Counter(lines)
    assert all(value.endswith(b"\n") for value in double.values())


def test_every_published_value_parses_as_one_json_object_despite_its_terminator():
    """THE COST OF CARRYING THE TERMINATOR, measured instead of cited.

    The module docstring of `opl.streaming.messages` argues from RFC 8259 that trailing
    whitespace is insignificant. This parses all 24 frames as they would arrive at a
    consumer and compares each to the record it was built from, so the argument is not
    load-bearing on its own."""
    records = records_for(_SPEC)
    values = message_values(records)
    assert len(values) == _PINNED_ROWS
    for record, value in zip(records, values, strict=True):
        assert json.loads(value.decode("utf-8")) == record


def test_a_declared_profile_publishes_the_byte_count_a_workspace_run_lands(
    cross_currency_records,
):
    """A DECLARED PROFILE, at its real size, through the producer.

    2,926,588 is `cross-currency`'s pinned and pool-independent byte count, so this ties a
    10,000-record publish to a number published in `docs/f-api-run-evidence.md` and landed
    by a workspace run -- an anchor outside this file, for a corpus whose digest a synthetic
    pool cannot reproduce.

    IT IS ALSO THE NUMBER THAT DISCRIMINATES THE FRAMING. Strip the `\\n` from each value and
    the total falls to 2,916,588 -- exactly one byte per record -- while the multiset, the
    parse and the message count all stay correct. That is why the byte count is asserted
    here rather than only the digest. (Measured, not predicted: the mutation run that
    stripped the terminator produced exactly 2,916,588 here.)

    AND THE REPORTED DIGEST IS TIED TO THE CAPTURED BYTES, which this test did not do until
    a mutation showed why it must. A byte changed BETWEEN the framing and the `produce()`
    call -- `BRL` for `BRX`, same length -- left `published.byte_count` and every captured
    length correct, so this test stayed green over a corpus the broker would have received
    wrong. `PublishedStream.sha256` is an evidence line an operator quotes; it is worth
    nothing unless it describes what the client was actually handed.

    THE DIGEST IS ALSO COMPARED AGAINST A LITERAL, and it has to be. Comparing
    `published.sha256` against a `hashlib.sha256(serialised_bytes(records))` computed here
    is not a check at all: `message_values` refuses to return unless that exact equality
    holds, so the assertion is the callee's own postcondition read back and cannot go red.
    `_CROSS_CURRENCY_SHA` is a constant `scripts/probe_byte_identity.py` committed, and it
    is what covers a change to the SHARED serialiser that no length would show:
    `_CROSS_CURRENCY_BYTES` above catches one that moves a byte count, `sort_keys=True`
    moves no byte count on this corpus, and the digest sees it."""
    records = cross_currency_records
    double = RecordingProducer(partitions=3)
    published = publish_records(records, topic=_TOPIC, producer=double)

    assert len(records) == _CROSS_CURRENCY_ROWS == PROFILES[CROSS_CURRENCY].delivered_row_count
    assert published.message_count == _CROSS_CURRENCY_ROWS
    assert published.byte_count == _CROSS_CURRENCY_BYTES
    assert sum(len(value) for value in double.values()) == _CROSS_CURRENCY_BYTES
    assert published.sha256 == _CROSS_CURRENCY_SHA
    assert published.sha256 == hashlib.sha256(b"".join(double.values())).hexdigest()
    assert sum(count for _, count in published.partition_counts) == _CROSS_CURRENCY_ROWS
    assert len(published.partition_counts) == 3, "a one-partition publish proves less"


def test_the_pool_and_the_baseline_this_file_copies_are_the_probes_own():
    """THE COUPLING, ASSERTED -- because it was a sentence, and a sentence cannot go red.

    `_CROSS_CURRENCY_SHA` is an anchor only while this file and
    `scripts/probe_byte_identity.py` describe ONE corpus, and the fixture above used to say
    they did while importing nothing from it. A reviewer found the gap and it was re-measured
    here: with the PROBE's pool moved to `range(9001, POOL_SIZE + 9001)` the probe reported
    `9a0459a6...` against its own baseline, and this file -- its fixture then declaring a pool
    of its own, as it did -- stayed green, 23 passed, on `a527b61c...`. The probe is in no CI
    job, so nothing else was watching that direction either.

    TWO DIRECTIONS, AND THEY ARE CLOSED DIFFERENTLY. The fixture now BUILDS from `probe._POOL`,
    so a pool that moves moves the corpus the digest is taken over; the first assertion here
    names that declaration, so the diagnosis is the pool rather than a digest mismatch 10,000
    records later. The second is the direction no corpus can reach -- the probe's three
    published `cross-currency` numbers against the three literals at the top of this file --
    because a baseline edited alone changes nothing this file computes."""
    assert probe._POOL == validated_pool(tuple(f"{n:08d}" for n in range(1, POOL_SIZE + 1)))
    assert probe._BASELINE[CROSS_CURRENCY] == (
        _CROSS_CURRENCY_ROWS,
        _CROSS_CURRENCY_BYTES,
        _CROSS_CURRENCY_SHA,
    )


# --- THE REFUSALS, EACH SHOWN TO FIRE --------------------------------------------------


def test_publishing_nothing_is_refused_rather_than_verified():
    """F4's SPECIES, REFUSED IN SHIPPED CODE. `b"".join(()) == serialised_bytes(())` holds,
    so an empty corpus would pass the byte-identity check for reasons unrelated to framing
    and leave every downstream count at zero and true. Both doors are refused, and the
    double is asserted to have received nothing -- otherwise this test would pass over a
    producer that published first and refused afterwards."""
    double = RecordingProducer()
    with pytest.raises(ValueError, match="empty record sequence"):
        message_values([])
    with pytest.raises(ValueError, match="empty record sequence"):
        publish_records([], topic=_TOPIC, producer=double)
    assert double.published == []


def test_frames_stripped_of_their_terminator_are_refused():
    """THE GUARD PROVEN TO FIRE, by making the defect it exists for.

    Frames that lost the trailing `\\n` still parse, still carry every column and still
    number 24 -- and their concatenation is 24 bytes short of the file. Without
    `_refuse_frames_that_do_not_rebuild_the_file` that stream would publish happily and
    disagree with every golden digest F1b pinned.

    STAGED AS THE FRAMES A DEFECTIVE `message_values` WOULD BUILD, handed to the guard that
    `message_values` calls on its way out, rather than by replacing `serialised_bytes` on
    the module. A replacement reaches BOTH of the guard's routes at once, so the only way to
    make one fire through it is a fake that behaves differently for one record than for
    many -- and no serialiser does that. The defect this guard is for lives inside this
    module's own frame-building, so that is where it is put."""
    records = records_for(_SPEC)
    assert len(records) == _PINNED_ROWS
    frames = tuple(serialised_bytes([record])[:-1] for record in records)
    assert all(json.loads(frame) == record for frame, record in zip(frames, records, strict=True))
    with pytest.raises(ValueError, match="rebuild"):
        _refuse_frames_that_do_not_rebuild_the_file(frames, records)


def test_frames_a_json_dumps_inside_this_module_would_build_are_refused():
    """THE DEFECT CLASS THIS PACKAGE EXISTS TO REFUSE, staged in the shape it would arrive.

    `opl.streaming.__init__` names it: a `json.dumps` under this package, through the one
    door F1b did not lock. Default separators emit `", "` and `": "` -- valid JSON, the same
    keys, the same values, and a byte string no golden digest was taken over. A producer
    that re-serialised would look correct in every assertion about CONTENT, which is why the
    parse below is asserted first: it passes, and the rebuild check still refuses.

    Built here rather than monkeypatched, for the reason the test above states."""
    records = records_for(_SPEC)
    assert len(records) == _PINNED_ROWS
    frames = tuple(
        (json.dumps(dict(record), ensure_ascii=False) + "\n").encode("utf-8")
        for record in records
    )
    assert all(json.loads(frame) == record for frame, record in zip(frames, records, strict=True))
    with pytest.raises(ValueError, match="rebuild"):
        _refuse_frames_that_do_not_rebuild_the_file(frames, records)


def test_a_terminator_that_vanished_from_both_routes_is_refused_by_the_line_count():
    """THE ONE THING THE REBUILD EQUALITY STRUCTURALLY CANNOT SEE, and the check that can.

    Both of the guard's routes call `serialised_bytes`, so a change to the SHARED serialiser
    moves them together. `to_jsonl`'s terminator dropped ENTIRELY is that shape and it is the
    worst of it: 24 one-record calls lose 24 bytes, the whole-sequence call loses the same
    24, the concatenation still equals the landed bytes -- asserted below, so this test also
    records what the equality does NOT catch -- and what is left is one fused line that no
    consumer could cut back into records.

    MEASURED AGAINST THE REAL SERIALISER, not a double: `events.LINE_SEPARATOR` is the
    constant `to_jsonl` joins on, and emptying it is the actual edit. The line count is what
    disagrees, 1 against 24, and it goes through the public `message_values`."""
    records = records_for(_SPEC)
    assert len(records) == _PINNED_ROWS
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(events_module, "LINE_SEPARATOR", "")
        fused = serialised_bytes(records)
        assert len(fused.splitlines(keepends=True)) == 1
        assert b"".join(serialised_bytes([record]) for record in records) == fused
        with pytest.raises(ValueError, match="cut into"):
            message_values(records)


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        ({"error_at": 3}, "were not delivered"),
        ({"left_queued": 2}, "still queued"),
        ({"silent_at": 7}, "delivery reports came back"),
    ],
    ids=["broker-error", "queue-not-drained", "report-never-arrived"],
)
def test_a_publish_the_broker_did_not_fully_acknowledge_is_refused(failure, expected):
    """THE COUNT IS MEASURED, AND THESE THREE ARE THE PROOF OF IT.

    `len(records)` is available before anything is published and would report 24 under all
    three of these. The third is the sharpest: no error, an empty queue, and one delivery
    report that never came -- a record that vanished between this process and the broker.
    A producer that returned `len(records)` would call that a complete publish.

    THE FLOOR IS ASSERTED AFTER THE REFUSAL, not before, because it is the thing the ids do
    not carry. `error_at=3` and `silent_at=7` imply at least four and eight records reached
    `produce()`; `left_queued=2` implies nothing at all, and would read exactly the same
    over an empty publish that raised for some other reason. `double.published` is what the
    client was handed, so 24 of them is the count this refusal fired over.

    THE DOUBLE IS BUILT HERE AND NOT IN THE ARGVALUES. A `RecordingProducer` in the
    `parametrize` list is constructed at COLLECTION time and lives for the whole session, so
    a `--count`-style repeat, a rerun plugin or a second parametrisation reusing it would
    replay against a double that already holds the previous run's records."""
    double = RecordingProducer(**failure)
    with pytest.raises(RuntimeError, match=expected):
        publish_records(records_for(_SPEC), topic=_TOPIC, producer=double)
    assert len(double.published) == _PINNED_ROWS


# The probe `test_the_producer_works_where_confluent_kafka_is_not_installed` runs. At module
# level so the test itself stays short; `sys.argv[1]` carries the pin rather than a third
# copy of the literal. The double is minimal on purpose -- what is under test is that the
# INJECTED door needs no client library, not the double's fidelity, which
# `RecordingProducer` above covers in-process.
_NO_KAFKA_PROBE = textwrap.dedent(
    """
    import hashlib
    import sys

    class _NoKafka:
        def find_spec(self, name, path=None, target=None):
            if name == "confluent_kafka" or name.startswith("confluent_kafka."):
                raise ImportError("confluent_kafka is not installed here")
            return None

    sys.meta_path.insert(0, _NoKafka())
    from opl.generator.cnpj_pool import validated_pool
    from opl.generator.stream import StreamSpec, records_for
    from opl.streaming.producer import BrokerConfig, publish_records

    class _Ack:
        def __init__(self, index):
            self._index = index
        def partition(self):
            return 0
        def offset(self):
            return self._index

    class _Double:
        def __init__(self):
            self.values = []
            self.pending = []
        def produce(self, topic, *, value, key, on_delivery):
            self.values.append(value)
            self.pending.append(on_delivery)
        def poll(self, timeout):
            return 0
        def flush(self, timeout):
            for index, callback in enumerate(self.pending):
                callback(None, _Ack(index))
            self.pending.clear()
            return 0

    pool = validated_pool([f"{n:08d}" for n in range(1, 21)])
    spec = StreamSpec(
        seed=20260809, stream_id="F1B-CLEAN", event_count=24, repeat_count=4,
        window_start="2026-06-01T00:00:00.000Z", event_interval_ms=250,
        emission_lag_ms=1500, cnpj_pool=pool,
    )
    records = records_for(spec)
    assert len(records) == 24
    double = _Double()
    published = publish_records(records, topic="opl-payments-test", producer=double)
    assert published.message_count == 24
    assert hashlib.sha256(b"".join(double.values)).hexdigest() == sys.argv[1]
    assert "confluent_kafka" not in sys.modules

    try:
        # `flush_timeout` SMALL, and it is about the NEGATIVE arm rather than this one. The
        # ImportError this expects is raised inside `_open_client`, before a flush exists, so
        # the value cannot weaken what the assertions below prove. What it bounds is the cost
        # of a REGRESSION: with the meta-path blocker above removed, this line opens a real
        # client against an unresolvable host, and the 120 s default made the whole probe take
        # 123 s to go red (measured). At 1 s the same regression is the same RuntimeError
        # about 24 still-queued records, and the probe goes red in 8 s (measured).
        publish_records(
            records, topic="t", broker=BrokerConfig(bootstrap="h:9092"), flush_timeout=1.0
        )
    except ImportError as absent:
        assert "dev" in str(absent), str(absent)
        assert "producer=" in str(absent), str(absent)
    else:
        raise AssertionError("the broker door opened a client with no client library")
    print("ok")
    """
)


def test_the_producer_works_where_confluent_kafka_is_not_installed():
    """THE INJECTED DOOR NEEDS NO CLIENT LIBRARY, and the other door says why it does.

    `confluent_kafka` is a compiled wheel this repository declares in the `dev` DEPENDENCY
    GROUP only: not a dependency of the opl wheel, not installed on Databricks serverless.
    So `import opl.streaming.producer` must not need it and neither must a publish through
    an injected client -- which is the door that makes the golden-digest tests above unit
    tests. `producer.py` puts the import inside `_open_client` for exactly that, and THIS
    TEST is the only thing holding the arrangement in place. Moved back to module level, the
    import leaves ruff clean and every OTHER test in this file passing -- this one is what
    goes red, which is why the sentence is in the present tense: the arrangement is
    unprotected everywhere except here. (Measured as a mutation: 24 passed with the import
    where it is; ruff clean, 23 passed and this one failed with it at module level.)

    A SUBPROCESS WITH A META-PATH BLOCKER rather than a grep for the import line, which is
    this repository's established shape for the property (`tests/vault/test_hashing.py`,
    `tests/bronze/test_registry_guards.py`, `tests/test_payment_generator.py`). In-process
    would prove nothing: `confluent_kafka` is installed here and
    `tests/integration/test_redpanda.py` imports it at module level.

    THE SAME SUBPROCESS CHECKS THE MESSAGE, because a bare `ImportError` names a package a
    reader of a serverless task log has no reason to expect and points at no remedy. It must
    name the dev group and the `producer=` door."""
    result = subprocess.run(
        [sys.executable, "-c", _NO_KAFKA_PROBE, _PINNED_SHA],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip() == "ok"


def test_a_record_without_an_identity_cannot_be_keyed():
    """A null key would put the record in a partition the partitioner chose at random,
    which is the one thing this corpus's layout is not allowed to be."""
    with pytest.raises(KeyError, match=IDENTITY_COLUMN):
        message_key({"amount": "1.00"})


def test_exactly_one_client_source_is_accepted():
    """Neither and both are refused: a publish whose destination is ambiguous is evidence
    of nothing, and the ambiguity is cheap to refuse before a socket exists."""
    records = records_for(_SPEC)
    with pytest.raises(ValueError, match="exactly one"):
        publish_records(records, topic=_TOPIC)
    with pytest.raises(ValueError, match="exactly one"):
        publish_records(
            records,
            topic=_TOPIC,
            broker=BrokerConfig(bootstrap="localhost:9092"),
            producer=RecordingProducer(),
        )


# --- THE KEY, AND WHAT IT IS FOR ------------------------------------------------------


def test_a_redelivery_shares_its_originals_key_and_therefore_its_partition():
    """WHY THE KEY IS THE IDENTITY COLUMN, measured over a stream that has redeliveries.

    A redelivery carries its original's bytes, identity included, so the two are one key and
    land in one partition however many partitions exist. A legitimate repeat -- same business
    attributes, its OWN identity -- is free to land elsewhere, which is correct: it is a
    different payment. The floor is stated first: five redeliveries, or this counts pairs
    that do not exist."""
    defects = DefectSpec(duplicate_count=5)
    records = delivered_records(_SPEC, defects)
    assert len(records) == _PINNED_ROWS + 5

    keys = Counter(message_key(record) for record in records)
    assert sum(1 for count in keys.values() if count == 2) == 5
    assert len(keys) == _PINNED_ROWS

    double = RecordingProducer(partitions=3)
    publish_records(records, topic=_TOPIC, producer=double)
    partitions: dict[bytes, set[int]] = {}
    for index, partition, _ in double.coordinates:
        partitions.setdefault(double.published[index][1], set()).add(partition)
    assert len(partitions) == _PINNED_ROWS
    assert all(len(seen) == 1 for seen in partitions.values())
    assert len({partition for _, partition, _ in double.coordinates}) > 1, (
        "one partition would make the claim above true for a reason that is not the key"
    )


# --- THE BROKER'S CONFIGURATION AND ITS ONE SECRET ------------------------------------


def test_the_local_broker_needs_no_credential_and_the_managed_one_is_sasl_ssl():
    """The two shapes, side by side, because the difference is one field's presence.

    Idempotence is asserted on BOTH: it is not a property of the managed broker but of what
    this corpus needs from any transport -- a retry that silently appends a second copy
    would make F1b's 150 injected redeliveries a measurement of librdkafka."""
    local = BrokerConfig(bootstrap="localhost:9092").client_config()
    assert local["bootstrap.servers"] == "localhost:9092"
    assert "security.protocol" not in local and "sasl.password" not in local
    assert (local["enable.idempotence"], local["acks"]) == (True, "all")

    managed = BrokerConfig(
        bootstrap="seed.example.redpanda.com:9092", username="opl", password="hunter2"
    ).client_config()
    assert managed["security.protocol"] == "SASL_SSL"
    assert managed["sasl.mechanism"] == SASL_MECHANISM
    assert (managed["sasl.username"], managed["sasl.password"]) == ("opl", "hunter2")
    assert (managed["enable.idempotence"], managed["acks"]) == (True, "all")


def test_the_password_is_the_only_secret_and_it_never_reaches_a_repr():
    """THE LEAK THIS PROJECT HAS ALREADY MEASURED ONCE, from the other side.

    F5 section 4.2 found that making the USERNAME a secret turned `opl` -- the prefix of this
    repository's catalog, topics and jobs -- into `[REDACTED]` throughout a run log. So the
    username is a coordinate and travels in clear, and it is asserted to be present here for
    exactly that reason: a `repr` that hid it would be the same defect pointing the other
    way. The password is the secret, and a frozen dataclass's generated `repr` is the most
    likely place for it to reach a traceback."""
    broker = BrokerConfig(bootstrap="seed.example:9092", username="opl", password="hunter2")
    assert "hunter2" not in repr(broker)
    assert "hunter2" not in str(broker)
    assert "opl" in repr(broker) and "seed.example:9092" in repr(broker)
    assert broker.password == "hunter2", "hidden from repr, not from the client"


def test_the_broker_is_read_from_a_mapping_the_caller_passes():
    """No `os.environ` inside the module, so a test states the environment it means.

    The blank-is-absent rule is asserted because `set -a && source .env` exports an empty
    value for a commented-out line, and an operator cannot see the difference between a
    variable that is unset and one that is set to nothing."""
    managed = broker_from_environment(
        {
            BOOTSTRAP_ENV_VAR: "seed.example:9092",
            USERNAME_ENV_VAR: "opl",
            PASSWORD_ENV_VAR: "hunter2",
        }
    )
    assert (managed.bootstrap, managed.username, managed.password) == (
        "seed.example:9092",
        "opl",
        "hunter2",
    )
    local = broker_from_environment({BOOTSTRAP_ENV_VAR: "localhost:9092", USERNAME_ENV_VAR: ""})
    assert (local.username, local.password) == (None, None)


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, "no bootstrap address"),
        ({BOOTSTRAP_ENV_VAR: "   "}, "no bootstrap address"),
        ({BOOTSTRAP_ENV_VAR: "h:9092", USERNAME_ENV_VAR: "opl"}, f"{PASSWORD_ENV_VAR} is not set"),
        (
            {BOOTSTRAP_ENV_VAR: "h:9092", PASSWORD_ENV_VAR: "hunter2"},
            f"{USERNAME_ENV_VAR} is not set",
        ),
    ],
    ids=["no-bootstrap", "blank-bootstrap", "user-without-password", "password-without-user"],
)
def test_an_unusable_broker_is_refused_naming_the_variable_that_is_missing(env, expected):
    """Refused before a socket exists, and NAMING the variable.

    Half a credential is the one worth refusing loudly: SASL is keyed off the password, so a
    username alone speaks PLAINTEXT to a broker expecting SASL_SSL and fails with a metadata
    timeout -- the same string an unreachable host, a wrong port and a stopped cluster all
    produce. F5's own Task 0 spent two statements learning that one error across three worlds
    is not a check.

    THE MATCHED STRINGS ARE THE DISCRIMINATING HALF OF EACH MESSAGE, on purpose. Both
    variable names appear in the half-credential refusal, so matching on a bare name would
    pass in either direction and this parametrisation would be four ids over two checks."""
    with pytest.raises(ValueError, match=expected):
        broker_from_environment(env)
