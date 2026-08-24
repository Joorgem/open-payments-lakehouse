# src/opl/streaming/producer.py
"""The transport: it opens a Kafka client, publishes the frames `messages` built, and
counts WHAT THE BROKER ACKNOWLEDGED.

WHAT IT DELIBERATELY DOES NOT DO: derive a record, serialise a record, or decide what a
record's bytes are. It is handed `Sequence[Mapping[str, str]]` -- the SAME parameter type
`opl.bronze.generated_landing.emit_records_file` takes, deliberately, so that the two
transports are two functions over one input rather than two pipelines -- and everything
about the bytes is `opl.streaming.messages`' business.

--- THE COUNT IS MEASURED, NOT RESTATED --------------------------------------------------

`Producer.produce()` is ASYNCHRONOUS. It appends to a local queue and returns; nothing
about the call says the record left the process, let alone that a broker took it. So
`len(records)` is available before anything is published and stays available when nothing
is -- and a `PublishedStream` reporting it would be F4's species exactly: a number that
reads the same whether the publish worked, half-worked, or never reached a socket.

`PublishedStream.message_count` is therefore assembled from DELIVERY REPORTS -- one
callback per record, carrying the partition and offset the broker assigned or the error it
returned -- and `_refuse_an_incomplete_delivery` compares that count against the number of
frames handed in, refusing on any disagreement, on any reported error, and on a `flush`
that leaves anything in the queue. Three separate ways for the number to be wrong, all
refused, so a returned `PublishedStream` means the broker holds exactly this corpus.

WHICH MEANS THE REFUSAL IS THE CHECK AND THE FIELD IS ITS CONSEQUENCE -- and no test can
tell the two apart, so no test here pretends to. Past `_refuse_an_incomplete_delivery` the
acknowledged count and `len(records)` are equal BY CONSTRUCTION; assembling `message_count`
from `len(records)` instead leaves both `tests/test_payment_streaming.py` and
`tests/integration/test_payment_streaming_live.py` fully green (measured as a mutation, not
supposed). Read the property off the refusal, which those tests stage three
failing publishes to show firing, and not off the field, whose value the refusal has already
forced. What the field is for is the diagnosis `EmittedFile.row_count` is for: which of the
three numbers moved.

--- THE PRODUCER IS IDEMPOTENT, AND THAT IS ABOUT THE MEASUREMENT ------------------------

`enable.idempotence=true` (which forces `acks=all` and a bounded in-flight window). Without
it, a producer retry after a lost acknowledgement appends the record a SECOND time, and
this corpus is one whose duplicates are counted: F1b injects exactly 150 redeliveries into
`promotable` and T5's whole claim is that the pipeline collapses those 150 and no others. A
transport free to add its own would make that count a measurement of librdkafka's retry
behaviour on the day the test ran. The dedup story stays a statement about DATA only while
the transport cannot contribute to it.

--- CREDENTIALS COME FROM THE ENVIRONMENT, AND ONLY THE PASSWORD IS A SECRET --------------

`KAFKA_BOOTSTRAP`, `KAFKA_USER`, `KAFKA_PASSWORD`, read out of a `Mapping` the CALLER
passes -- this module never touches `os.environ` itself, so a test states the environment
it means instead of patching a global. The local Redpanda container needs none of them and
speaks PLAINTEXT; the managed broker needs all three and speaks SASL_SSL / SCRAM-SHA-256.

ONLY THE PASSWORD IS A SECRET, and F5 measured what happens when that line is drawn wrongly
(plan section 4.2, controller-verified with a negative control). The Kafka username was put
in a Databricks secret scope alongside the password, and because the platform replaces every
occurrence of a secret's VALUE in task output, the username `opl` -- the prefix of this
repository's catalog, topic, job names and package -- turned every one of those into
`[REDACTED]` in the run log. A reader cannot tell a redaction from a value. So a bootstrap
address and a username are COORDINATES and travel in clear; the password is a secret, is
never defaulted in code, and carries `repr=False` so that a `BrokerConfig` reaching a
traceback or a log line does not print it.

--- THE CLIENT IS INJECTABLE, AND THAT IS WHAT MAKES THE DIGEST TEST A UNIT TEST ----------

`publish_records` takes either a `BrokerConfig` (it opens a client) or a ready `producer`
(it uses it), and refuses both or neither. A recording double passed through the second
door captures the exact `value=` bytes handed to `produce()`, so
`tests/test_payment_streaming.py` can digest what the PRODUCER would publish -- not what a
helper says it would -- and compare it against F1b's golden pin with no broker running,
in CI's default invocation. `tests/integration/test_payment_streaming_live.py` then does
the same thing through the real client and a real Redpanda, and the two must agree.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from opl.streaming.messages import message_key, message_values

BOOTSTRAP_ENV_VAR = "KAFKA_BOOTSTRAP"
USERNAME_ENV_VAR = "KAFKA_USER"
PASSWORD_ENV_VAR = "KAFKA_PASSWORD"

# The managed broker's mechanism (plan section 4.1: the SASL user was created as
# `SASL_MECHANISM_SCRAM_SHA_256`). Named rather than inlined so the one place it is
# decided is the one place it would be changed.
SASL_MECHANISM = "SCRAM-SHA-256"

# How long `flush` may take before the publish is called incomplete. Generous, because one
# call publishes one profile and the largest declared profile is `drifting` at 2,989,447
# bytes over 10,000 records (`scripts/probe_byte_identity.py`), and a cold managed broker
# across the public internet is not a local socket -- and cheap, because it is a CEILING: a
# healthy flush returns as soon as the queue drains.
FLUSH_TIMEOUT_SECONDS = 120.0

# `produce()` raises `BufferError` when the local queue is full, which is back-pressure
# rather than a failure: the remedy is to serve delivery callbacks and try again. Bounded,
# so a broker that stops draining fails with a message instead of looping forever.
_QUEUE_DRAIN_SECONDS = 1.0
_QUEUE_DRAIN_ATTEMPTS = 60


@dataclass(frozen=True, kw_only=True)
class BrokerConfig:
    """Where the corpus is published and who it authenticates as. Frozen, `kw_only`.

    `kw_only` for `StreamSpec`'s reason with a credential's edge: `username` and
    `password` are adjacent and both `str | None`, and a positional construction that
    swapped them would send the password as the SASL username -- putting the secret in
    the broker's own audit log, where nothing in this repository could redact it."""

    bootstrap: str
    username: str | None = None
    # `repr=False`, so the dataclass's generated `__repr__` cannot put the password into a
    # traceback, a pytest failure line, or a run log. See the module docstring.
    password: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Refuse an unusable broker before a socket is opened.

        THE SECOND CHECK IS THE ADDRESS AGAINST ITSELF, NOT AGAINST ITS OWN STRIP. This
        used to test `bootstrap.strip()` and then store `bootstrap` -- it validated one
        string and kept another -- so an address with whitespace round it passed the blank
        test and went to librdkafka verbatim, where it resolves as a host nobody is
        listening on and comes back as a METADATA TIMEOUT. That is the same string an
        unreachable host, a wrong port and a stopped cluster produce, which is the one
        confusion `_refuse_half_a_credential` below exists to stop this module adding to.
        NO PROVENANCE IS CLAIMED FOR THE PADDING and an earlier version of this paragraph
        blamed `set -a && source .env`, which was checked and does not do it: a sourced
        `VAR=value` line loses its own terminator. The defect being repaired is the
        mismatch between the string checked and the string kept, whatever put it there.

        REFUSED RATHER THAN TRIMMED, so what the client is handed is what the operator can
        read in the variable. `databricks/src/stream_managed_broker.py::_secret` makes the
        same call one layer up, over the scope value that fills this field."""
        if not self.bootstrap.strip():
            raise ValueError(
                f"no bootstrap address: pass one, or set {BOOTSTRAP_ENV_VAR}. There is no "
                "default -- a fallback to the local container would publish a corpus meant "
                "for the managed broker into a topic nobody is reading, and report success."
            )
        if self.bootstrap != self.bootstrap.strip():
            raise ValueError(
                f"the bootstrap address {self.bootstrap!r} carries leading or trailing "
                f"whitespace and is passed to the client verbatim. Set {BOOTSTRAP_ENV_VAR} "
                "without it: the address resolves as a host nobody is listening on, and "
                "the metadata timeout that follows is also what a stopped cluster, a wrong "
                "port and an unreachable host produce."
            )
        _refuse_half_a_credential(self.username, self.password)

    def client_config(self) -> dict[str, Any]:
        """The librdkafka configuration for this broker.

        CARRIES THE PASSWORD, so it goes to the client and to nothing that prints. See the
        module docstring for why idempotence is not optional here."""
        config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        }
        if self.password is None:
            return config
        return {
            **config,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": SASL_MECHANISM,
            "sasl.username": self.username,
            "sasl.password": self.password,
        }


def _refuse_half_a_credential(username: str | None, password: str | None) -> None:
    """Refuse a username without a password, or a password without a username.

    THE DANGEROUS HALF IS A USERNAME ALONE. `client_config` keys the whole SASL block off
    the password, so a config carrying only a username would speak PLAINTEXT to a broker
    that requires SASL_SSL -- and the symptom is a metadata timeout, which is also what an
    unreachable host, a wrong port and a stopped cluster produce. F5's own Task 0 spent two
    statements on exactly that: one error string across three different worlds is not a
    check. Refusing here names the actual cause while the process is still local."""
    if (username is None) == (password is None):
        return
    missing = PASSWORD_ENV_VAR if password is None else USERNAME_ENV_VAR
    raise ValueError(
        f"half a credential: one of {USERNAME_ENV_VAR} / {PASSWORD_ENV_VAR} is set and "
        f"{missing} is not set. SASL is enabled by the password, so this configuration would "
        "speak PLAINTEXT to a broker expecting SASL_SSL and fail with a metadata timeout "
        "-- the same string an unreachable host and a stopped cluster produce."
    )


def broker_from_environment(env: Mapping[str, str]) -> BrokerConfig:
    """A `BrokerConfig` from `env`. The caller passes the mapping; this reads no globals.

    An UNSET variable and one set to the empty string are the same thing here, because
    `set -a && source .env` exports an empty value for a commented-out line and the
    difference between "absent" and "blank" is not one an operator can see."""
    return BrokerConfig(
        bootstrap=env.get(BOOTSTRAP_ENV_VAR, ""),
        username=env.get(USERNAME_ENV_VAR) or None,
        password=env.get(PASSWORD_ENV_VAR) or None,
    )


@dataclass(frozen=True, kw_only=True)
class PublishedStream:
    """What one call to `publish_records` put on the topic. The evidence, not decoration.

    THE DIGEST AND THE BYTE COUNT ARE THE SAME TWO NUMBERS `generated_landing.EmittedFile`
    reports and the same two the golden pins are stated in, so a Kafka publish and a Volume
    landing of one corpus compare directly with nothing re-derived. `message_count` is the
    third for `EmittedFile.row_count`'s reason -- a bare digest mismatch names nothing, and
    which of the three moved is itself the diagnosis -- and it is assembled from the
    ACKNOWLEDGED count rather than the offered one, though the module docstring is where to
    read why that spelling is a consequence of `_refuse_an_incomplete_delivery` rather than
    a second check on top of it.

    `partition_counts` is the fourth and is the only field with no file-side twin: a file
    has one order and a topic has as many as it has partitions. It is what lets a consumer
    -side multiset assertion state a floor -- "these records arrived across three
    partitions" -- instead of passing over a topic that turned out to have one."""

    topic: str
    message_count: int
    byte_count: int
    sha256: str
    partition_counts: tuple[tuple[int, int], ...]


class KafkaProducer(Protocol):
    """The three methods this module needs from `confluent_kafka.Producer`.

    A PROTOCOL RATHER THAN THE CLASS, so the recording double in the tests is checked
    against what is actually used rather than against a `Mock` that would grow whatever
    attribute it was asked for -- `tests/conftest.py`'s `FakeFilesApi` states the same
    reason for the Files API."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class _DeliveryLedger:
    """What the broker said about each record, collected from its delivery callback.

    Mutable on purpose and the only mutable thing in this package: librdkafka reports
    deliveries by calling back, so accumulating is what the client's own contract asks
    for. Nothing outside `publish_records` sees an instance."""

    def __init__(self) -> None:
        self.delivered: list[tuple[int, int]] = []
        self.failures: list[str] = []

    def report(self, error: Any, message: Any) -> None:
        """The `on_delivery` callback. One call per produced record, exactly once."""
        if error is not None:
            self.failures.append(str(error))
            return
        self.delivered.append((message.partition(), message.offset()))

    def partition_counts(self) -> tuple[tuple[int, int], ...]:
        """(partition, count) pairs, sorted by partition."""
        return tuple(sorted(Counter(partition for partition, _ in self.delivered).items()))


def _open_client(broker: BrokerConfig | None, producer: KafkaProducer | None) -> KafkaProducer:
    """Resolve the client from exactly one of the two doors, or refuse.

    IMPORTED HERE AND NOT AT MODULE LEVEL. `confluent_kafka` is a compiled wheel declared
    in this repository's `dev` DEPENDENCY GROUP only -- it is not a wheel dependency and is
    not installed on Databricks serverless. A module level import would make `import
    opl.streaming.producer` fail there, which is a different failure from "this job cannot
    reach a broker" and would arrive at import time, before any message could explain it.
    `tests/test_payment_streaming.py` holds that shape open with a meta-path blocker in a
    subprocess, the way `tests/vault/test_hashing.py` does for pyspark, so the property is
    checked rather than the one import line grepped for.

    AND THE FAILURE CARRIES A MESSAGE, because the bare `ImportError` names a package a
    reader of a serverless task log has no reason to expect and says nothing about the
    other door."""
    if (broker is None) == (producer is None):
        raise ValueError(
            "pass exactly one of `broker` (open a client from it) or `producer` (use this "
            "one). Both would leave it ambiguous which broker the corpus reached, and a "
            "publish whose destination is ambiguous is evidence of nothing."
        )
    if broker is None:
        return producer  # narrowed by the refusal above: exactly one of the two is set
    try:
        from confluent_kafka import Producer
    except ImportError as absent:
        raise ImportError(
            "opening a Kafka client needs `confluent_kafka`, which this repository declares "
            "in the `dev` dependency group only: it is not a dependency of the opl wheel and "
            "is not installed on Databricks serverless. Either install the dev group "
            "(`uv sync --all-groups`), or hand an already-opened client to "
            "`publish_records(..., producer=...)` -- that door needs no client library at "
            "all, and it is the door the unit tests publish through with no broker running."
        ) from absent

    return Producer(broker.client_config())


def _produce_one(
    handle: KafkaProducer, topic: str, key: bytes, value: bytes, ledger: _DeliveryLedger
) -> None:
    """Enqueue one record, waiting out local back-pressure rather than dropping it."""
    for _ in range(_QUEUE_DRAIN_ATTEMPTS):
        try:
            handle.produce(topic, value=value, key=key, on_delivery=ledger.report)
            return
        except BufferError:
            handle.poll(_QUEUE_DRAIN_SECONDS)
    raise RuntimeError(
        f"the local produce queue stayed full for {_QUEUE_DRAIN_ATTEMPTS} polls of "
        f"{_QUEUE_DRAIN_SECONDS}s while publishing to {topic!r}. Records are enqueued and "
        "delivered asynchronously, so a queue that never drains means the broker stopped "
        "acknowledging -- and continuing would publish a corpus with holes in it."
    )


def _refuse_an_incomplete_delivery(
    ledger: _DeliveryLedger, offered: int, remaining: int, topic: str, timeout: float
) -> None:
    """Refuse unless the broker acknowledged every record and the queue emptied.

    THREE DISAGREEMENTS, ALL FATAL, and they are separate because they mean different
    things: a reported error is a record the broker rejected, a non-zero `flush` remainder
    is a record that never left this process, and a short acknowledged count is a callback
    that never arrived for a record neither of the first two explained."""
    if ledger.failures:
        raise RuntimeError(
            f"{len(ledger.failures)} of {offered} records were not delivered to {topic!r}: "
            f"{sorted(set(ledger.failures))}"
        )
    if remaining:
        raise RuntimeError(
            f"{remaining} of {offered} records were still queued when the flush timed out "
            f"after {timeout}s. The topic holds a truncated corpus, and every "
            "count taken over it would be smaller than the profile declares."
        )
    if len(ledger.delivered) != offered:
        raise RuntimeError(
            f"{len(ledger.delivered)} delivery reports came back for {offered} records "
            f"published to {topic!r}, with no error and an empty queue. The published count "
            "cannot be taken from what was offered, so it is refused rather than assumed."
        )


def publish_records(
    records: Sequence[Mapping[str, str]],
    *,
    topic: str,
    broker: BrokerConfig | None = None,
    producer: KafkaProducer | None = None,
    flush_timeout: float = FLUSH_TIMEOUT_SECONDS,
) -> PublishedStream:
    """Publish `records` to `topic` as the same bytes the Volume would land. THE producer.

    See the module docstring for why the returned count is the acknowledged one, why the
    client is idempotent, and why it may be injected. The framing -- and the refusal of an
    empty corpus -- is `opl.streaming.messages`'."""
    values = message_values(records)
    keys = tuple(message_key(record) for record in records)
    handle = _open_client(broker, producer)
    ledger = _DeliveryLedger()
    for key, value in zip(keys, values, strict=True):
        _produce_one(handle, topic, key, value, ledger)
    remaining = handle.flush(flush_timeout)
    _refuse_an_incomplete_delivery(ledger, len(values), remaining, topic, flush_timeout)
    payload = b"".join(values)
    return PublishedStream(
        topic=topic,
        message_count=len(ledger.delivered),
        byte_count=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        partition_counts=ledger.partition_counts(),
    )
