# tests/integration/conftest.py
"""Fixtures shared by the container-backed tests: a topic nothing has published to, and
the ONE Spark session that carries the Kafka connector.

WHY THE SESSION FIXTURE IS HERE AND NOT IN `tests/conftest.py`. That file's `spark`
fixture builds `local_session()` -- no Kafka connector, by the pre-decision in
`opl.spark.KAFKA_CONNECTOR_PACKAGE` -- and the JVM behind both is process-wide:
`getOrCreate` hands every caller the SAME session, and `spark.jars.packages` is applied
only when the SparkContext is created. So the two cannot coexist in one invocation, and
putting `kafka_spark` in the deselected-by-default directory is what keeps them apart:
`-m redpanda` selects these files and nothing that takes `spark`. `local_session(kafka=True)`
refuses by name if they ever do meet, rather than failing later inside `readStream`.

`tests/integration/test_payment_streaming_live.py` predates this file and keeps its own
`fresh_topic` and `BOOTSTRAP`; a module-level fixture shadows a conftest one, so nothing
here changes that file's behaviour. Consolidating the two is a follow-up, not a silent
edit to committed T1 work -- hence the different name below.
"""
from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

# The compose broker. `docker-compose.yml` advertises `PLAINTEXT://localhost:9092`; it is
# a COORDINATE and not a credential (`opl.streaming.producer`'s docstring draws that line).
_BROKER_BOOTSTRAP = "localhost:9092"


@pytest.fixture(scope="session")
def bootstrap() -> str:
    """The broker address, handed out as a FIXTURE rather than importable as a constant.

    `tests/` is not a package, so `from tests.integration.conftest import ...` is not a
    thing a test module can do; a fixture is how a conftest hands a value to the files
    beside it without either a sys.path edit or a fourth copy of the address."""
    return _BROKER_BOOTSTRAP


@pytest.fixture(scope="session")
def kafka_spark():
    """The local Delta session WITH the Structured Streaming Kafka connector.

    SESSION-SCOPED AND THE ONLY THING ALLOWED TO STOP IT, for `tests/conftest.py::spark`'s
    reason: the JVM is process-wide, so a test that stopped "its own" session would stop
    the one every later test is still using, and the symptom is a py4j error in an
    unrelated module."""
    from opl.spark import local_session

    session = local_session("f5-streaming", kafka=True)
    yield session
    session.stop()


def _topic_factory():
    """Create topics on the compose broker and delete them again. The body BOTH topic
    fixtures share, because a fixture cannot depend on one of a narrower scope and the
    module-scoped experiment in `test_exactly_once_proof.py` needs one topic across its
    assertions while every other test needs a fresh one per function."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": _BROKER_BOOTSTRAP})
    created: list[str] = []

    def _make(partitions: int) -> str:
        name = f"opl-f5-{uuid.uuid4().hex[:8]}"
        futures = admin.create_topics(
            [NewTopic(name, num_partitions=partitions, replication_factor=1)]
        )
        futures[name].result(timeout=30)
        created.append(name)
        return name

    def _drop() -> None:
        _confirm_deleted(admin, created)

    return _make, _drop


def _confirm_deleted(admin, created: list[str]) -> None:
    """DELETION IS WAITED ON, THE WAY CREATION IS. `delete_topics` returns a dict of futures
    and marks the topics for deletion; discarding the futures returns before the broker has
    propagated it, and the topic outlives the fixture that owned it. That is the ORPHAN this
    file's own docstring calls the doubled-corpus hazard -- `test_payment_streaming_live.py`
    commits a test for it -- and it was reproduced here. `operation_timeout` makes the broker
    block until the deletion has propagated; `result()` makes THIS process block until the
    broker answers.

    EVERY TOPIC IS WAITED ON, INCLUDING THE ONES AFTER A FAILURE. Raising out of the loop on
    the first `result()` would leave the topics behind it unwaited -- orphans of exactly the
    shape this function exists to prevent, manufactured by the reporting of the first one.
    Only a test that called `make` twice could reach that, and none does today, which is why
    it is written this way rather than discovered later. What the collection does NOT change
    is the cost of an unreachable broker: `timeout=30` is per topic, so teardown then stalls
    30 s times the number of topics created.

    IT IS A MODULE-LEVEL FUNCTION RATHER THAN A CLOSURE because the two paragraphs above put
    `_topic_factory` over this repository's 50-line function cap, and `tests/test_size_caps.py`
    states the repair: lift the prose or extract a named helper, never delete the explanation
    to fit. It takes `admin` and `created` as arguments for that reason and no other."""
    if not created:
        return
    futures = admin.delete_topics(created, operation_timeout=30)
    failures: list[str] = []
    for name in created:
        try:
            futures[name].result(timeout=30)
        except Exception as failure:  # noqa: BLE001 -- collected and re-reported below
            failures.append(f"{name}: {failure!r}")
    if failures:
        raise RuntimeError(
            f"{len(failures)} of {len(created)} topics were not confirmed deleted "
            f"({'; '.join(failures)}). A topic that outlives the fixture that owned it "
            "still holds this run's records, and the next run to take that name reads "
            "them as its own corpus."
        )


@pytest.fixture
def kafka_topic() -> Iterator[object]:
    """`kafka_topic(partitions)` -> a topic name nothing has published to, deleted after.

    CREATED EXPLICITLY RATHER THAN BY AUTO-CREATION, so the partition count is a decision
    of the test rather than of whatever the broker's default happens to be -- and a topic
    that survived a previous run would hold that run's records, which is the doubled-corpus
    hazard `test_payment_streaming_live.py` already commits a test for."""
    make, drop = _topic_factory()
    yield make
    drop()


@pytest.fixture(scope="module")
def module_kafka_topic() -> Iterator[object]:
    """`kafka_topic` at module scope: ONE topic for an experiment whose arms must read the
    same offsets, with several tests asserting over the result."""
    make, drop = _topic_factory()
    yield make
    drop()
