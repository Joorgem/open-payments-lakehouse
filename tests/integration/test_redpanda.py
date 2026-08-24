# tests/integration/test_redpanda.py
import uuid

import pytest
from confluent_kafka import Consumer, Producer

# BOTH MARKERS, the way the three Postgres files carry both. `integration` deselects it from
# every default invocation; `redpanda` is what a `-m redpanda` CI job selects, and this file
# needs the broker and nothing else -- so without it that job would skip the repository's
# only other broker test and still report green.
pytestmark = [pytest.mark.integration, pytest.mark.redpanda]


def test_kafka_produce_consume():
    topic = f"t-{uuid.uuid4().hex[:8]}"
    payload = b"hello-opl"
    p = Producer({"bootstrap.servers": "localhost:9092"})
    p.produce(topic, payload)
    p.flush(10)
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"g-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([topic])
    msg = c.poll(15)
    c.close()
    assert msg is not None and msg.value() == payload
