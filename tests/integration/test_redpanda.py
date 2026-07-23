# tests/integration/test_redpanda.py
import uuid

import pytest
from confluent_kafka import Consumer, Producer

pytestmark = pytest.mark.integration


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
