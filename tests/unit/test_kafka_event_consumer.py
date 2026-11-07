# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the example Kafka consumer's handling of malformed messages."""

import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

CONSUMER_DIR = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "rag_event_ingest"
    / "kafka_consumer"
)
LOCAL_PACKAGES = ("consumer", "config", "router", "models", "handlers", "services")


class FakeKafkaConsumer:
    """Mimics kafka-python: the value deserializer runs inside poll()."""

    batches: list = []

    def __init__(self, *topics, **kwargs):
        self.deserializer = kwargs.get("value_deserializer")

    def poll(self, timeout_ms=0, max_records=None):
        if not FakeKafkaConsumer.batches:
            raise KeyboardInterrupt
        offset, raw = FakeKafkaConsumer.batches.pop(0)
        value = self.deserializer(raw) if self.deserializer else raw
        msg = SimpleNamespace(partition=0, offset=offset, value=value)
        return {"tp": [msg]}

    def subscription(self):
        return {"topic"}

    def close(self):
        pass


@pytest.fixture
def consumer_module(monkeypatch):
    fake_kafka = types.ModuleType("kafka")
    fake_kafka.KafkaConsumer = FakeKafkaConsumer
    monkeypatch.setitem(sys.modules, "kafka", fake_kafka)
    monkeypatch.syspath_prepend(str(CONSUMER_DIR))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    for name in LOCAL_PACKAGES:
        for mod in [m for m in sys.modules if m == name or m.startswith(name + ".")]:
            monkeypatch.delitem(sys.modules, mod, raising=False)
    import consumer

    yield consumer
    for mod in [m for m in sys.modules if m.split(".")[0] in LOCAL_PACKAGES]:
        sys.modules.pop(mod, None)


def test_poison_pill_does_not_stop_consumer(consumer_module):
    good = json.dumps({"EventName": "s3:ObjectCreated:Put"}).encode()
    FakeKafkaConsumer.batches = [
        (0, b"\xff\xfenot json"),
        (1, b"{truncated"),
        (2, b"[1, 2]"),
        (3, good),
    ]
    processed = []
    c = consumer_module.KafkaEventConsumer(handlers={}, storage=None)
    c.process_event = processed.append

    c.run()

    assert processed == [{"EventName": "s3:ObjectCreated:Put"}]
