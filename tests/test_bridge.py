import json
from types import SimpleNamespace

import psycopg
import pytest
from test_telemetry import device, drain, event

from wireiot import bridge, service
from wireiot.db import connect


class Receiver:
    def __init__(self):
        self.acks = []
        self.disconnected = False

    def ack(self, mid, qos):
        self.acks.append((mid, qos))

    def disconnect(self):
        self.disconnected = True


def deliver(receiver, envelope, mid=1, topic="telemetry/sensor"):
    bridge.on_message(
        receiver,
        None,
        SimpleNamespace(payload=json.dumps(envelope).encode(), topic=topic, mid=mid, qos=1),
    )


@pytest.mark.parametrize("token", [None, 123, False, [], {}, "", "x" * 257])
def test_invalid_token_ack_before_ingest_and_next_valid_once(client, monkeypatch, token):
    key = device(client)
    sample = event("25")
    receiver = Receiver()
    original = bridge.ingest
    calls = []

    def ingest(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(bridge, "ingest", ingest)
    deliver(receiver, {"token": token, "event": sample.model_dump(mode="json")})
    assert receiver.acks == [(1, 1)] and not receiver.disconnected
    assert not calls
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == 0
    envelope = {"token": key, "event": sample.model_dump(mode="json")}
    deliver(receiver, envelope, 2)
    deliver(receiver, envelope, 3)
    assert receiver.acks == [(1, 1), (2, 1), (3, 1)]
    drain()
    assert client.get("/devices/sensor/windows").json()[0]["count"] == 1


@pytest.mark.parametrize(
    "envelope,topic",
    [
        (None, "telemetry/sensor"),
        ([], "telemetry/sensor"),
        ({"token": "valid", "event": None}, "telemetry/sensor"),
        ({}, "telemetry"),
        ({}, "telemetry/sensor/extra"),
    ],
)
def test_invalid_envelope_or_topic_acked_without_db(envelope, topic, monkeypatch):
    def forbidden(*args):
        pytest.fail("invalid envelope reached ingest")

    monkeypatch.setattr(bridge, "ingest", forbidden)
    receiver = Receiver()
    deliver(receiver, envelope, topic=topic)
    assert receiver.acks == [(1, 1)] and not receiver.disconnected


def test_transient_db_failure_has_no_ack_and_retry_commits_once(client, monkeypatch):
    token = device(client)
    sample = event("25")
    envelope = {"token": token, "event": sample.model_dump(mode="json")}
    original = service.connect

    def unavailable():
        raise psycopg.OperationalError("temporary test outage")

    monkeypatch.setattr(service, "connect", unavailable)
    failed = Receiver()
    deliver(failed, envelope)
    assert not failed.acks and failed.disconnected
    monkeypatch.setattr(service, "connect", original)
    retried = Receiver()
    deliver(retried, envelope)
    deliver(retried, envelope, 2)
    assert retried.acks == [(1, 1), (2, 1)] and not retried.disconnected
    drain()
    assert client.get("/devices/sensor/windows").json()[0]["count"] == 1
