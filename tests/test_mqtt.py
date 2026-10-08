"""QoS 1 проверяется на настоящем Mosquitto, отдельно от publisher PUBACK."""

import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
import psycopg
import pytest
from test_telemetry import drain, event

from wireiot import bridge, service
from wireiot.db import connect

pytestmark = pytest.mark.skipif(
    not os.environ.get("MQTT_TEST_PORT"), reason="Нужен отдельный тестовый Mosquitto"
)


def pump(receiver, predicate):
    deadline = time.monotonic() + 10
    while not predicate() and time.monotonic() < deadline:
        receiver.loop(timeout=0.1)
    assert predicate(), "MQTT condition timed out"


@pytest.fixture
def mqtt_pair(client):
    device = "probe_" + uuid.uuid4().hex
    key = client.post("/devices", json={"id": device}).json()["token"]
    host = os.environ.get("MQTT_TEST_HOST", "127.0.0.1")
    port = int(os.environ["MQTT_TEST_PORT"])
    receiver = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="g-test-" + uuid.uuid4().hex,
        clean_session=False,
        manual_ack=True,
    )
    publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    state = {"ready": False, "deliveries": [], "acks": [], "sessions": []}
    original_ack = receiver.ack

    def ack(mid, qos):
        result = original_ack(mid, qos)
        assert result == mqtt.MQTT_ERR_SUCCESS
        state["acks"].append((mid, qos))
        return result

    def on_connect(c, userdata, flags, code, properties):
        assert not code.is_failure
        state["sessions"].append(flags.session_present)
        c.subscribe("telemetry/" + device, qos=1)

    def on_subscribe(c, userdata, mid, codes, properties):
        assert all(not code.is_failure for code in codes)
        state["ready"] = True

    def on_message(c, userdata, message):
        assert message.qos == 1
        state["deliveries"].append((json.loads(message.payload), message.dup, message.mid))
        bridge.on_message(c, userdata, message)

    receiver.ack = ack
    receiver.on_connect = on_connect
    receiver.on_subscribe = on_subscribe
    receiver.on_message = on_message
    receiver.connect(host, port)
    pump(receiver, lambda: state["ready"])
    publisher.connect(host, port)
    publisher.loop_start()

    def publish(envelope):
        info = publisher.publish("telemetry/" + device, json.dumps(envelope), qos=1)
        info.wait_for_publish(timeout=10)
        assert info.is_published()

    def reconnect():
        state["ready"] = False
        receiver.reconnect()
        pump(receiver, lambda: state["ready"])

    try:
        yield device, key, receiver, state, publish, reconnect
    finally:
        receiver.disconnect()
        receiver.loop(timeout=0.1)
        publisher.disconnect()
        publisher.loop_stop()


def test_real_invalid_ack_valid_duplicate_and_no_poison_redelivery(client, mqtt_pair):
    device, key, receiver, state, publish, reconnect = mqtt_pair
    sample = event("25").model_dump(mode="json")
    invalid = {"token": None, "event": sample}
    valid = {"token": key, "event": sample}
    publish(invalid)
    pump(receiver, lambda: len(state["acks"]) == 1)
    assert receiver.is_connected()
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == 0
    publish(valid)
    publish(valid)
    pump(receiver, lambda: len(state["acks"]) == 3)
    drain()
    assert client.get(f"/devices/{device}/windows").json()[0]["count"] == 1
    receiver.disconnect()
    pump(receiver, lambda: not receiver.is_connected())
    reconnect()
    assert state["sessions"] == [False, True]
    publish(valid)
    pump(receiver, lambda: len(state["acks"]) >= 4)
    assert [row[0] for row in state["deliveries"]].count(invalid) == 1
    assert len(state["deliveries"]) == 4
    drain()
    assert client.get(f"/devices/{device}/windows").json()[0]["count"] == 1


def test_real_db_failure_no_ack_persistent_redelivery_and_commit(client, mqtt_pair, monkeypatch):
    device, key, receiver, state, publish, reconnect = mqtt_pair
    original = service.connect
    outage = True

    def database():
        nonlocal outage
        if outage:
            outage = False
            raise psycopg.OperationalError("temporary test outage")
        return original()

    monkeypatch.setattr(service, "connect", database)
    sample = event("25").model_dump(mode="json")
    envelope = {"token": key, "event": sample}
    publish(envelope)
    pump(receiver, lambda: len(state["deliveries"]) == 1 and not receiver.is_connected())
    assert not state["acks"]
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == 0
    reconnect()
    pump(receiver, lambda: len(state["acks"]) == 1)
    assert state["sessions"] == [False, True]
    assert len(state["deliveries"]) == 2
    assert state["deliveries"][0][1] is False and state["deliveries"][1][1] is True
    assert state["deliveries"][0][2] == state["deliveries"][1][2]
    publish(envelope)
    from datetime import datetime

    publish(
        {
            "token": key,
            "event": event("35", datetime.fromisoformat(sample["happened_at"])).model_dump(
                mode="json"
            ),
        }
    )
    pump(receiver, lambda: len(state["acks"]) == 3)
    drain()
    row = client.get(f"/devices/{device}/windows").json()[0]
    assert row["count"] == 2 and float(row["total"]) == 60
