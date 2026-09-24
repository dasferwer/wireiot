import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import paho.mqtt.client as mqtt

base = os.environ.get("API_URL", "http://localhost:8097")
device = "demo-" + uuid.uuid4().hex[:8]
start = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=3)
samples = [
    {
        "id": str(uuid.uuid4()),
        "happened_at": (start + timedelta(seconds=i * 2)).isoformat(),
        "value": str(20 + i % 80),
    }
    for i in range(90)
]
reference = defaultdict(list)
for sample in samples:
    bucket = datetime.fromisoformat(sample["happened_at"]).replace(second=0).isoformat()
    reference[bucket].append(Decimal(sample["value"]))
random.Random(42).shuffle(samples)
with httpx.Client(base_url=base, headers={"X-API-Key": "local-demo-key"}, timeout=30) as api:
    response = api.post("/devices", json={"id": device, "threshold": 70, "timeout": 3})
    response.raise_for_status()
    token = response.json()["token"]
    ready = threading.Event()
    publisher = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def connected(client, userdata, flags, reason_code, properties):
        client.subscribe("wireiot/bridge/status", qos=1)

    def message(client, userdata, incoming):
        if incoming.payload == b"online":
            ready.set()

    publisher.on_connect = connected
    publisher.on_message = message
    host, port = os.environ.get("MQTT_HOST", "localhost"), int(os.environ.get("MQTT_PORT", "18837"))
    publisher.connect(host, port)
    publisher.loop_start()
    assert ready.wait(15), "MQTT-мост не подтвердил готовность"
    for i, sample in enumerate(samples + samples[:15]):
        if i == 45:
            publisher.disconnect()
            publisher.loop_stop()
            time.sleep(0.5)
            publisher.connect(host, port)
            publisher.loop_start()
        payload = json.dumps({"token": token, "event": sample})
        sent = publisher.publish(f"telemetry/{device}", payload, qos=1)
        sent.wait_for_publish(timeout=10)
        assert sent.is_published()
    publisher.disconnect()
    publisher.loop_stop()
    deadline = time.monotonic() + 60
    while True:
        windows = api.get(f"/devices/{device}/windows").json()
        if sum(row["count"] for row in windows) == 90:
            break
        if time.monotonic() > deadline:
            raise TimeoutError("Окна не обработаны")
        time.sleep(0.2)
    for row in windows:
        bucket = datetime.fromisoformat(row["bucket"]).isoformat()
        values = reference[bucket]
        assert row["count"] == len(values)
        assert Decimal(row["total"]) == sum(values)
        assert Decimal(row["minimum"]) == min(values)
        assert Decimal(row["maximum"]) == max(values)
    time.sleep(3.2)
    state = next(row for row in api.get("/devices").json() if row["id"] == device)
    assert state["offline"]
    print(
        json.dumps(
            {
                "unique_events": 90,
                "mqtt_deliveries": 105,
                "windows": len(windows),
                "batch_reconciliation": True,
                "offline_detected": True,
            },
            indent=2,
        )
    )
