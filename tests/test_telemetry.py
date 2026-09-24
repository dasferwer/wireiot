import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from wireiot.api import Event
from wireiot.db import connect
from wireiot.service import Rejected, ingest, tick


def device(client):
    response = client.post("/devices", json={"id": "sensor", "threshold": 70, "timeout": 1})
    assert response.status_code == 200
    return response.json()["token"]


def event(value, timestamp=None):
    return Event(id=uuid.uuid4(), happened_at=timestamp or datetime.now(UTC), value=Decimal(value))


def drain():
    while tick():
        pass


def test_duplicates_and_window_reference(client):
    token = device(client)
    bucket = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    samples = [
        event(str(value), bucket + timedelta(seconds=i))
        for i, value in enumerate([80, 20, 50, 100])
    ]
    for sample in reversed(samples):
        ingest("sensor", token, sample)
    assert ingest("sensor", token, samples[0])["duplicate"]
    drain()
    row = client.get("/devices/sensor/windows").json()[0]
    assert row["count"] == row["revision"] == 4
    assert Decimal(row["total"]) == 250
    assert Decimal(row["mean"]) == Decimal("62.5")
    assert Decimal(row["minimum"]) == 20
    assert Decimal(row["maximum"]) == 100
    assert not row["active"]


def test_rule_retraction_after_late_correction(client):
    token = device(client)
    timestamp = datetime.now(UTC).replace(second=30, microsecond=0) - timedelta(minutes=1)
    ingest("sensor", token, event("100", timestamp))
    drain()
    ingest("sensor", token, event("0", timestamp - timedelta(seconds=1)))
    drain()
    transitions = client.get("/devices/sensor/alerts").json()
    assert [row["active"] for row in transitions] == [True, False]


def test_closed_window_is_not_changed(client):
    token = device(client)
    now = datetime.now(UTC)
    accepted = ingest("sensor", token, event("80", now - timedelta(minutes=12)), now)
    assert not accepted["included"]
    drain()
    assert client.get("/devices/sensor/windows").json() == []


def test_bad_clocks_and_conflicting_duplicates(client):
    token = device(client)
    for timestamp in [
        datetime.now(UTC) + timedelta(minutes=1),
        datetime.now(UTC) - timedelta(days=2),
        datetime.now(UTC).replace(tzinfo=None),
    ]:
        with pytest.raises(Rejected):
            ingest("sensor", token, event("10", timestamp))
    original = event("10")
    ingest("sensor", token, original)
    with pytest.raises(Rejected):
        ingest("sensor", token, original.model_copy(update={"value": Decimal(20)}))
    with pytest.raises(PermissionError):
        ingest("sensor", "bad-key", event("10"))


def test_offline_device_and_replay_does_not_refresh_liveness(client):
    token = device(client)
    sample = event("10")
    ingest("sensor", token, sample)
    with connect() as conn:
        conn.execute("UPDATE devices SET last_received=now()-interval '10 seconds'")
    ingest("sensor", token, sample)
    assert client.get("/devices").json()[0]["offline"]
    ingest("sensor", token, event("10"))
    assert not client.get("/devices").json()[0]["offline"]


def test_http_device_auth_and_registration_conflict(client):
    token = device(client)
    data = event("50").model_dump(mode="json")
    assert (
        client.post(
            "/devices/sensor/events", json=data, headers={"X-Device-Key": token}
        ).status_code
        == 200
    )
    assert (
        client.post(
            "/devices/sensor/events", json=data, headers={"X-Device-Key": "bad"}
        ).status_code
        == 401
    )
    assert client.post("/devices", json={"id": "sensor"}).status_code == 409


def test_rule_version_rebuild_preserves_data_and_retracts_alert(client):
    token = device(client)
    bucket = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    ingest("sensor", token, event("80", bucket))
    drain()
    before = client.get("/devices/sensor/windows").json()[0]
    assert before["active"]
    body = {
        "expected_revision": 1,
        "effective_at": bucket.isoformat(),
        "threshold": 90,
        "aggregate": "maximum",
        "minimum_count": 1,
    }
    changed = client.post("/devices/sensor/rules", json=body)
    assert changed.json() == {"version": 2, "windows_queued": 1}
    assert client.post("/devices/sensor/rules", json=body).status_code == 409
    drain()
    after = client.get("/devices/sensor/windows").json()[0]
    assert after["count"] == before["count"]
    assert after["total"] == before["total"]
    assert after["rule_version"] == 2
    assert not after["active"]
    transition = client.get("/devices/sensor/alerts").json()[-1]
    assert transition["reason"] == "rule_change"
    assert transition["rule_version"] == 2


def test_future_rule_does_not_rewrite_earlier_bucket(client):
    token = device(client)
    bucket = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=2)
    ingest("sensor", token, event("80", bucket))
    body = {
        "expected_revision": 1,
        "effective_at": (bucket + timedelta(minutes=1)).isoformat(),
        "threshold": 100,
        "minimum_count": 2,
    }
    assert client.post("/devices/sensor/rules", json=body).status_code == 200
    drain()
    before = client.get("/devices/sensor/windows").json()[0]
    assert before["active"]
    assert before["rule_version"] == 1
    ingest("sensor", token, event("100", bucket + timedelta(minutes=1)))
    drain()
    assert not client.get("/devices/sensor/windows").json()[0]["active"]
    ingest("sensor", token, event("100", bucket + timedelta(minutes=1, seconds=1)))
    drain()
    assert client.get("/devices/sensor/windows").json()[0]["active"]


def test_key_rotation_revokes_old_key_without_faking_liveness(client):
    old = device(client)
    with connect() as conn:
        conn.execute("UPDATE devices SET last_received=now()-interval '10 seconds'")
    response = client.post("/devices/sensor/rotate-key", json={"expected_revision": 1})
    new = response.json()["token"]
    assert new != old
    assert client.get("/devices").json()[0]["offline"]
    assert (
        client.post("/devices/sensor/rotate-key", json={"expected_revision": 1}).status_code == 409
    )
    with pytest.raises(PermissionError):
        ingest("sensor", old, event("10"))
    ingest("sensor", new, event("10"))
    assert not client.get("/devices").json()[0]["offline"]
