import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from wireiot.db import connect


class Rejected(ValueError):
    pass


def ingest(device, token, event, now=None):
    now = now or datetime.now(UTC)
    timestamp = event.happened_at
    if (
        timestamp.tzinfo is None
        or timestamp > now + timedelta(seconds=30)
        or timestamp < now - timedelta(days=1)
    ):
        raise Rejected(
            "Неверное время события: нужен часовой пояс, не старше суток и не дальше 30 секунд в будущем"
        )
    timestamp = timestamp.astimezone(UTC)
    bucket = timestamp.replace(second=0, microsecond=0)
    with connect() as conn:
        registered = conn.execute(
            "SELECT token_hash FROM devices WHERE id=%s FOR UPDATE", (device,)
        ).fetchone()
        if not registered or not secrets.compare_digest(
            registered["token_hash"], hashlib.sha256(token.encode()).hexdigest()
        ):
            raise PermissionError("Неверный ключ устройства")
        old = conn.execute(
            "SELECT happened_at,value,included FROM events WHERE device=%s AND id=%s",
            (device, event.id),
        ).fetchone()
        if old:
            if old["happened_at"] != timestamp or old["value"] != event.value:
                raise Rejected("ID события уже связан с другими данными")
            return {"duplicate": True, "included": old["included"]}
        included = now < bucket + timedelta(minutes=11)
        conn.execute(
            "INSERT INTO events(device,id,happened_at,received_at,bucket,value,included) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (device, event.id, timestamp, now, bucket, event.value, included),
        )
        conn.execute(
            "UPDATE devices SET last_received=GREATEST(last_received,%s) WHERE id=%s", (now, device)
        )
        return {"duplicate": False, "included": included}


def evaluate(conn, window, reason):
    rule = conn.execute(
        "SELECT version,threshold,aggregate,minimum_count FROM rules WHERE device=%s AND effective_at<=%s ORDER BY effective_at DESC,version DESC LIMIT 1",
        (window["device"], window["bucket"]),
    ).fetchone()
    mean = window["total"] / window["count"]
    measured = mean if rule["aggregate"] == "mean" else window[rule["aggregate"]]
    active = window["count"] >= rule["minimum_count"] and measured >= rule["threshold"]
    if active != window["active"] or rule["version"] != window["rule_version"]:
        conn.execute(
            "INSERT INTO transitions(device,bucket,revision,active,mean,rule_version,reason) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                window["device"],
                window["bucket"],
                window["revision"],
                active,
                mean,
                rule["version"],
                reason,
            ),
        )
    conn.execute(
        "UPDATE windows SET active=%s,rule_version=%s,dirty=false WHERE device=%s AND bucket=%s",
        (active, rule["version"], window["device"], window["bucket"]),
    )


def tick():
    with connect() as conn:
        # Общий порядок блокировок: устройство, затем событие и окно. Правило не меняется посреди оценки.
        device = conn.execute("""SELECT d.id FROM devices d WHERE EXISTS (
            SELECT 1 FROM events e WHERE e.device=d.id AND NOT e.processed)
            OR EXISTS(SELECT 1 FROM windows w WHERE w.device=d.id AND w.dirty)
            ORDER BY d.served_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT 1""").fetchone()
        if not device:
            return False
        conn.execute("UPDATE devices SET served_at=clock_timestamp() WHERE id=%s", (device["id"],))
        dirty = conn.execute(
            "SELECT * FROM windows WHERE device=%s AND dirty ORDER BY bucket LIMIT 1 FOR UPDATE",
            (device["id"],),
        ).fetchone()
        if dirty:
            window = conn.execute(
                "UPDATE windows SET revision=revision+1 WHERE device=%s AND bucket=%s RETURNING *",
                (device["id"], dirty["bucket"]),
            ).fetchone()
            evaluate(conn, window, "rule_change")
            return True
        event = conn.execute(
            "SELECT * FROM events WHERE device=%s AND NOT processed ORDER BY received_at,id LIMIT 1 FOR UPDATE",
            (device["id"],),
        ).fetchone()
        if not event:
            return False
        if event["included"]:
            window = conn.execute(
                """INSERT INTO windows(device,bucket,count,total,minimum,maximum,revision)
                VALUES (%s,%s,1,%s,%s,%s,1) ON CONFLICT(device,bucket) DO UPDATE SET
                count=windows.count+1,total=windows.total+EXCLUDED.total,
                minimum=LEAST(windows.minimum,EXCLUDED.minimum),maximum=GREATEST(windows.maximum,EXCLUDED.maximum),
                revision=windows.revision+1 RETURNING *""",
                (event["device"], event["bucket"], event["value"], event["value"], event["value"]),
            ).fetchone()
            evaluate(conn, window, "telemetry")
        conn.execute(
            "UPDATE events SET processed=true WHERE device=%s AND id=%s",
            (event["device"], event["id"]),
        )
        return True
