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
            "SELECT * FROM devices WHERE id=%s FOR UPDATE", (device,)
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


def tick():
    with connect() as conn:
        event = conn.execute(
            "SELECT * FROM events WHERE NOT processed ORDER BY received_at,device,id LIMIT 1 FOR UPDATE SKIP LOCKED"
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
            threshold = conn.execute(
                "SELECT threshold FROM devices WHERE id=%s", (event["device"],)
            ).fetchone()["threshold"]
            mean = window["total"] / window["count"]
            active = mean >= threshold
            if active != window["active"]:
                conn.execute(
                    "UPDATE windows SET active=%s WHERE device=%s AND bucket=%s",
                    (active, event["device"], event["bucket"]),
                )
                conn.execute(
                    "INSERT INTO transitions(device,bucket,revision,active,mean) VALUES (%s,%s,%s,%s,%s)",
                    (event["device"], event["bucket"], window["revision"], active, mean),
                )
        conn.execute(
            "UPDATE events SET processed=true WHERE device=%s AND id=%s",
            (event["device"], event["id"]),
        )
        return True
