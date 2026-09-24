import hashlib
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal

import pika
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from wireiot.db import connect, init
from wireiot.service import Rejected, ingest


@asynccontextmanager
async def lifespan(app):
    init()
    yield


def authorize(x_api_key: str = Header(default="")):
    key = os.environ.get("API_KEY", "")
    if not key or not secrets.compare_digest(key, x_api_key):
        raise HTTPException(401, "Неверный API-ключ")


app = FastAPI(title="WireIoT", lifespan=lifespan)


class Device(BaseModel):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    threshold: Decimal = Field(default=Decimal(70), ge=-1000, le=1000, decimal_places=3)
    timeout: int = Field(default=60, ge=1, le=86400)


class Event(BaseModel):
    id: uuid.UUID
    happened_at: datetime
    value: Decimal = Field(ge=-1000, le=1000, decimal_places=3)


@app.post("/devices", dependencies=[Depends(authorize)])
def register(body: Device):
    token = secrets.token_urlsafe(32)
    with connect() as conn:
        inserted = conn.execute(
            "INSERT INTO devices(id,token_hash,threshold,timeout) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id",
            (body.id, hashlib.sha256(token.encode()).hexdigest(), body.threshold, body.timeout),
        ).fetchone()
        if not inserted:
            raise HTTPException(409, "Устройство уже зарегистрировано")
        conn.execute(
            "INSERT INTO rules VALUES (%s,1,'-infinity',%s,'mean',1)", (body.id, body.threshold)
        )
    return {"id": body.id, "token": token, "key_revision": 1}


@app.post("/devices/{device}/events")
def receive(device: str, body: Event, x_device_key: str = Header(default="")):
    try:
        result = ingest(device, x_device_key, body)
    except PermissionError as exc:
        raise HTTPException(401, str(exc)) from exc
    except Rejected as exc:
        raise HTTPException(422, str(exc)) from exc
    try:
        with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
            channel = broker.channel()
            channel.queue_declare(queue="telemetry", durable=True)
            channel.basic_publish(
                exchange="",
                routing_key="telemetry",
                body=device,
                properties=pika.BasicProperties(delivery_mode=2),
            )
    except (pika.exceptions.AMQPError, OSError):
        pass
    return result


@app.get("/devices", dependencies=[Depends(authorize)])
def devices():
    with connect() as conn:
        return conn.execute(
            "SELECT id,threshold,timeout,last_received,now()-last_received>timeout*interval '1 second' AS offline FROM devices ORDER BY id"
        ).fetchall()


@app.get("/devices/{device}/windows", dependencies=[Depends(authorize)])
def windows(device: str):
    with connect() as conn:
        return conn.execute(
            """SELECT w.*,total/count AS mean,
            now()>=w.bucket+interval '11 minutes' AND NOT EXISTS (
                SELECT 1 FROM events e WHERE e.device=w.device AND e.bucket=w.bucket AND NOT e.processed AND e.included) AS final
            FROM windows w WHERE device=%s ORDER BY bucket DESC LIMIT 1000""",
            (device,),
        ).fetchall()


@app.get("/devices/{device}/alerts", dependencies=[Depends(authorize)])
def alerts(device: str):
    with connect() as conn:
        return conn.execute(
            "SELECT * FROM transitions WHERE device=%s ORDER BY created_at,revision LIMIT 1000",
            (device,),
        ).fetchall()


@app.get("/health", dependencies=[Depends(authorize)])
def health():
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


class Rule(BaseModel):
    expected_revision: int = Field(ge=1)
    effective_at: datetime
    threshold: Decimal = Field(ge=-1000, le=1000, decimal_places=3)
    aggregate: Literal["mean", "minimum", "maximum"] = "mean"
    minimum_count: int = Field(default=1, ge=1, le=100000)


@app.post("/devices/{device}/rules", dependencies=[Depends(authorize)])
def change_rule(device: str, body: Rule):
    instant = body.effective_at
    if (
        instant.tzinfo is None
        or instant.second
        or instant.microsecond
        or instant < datetime.now(UTC) - timedelta(days=1)
    ):
        raise HTTPException(422, "Нужна граница минуты с часовым поясом не старше суток")
    with connect() as conn:
        current = conn.execute(
            "SELECT rule_revision FROM devices WHERE id=%s FOR UPDATE", (device,)
        ).fetchone()
        if current is None:
            raise HTTPException(404, "Устройство не найдено")
        if current["rule_revision"] != body.expected_revision:
            raise HTTPException(409, "Версия правил изменилась")
        version = body.expected_revision + 1
        conn.execute(
            "INSERT INTO rules VALUES (%s,%s,%s,%s,%s,%s)",
            (device, version, instant, body.threshold, body.aggregate, body.minimum_count),
        )
        conn.execute("UPDATE devices SET rule_revision=%s WHERE id=%s", (version, device))
        count = conn.execute(
            "UPDATE windows SET dirty=true WHERE device=%s AND bucket>=%s", (device, instant)
        ).rowcount
    return {"version": version, "windows_queued": count}


@app.get("/devices/{device}/rules", dependencies=[Depends(authorize)])
def rules(device: str):
    with connect() as conn:
        return conn.execute(
            "SELECT device,version,effective_at::text,threshold,aggregate,minimum_count FROM rules WHERE device=%s ORDER BY version",
            (device,),
        ).fetchall()


class Rotation(BaseModel):
    expected_revision: int = Field(ge=1)


@app.post("/devices/{device}/rotate-key", dependencies=[Depends(authorize)])
def rotate(device: str, body: Rotation):
    token = secrets.token_urlsafe(32)
    with connect() as conn:
        row = conn.execute(
            "UPDATE devices SET token_hash=%s,key_revision=key_revision+1 WHERE id=%s AND key_revision=%s RETURNING key_revision",
            (hashlib.sha256(token.encode()).hexdigest(), device, body.expected_revision),
        ).fetchone()
        if row is None:
            raise HTTPException(409, "Устройство не найдено или ключ уже изменён")
        conn.execute(
            "INSERT INTO key_changes(device,revision) VALUES (%s,%s)", (device, row["key_revision"])
        )
    return {"token": token, "key_revision": row["key_revision"]}
