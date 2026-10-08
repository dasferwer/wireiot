import logging
import os
import re
import time

import paho.mqtt.client as mqtt
from pydantic import BaseModel, Field, StrictStr, ValidationError

from wireiot.api import Event
from wireiot.db import init
from wireiot.service import Rejected, ingest

logger = logging.getLogger(__name__)


class Envelope(BaseModel):
    token: StrictStr = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    event: Event


def on_connect(client, userdata, flags, reason_code, properties):
    if not reason_code.is_failure:
        client.subscribe("telemetry/+", qos=1)


def on_subscribe(client, userdata, mid, reason_codes, properties):
    client.publish("wireiot/bridge/status", "online", qos=1, retain=True)


def on_message(client, userdata, message):
    try:
        if len(message.payload) > 8192:
            raise Rejected("Слишком большое сообщение")
        topic = message.topic.split("/")
        if (
            len(topic) != 2
            or topic[0] != "telemetry"
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", topic[1])
        ):
            raise Rejected("Неверный topic устройства")
        envelope = Envelope.model_validate_json(message.payload)
        ingest(topic[1], envelope.token, envelope.event)
    except (Rejected, PermissionError, ValidationError, ValueError, KeyError, TypeError):
        logger.warning("Некорректное MQTT-событие отклонено")
        client.ack(message.mid, message.qos)
    except Exception:
        logger.exception("Событие не зафиксировано; повтор после переподключения")
        client.disconnect()
    else:
        # QoS 1 подтверждается только после фиксации события в PostgreSQL.
        client.ack(message.mid, message.qos)


def main():
    logging.basicConfig(level=logging.WARNING)
    init()
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="wireiot-bridge",
        clean_session=False,
        manual_ack=True,
    )
    client.will_set("wireiot/bridge/status", "offline", qos=1, retain=True)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    while True:
        try:
            client.connect(
                os.environ.get("MQTT_HOST", "localhost"),
                int(os.environ.get("MQTT_PORT", "1883")),
                keepalive=30,
            )
            client.loop_forever(retry_first_connection=True)
        except OSError:
            logger.exception("MQTT недоступен")
        time.sleep(1)


if __name__ == "__main__":
    main()
