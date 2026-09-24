import json
import logging
import os
import time

import paho.mqtt.client as mqtt
from pydantic import ValidationError

from wireiot.api import Event
from wireiot.db import init
from wireiot.service import Rejected, ingest

logger = logging.getLogger(__name__)


def on_connect(client, userdata, flags, reason_code, properties):
    if not reason_code.is_failure:
        client.subscribe("telemetry/+", qos=1)


def on_subscribe(client, userdata, mid, reason_codes, properties):
    client.publish("wireiot/bridge/status", "online", qos=1, retain=True)


def on_message(client, userdata, message):
    try:
        if len(message.payload) > 8192:
            raise Rejected("Слишком большое сообщение")
        payload = json.loads(message.payload)
        ingest(
            message.topic.split("/")[1], payload["token"], Event.model_validate(payload["event"])
        )
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
