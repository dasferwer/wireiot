import logging
import os
import time

import pika

from wireiot.db import init
from wireiot.service import tick

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.WARNING)
    init()
    while True:
        try:
            if tick():
                continue
            try:
                with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
                    channel = broker.channel()
                    channel.queue_declare(queue="telemetry", durable=True)
                    method, _, _ = channel.basic_get(queue="telemetry", auto_ack=False)
                    if method:
                        channel.basic_ack(method.delivery_tag)
            except (pika.exceptions.AMQPError, OSError):
                pass
        except Exception:
            logger.exception("Ошибка обработки телеметрии")
        time.sleep(0.2)


if __name__ == "__main__":
    main()
