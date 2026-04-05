"""Kafka consumer for telemetry messages."""
import asyncio
import json
import logging
import threading
from typing import Optional

from IoTKafka import IoTKafkaConsumer

from app.config import get_settings
from app.ws_manager import connected_clients

logger = logging.getLogger(__name__)

_consumer_task: Optional[asyncio.Task] = None
_consumer_stop_event: threading.Event = threading.Event()


def _create_consumer() -> IoTKafkaConsumer:
    """Create Kafka consumer from settings."""
    settings = get_settings()
    return IoTKafkaConsumer(
        group_id=settings.kafka_consumer_group,
        enable_auto_offset_store=True,
    )


async def _consumer_loop() -> None:
    """
    Consume messages from telemetry.raw and broadcast to WebSocket clients.
    Runs Kafka polling in a thread executor to avoid blocking the event loop.
    """
    settings = get_settings()
    topic = settings.kafka_topic_telemetry_raw
    consumer = _create_consumer()
    consumer.subscribe([topic])

    logger.info(
        "kafka_consumer.started",
        extra={"topic": topic, "group": settings.kafka_consumer_group},
    )

    loop = asyncio.get_event_loop()
    _consumer_stop_event.clear()

    try:
        while not _consumer_stop_event.is_set():
            # Poll in thread to avoid blocking async event loop
            messages = await loop.run_in_executor(
                None, lambda: consumer.consume(num_messages=1, timeout=1)
            )

            if not messages:
                continue

            for msg in messages:
                if msg.error():
                    logger.error(
                        "kafka_consumer.error",
                        extra={"error": str(msg.error())},
                    )
                    continue

                # Parse and broadcast message
                try:
                    value = msg.value()
                    if value:
                        data = json.loads(value.decode("utf-8"))
                        data["type"] = "telemetry"

                        if connected_clients:
                            await broadcast_telemetry(data)

                            logger.debug(
                                "kafka_consumer.broadcast",
                                extra={
                                    "serial_number": data.get("serial_number"),
                                    "clients": len(connected_clients),
                                },
                            )
                except json.JSONDecodeError as e:
                    logger.warning(
                        "kafka_consumer.invalid_json",
                        extra={
                            "error": str(e),
                            "raw": str(msg.value()[:100]) if msg.value() else None,
                        },
                    )
                except Exception as e:
                    logger.error(
                        "kafka_consumer.broadcast_error",
                        extra={"error": str(e)},
                    )

    except asyncio.CancelledError:
        logger.info("kafka_consumer.cancelled")
    finally:
        consumer.close()
        logger.info("kafka_consumer.stopped")


async def broadcast_telemetry(data: dict) -> None:
    """
    Broadcast telemetry data to subscribed clients.

    Args:
        data: The telemetry message to broadcast
    """
    from app.ws_manager import ConnectionManager

    manager = ConnectionManager()
    await manager.broadcast(data)


def start_kafka_consumer() -> None:
    """Start the Kafka consumer background task."""
    global _consumer_task

    if _consumer_task is not None and not _consumer_task.done():
        logger.debug("kafka_consumer.already_running")
        return

    _consumer_stop_event.clear()
    _consumer_task = asyncio.create_task(_consumer_loop())
    logger.info("kafka_consumer.task_started")


async def stop_kafka_consumer() -> None:
    """Stop the Kafka consumer background task and wait for cleanup."""
    global _consumer_task

    _consumer_stop_event.set()

    if _consumer_task is not None and not _consumer_task.done():
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
        _consumer_task = None
        logger.info("kafka_consumer.task_stopped")
