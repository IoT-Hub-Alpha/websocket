"""WebSocket connection manager for telemetry subscriptions."""

import json
import logging
from dataclasses import dataclass, field
from typing import Set

from fastapi import WebSocket

logger = logging.getLogger(__name__)

# Module-level registry of connected clients
connected_clients: Set["WsConnection"] = set()


@dataclass(unsafe_hash=True)
class WsConnection:
    """Represents a connected WebSocket client."""

    websocket: WebSocket = field(hash=False, compare=False)
    subscribed_devices: Set[str] = field(default_factory=set, hash=False, compare=False)


class ConnectionManager:
    """Manages WebSocket connections and device subscriptions."""

    async def connect(self, websocket: WebSocket) -> WsConnection:
        """
        Accept a new WebSocket connection and register it.

        Args:
            websocket: The FastAPI WebSocket connection

        Returns:
            WsConnection: The connection object with subscription info
        """
        await websocket.accept()
        conn = WsConnection(websocket=websocket)
        connected_clients.add(conn)

        # Log connection
        client_addr = websocket.client
        logger.info(
            "websocket.connected",
            extra={
                "client": (
                    f"{client_addr.host}:{client_addr.port}"
                    if client_addr
                    else "unknown"
                ),
                "total_clients": len(connected_clients),
            },
        )

        # Send welcome message
        await websocket.send_json(
            {
                "type": "connection",
                "status": "connected",
                "user": "anonymous",
                "subscriptions": [],
                "message": "Connected. Subscribe to devices to receive telemetry.",
            }
        )

        return conn

    async def disconnect(self, conn: WsConnection) -> None:
        """
        Remove a client from the registry on disconnect.

        Args:
            conn: The WsConnection to disconnect
        """
        connected_clients.discard(conn)

        client_addr = conn.websocket.client
        logger.info(
            "websocket.disconnected",
            extra={
                "client": (
                    f"{client_addr.host}:{client_addr.port}"
                    if client_addr
                    else "unknown"
                ),
                "total_clients": len(connected_clients),
            },
        )

    async def handle_message(self, conn: WsConnection, data: dict) -> None:
        """
        Handle incoming message from client.

        Args:
            conn: The WsConnection sending the message
            data: The parsed JSON message
        """
        action = data.get("action")

        if action == "subscribe":
            await self._handle_subscribe(conn, data)
        elif action == "unsubscribe":
            await self._handle_unsubscribe(conn, data)
        elif action == "list":
            await self._handle_list(conn)
        elif action is None:
            logger.warning(
                "websocket.missing_action",
                extra={"data": data},
            )
            await conn.websocket.send_json(
                {
                    "type": "error",
                    "error": "missing_action",
                    "message": "Message must include an 'action' field",
                    "available_actions": ["subscribe", "unsubscribe", "list"],
                }
            )
        else:
            logger.warning(
                "websocket.unknown_action",
                extra={"action": action},
            )
            await conn.websocket.send_json(
                {
                    "type": "error",
                    "error": "unknown_action",
                    "message": f"Unknown action: {action}",
                    "available_actions": ["subscribe", "unsubscribe", "list"],
                }
            )

    async def _handle_subscribe(self, conn: WsConnection, data: dict) -> None:
        """Handle subscribe action."""
        devices = data.get("devices", [])
        if not isinstance(devices, list):
            devices = [devices]

        # Filter valid device serial numbers
        valid_devices = [d for d in devices if d and isinstance(d, str)]
        conn.subscribed_devices.update(valid_devices)

        logger.info(
            "websocket.subscribed",
            extra={
                "devices": valid_devices,
                "subscriptions": list(conn.subscribed_devices),
            },
        )

        await conn.websocket.send_json(
            {
                "type": "subscription",
                "action": "subscribed",
                "subscriptions": list(conn.subscribed_devices),
            }
        )

    async def _handle_unsubscribe(self, conn: WsConnection, data: dict) -> None:
        """Handle unsubscribe action."""
        devices = data.get("devices", [])
        if not isinstance(devices, list):
            devices = [devices]

        # Filter valid device serial numbers
        valid_devices = [d for d in devices if d and isinstance(d, str)]
        conn.subscribed_devices.difference_update(valid_devices)

        logger.info(
            "websocket.unsubscribed",
            extra={
                "devices": valid_devices,
                "subscriptions": list(conn.subscribed_devices),
            },
        )

        await conn.websocket.send_json(
            {
                "type": "subscription",
                "action": "unsubscribed",
                "subscriptions": list(conn.subscribed_devices),
            }
        )

    async def _handle_list(self, conn: WsConnection) -> None:
        """Handle list action - return current subscriptions."""
        logger.info(
            "websocket.list",
            extra={
                "subscriptions": list(conn.subscribed_devices),
            },
        )

        await conn.websocket.send_json(
            {
                "type": "subscription",
                "action": "list",
                "subscriptions": list(conn.subscribed_devices),
            }
        )

    async def broadcast(self, data: dict) -> None:
        """
        Broadcast telemetry data to subscribed clients.

        Args:
            data: The telemetry message to broadcast
        """
        if not connected_clients:
            return

        serial_number = data.get("serial_number")
        disconnected = []

        for client in connected_clients:
            # Only send to clients subscribed to this device
            if (
                not client.subscribed_devices
                or serial_number not in client.subscribed_devices
            ):
                continue

            try:
                await client.websocket.send_json(data)
            except Exception as e:
                logger.warning(
                    "websocket.broadcast_failed",
                    extra={"error": str(e)},
                )
                disconnected.append(client)

        # Clean up any disconnected clients
        for client in disconnected:
            connected_clients.discard(client)
