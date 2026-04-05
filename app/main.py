"""FastAPI application for WebSocket telemetry service."""
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from iot_logging import FastAPIRequestContextMiddleware, StructuredJsonFormatter

from app.config import get_settings
from app.health import router as health_router
from app.kafka_consumer import start_kafka_consumer, stop_kafka_consumer
from app.ws_manager import ConnectionManager

# Configure logging with iot_logging
logging.basicConfig(level=logging.INFO)
for handler in logging.root.handlers:
    handler.setFormatter(StructuredJsonFormatter())

logger = logging.getLogger(__name__)

# Global connection manager instance
manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifecycle manager.

    Handles startup and shutdown of background tasks.
    """
    logger.info("app.startup", extra={"service": "websocket-service"})
    start_kafka_consumer()
    yield
    logger.info("app.shutdown", extra={"service": "websocket-service"})
    await stop_kafka_consumer()


# Create FastAPI app
app = FastAPI(
    title="WebSocket Telemetry Service",
    description="Real-time telemetry delivery via WebSocket",
    lifespan=lifespan,
)

# Add request context middleware for structured logging
app.add_middleware(FastAPIRequestContextMiddleware)

# Include health check endpoints
app.include_router(health_router)


@app.get("/demo")
async def demo():
    """
    Serve the demo dashboard HTML file.

    Returns:
        FileResponse: The demo.html file
    """
    demo_path = Path(__file__).parent.parent / "frontend" / "demo.html"
    return FileResponse(demo_path, media_type="text/html")


@app.websocket("/ws/telemetry/")
async def ws_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time telemetry streaming.

    Protocol:
    - Client can subscribe to devices: {"action": "subscribe", "devices": ["SERIAL"]}
    - Server broadcasts telemetry: {"type": "telemetry", "serial_number": "...", ...}
    """
    conn = await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            await manager.handle_message(conn, data)
    except WebSocketDisconnect:
        await manager.disconnect(conn)
    except Exception as e:
        logger.error(
            "websocket.error",
            extra={"error": str(e)},
        )
        await manager.disconnect(conn)


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.http_host,
        port=settings.http_port,
        reload=False,
    )
