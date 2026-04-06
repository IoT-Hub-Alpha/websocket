# WebSocket Telemetry Service

Real-time telemetry delivery microservice. Consumes device data from Kafka and broadcasts it to connected WebSocket clients with per-device subscription management.

## Architecture

```
Client (Browser)
    ↓ wss://host/ws/telemetry/
nginx (Reverse Proxy)
    ↓
FastAPI WebSocket Server (port 8006)
    ├─ ConnectionManager (in-memory client registry)
    └─ Kafka Consumer (asyncio task)
         ↓
    Kafka Topic: telemetry.raw
```

## Quick Start

```bash
# From repository root
docker compose up websocket-service

# Access demo dashboard
open https://localhost/demo
```

## API Protocol

### WebSocket Connection

Connect to: `wss://host/ws/telemetry/?token=<jwt>`

**Authentication Required**: All WebSocket connections require a valid JWT token. Provide the token via:
- Query string parameter: `?token=<jwt>`
- Authorization header: `Authorization: Bearer <jwt>`

### Client → Server Messages

**Subscribe to devices:**
```json
{
  "action": "subscribe",
  "devices": ["SERIAL-001", "SERIAL-002"]
}
```

**Unsubscribe from devices:**
```json
{
  "action": "unsubscribe",
  "devices": ["SERIAL-001"]
}
```

**List current subscriptions:**
```json
{
  "action": "list"
}
```

### Server → Client Messages

**Connection confirmation:**
```json
{
  "type": "connection",
  "status": "connected",
  "user": "anonymous",
  "subscriptions": [],
  "message": "Connected. Subscribe to devices to receive telemetry."
}
```

**Subscription update:**
```json
{
  "type": "subscription",
  "action": "subscribed",
  "subscriptions": ["SERIAL-001", "SERIAL-002"]
}
```

**Telemetry data (broadcast):**
```json
{
  "type": "telemetry",
  "serial_number": "SERIAL-001",
  "payload": {"value": 23.5},
  "received_at": "2026-04-05T10:00:00Z",
  "ingest_protocol": "MQTT"
}
```

**Error:**
```json
{
  "type": "error",
  "error": "unknown_action",
  "message": "Unknown action: foo",
  "available_actions": ["subscribe", "unsubscribe", "list"]
}
```

## Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `kafka:9092` | No |
| `KAFKA_TOPIC_TELEMETRY_RAW` | Telemetry topic name | `telemetry.raw` | No |
| `KAFKA_CONSUMER_GROUP` | Consumer group ID | `websocket-service-telemetry` | No |
| `KAFKA_SECURITY_PROTOCOL` | Kafka security (PLAINTEXT/SSL/SASL_SSL) | `PLAINTEXT` | No |
| `KAFKA_SASL_MECHANISM` | SASL mechanism if using SASL | `` | No |
| `KAFKA_SASL_USERNAME` | SASL username if using SASL | `` | No |
| `KAFKA_SASL_PASSWORD` | SASL password if using SASL | `` | No |
| `HTTP_HOST` | Server bind address | `0.0.0.0` | No |
| `HTTP_PORT` | Server port | `8006` | No |
| `LOG_LEVEL` | Logging level | `INFO` | No |
| `SERVICE_NAME` | Service identifier for logs | `websocket-service` | No |
| `JWT_SECRET_KEY` | Secret key for JWT validation | `dev-jwt-secret-change-in-production` | No |
| `JWT_ALGORITHM` | JWT signing algorithm | `HS256` | No |

## Health Checks

| Endpoint | Purpose |
|----------|---------|
| `GET /live` | Liveness probe (process running) |
| `GET /ready` | Readiness probe (service ready) |
| `GET /health` | Full health status |

Example:
```bash
curl -s http://localhost:8006/live
# {"status":"alive"}
```

## Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/demo` | GET | Interactive demo dashboard |
| `/ws/telemetry/` | WS | WebSocket telemetry stream |
| `/live` | GET | Liveness probe |
| `/ready` | GET | Readiness probe |
| `/health` | GET | Health status |

## Development

### Local Setup

```bash
# Install dependencies
pip install -r services/websocket/requirements.txt

# Run with uvicorn
python -m uvicorn app.main:app --host 0.0.0.0 --port 8006 --reload
```

### File Structure

```
services/websocket/
├── app/
│   ├── main.py              # FastAPI app, lifespan, endpoints
│   ├── auth.py              # JWT authentication logic
│   ├── config.py            # Pydantic settings
│   ├── ws_manager.py        # ConnectionManager, WsConnection
│   ├── kafka_consumer.py    # Kafka consumer task
│   └── health.py            # Health check routes
├── frontend/
│   └── demo.html            # Interactive demo dashboard
├── Dockerfile               # Two-stage build
├── requirements.txt         # Python dependencies
└── .env.example             # Environment template
```

### Key Components

- **JWT Authentication**: Validates JWT tokens from query string or Authorization header
- **ConnectionManager**: Manages active WebSocket connections and subscriptions
- **Kafka Consumer**: Background asyncio task consuming from `telemetry.raw`
- **FastAPI Lifespan**: Handles startup/shutdown of Kafka consumer
- **StructuredJsonFormatter**: Logs in JSON format for observability

## Testing

### Test WebSocket Connection

First, generate a valid JWT token (or use a token from your auth service). Then connect with the token:

```bash
# From inside container or with websocket client
python3 << 'EOF'
import asyncio, websockets, json

async def test():
    # Replace with a valid JWT token
    token = "your-jwt-token-here"
    uri = f"ws://localhost:8006/ws/telemetry/?token={token}"

    async with websockets.connect(uri) as ws:
        print(await ws.recv())  # Connection message
        await ws.send(json.dumps({"action": "subscribe", "devices": ["DEVICE-ID"]}))
        print(await ws.recv())  # Subscription confirmation

asyncio.run(test())
EOF
```

To test authentication rejection (missing token):
```bash
python3 << 'EOF'
import asyncio, websockets, json

async def test():
    try:
        # This should fail - no token provided
        async with websockets.connect("ws://localhost:8006/ws/telemetry/") as ws:
            print(await ws.recv())
    except Exception as e:
        print(f"Connection rejected: {e}")

asyncio.run(test())
EOF
```

### Test Health Endpoints

```bash
curl -s http://localhost:8006/live
curl -s http://localhost:8006/ready
curl -s http://localhost:8006/health
```

## Logs

Logs are output in structured JSON format:

```json
{"message": "websocket.connected", "client": "172.18.0.1:12345", "total_clients": 1, "level": "info"}
{"message": "kafka_consumer.broadcast", "serial_number": "DEVICE-001", "clients": 1, "level": "debug"}
```

Use the `SERVICE_NAME` environment variable to identify this service in logs.

## Authentication

The service requires **JWT authentication** for all WebSocket connections. The JWT token can be provided via:

1. **Query String**: `wss://host/ws/telemetry/?token=<jwt>`
2. **Authorization Header**: `Authorization: Bearer <jwt>`

The JWT payload is decoded and validated using the configured secret key. Invalid or missing tokens will result in connection rejection (WebSocket close code 4401).

**Environment Variable**: `JWT_SECRET_KEY` - Secret key for JWT validation (default: `dev-jwt-secret-change-in-production`)

## Notes

- **JWT Authentication**: All WebSocket connections require valid JWT tokens
- **In-Memory Registry**: Subscriptions are not persisted across restarts
- **Per-Client Subscriptions**: Each client manages its own device subscriptions independently
