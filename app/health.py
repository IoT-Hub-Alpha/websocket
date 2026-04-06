"""Health check endpoints for the service."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/live")
async def live():
    """
    Liveness probe - indicates the service process is running.

    Returns:
        dict: {"status": "alive"}
    """
    return {"status": "alive"}


@router.get("/ready")
async def ready():
    """
    Readiness probe - indicates the service is ready to handle requests.

    Returns:
        dict: {"status": "ready"}
    """
    return {"status": "ready"}


@router.get("/health")
async def health():
    """
    Full health check - indicates overall service health.

    Returns:
        dict: Health status and service info
    """
    return {"status": "healthy", "service": "websocket-service"}
