"""Configuration management using pydantic-settings."""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Settings for WebSocket telemetry service."""

    # Kafka configuration
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic_telemetry_raw: str = Field(
        default="telemetry.raw", alias="KAFKA_TOPIC_TELEMETRY_RAW"
    )
    kafka_consumer_group: str = Field(
        default="websocket-service-telemetry", alias="KAFKA_CONSUMER_GROUP"
    )
    kafka_security_protocol: str = Field(
        default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL"
    )
    kafka_sasl_mechanism: str = Field(default="", alias="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: str = Field(default="", alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str = Field(default="", alias="KAFKA_SASL_PASSWORD")

    # HTTP configuration
    http_host: str = Field(default="0.0.0.0", alias="HTTP_HOST")
    http_port: int = Field(default=8006, alias="HTTP_PORT")

    # Service configuration
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    service_name: str = Field(default="websocket-service", alias="SERVICE_NAME")

    # JWT authentication
    jwt_secret_key: str = Field(
        default="dev-jwt-secret-change-in-production", alias="JWT_SECRET_KEY"
    )
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")

    class Config:
        env_file = ".env"
        case_sensitive = False


_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
