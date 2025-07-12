import json
import os
from enum import Enum

from pydantic_settings import BaseSettings
from starlette.config import Config

current_file_dir = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(current_file_dir, ".env")
config = Config(env_path)


class DatabaseSettings(BaseSettings):
    pass


class PostgresSettings(DatabaseSettings):
    POSTGRES_USER: str = config("POSTGRES_USER", default="btj-academy")
    POSTGRES_PASSWORD: str = config("POSTGRES_PASSWORD", default="btj-academy")
    POSTGRES_SERVER: str = config("POSTGRES_SERVER", default="localhost")
    POSTGRES_PORT: int = config("POSTGRES_PORT", default=5432)
    POSTGRES_DB: str = config("POSTGRES_DB", default="btj-academy")
    POSTGRES_SYNC_PREFIX: str = config("POSTGRES_SYNC_PREFIX", default="postgresql://")
    POSTGRES_ASYNC_PREFIX: str = config(
        "POSTGRES_ASYNC_PREFIX", default="postgresql+asyncpg://"
    )
    POSTGRES_URI: str = f"{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"
    POSTGRES_URL: str | None = config("POSTGRES_URL", default=None)


class RedisCacheSettings(BaseSettings):
    REDIS_HOST: str = config("REDIS_HOST", default="localhost")
    REDIS_PORT: int = config("REDIS_PORT", default=6379)
    REDIS_DB: int = config("REDIS_DB", default=1)
    REDIS_URL: str = f"redis://{REDIS_HOST}:{REDIS_PORT}"


class RabbitMQSettings(BaseSettings):
    RABBITMQ_HOST: str = config("RABBITMQ_HOST", default="localhost")
    RABBITMQ_PORT: int = config("RABBITMQ_PORT", default=5672)
    RABBITMQ_USER: str = config("RABBITMQ_USER", default="btj-academy")
    RABBITMQ_PASSWORD: str = config("RABBITMQ_PASSWORD", default="btj-academy")
    RABBITMQ_VHOST: str = config("RABBITMQ_VHOST", default="/")
    RABBITMQ_URL: str = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}{RABBITMQ_VHOST}"


class GrpcSettings(BaseSettings):
    GRPC_HOST: str = config("GRPC_HOST", default="localhost")
    GRPC_PORT: int = config("GRPC_PORT", default=3000)
    GRPC_URL: str = f"{GRPC_HOST}:{GRPC_PORT}"
    GRPC_SERVICE_CONFIG: str = json.dumps(
        {
            "methodConfig": [
                {
                    "name": [{}],
                    "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "0.1s",
                        "maxBackoff": "1s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": ["UNAVAILABLE"],
                    },
                }
            ]
        }
    )


class EnvironmentOption(Enum):
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


class EnvironmentSettings(BaseSettings):
    ENVIRONMENT: EnvironmentOption = config(
        "ENVIRONMENT", default=EnvironmentOption.LOCAL
    )


class Settings(
    PostgresSettings,
    RedisCacheSettings,
    RabbitMQSettings,
    GrpcSettings,
    EnvironmentSettings,
):
    pass


settings = Settings()
