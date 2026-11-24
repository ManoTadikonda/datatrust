"""Runtime configuration.

All settings come from the environment (optionally via a `.env` file) so that
the same image runs in Compose, in CI, and on a laptop without code changes.
Defaults target the local Compose stack; none of them reach a paid service.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, PostgresDsn, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Profile(StrEnum):
    """Which infrastructure the process should bind to.

    ``DOCKER`` uses real Kafka and PostgreSQL from the Compose stack.
    ``LIGHTWEIGHT`` swaps Kafka for an embedded file-backed log so the whole
    reliability loop runs in one process on a constrained machine. The domain
    logic is identical under both.
    """

    DOCKER = "docker"
    LIGHTWEIGHT = "lightweight"


class LogFormat(StrEnum):
    JSON = "json"
    CONSOLE = "console"


class Settings(BaseSettings):
    """Process-wide settings. Construct via :func:`get_settings`."""

    model_config = SettingsConfigDict(
        env_prefix="DATATRUST_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    profile: Profile = Profile.DOCKER

    # --- PostgreSQL ---------------------------------------------------------
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "datatrust"
    pg_user: str = "datatrust"
    pg_password: str = "change-me-locally"
    pg_min_pool: int = Field(default=1, ge=1)
    pg_max_pool: int = Field(default=8, ge=1)

    # --- Kafka --------------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_consumer_group: str = "datatrust-ingest"
    kafka_topic_prefix: str = "datatrust"
    kafka_partitions: int = Field(default=3, ge=1)
    kafka_replication_factor: int = Field(default=1, ge=1)
    kafka_batch_size: int = Field(default=500, ge=1)
    kafka_poll_timeout_seconds: float = Field(default=1.0, gt=0)
    kafka_max_retries: int = Field(default=3, ge=0)

    # --- Storage ------------------------------------------------------------
    warehouse_root: Path = Path("./warehouse")

    # --- Quality / recovery policy -----------------------------------------
    lateness_watermark_minutes: int = Field(default=60, ge=0)
    freshness_sla_minutes: int = Field(default=15, ge=1)
    volume_anomaly_sigma: float = Field(default=3.0, gt=0)
    recovery_max_attempts: int = Field(default=3, ge=1)
    recovery_backoff_seconds: float = Field(default=2.0, ge=0)

    # --- Simulation ---------------------------------------------------------
    sim_seed: int = 20260101
    sim_events_per_second: int = Field(default=200, ge=1)

    # --- Observability ------------------------------------------------------
    metrics_port: int = 9108
    log_level: str = "INFO"
    log_format: LogFormat = LogFormat.JSON

    @computed_field  # type: ignore[prop-decorator]
    @property
    def pg_dsn(self) -> str:
        """libpq connection string for psycopg."""
        dsn = PostgresDsn.build(
            scheme="postgresql",
            username=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port,
            path=self.pg_database,
        )
        return str(dsn)

    @property
    def raw_root(self) -> Path:
        return self.warehouse_root / "raw"

    @property
    def validated_root(self) -> Path:
        return self.warehouse_root / "validated"

    @property
    def curated_root(self) -> Path:
        return self.warehouse_root / "curated"

    @property
    def quarantine_root(self) -> Path:
        return self.warehouse_root / "quarantine"

    def topic(self, event_type: str) -> str:
        """Kafka topic name for an event type."""
        return f"{self.kafka_topic_prefix}.{event_type}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached process settings.

    Cached so that configuration is read once and cannot drift mid-run. Tests
    that need different settings should call ``get_settings.cache_clear()`` or
    construct :class:`Settings` directly.
    """
    return Settings()
