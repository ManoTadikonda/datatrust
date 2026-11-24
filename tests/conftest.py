"""Shared fixtures."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from datatrust.config import Profile, Settings
from datatrust.contracts import ContractRegistry, EventEnvelope, EventType, default_registry


@pytest.fixture
def registry() -> ContractRegistry:
    return default_registry


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Settings isolated to a temp warehouse, lightweight profile."""
    return Settings(
        profile=Profile.LIGHTWEIGHT,
        warehouse_root=tmp_path / "warehouse",
        pg_password="test-only",
    )


@pytest.fixture
def now() -> datetime:
    """A fixed 'now' so event-time assertions are stable."""
    return datetime(2026, 11, 23, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def order_payload() -> dict[str, Any]:
    """A valid v1 order payload whose header reconciles with its lines."""
    return {
        "event_type": "orders",
        "schema_version": 1,
        "order_id": "ord-1001",
        "customer_id": "cus-77",
        "order_timestamp": "2026-11-23T11:58:00+00:00",
        "currency": "USD",
        "status": "placed",
        "lines": [
            {"product_id": "sku-1", "quantity": 2, "unit_price": "19.99"},
            {"product_id": "sku-2", "quantity": 1, "unit_price": "5.00"},
        ],
        "gross_amount": "44.98",
    }


@pytest.fixture
def make_envelope(now: datetime) -> Iterator[Any]:
    """Factory for envelopes with sane defaults."""

    def _make(
        payload: dict[str, Any],
        *,
        event_id: str = "evt-1",
        event_type: EventType | str = EventType.ORDER,
        schema_version: int = 1,
        entity_id: str = "ord-1001",
        lateness: timedelta = timedelta(seconds=2),
    ) -> EventEnvelope:
        event_time = now - lateness
        return EventEnvelope(
            event_id=event_id,
            event_type=EventType(event_type),
            schema_version=schema_version,
            entity_id=entity_id,
            event_timestamp=event_time,
            ingestion_timestamp=now,
            payload=payload,
        )

    yield _make


@pytest.fixture
def money() -> type[Decimal]:
    return Decimal
