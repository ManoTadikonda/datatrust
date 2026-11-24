"""Envelope and identity rules shared by every event type.

The envelope is deliberately boring and stable: it carries routing and
provenance, and the versioned, domain-specific part lives in ``payload``. That
split is what lets a schema evolve without breaking the ingestion path — the
consumer can always read ``event_type`` and ``schema_version`` even when it
cannot read the payload.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

SchemaVersion = Annotated[int, Field(ge=1, le=99)]


class EventType(StrEnum):
    """The five domains DataTrust ingests."""

    ORDER = "orders"
    PAYMENT = "payments"
    REFUND = "refunds"
    CUSTOMER = "customers"
    INVENTORY = "inventory"


class EventPayload(BaseModel):
    """Base class for the versioned, domain-specific body of an event.

    Subclasses declare ``event_type`` and ``schema_version`` as class
    attributes so the registry can index them without instantiation.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_type: EventType
    schema_version: SchemaVersion

    def business_key(self) -> str:
        """The natural key this event is about (order id, payment id, ...).

        Used for Kafka partitioning and for duplicate detection.
        """
        raise NotImplementedError


class EventEnvelope(BaseModel):
    """What actually travels on the wire.

    ``payload`` stays an untyped mapping at this layer on purpose. Envelope
    parsing must succeed even for an event whose payload violates its contract,
    because that is precisely the event we need to quarantine *with* its
    routing metadata intact.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1, max_length=64)
    event_type: EventType
    schema_version: SchemaVersion
    entity_id: str = Field(min_length=1, max_length=64)
    event_timestamp: datetime
    ingestion_timestamp: datetime
    producer: str = Field(default="event-generator", max_length=64)
    payload: dict[str, Any]

    @field_validator("event_timestamp", "ingestion_timestamp")
    @classmethod
    def _require_timezone(cls, value: datetime) -> datetime:
        """Reject naive timestamps.

        Event-time versus processing-time logic is meaningless if some
        producers send local time, so we refuse naive datetimes at the edge
        rather than guessing a zone later.
        """
        if value.tzinfo is None:
            msg = "timestamps must be timezone-aware"
            raise ValueError(msg)
        return value.astimezone(UTC)

    @property
    def idempotency_key(self) -> str:
        """Stable key used to make reapplication a no-op."""
        return idempotency_key(self.event_type, self.entity_id, self.event_id)

    @property
    def lateness_seconds(self) -> float:
        """How far behind processing time this event's event-time is."""
        return (self.ingestion_timestamp - self.event_timestamp).total_seconds()


def idempotency_key(event_type: EventType | str, entity_id: str, event_id: str) -> str:
    """Derive the deterministic idempotency key for an event.

    Hashing rather than concatenating keeps the key a fixed 64 characters, so
    it can carry a unique index without bloating every fact table.

    The parts are *length-prefixed* rather than delimiter-joined. Joining on a
    separator is ambiguous whenever the separator can appear inside a part:
    ``("a|b", "c")`` and ``("a", "b|c")`` both render as ``a|b|c``, so two
    genuinely different events would share an idempotency key and the second
    would be silently discarded as a duplicate. Length prefixing makes the
    encoding injective for any input, including ids containing ``|``.
    """
    parts = (EventType(event_type).value, entity_id, event_id)
    digest = hashlib.sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(str(len(encoded)).encode("ascii"))
        digest.update(b":")
        digest.update(encoded)
    return digest.hexdigest()
