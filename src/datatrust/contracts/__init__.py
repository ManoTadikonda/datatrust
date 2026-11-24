"""Data contracts: versioned event schemas and compatibility rules."""

from datatrust.contracts.base import (
    EventEnvelope,
    EventPayload,
    EventType,
    SchemaVersion,
    idempotency_key,
)
from datatrust.contracts.events import (
    CustomerUpdatedV1,
    InventoryUpdatedV1,
    OrderPlacedV1,
    OrderPlacedV2,
    PaymentCapturedV1,
    RefundIssuedV1,
)
from datatrust.contracts.registry import (
    Compatibility,
    ContractRegistry,
    default_registry,
)

__all__ = [
    "Compatibility",
    "ContractRegistry",
    "CustomerUpdatedV1",
    "EventEnvelope",
    "EventPayload",
    "EventType",
    "InventoryUpdatedV1",
    "OrderPlacedV1",
    "OrderPlacedV2",
    "PaymentCapturedV1",
    "RefundIssuedV1",
    "SchemaVersion",
    "default_registry",
    "idempotency_key",
]
