"""Versioned payload schemas for the synthetic e-commerce domain.

``OrderPlaced`` exists in two versions so that schema evolution is a real,
testable path rather than a claim: v2 adds an optional ``channel`` and renames
nothing, which makes it backward compatible. Failure scenario 2 injects a
*third*, deliberately incompatible shape to prove the consumer rejects it.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datatrust.contracts.base import EventPayload, EventType

# Money is Decimal everywhere. Floats silently lose cents under aggregation,
# and "revenue is off by $0.03" is exactly the class of bug this project
# claims to catch.
Money = Decimal


class Currency(StrEnum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


class OrderStatus(StrEnum):
    PLACED = "placed"
    FULFILLED = "fulfilled"
    CANCELLED = "cancelled"


class PaymentStatus(StrEnum):
    CAPTURED = "captured"
    FAILED = "failed"
    PENDING = "pending"


class SalesChannel(StrEnum):
    WEB = "web"
    MOBILE = "mobile"
    STORE = "store"


class OrderLine(BaseModel):
    """A single line on an order. Totals must agree with the lines."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    product_id: str = Field(min_length=1, max_length=64)
    quantity: int = Field(ge=1, le=1000)
    unit_price: Money = Field(ge=0, max_digits=12, decimal_places=2)

    @property
    def line_total(self) -> Money:
        return (self.unit_price * self.quantity).quantize(Decimal("0.01"))


class OrderPlacedV1(EventPayload):
    """An order was placed. Schema version 1."""

    event_type: Literal[EventType.ORDER] = EventType.ORDER
    schema_version: Literal[1] = 1

    order_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    order_timestamp: datetime
    currency: Currency = Currency.USD
    status: OrderStatus = OrderStatus.PLACED
    lines: list[OrderLine] = Field(min_length=1, max_length=50)
    gross_amount: Money = Field(ge=0, max_digits=14, decimal_places=2)

    def business_key(self) -> str:
        return self.order_id

    @model_validator(mode="after")
    def _totals_must_reconcile(self) -> OrderPlacedV1:
        """The declared total must equal the sum of the lines.

        This is a contract-level check, not a quality check: an order whose
        header disagrees with its own lines is malformed at the source, and
        letting it into ``raw_events`` would mean the immutable evidence is
        itself inconsistent.
        """
        expected = sum((line.line_total for line in self.lines), start=Decimal("0.00"))
        if expected != self.gross_amount:
            msg = f"gross_amount {self.gross_amount} != sum(lines) {expected}"
            raise ValueError(msg)
        return self


class OrderPlacedV2(EventPayload):
    """Schema version 2: adds an optional sales channel.

    Declared standalone rather than as a subclass of :class:`OrderPlacedV1`.
    Subclassing would make every v2 instance pass ``isinstance(x, OrderPlacedV1)``,
    which hides exactly the version confusion this project exists to catch.
    Compatibility between the two is asserted by the registry, not inherited.
    """

    event_type: Literal[EventType.ORDER] = EventType.ORDER
    schema_version: Literal[2] = 2

    order_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    order_timestamp: datetime
    currency: Currency = Currency.USD
    status: OrderStatus = OrderStatus.PLACED
    lines: list[OrderLine] = Field(min_length=1, max_length=50)
    gross_amount: Money = Field(ge=0, max_digits=14, decimal_places=2)
    # The one additive change in v2. A default is what makes it backward
    # compatible: a v1 payload deserializes cleanly as v2.
    channel: SalesChannel = SalesChannel.WEB

    def business_key(self) -> str:
        return self.order_id

    @model_validator(mode="after")
    def _totals_must_reconcile(self) -> OrderPlacedV2:
        expected = sum((line.line_total for line in self.lines), start=Decimal("0.00"))
        if expected != self.gross_amount:
            msg = f"gross_amount {self.gross_amount} != sum(lines) {expected}"
            raise ValueError(msg)
        return self


class PaymentCapturedV1(EventPayload):
    """A payment attempt against an order."""

    event_type: Literal[EventType.PAYMENT] = EventType.PAYMENT
    schema_version: Literal[1] = 1

    payment_id: str = Field(min_length=1, max_length=64)
    order_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    captured_at: datetime
    currency: Currency = Currency.USD
    status: PaymentStatus = PaymentStatus.CAPTURED
    amount: Money = Field(ge=0, max_digits=14, decimal_places=2)

    def business_key(self) -> str:
        return self.payment_id


class RefundIssuedV1(EventPayload):
    """A refund against a previously captured payment."""

    event_type: Literal[EventType.REFUND] = EventType.REFUND
    schema_version: Literal[1] = 1

    refund_id: str = Field(min_length=1, max_length=64)
    payment_id: str = Field(min_length=1, max_length=64)
    order_id: str = Field(min_length=1, max_length=64)
    refunded_at: datetime
    currency: Currency = Currency.USD
    amount: Money = Field(gt=0, max_digits=14, decimal_places=2)
    reason: str = Field(default="unspecified", max_length=120)

    def business_key(self) -> str:
        return self.refund_id


class CustomerUpdatedV1(EventPayload):
    """A customer dimension change. Applied last-write-wins by `updated_at`."""

    event_type: Literal[EventType.CUSTOMER] = EventType.CUSTOMER
    schema_version: Literal[1] = 1

    customer_id: str = Field(min_length=1, max_length=64)
    updated_at: datetime
    email: str = Field(min_length=3, max_length=255)
    country: str = Field(min_length=2, max_length=2)
    signup_date: date
    segment: str = Field(default="standard", max_length=32)

    def business_key(self) -> str:
        return self.customer_id

    @field_validator("email")
    @classmethod
    def _email_shape(cls, value: str) -> str:
        """Shape check only — we are not validating deliverability."""
        if "@" not in value or value.startswith("@") or value.endswith("@"):
            msg = f"malformed email: {value!r}"
            raise ValueError(msg)
        return value.lower()

    @field_validator("country")
    @classmethod
    def _country_upper(cls, value: str) -> str:
        if not value.isalpha():
            msg = f"country must be alphabetic ISO-3166-1 alpha-2: {value!r}"
            raise ValueError(msg)
        return value.upper()


class InventoryUpdatedV1(EventPayload):
    """A stock movement for a product at a location."""

    event_type: Literal[EventType.INVENTORY] = EventType.INVENTORY
    schema_version: Literal[1] = 1

    movement_id: str = Field(min_length=1, max_length=64)
    product_id: str = Field(min_length=1, max_length=64)
    location_id: str = Field(min_length=1, max_length=64)
    moved_at: datetime
    # Negative for shipments, positive for restocks. Zero is meaningless and
    # usually indicates a producer bug, so it is rejected.
    quantity_delta: int = Field(ge=-100_000, le=100_000)
    on_hand_after: int = Field(ge=0)

    def business_key(self) -> str:
        return self.movement_id

    @field_validator("quantity_delta")
    @classmethod
    def _no_zero_movement(cls, value: int) -> int:
        if value == 0:
            msg = "quantity_delta of zero is not a movement"
            raise ValueError(msg)
        return value
