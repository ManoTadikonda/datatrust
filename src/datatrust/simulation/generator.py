"""Deterministic synthetic e-commerce event generation.

Two properties matter more than realism here:

1. **Reproducibility.** The same seed produces byte-identical events, so a
   failure scenario can be replayed and a benchmark can be compared against an
   earlier run. Every random decision draws from one seeded
   :class:`random.Random`; nothing touches the global RNG.

2. **Referential consistency.** Payments reference orders that exist, refunds
   reference payments that exist, and order headers reconcile with their lines.
   A generator that emits dangling references would make the referential
   integrity check fire constantly and mean nothing — the broken data must come
   from the Failure Lab, deliberately, not from the generator by accident.
"""

from __future__ import annotations

import random
import string
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from datatrust.contracts.base import EventEnvelope, EventPayload, EventType
from datatrust.contracts.events import (
    Currency,
    CustomerUpdatedV1,
    InventoryUpdatedV1,
    OrderLine,
    OrderPlacedV1,
    OrderPlacedV2,
    PaymentCapturedV1,
    PaymentStatus,
    RefundIssuedV1,
    SalesChannel,
)

_CATEGORIES = ("apparel", "electronics", "home", "grocery", "sports")
_SEGMENTS = ("standard", "plus", "enterprise")
_COUNTRIES = ("US", "GB", "DE", "FR", "IN", "CA")
_REFUND_REASONS = ("damaged", "not_as_described", "late_delivery", "changed_mind")


@dataclass(frozen=True, slots=True)
class GeneratorConfig:
    """Shape of the synthetic world and the event mix."""

    seed: int = 20260101
    customer_count: int = 500
    product_count: int = 200
    location_count: int = 5

    # Relative weights for what to emit next. Orders dominate; refunds are rare,
    # which is what makes a refund spike a meaningful volume anomaly.
    order_weight: int = 50
    payment_weight: int = 25
    refund_weight: int = 3
    customer_weight: int = 12
    inventory_weight: int = 10

    max_lines_per_order: int = 5
    # Fraction of events that arrive mildly out of order, as real streams do.
    # Scenario 4 injects *extreme* lateness on top of this baseline.
    natural_lateness_rate: float = 0.05
    max_natural_lateness_seconds: int = 120

    # Share of orders emitted at the older v1 schema. A stream that only ever
    # carries the newest version never exercises the consumer's backward
    # compatibility path, which is the path most likely to rot unnoticed.
    v1_order_rate: float = 0.15

    # How far back the generator looks when choosing an order to pay or a
    # payment to refund. Bounded so selection stays O(1) in stream length.
    recency_window: int = 200

    def __post_init__(self) -> None:
        if self.customer_count < 1 or self.product_count < 1:
            msg = "customer_count and product_count must be positive"
            raise ValueError(msg)
        if not 0.0 <= self.natural_lateness_rate <= 1.0:
            msg = "natural_lateness_rate must be a fraction between 0 and 1"
            raise ValueError(msg)
        if not 0.0 <= self.v1_order_rate <= 1.0:
            msg = "v1_order_rate must be a fraction between 0 and 1"
            raise ValueError(msg)
        if self.recency_window < 1:
            msg = "recency_window must be positive"
            raise ValueError(msg)
        if self.total_weight <= 0:
            msg = "at least one event weight must be positive"
            raise ValueError(msg)

    @property
    def total_weight(self) -> int:
        return (
            self.order_weight
            + self.payment_weight
            + self.refund_weight
            + self.customer_weight
            + self.inventory_weight
        )


@dataclass
class WorldState:
    """What the generator has emitted so far.

    Kept so that later events can reference earlier ones. This is also what the
    reconciliation checks are measured against: the generator knows the true
    totals, so a test can assert the warehouse agrees with reality rather than
    merely agreeing with itself.
    """

    orders: dict[str, Decimal] = field(default_factory=dict)
    """order_id -> gross_amount"""

    payments: dict[str, tuple[str, Decimal]] = field(default_factory=dict)
    """payment_id -> (order_id, amount)"""

    refunded: dict[str, Decimal] = field(default_factory=dict)
    """payment_id -> total refunded so far"""

    on_hand: dict[tuple[str, str], int] = field(default_factory=dict)
    """(product_id, location_id) -> current stock"""

    emitted: int = 0

    @property
    def gross_revenue(self) -> Decimal:
        return sum(self.orders.values(), start=Decimal("0.00"))

    @property
    def total_refunded(self) -> Decimal:
        return sum(self.refunded.values(), start=Decimal("0.00"))

    @property
    def net_revenue(self) -> Decimal:
        """Ground truth for the net-revenue reconciliation check."""
        return self.gross_revenue - self.total_refunded


class EventGenerator:
    """Emits reproducible, referentially consistent event envelopes."""

    def __init__(
        self,
        config: GeneratorConfig | None = None,
        *,
        start_time: datetime | None = None,
    ) -> None:
        self.config = config or GeneratorConfig()
        # One seeded RNG, never the module-level functions, so that two
        # generators with the same seed cannot diverge because unrelated code
        # happened to call random.random().
        self._rng = random.Random(self.config.seed)
        self._clock = start_time or datetime(2026, 11, 1, 0, 0, 0, tzinfo=UTC)
        if self._clock.tzinfo is None:
            msg = "start_time must be timezone-aware"
            raise ValueError(msg)
        self.state = WorldState()
        self._sequence = 0
        self._customers = [f"cus-{i:05d}" for i in range(self.config.customer_count)]
        self._products = [f"sku-{i:05d}" for i in range(self.config.product_count)]
        self._locations = [f"loc-{i:02d}" for i in range(self.config.location_count)]
        self._prices = {
            product: Decimal(str(self._rng.randrange(199, 49999))) / 100
            for product in self._products
        }
        # Selection indexes. Picking "an unpaid order" or "a refundable
        # payment" by filtering the whole world on every event is O(n) per
        # call and O(n^2) per run, which made the generator unusable past a
        # few thousand events. These keep both choices O(1): a list for
        # random access plus a position map so removal is a swap with the
        # last element instead of a scan.
        self._unpaid: list[str] = []
        self._unpaid_at: dict[str, int] = {}
        self._refundable: list[str] = []
        self._refundable_at: dict[str, int] = {}
        # order_id -> customer_id, so a payment credits the customer who
        # actually placed the order rather than an unrelated one.
        self._order_customer: dict[str, str] = {}

    # -- public API ---------------------------------------------------------

    def generate(self, count: int) -> list[EventEnvelope]:
        """Emit exactly ``count`` envelopes."""
        if count < 0:
            msg = "count must be non-negative"
            raise ValueError(msg)
        return list(self.stream(limit=count))

    def stream(self, *, limit: int | None = None) -> Iterator[EventEnvelope]:
        """Yield envelopes until ``limit`` is reached, or forever if ``None``."""
        produced = 0
        while limit is None or produced < limit:
            envelope = self._next_event()
            if envelope is None:
                # The chosen type had no valid antecedent yet (e.g. a refund
                # before any payment). Advance and retry rather than emitting a
                # dangling reference.
                continue
            produced += 1
            self.state.emitted += 1
            yield envelope

    # -- event construction -------------------------------------------------

    def _next_event(self) -> EventEnvelope | None:
        self._advance_clock()
        choice = self._rng.randrange(self.config.total_weight)
        cfg = self.config

        if choice < cfg.order_weight:
            return self._order()
        choice -= cfg.order_weight

        if choice < cfg.payment_weight:
            return self._payment()
        choice -= cfg.payment_weight

        if choice < cfg.refund_weight:
            return self._refund()
        choice -= cfg.refund_weight

        if choice < cfg.customer_weight:
            return self._customer()
        return self._inventory()

    def _order(self) -> EventEnvelope:
        order_id = f"ord-{self._next_sequence():09d}"
        customer_id = self._rng.choice(self._customers)
        line_count = self._rng.randint(1, self.config.max_lines_per_order)

        lines: list[OrderLine] = []
        for _ in range(line_count):
            product_id = self._rng.choice(self._products)
            lines.append(
                OrderLine(
                    product_id=product_id,
                    quantity=self._rng.randint(1, 4),
                    unit_price=self._prices[product_id],
                )
            )
        # Derived, never guessed: the contract requires the header to agree.
        gross = sum((line.line_total for line in lines), start=Decimal("0.00"))

        event_time = self._event_time()
        payload: OrderPlacedV1 | OrderPlacedV2
        if self._rng.random() < self.config.v1_order_rate:
            # Older producers still on v1. The consumer must read these via
            # the registry's backward-compatibility path.
            payload = OrderPlacedV1(
                order_id=order_id,
                customer_id=customer_id,
                order_timestamp=event_time,
                currency=Currency.USD,
                lines=lines,
                gross_amount=gross,
            )
            version = 1
        else:
            payload = OrderPlacedV2(
                order_id=order_id,
                customer_id=customer_id,
                order_timestamp=event_time,
                currency=Currency.USD,
                lines=lines,
                gross_amount=gross,
                channel=self._rng.choice(list(SalesChannel)),
            )
            version = 2

        self.state.orders[order_id] = gross
        self._order_customer[order_id] = customer_id
        self._track_unpaid(order_id)
        return self._envelope(payload, entity_id=order_id, event_time=event_time, version=version)

    def _payment(self) -> EventEnvelope | None:
        order_id = self._take_recent(self._unpaid, self._unpaid_at)
        if order_id is None:
            return None

        payment_id = f"pay-{self._next_sequence():09d}"
        amount = self.state.orders[order_id]
        event_time = self._event_time()

        payload = PaymentCapturedV1(
            payment_id=payment_id,
            order_id=order_id,
            customer_id=self._order_customer[order_id],
            captured_at=event_time,
            currency=Currency.USD,
            status=PaymentStatus.CAPTURED,
            amount=amount,
        )
        self.state.payments[payment_id] = (order_id, amount)
        if amount > Decimal("0.01"):
            self._track_refundable(payment_id)
        return self._envelope(payload, entity_id=payment_id, event_time=event_time)

    def _refund(self) -> EventEnvelope | None:
        payment_id = self._peek_recent(self._refundable)
        if payment_id is None:
            return None

        order_id, captured = self.state.payments[payment_id]
        already = self.state.refunded.get(payment_id, Decimal("0.00"))
        remaining = captured - already
        if remaining <= Decimal("0.01"):
            self._drop(payment_id, self._refundable, self._refundable_at)
            return None

        # Partial or full, but never more than was captured. Over-refunding is
        # a failure the Failure Lab injects on purpose, not an accident here.
        fraction = Decimal(self._rng.choice(["0.25", "0.50", "1.00"]))
        amount = (remaining * fraction).quantize(Decimal("0.01"))
        if amount <= Decimal("0.00"):
            return None

        refund_id = f"ref-{self._next_sequence():09d}"
        event_time = self._event_time()
        payload = RefundIssuedV1(
            refund_id=refund_id,
            payment_id=payment_id,
            order_id=order_id,
            refunded_at=event_time,
            currency=Currency.USD,
            amount=amount,
            reason=self._rng.choice(_REFUND_REASONS),
        )
        self.state.refunded[payment_id] = already + amount
        if captured - (already + amount) <= Decimal("0.01"):
            self._drop(payment_id, self._refundable, self._refundable_at)
        return self._envelope(payload, entity_id=refund_id, event_time=event_time)

    def _customer(self) -> EventEnvelope:
        customer_id = self._rng.choice(self._customers)
        event_time = self._event_time()
        suffix = "".join(self._rng.choices(string.ascii_lowercase, k=6))
        payload = CustomerUpdatedV1(
            customer_id=customer_id,
            updated_at=event_time,
            email=f"{customer_id}.{suffix}@example.invalid",
            country=self._rng.choice(_COUNTRIES),
            signup_date=(event_time - timedelta(days=self._rng.randrange(1, 900))).date(),
            segment=self._rng.choice(_SEGMENTS),
        )
        return self._envelope(payload, entity_id=customer_id, event_time=event_time)

    def _inventory(self) -> EventEnvelope:
        product_id = self._rng.choice(self._products)
        location_id = self._rng.choice(self._locations)
        key = (product_id, location_id)
        current = self.state.on_hand.get(key, 100)

        # Never let stock go negative: on_hand_after has a CHECK (>= 0), and a
        # generator that violates the schema teaches nothing.
        delta = self._rng.randint(-min(current, 20), 30) or 1
        after = max(current + delta, 0)
        self.state.on_hand[key] = after

        event_time = self._event_time()
        movement_id = f"mov-{self._next_sequence():09d}"
        payload = InventoryUpdatedV1(
            movement_id=movement_id,
            product_id=product_id,
            location_id=location_id,
            moved_at=event_time,
            quantity_delta=delta,
            on_hand_after=after,
        )
        return self._envelope(payload, entity_id=movement_id, event_time=event_time)

    # -- helpers ------------------------------------------------------------

    def _envelope(
        self,
        payload: EventPayload,
        *,
        entity_id: str,
        event_time: datetime,
        version: int | None = None,
    ) -> EventEnvelope:
        return EventEnvelope(
            event_id=f"evt-{self._next_sequence():012d}",
            event_type=EventType(payload.event_type),
            schema_version=version or payload.schema_version,
            entity_id=entity_id,
            event_timestamp=event_time,
            ingestion_timestamp=self._clock,
            producer="event-generator",
            payload=payload.model_dump(mode="json"),
        )

    # -- O(1) selection indexes ---------------------------------------------

    def _track_unpaid(self, order_id: str) -> None:
        self._unpaid_at[order_id] = len(self._unpaid)
        self._unpaid.append(order_id)

    def _track_refundable(self, payment_id: str) -> None:
        self._refundable_at[payment_id] = len(self._refundable)
        self._refundable.append(payment_id)

    def _pick_recent_index(self, pool: list[str]) -> int | None:
        """Index of a randomly chosen member of the pool's recent tail.

        Biased toward recent entries because a payment usually follows its
        order closely, and because an unbounded window would make the choice
        depend on total stream length.
        """
        size = len(pool)
        if size == 0:
            return None
        window = min(self.config.recency_window, size)
        return size - 1 - self._rng.randrange(window)

    def _peek_recent(self, pool: list[str]) -> str | None:
        """Choose from the recent tail without removing."""
        index = self._pick_recent_index(pool)
        return None if index is None else pool[index]

    def _take_recent(self, pool: list[str], positions: dict[str, int]) -> str | None:
        """Choose from the recent tail and remove it from the pool."""
        index = self._pick_recent_index(pool)
        if index is None:
            return None
        chosen = pool[index]
        self._remove_at(index, pool, positions)
        return chosen

    def _drop(self, member: str, pool: list[str], positions: dict[str, int]) -> None:
        index = positions.get(member)
        if index is not None:
            self._remove_at(index, pool, positions)

    @staticmethod
    def _remove_at(index: int, pool: list[str], positions: dict[str, int]) -> None:
        """Remove by swapping the last element into the gap.

        O(1) rather than O(n): list.remove would scan, and popping from the
        middle would shift every later element and invalidate the position map.
        """
        last = pool[-1]
        removed = pool[index]
        pool[index] = last
        positions[last] = index
        pool.pop()
        positions.pop(removed, None)
        if removed == last:
            positions.pop(last, None)

    def _advance_clock(self) -> None:
        """Move processing time forward by a small, seeded amount."""
        self._clock += timedelta(milliseconds=self._rng.randrange(1, 50))

    def _event_time(self) -> datetime:
        """Event time, usually ~now, occasionally slightly behind."""
        if self._rng.random() < self.config.natural_lateness_rate:
            lag = self._rng.randrange(1, self.config.max_natural_lateness_seconds)
            return self._clock - timedelta(seconds=lag)
        return self._clock

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence
