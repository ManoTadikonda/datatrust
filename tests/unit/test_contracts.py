"""Contract, envelope, and compatibility behavior."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from datatrust.contracts import (
    Compatibility,
    ContractRegistry,
    EventEnvelope,
    EventType,
    OrderPlacedV1,
    OrderPlacedV2,
    PaymentCapturedV1,
    idempotency_key,
)
from datatrust.errors import ContractViolationError, SchemaIncompatibilityError

pytestmark = pytest.mark.unit


class TestEnvelope:
    def test_naive_timestamps_are_rejected(self, order_payload: dict[str, Any]) -> None:
        """Mixing naive and aware time makes lateness policy meaningless."""
        with pytest.raises(ValueError, match="timezone-aware"):
            EventEnvelope(
                event_id="evt-1",
                event_type=EventType.ORDER,
                schema_version=1,
                entity_id="ord-1001",
                event_timestamp=datetime(2026, 11, 23, 12, 0, 0),
                ingestion_timestamp=datetime.now(UTC),
                payload=order_payload,
            )

    def test_timestamps_normalize_to_utc(self, make_envelope: Any, order_payload: dict) -> None:
        envelope = make_envelope(order_payload)
        assert envelope.event_timestamp.tzinfo is UTC
        assert envelope.ingestion_timestamp.tzinfo is UTC

    def test_lateness_is_ingestion_minus_event_time(
        self, make_envelope: Any, order_payload: dict
    ) -> None:
        envelope = make_envelope(order_payload, lateness=timedelta(minutes=90))
        assert envelope.lateness_seconds == pytest.approx(5400.0)

    def test_unknown_envelope_fields_are_rejected(self, order_payload: dict) -> None:
        """extra='forbid' is what turns a producer typo into a visible failure."""
        with pytest.raises(ValueError, match=r"extra_forbidden|Extra inputs"):
            EventEnvelope(
                event_id="evt-1",
                event_type=EventType.ORDER,
                schema_version=1,
                entity_id="ord-1001",
                event_timestamp=datetime.now(UTC),
                ingestion_timestamp=datetime.now(UTC),
                payload=order_payload,
                surprise="boo",
            )


class TestIdempotencyKey:
    def test_is_deterministic(self) -> None:
        a = idempotency_key(EventType.ORDER, "ord-1", "evt-1")
        b = idempotency_key(EventType.ORDER, "ord-1", "evt-1")
        assert a == b

    def test_accepts_str_or_enum(self) -> None:
        assert idempotency_key("orders", "ord-1", "evt-1") == idempotency_key(
            EventType.ORDER, "ord-1", "evt-1"
        )

    def test_differs_across_event_types(self) -> None:
        assert idempotency_key(EventType.ORDER, "x", "y") != idempotency_key(
            EventType.PAYMENT, "x", "y"
        )

    def test_is_not_vulnerable_to_delimiter_collision(self) -> None:
        """('a|b', 'c') and ('a', 'b|c') must not hash to the same key."""
        assert idempotency_key(EventType.ORDER, "a|b", "c") != idempotency_key(
            EventType.ORDER, "a", "b|c"
        )

    def test_is_fixed_width(self) -> None:
        key = idempotency_key(EventType.ORDER, "x" * 64, "y" * 64)
        assert len(key) == 64


class TestOrderContract:
    def test_valid_payload_parses(self, order_payload: dict) -> None:
        order = OrderPlacedV1.model_validate(order_payload)
        assert order.gross_amount == Decimal("44.98")
        assert order.business_key() == "ord-1001"

    def test_header_must_reconcile_with_lines(self, order_payload: dict) -> None:
        """An order disagreeing with its own lines is malformed at the source."""
        order_payload["gross_amount"] = "99.00"
        with pytest.raises(ValueError, match=r"gross_amount .* != sum\(lines\)"):
            OrderPlacedV1.model_validate(order_payload)

    def test_empty_line_list_is_rejected(self, order_payload: dict) -> None:
        order_payload["lines"] = []
        order_payload["gross_amount"] = "0.00"
        with pytest.raises(ValueError, match=r"too_short|at least 1"):
            OrderPlacedV1.model_validate(order_payload)

    def test_money_keeps_two_decimal_places(self, order_payload: dict) -> None:
        order = OrderPlacedV1.model_validate(order_payload)
        assert order.lines[0].line_total == Decimal("39.98")

    def test_payload_is_frozen(self, order_payload: dict) -> None:
        """Raw evidence must stay immutable once parsed."""
        order = OrderPlacedV1.model_validate(order_payload)
        with pytest.raises(ValueError, match=r"frozen|Instance is frozen"):
            order.order_id = "tampered"


class TestSchemaEvolution:
    def test_v1_payload_is_readable_as_v2(self, order_payload: dict) -> None:
        """The additive change in v2 is what makes it backward compatible."""
        promoted = {**order_payload, "schema_version": 2}
        order = OrderPlacedV2.model_validate(promoted)
        assert order.channel.value == "web"

    def test_v2_is_not_a_subclass_of_v1(self) -> None:
        """Subclassing would hide version confusion behind isinstance()."""
        assert not issubclass(OrderPlacedV2, OrderPlacedV1)

    def test_v1_against_latest_is_backward_compatible(self, registry: ContractRegistry) -> None:
        assert registry.compatibility(EventType.ORDER, 1) is Compatibility.BACKWARD

    def test_latest_against_itself_is_full(self, registry: ContractRegistry) -> None:
        assert registry.compatibility(EventType.ORDER, 2) is Compatibility.FULL

    def test_unregistered_version_is_unknown(self, registry: ContractRegistry) -> None:
        assert registry.compatibility(EventType.ORDER, 9) is Compatibility.UNKNOWN

    def test_single_version_type_is_full(self, registry: ContractRegistry) -> None:
        assert registry.compatibility(EventType.PAYMENT, 1) is Compatibility.FULL

    def test_version_markers_do_not_count_as_breaking_changes(
        self, registry: ContractRegistry
    ) -> None:
        """event_type/schema_version differ by construction between versions."""
        from datatrust.contracts.registry import field_specs

        assert "schema_version" not in field_specs(OrderPlacedV1)
        assert "schema_version" in field_specs(OrderPlacedV1, include_markers=True)


class TestRegistry:
    def test_duplicate_registration_is_rejected(self) -> None:
        registry = ContractRegistry()
        registry.register(PaymentCapturedV1)
        registry.register(PaymentCapturedV1)  # same object is a no-op

        class Impostor(PaymentCapturedV1):  # type: ignore[misc]
            pass

        with pytest.raises(ValueError, match="already registered"):
            registry.register(Impostor)

    def test_unknown_version_raises_schema_incompatibility(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        envelope = make_envelope(order_payload, schema_version=7)
        with pytest.raises(SchemaIncompatibilityError, match="no schema registered"):
            registry.validate(envelope)

    def test_validate_returns_typed_payload(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        payload = registry.validate(make_envelope(order_payload))
        assert isinstance(payload, OrderPlacedV1)

    def test_contract_violation_carries_field_evidence(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        del order_payload["customer_id"]
        with pytest.raises(ContractViolationError) as caught:
            registry.validate(make_envelope(order_payload))
        assert caught.value.field == "customer_id"
        assert caught.value.failures  # type: ignore[attr-defined]

    def test_collect_failures_reports_every_broken_field(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        """Operators need all the broken fields, not just the first."""
        del order_payload["customer_id"]
        del order_payload["order_timestamp"]
        failures = registry.collect_failures(make_envelope(order_payload))
        paths = {failure.field_path for failure in failures}
        assert {"customer_id", "order_timestamp"} <= paths

    def test_collect_failures_reports_unknown_schema_as_evidence(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        failures = registry.collect_failures(make_envelope(order_payload, schema_version=7))
        assert len(failures) == 1
        assert failures[0].error_type == "schema_incompatibility"

    def test_valid_payload_collects_no_failures(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        assert registry.collect_failures(make_envelope(order_payload)) == []

    def test_evidence_is_truncated(
        self, registry: ContractRegistry, make_envelope: Any, order_payload: dict
    ) -> None:
        """Quarantine rows must not become the biggest thing in the database."""
        order_payload["customer_id"] = "x" * 5000
        failures = registry.collect_failures(make_envelope(order_payload))
        assert failures
        assert all(len(str(f.observed or "")) <= 200 for f in failures)
