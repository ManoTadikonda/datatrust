"""Generator determinism and referential consistency.

These properties are load-bearing: scenario replay and benchmark comparison
both assume the same seed yields the same stream, and the referential integrity
check only means something if the generator never emits a dangling reference.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datatrust.contracts import EventType, default_registry
from datatrust.simulation import EventGenerator, GeneratorConfig

pytestmark = pytest.mark.unit

START = datetime(2026, 11, 1, tzinfo=UTC)


def _gen(seed: int = 42, **kwargs: object) -> EventGenerator:
    return EventGenerator(GeneratorConfig(seed=seed, **kwargs), start_time=START)  # type: ignore[arg-type]


class TestDeterminism:
    def test_same_seed_produces_identical_streams(self) -> None:
        a = _gen().generate(400)
        b = _gen().generate(400)
        assert [e.model_dump(mode="json") for e in a] == [e.model_dump(mode="json") for e in b]

    def test_different_seeds_diverge(self) -> None:
        """Different seeds must not produce the same stream.

        Event ids are not comparable across seeds: the sequence counter is
        shared with entity ids, and a seed that picks a different event mix
        (or skips a refund with no eligible payment) consumes a different
        number of sequence values.
        """
        a = _gen(seed=1).generate(200)
        b = _gen(seed=2).generate(200)
        assert [e.payload for e in a] != [e.payload for e in b]
        assert [e.event_type for e in a] != [e.event_type for e in b]

    def test_does_not_touch_the_global_rng(self) -> None:
        """A generator that used random.random() would not be reproducible."""
        import random

        random.seed(7)
        before = random.random()
        _gen().generate(100)
        random.seed(7)
        assert random.random() == before

    def test_ground_truth_totals_are_reproducible(self) -> None:
        a, b = _gen(), _gen()
        a.generate(500)
        b.generate(500)
        assert a.state.net_revenue == b.state.net_revenue


class TestIdentity:
    def test_event_ids_are_unique(self) -> None:
        events = _gen().generate(3000)
        assert len({e.event_id for e in events}) == len(events)

    def test_idempotency_keys_are_unique(self) -> None:
        """The generator must not emit duplicates; scenario 1 injects those."""
        events = _gen().generate(3000)
        assert len({e.idempotency_key for e in events}) == len(events)


class TestReferentialConsistency:
    def test_payments_reference_known_orders(self) -> None:
        gen = _gen()
        events = gen.generate(3000)
        order_ids = set(gen.state.orders)
        for event in events:
            if event.event_type is EventType.PAYMENT:
                assert event.payload["order_id"] in order_ids

    def test_refunds_reference_known_payments(self) -> None:
        gen = _gen()
        events = gen.generate(3000)
        payment_ids = set(gen.state.payments)
        for event in events:
            if event.event_type is EventType.REFUND:
                assert event.payload["payment_id"] in payment_ids

    def test_refunds_never_exceed_the_captured_amount(self) -> None:
        """Over-refunding is scenario 7's job, not an accident of the generator."""
        gen = _gen()
        gen.generate(4000)
        for payment_id, refunded in gen.state.refunded.items():
            _, captured = gen.state.payments[payment_id]
            assert refunded <= captured

    def test_stock_never_goes_negative(self) -> None:
        gen = _gen()
        gen.generate(4000)
        assert all(quantity >= 0 for quantity in gen.state.on_hand.values())


class TestContractConformance:
    def test_every_generated_event_satisfies_its_contract(self) -> None:
        """If the generator emitted invalid events, quarantine would be noise."""
        events = _gen().generate(2000)
        for event in events:
            assert default_registry.collect_failures(event) == [], event.event_id

    def test_order_headers_reconcile_with_their_lines(self) -> None:
        events = _gen().generate(2000)
        for event in events:
            if event.event_type is not EventType.ORDER:
                continue
            lines = event.payload["lines"]
            expected = sum(
                (Decimal(line["unit_price"]) * line["quantity"] for line in lines),
                start=Decimal("0.00"),
            ).quantize(Decimal("0.01"))
            assert Decimal(event.payload["gross_amount"]) == expected


class TestEventTime:
    def test_most_events_are_not_late(self) -> None:
        events = _gen(natural_lateness_rate=0.05).generate(2000)
        late = sum(1 for e in events if e.lateness_seconds > 0)
        assert 0 < late < len(events) * 0.15

    def test_lateness_can_be_disabled(self) -> None:
        events = _gen(natural_lateness_rate=0.0).generate(500)
        assert all(e.lateness_seconds == 0 for e in events)

    def test_ingestion_time_is_monotonic(self) -> None:
        events = _gen().generate(1000)
        stamps = [e.ingestion_timestamp for e in events]
        assert stamps == sorted(stamps)


class TestConfigValidation:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"customer_count": 0}, "must be positive"),
            ({"product_count": 0}, "must be positive"),
            ({"natural_lateness_rate": 1.5}, "fraction between 0 and 1"),
        ],
    )
    def test_invalid_config_is_rejected(self, kwargs: dict, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            GeneratorConfig(**kwargs)

    def test_all_zero_weights_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one event weight"):
            GeneratorConfig(
                order_weight=0,
                payment_weight=0,
                refund_weight=0,
                customer_weight=0,
                inventory_weight=0,
            )

    def test_naive_start_time_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            EventGenerator(start_time=datetime(2026, 11, 1))

    def test_negative_count_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            _gen().generate(-1)


class TestProperties:
    @settings(max_examples=25, deadline=None)
    @given(seed=st.integers(min_value=0, max_value=2**31 - 1), count=st.integers(1, 200))
    def test_any_seed_yields_contract_valid_events(self, seed: int, count: int) -> None:
        for event in _gen(seed=seed).generate(count):
            assert default_registry.collect_failures(event) == []

    @settings(max_examples=15, deadline=None)
    @given(seed=st.integers(min_value=0, max_value=2**31 - 1))
    def test_net_revenue_never_exceeds_gross(self, seed: int) -> None:
        gen = _gen(seed=seed)
        gen.generate(300)
        assert gen.state.net_revenue <= gen.state.gross_revenue

    @settings(max_examples=15, deadline=None)
    @given(count=st.integers(1, 300))
    def test_generate_emits_exactly_the_requested_count(self, count: int) -> None:
        assert len(_gen().generate(count)) == count


def test_event_mix_roughly_follows_weights() -> None:
    """Orders should dominate and refunds should stay rare."""
    events = _gen().generate(5000)
    mix = Counter(e.event_type for e in events)
    assert mix[EventType.ORDER] > mix[EventType.PAYMENT] > mix[EventType.REFUND]
    assert mix[EventType.REFUND] / len(events) < 0.05


class TestSchemaVersionMix:
    """A stream carrying only the newest version never exercises the
    consumer's backward-compatibility path, which is the path most likely to
    rot unnoticed."""

    def test_both_order_versions_are_emitted(self) -> None:
        versions = {
            e.schema_version for e in _gen().generate(2000) if e.event_type is EventType.ORDER
        }
        assert versions == {1, 2}

    def test_v1_orders_carry_no_channel_field(self) -> None:
        for event in _gen().generate(2000):
            if event.event_type is EventType.ORDER and event.schema_version == 1:
                assert "channel" not in event.payload

    def test_v2_orders_carry_a_channel(self) -> None:
        seen = False
        for event in _gen().generate(2000):
            if event.event_type is EventType.ORDER and event.schema_version == 2:
                assert event.payload["channel"]
                seen = True
        assert seen

    def test_the_mix_can_be_turned_off(self) -> None:
        versions = {
            e.schema_version
            for e in _gen(v1_order_rate=0.0).generate(800)
            if e.event_type is EventType.ORDER
        }
        assert versions == {2}

    def test_invalid_v1_rate_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="v1_order_rate must be a fraction"):
            GeneratorConfig(v1_order_rate=2.0)


class TestPaymentAttribution:
    def test_payment_credits_the_customer_who_placed_the_order(self) -> None:
        """A payment attributed to an unrelated customer would corrupt every
        per-customer aggregate while passing referential integrity checks."""
        gen = _gen()
        events = gen.generate(4000)
        orders = {
            e.payload["order_id"]: e.payload["customer_id"]
            for e in events
            if e.event_type is EventType.ORDER
        }
        payments = [e for e in events if e.event_type is EventType.PAYMENT]
        assert payments
        for payment in payments:
            assert payment.payload["customer_id"] == orders[payment.payload["order_id"]]

    def test_each_order_is_paid_at_most_once(self) -> None:
        events = _gen().generate(6000)
        paid = [e.payload["order_id"] for e in events if e.event_type is EventType.PAYMENT]
        assert len(paid) == len(set(paid))


class TestSelectionIndexInvariants:
    """The O(1) indexes are only safe if their position maps stay in sync with
    their lists; a stale index would hand out a wrong or missing id."""

    def test_position_maps_stay_consistent(self) -> None:
        gen = _gen()
        gen.generate(5000)
        assert len(gen._unpaid) == len(gen._unpaid_at)
        assert all(gen._unpaid[i] == oid for oid, i in gen._unpaid_at.items())
        assert len(gen._refundable) == len(gen._refundable_at)
        assert all(gen._refundable[i] == pid for pid, i in gen._refundable_at.items())

    def test_fully_refunded_payments_leave_the_pool(self) -> None:
        gen = _gen()
        gen.generate(5000)
        for payment_id in gen._refundable:
            _, captured = gen.state.payments[payment_id]
            already = gen.state.refunded.get(payment_id, Decimal("0.00"))
            assert captured - already > Decimal("0.01")

    def test_unpaid_pool_holds_only_unpaid_orders(self) -> None:
        gen = _gen()
        gen.generate(5000)
        paid = {order_id for order_id, _ in gen.state.payments.values()}
        assert not (set(gen._unpaid) & paid)


@pytest.mark.performance
class TestThroughput:
    def test_generation_is_linear_not_quadratic(self) -> None:
        """Regression guard for the O(n^2) selection this replaced.

        Asserts only that per-event cost does not blow up with stream length,
        not an absolute rate, so the test does not fail on a slow CI runner.
        """
        import time

        def rate(count: int) -> float:
            gen = _gen()
            started = time.perf_counter()
            gen.generate(count)
            return count / (time.perf_counter() - started)

        small = rate(2_000)
        large = rate(50_000)
        # Before the fix this ratio was ~50x. Linear work keeps it near 1.
        assert small / large < 4.0, f"throughput degraded {small / large:.1f}x with scale"
