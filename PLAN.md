# DataTrust — Implementation Plan

> A fault-tolerant data reliability platform with automated detection,
> quarantine, recovery, and verification.

This document is the design contract for the build. `PROGRESS.md` tracks what
has actually been implemented and verified against it.

---

## 1. Problem statement

Analytical data breaks quietly. A producer ships a schema change, a consumer
is restarted mid-batch, a payment event arrives twice, a derived partition is
overwritten with a bad backfill. The data keeps flowing and the dashboards keep
rendering — they are simply wrong, and nobody finds out until someone
reconciles revenue by hand.

DataTrust treats data failures the way SRE treats service failures: detect
them, page on them, scope their blast radius, repair what is safely repairable,
and *prove* the repair worked before declaring the data trustworthy again.

## 2. Non-goals

- Not a general-purpose ETL framework or an Airflow replacement.
- Not a multi-tenant hosted service. Single-node, local-first, laptop-sized.
- Not field-level lineage. Dataset-level lineage with explicit edges only.
- Not a Kafka exactly-once showcase. We implement *exactly-once effects*
  through idempotent writes, which is a weaker and more honest claim.

## 3. Architecture

```
                         ┌──────────────────────────┐
                         │  Synthetic Event Gen     │  deterministic seeds
                         └────────────┬─────────────┘
                                      │ produce
                         ┌────────────▼─────────────┐
                         │  Kafka topics (KRaft)    │  orders, payments,
                         │  partitioned by entity   │  refunds, customers,
                         └────────────┬─────────────┘  inventory
                                      │ consume (batch, offset-aware)
                         ┌────────────▼─────────────┐
                         │  Ingestion worker        │
                         │  - contract validation   │
                         │  - idempotency keys      │
                         └───┬──────────────────┬───┘
                   valid     │                  │ invalid
                         ┌───▼───────┐    ┌─────▼─────────┐
                         │ raw_events│    │ quarantine    │
                         │ (PG+parq) │    │ (PG + parquet)│
                         └───┬───────┘    └─────┬─────────┘
                             │                  │
                   ┌─────────▼────────┐   ┌─────▼─────────┐
                   │ Quality engine   │──▶│ Incident mgmt │
                   │ 10 check classes │   │ state machine │
                   └─────────┬────────┘   └─────┬─────────┘
                             │                  │ eligible
                   ┌─────────▼────────┐   ┌─────▼─────────┐
                   │ SQL transforms   │   │ Recovery eng. │
                   │ facts + dims     │◀──│ replay/rebuild│
                   └─────────┬────────┘   └─────┬─────────┘
                             │                  │
                   ┌─────────▼────────┐   ┌─────▼─────────┐
                   │ Curated / trusted│   │ Verification  │
                   │ analytics tables │◀──│ reconciliation│
                   └─────────┬────────┘   └───────────────┘
                             │
                   ┌─────────▼────────────────────────────┐
                   │ Lineage graph + trust status         │
                   │ Prometheus metrics → Grafana         │
                   │ Streamlit dashboard + Failure Lab    │
                   └──────────────────────────────────────┘
```

### Layering rule

Domain logic (`contracts`, `quality`, `incidents`, `recovery`, `lineage`) must
not import infrastructure (`psycopg`, `confluent_kafka`, `pyspark`,
`streamlit`). Infrastructure adapters live in `storage/`, `ingestion/`, and
`processing/spark/`. This is enforced by an import-linter test in
`tests/unit/test_layering.py`.

## 4. Correctness model

The hard problem is the window between "Kafka offset committed" and "Postgres
transaction committed". We do not get a distributed transaction across the two.
Our chosen posture:

1. **Process → persist → then commit offsets.** Offsets are committed only
   after the batch's effects are durable in Postgres. A crash in the window
   therefore causes *reprocessing*, never data loss. Delivery semantics are
   **at-least-once from Kafka**.
2. **Idempotent writes make reprocessing harmless.** Every event carries a
   deterministic `idempotency_key` derived from
   `(event_type, entity_id, event_id)`. Fact tables declare a unique
   constraint on that key, and all inserts use `ON CONFLICT DO NOTHING`
   (or `DO UPDATE` where last-write-wins is correct, e.g. dimensions).
   Replaying an event cannot increase its financial effect.
3. **Raw evidence is immutable.** `raw_events` is append-only. Corrections
   never mutate raw rows; they create new curated rows with provenance
   pointing at the recovery run that produced them.
4. **Resolution requires verification.** An incident moves to `RESOLVED` only
   when an independent reconciliation query passes. A finished recovery job
   with a failing check lands in `FAILED` or `MANUAL_REVIEW`.

Claim we make: **exactly-once effects on analytical outputs**, achieved by
at-least-once delivery plus idempotent application. We do *not* claim Kafka
exactly-once transactional semantics.

## 5. Incident state machine

```
DETECTED ──▶ TRIAGED ──▶ QUARANTINED ──▶ RECOVERING ──▶ VERIFYING ──▶ RESOLVED
    │            │            │               │             │
    │            │            │               ▼             ▼
    │            └────────────┴──────────▶ FAILED      MANUAL_REVIEW
    └──────────────────────────────────────────┘
```

Transitions are validated in code; every transition writes an audit row with
actor, reason, and timestamp. Illegal transitions raise rather than warn.

## 6. Failure Lab — the differentiator

Eight reproducible scenarios, each with a trigger, an expected behavior, and an
automated verifier that returns pass/fail from real system state:

| # | Scenario | Expected | Recovery |
|---|----------|----------|----------|
| 1 | Duplicate events | dedup on idempotency key | automatic, idempotent |
| 2 | Schema drift | incompatible version quarantined | manual review |
| 3 | Missing required fields | quarantined with field-level evidence | manual review |
| 4 | Late-arriving data | lateness policy applied, aggregates updated | automatic rebuild |
| 5 | Consumer interruption | resume from committed offset, no dup effects | automatic replay |
| 6 | Processing failure | failed batch recorded, bounded retry | automatic retry |
| 7 | Financial reconciliation | discrepancy detected | MANUAL_REVIEW (never auto-fix) |
| 8 | Downstream corruption | reconciliation fails, partition rebuilt | automatic rebuild |

Scenario results are never hardcoded. Each verifier queries Postgres/Parquet
and reports what it actually found.

## 7. Analytical model

Grain and keys are documented in `docs/architecture/data-model.md`.

- `fact_orders` — one row per order event, grain: `order_id`
- `fact_payments` — one row per payment, grain: `payment_id`
- `fact_refunds` — one row per refund, grain: `refund_id`
- `fact_inventory_movements` — grain: `(product_id, movement_id)`
- `dim_customers` — SCD-1, grain: `customer_id`
- `dim_products` — SCD-1, grain: `product_id`
- `dim_date` — grain: `date_day`

Transformations use CTEs, window functions, and incremental aggregation keyed
on a high-water mark. Each curated table carries a `trust_status` derived from
open incidents on its lineage ancestors.

## 8. Phases and acceptance criteria

| Phase | Scope | Acceptance |
|-------|-------|------------|
| 1 | Skeleton, config, logging, Compose, migrations, CI | services pass health checks |
| 2 | Generator, produce/consume, raw + curated | events flow, SQL results correct |
| 3 | Contracts, quality checks, quarantine, incidents | bad events isolated, trusted tables clean |
| 4 | Recovery engine, verification | repair without duplicate effects |
| 5 | Failure Lab, 8 scenarios + tests | every scenario triggerable and inspectable |
| 6 | Lineage, impact analysis, metrics, dashboard | blast radius and trust visible |
| 7 | PySpark, Airflow | real work, observable + recoverable failures |
| 8 | Load generation, benchmarks | measured numbers with hardware context |
| 9 | README, diagrams, interview guide | clone → demo → understand, unaided |

## 9. Resource posture

The target machine is a personal laptop (this build: Apple Silicon, 10 cores,
16 GB RAM). The Compose stack is deliberately small: single-broker Kafka in
KRaft mode (no ZooKeeper), one Postgres, one worker, one dashboard. Prometheus,
Grafana, Spark, and Airflow sit behind Compose profiles so they are opt-in.

A `lightweight` profile swaps Kafka for an embedded file-backed log and runs
the entire reliability loop in a single process, for machines that cannot spare
the RAM. Core detection/quarantine/recovery/verification behavior is identical
in both profiles and is covered by the same tests.

## 10. Decision log

Material decisions are recorded in `DESIGN_DECISIONS.md` with context,
alternatives, and consequences.
