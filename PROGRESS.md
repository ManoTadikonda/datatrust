# DataTrust — Progress

Live build log. Every claim here is backed by a command that was actually run.
Anything authored but not executed is listed under **Not yet verified**.

Last updated: 2026-11-23

---

## Environment

| Item | Value |
|------|-------|
| Host | macOS (Darwin 25.5.0), Apple Silicon, 10 cores, 16 GB RAM |
| Python | 3.12.15 via `uv` (host system Python is 3.9.6 and is not used) |
| Docker | Docker Desktop 29.9.2 — **first-run setup not completed; daemon down** |
| Java | not installed on host (Spark would run in a container) |

## Status by phase

| Phase | Status |
|-------|--------|
| 1 — Foundation | partial: config/logging/schema authored, Compose and CI missing |
| 2 — Ingestion | generator done; Kafka and storage not started |
| 3 — Contracts & quality | contracts done; quality engine not started |
| 4 — Recovery | not started |
| 5 — Failure Lab | not started |
| 6 — Lineage & observability | not started |
| 7 — Spark & Airflow | not started |
| 8 — Performance | generator benchmarked only |
| 9 — Portfolio polish | not started |

## Completed and verified

Commands run, with results:

- `ruff check src tests` → **All checks passed**
- `ruff format --check src tests` → **clean**
- `mypy` → **no issues in 19 source files**
- `pytest tests/unit` → **65 passed in 1.90s**

Implemented:

- Versioned event contracts for all five domains, `OrderPlaced` at v1 and v2.
- Contract registry with backward/forward/full/breaking classification.
- Config with `docker` and `lightweight` profiles; structured logging; typed
  transient-vs-permanent error hierarchy.
- Three SQL migrations (authored, not executed — see below).
- Deterministic synthetic event generator, referentially consistent.

## Measured results

Generator throughput, this host, `seed=1`, after replacing O(n^2) selection
with O(1) indexes:

| Events | Before | After |
|--------|--------|-------|
| 1,000 | 6,877/s | 72,914/s |
| 4,000 | 312/s | 55,876/s |
| 16,000 | (did not finish) | 43,517/s |
| 64,000 | (did not finish) | 43,247/s |
| 256,000 | (did not finish) | 66,882/s |

The unit suite dropped from 80s to 1.9s as a side effect — it was slow for the
same reason.

## Bugs found by the test suite

1. **Idempotency key collision.** Keys were built by joining fields with `|`,
   which is ambiguous for ids containing `|`: `("a|b","c")` and `("a","b|c")`
   hashed identically, so one of two distinct events would be silently dropped
   as a duplicate. Now length-prefixed before hashing.
2. **Version markers broke compatibility checks.** The structural comparison
   counted `schema_version` itself as a differing field, so every pair of
   versions classified as `BREAKING`. Markers are now excluded.
3. **Payments were attributed to a random customer** rather than the one who
   placed the order, which would corrupt per-customer aggregates while still
   passing referential integrity checks.
4. **The generator never emitted v1 orders**, so the backward-compatibility
   path was never exercised by real data. Now mixed at `v1_order_rate`.

## Outstanding

Not started, in rough dependency order:

- `docker-compose.yml`, `Makefile`, `.dockerignore`, CI workflow, pre-commit
- Storage layer (psycopg) and Parquet writers
- Kafka producer/consumer, offset checkpointing, batch bookkeeping
- Quality engine (0 of 10 check classes)
- Incident state machine code (tables exist, logic does not)
- Recovery engine and independent verification
- Failure Lab (0 of 8 scenarios)
- Lineage graph, impact analysis, trust propagation
- Prometheus metrics, Streamlit dashboard, Grafana
- PySpark jobs, Airflow DAGs
- Benchmarks at 10k / 100k / 1M
- `ARCHITECTURE.md`, `DESIGN_DECISIONS.md`, interview guide, README rewrite
- `lightweight` profile is an enum value that nothing honors yet

## Not yet verified

- **No SQL has ever been executed.** All three migrations (~490 lines) are
  authored but unrun; syntax, triggers, and CHECK constraints are unconfirmed.
  Blocked on Docker Desktop first-run setup.
- No integration, end-to-end, or scenario tests exist — unit tests only.
- No service has been started.
- The `require_transition_audit` trigger matches an audit row within a
  5-second window. That is a heuristic, not a true invariant, and is
  race-prone. It needs replacing with a transition-id link.

## Resume command

```bash
cd ~/Desktop/datatrust && cat PROGRESS.md && .venv/bin/python -m pytest tests/unit -q
```
