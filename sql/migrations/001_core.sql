-- ---------------------------------------------------------------------------
-- 001_core.sql — raw evidence, quarantine, and processing bookkeeping.
--
-- Design notes:
--  * raw_events is append-only. Corrections never mutate it; they produce new
--    curated rows whose provenance points at the recovery run.
--  * Every table that an event can land in carries idempotency_key with a
--    UNIQUE constraint. That constraint, not application logic, is what makes
--    at-least-once delivery safe.
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS datatrust;
CREATE SCHEMA IF NOT EXISTS analytics;

SET search_path TO datatrust, public;

-- --- enumerations ----------------------------------------------------------

CREATE TYPE datatrust.event_type AS ENUM (
    'orders', 'payments', 'refunds', 'customers', 'inventory'
);

CREATE TYPE datatrust.incident_state AS ENUM (
    'DETECTED', 'TRIAGED', 'QUARANTINED', 'RECOVERING',
    'VERIFYING', 'RESOLVED', 'FAILED', 'MANUAL_REVIEW'
);

CREATE TYPE datatrust.severity AS ENUM ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL');

CREATE TYPE datatrust.failure_category AS ENUM (
    'DUPLICATE_EVENT',
    'SCHEMA_DRIFT',
    'MISSING_REQUIRED_FIELD',
    'LATE_ARRIVING_DATA',
    'CONSUMER_INTERRUPTION',
    'PROCESSING_FAILURE',
    'FINANCIAL_RECONCILIATION',
    'DOWNSTREAM_CORRUPTION',
    'REFERENTIAL_INTEGRITY',
    'FRESHNESS_VIOLATION',
    'VOLUME_ANOMALY',
    'RANGE_VIOLATION',
    'NULL_VIOLATION'
);

CREATE TYPE datatrust.trust_status AS ENUM ('TRUSTED', 'DEGRADED', 'UNTRUSTED', 'REBUILDING');

CREATE TYPE datatrust.batch_status AS ENUM ('OPEN', 'COMMITTED', 'FAILED', 'REPLAYED');

-- --- immutable raw evidence ------------------------------------------------

CREATE TABLE datatrust.raw_events (
    raw_event_id        BIGSERIAL PRIMARY KEY,
    idempotency_key     CHAR(64)      NOT NULL,
    event_id            VARCHAR(64)   NOT NULL,
    event_type          datatrust.event_type NOT NULL,
    schema_version      SMALLINT      NOT NULL CHECK (schema_version BETWEEN 1 AND 99),
    entity_id           VARCHAR(64)   NOT NULL,
    event_timestamp     TIMESTAMPTZ   NOT NULL,
    ingestion_timestamp TIMESTAMPTZ   NOT NULL,
    producer            VARCHAR(64)   NOT NULL DEFAULT 'event-generator',
    payload             JSONB         NOT NULL,
    -- Kafka coordinates, so a batch can be replayed from exactly where it was.
    kafka_topic         VARCHAR(128),
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    batch_id            UUID,
    recorded_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT raw_events_idempotency_key_uniq UNIQUE (idempotency_key),
    CONSTRAINT raw_events_event_time_sane CHECK (event_timestamp > TIMESTAMPTZ '2000-01-01')
);

COMMENT ON TABLE datatrust.raw_events IS
    'Append-only evidence. Never updated or deleted by recovery.';

CREATE INDEX raw_events_type_event_time_idx
    ON datatrust.raw_events (event_type, event_timestamp DESC);
CREATE INDEX raw_events_entity_idx
    ON datatrust.raw_events (event_type, entity_id);
CREATE INDEX raw_events_batch_idx
    ON datatrust.raw_events (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX raw_events_offsets_idx
    ON datatrust.raw_events (kafka_topic, kafka_partition, kafka_offset);

-- Guard the append-only promise in the database, not just in code.
CREATE OR REPLACE FUNCTION datatrust.reject_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'relation % is append-only (attempted %)', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER raw_events_append_only
    BEFORE UPDATE OR DELETE ON datatrust.raw_events
    FOR EACH ROW EXECUTE FUNCTION datatrust.reject_mutation();

-- --- quarantine ------------------------------------------------------------

CREATE TABLE datatrust.quarantine (
    quarantine_id       BIGSERIAL PRIMARY KEY,
    idempotency_key     CHAR(64),
    event_id            VARCHAR(64),
    event_type          datatrust.event_type,
    schema_version      SMALLINT,
    entity_id           VARCHAR(64),
    -- The raw bytes exactly as received. An event can be quarantined precisely
    -- because it could not be parsed, so this must not assume valid JSON.
    raw_payload         TEXT          NOT NULL,
    parsed_payload      JSONB,
    failure_category    datatrust.failure_category NOT NULL,
    -- Array of {field_path, message, error_type, observed} objects.
    evidence            JSONB         NOT NULL DEFAULT '[]'::jsonb,
    quality_check_id    VARCHAR(64),
    incident_id         UUID,
    kafka_topic         VARCHAR(128),
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    batch_id            UUID,
    quarantined_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    released_at         TIMESTAMPTZ,
    release_reason      TEXT,

    CONSTRAINT quarantine_evidence_is_array CHECK (jsonb_typeof(evidence) = 'array')
);

CREATE INDEX quarantine_incident_idx ON datatrust.quarantine (incident_id);
CREATE INDEX quarantine_category_idx ON datatrust.quarantine (failure_category, quarantined_at DESC);
CREATE INDEX quarantine_open_idx ON datatrust.quarantine (quarantined_at DESC)
    WHERE released_at IS NULL;

-- --- consumer batches ------------------------------------------------------

-- One row per attempt to turn a poll of Kafka records into durable state.
-- This is the table that makes "the consumer died mid-batch" a recoverable,
-- inspectable fact rather than a gap someone notices a week later.
CREATE TABLE datatrust.processing_batches (
    batch_id            UUID PRIMARY KEY,
    consumer_group      VARCHAR(128)  NOT NULL,
    topic               VARCHAR(128)  NOT NULL,
    status              datatrust.batch_status NOT NULL DEFAULT 'OPEN',
    record_count        INTEGER       NOT NULL DEFAULT 0,
    accepted_count      INTEGER       NOT NULL DEFAULT 0,
    quarantined_count   INTEGER       NOT NULL DEFAULT 0,
    duplicate_count     INTEGER       NOT NULL DEFAULT 0,
    -- {"<partition>": {"first": <offset>, "last": <offset>}}
    offset_range        JSONB         NOT NULL DEFAULT '{}'::jsonb,
    attempt             SMALLINT      NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    error_type          VARCHAR(128),
    error_message       TEXT,
    started_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    committed_at        TIMESTAMPTZ,
    duration_ms         INTEGER,

    CONSTRAINT batches_counts_nonneg CHECK (
        record_count >= 0 AND accepted_count >= 0
        AND quarantined_count >= 0 AND duplicate_count >= 0
    ),
    -- A committed batch must account for every record it claimed to read.
    CONSTRAINT batches_committed_accounts_for_records CHECK (
        status <> 'COMMITTED'
        OR accepted_count + quarantined_count + duplicate_count = record_count
    ),
    CONSTRAINT batches_committed_has_timestamp CHECK (
        (status = 'COMMITTED') = (committed_at IS NOT NULL)
    )
);

CREATE INDEX batches_status_idx ON datatrust.processing_batches (status, started_at DESC);
CREATE INDEX batches_open_idx ON datatrust.processing_batches (started_at)
    WHERE status = 'OPEN';

-- --- offset checkpoints ----------------------------------------------------

-- We commit offsets to Kafka only after the batch is durable here. This table
-- is the tie-breaker during recovery: it records the highest offset whose
-- effects we know are persisted, which lets a restarted consumer detect that
-- Kafka's committed offset is *behind* our durable state and skip the replay.
CREATE TABLE datatrust.offset_checkpoints (
    consumer_group      VARCHAR(128)  NOT NULL,
    topic               VARCHAR(128)  NOT NULL,
    partition_id        INTEGER       NOT NULL,
    durable_offset      BIGINT        NOT NULL,
    batch_id            UUID,
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (consumer_group, topic, partition_id)
);

COMMENT ON TABLE datatrust.offset_checkpoints IS
    'Highest offset per partition whose processing effects are durable in Postgres.';
