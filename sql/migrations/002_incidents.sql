-- ---------------------------------------------------------------------------
-- 002_incidents.sql — incidents, state audit, quality results, recovery runs.
--
-- The central invariant encoded here: an incident may only be RESOLVED when a
-- verification row proves it. That is a CHECK constraint plus a trigger, not a
-- convention, because "the recovery job finished" is not the same claim as
-- "the data is correct again".
-- ---------------------------------------------------------------------------

SET search_path TO datatrust, public;

-- --- quality check catalogue -----------------------------------------------

CREATE TABLE datatrust.quality_checks (
    check_id            VARCHAR(64) PRIMARY KEY,
    description         TEXT          NOT NULL,
    failure_category    datatrust.failure_category NOT NULL,
    default_severity    datatrust.severity NOT NULL DEFAULT 'MEDIUM',
    -- Checks are configurable: thresholds live here, not in code.
    config              JSONB         NOT NULL DEFAULT '{}'::jsonb,
    enabled             BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE datatrust.quality_results (
    result_id           BIGSERIAL PRIMARY KEY,
    check_id            VARCHAR(64)   NOT NULL REFERENCES datatrust.quality_checks(check_id),
    dataset             VARCHAR(128)  NOT NULL,
    partition_key       VARCHAR(64),
    passed              BOOLEAN       NOT NULL,
    observed_value      NUMERIC,
    expected_value      NUMERIC,
    rows_scanned        BIGINT        NOT NULL DEFAULT 0,
    rows_failed         BIGINT        NOT NULL DEFAULT 0,
    -- Structured evidence. Empty array when the check passed.
    evidence            JSONB         NOT NULL DEFAULT '[]'::jsonb,
    batch_id            UUID,
    evaluated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    duration_ms         INTEGER,

    CONSTRAINT quality_results_failed_has_evidence CHECK (
        passed OR jsonb_array_length(evidence) > 0
    ),
    CONSTRAINT quality_results_rows_sane CHECK (rows_failed <= rows_scanned)
);

COMMENT ON CONSTRAINT quality_results_failed_has_evidence ON datatrust.quality_results IS
    'A failing check with no evidence is unactionable; the schema forbids it.';

CREATE INDEX quality_results_dataset_idx
    ON datatrust.quality_results (dataset, evaluated_at DESC);
CREATE INDEX quality_results_failures_idx
    ON datatrust.quality_results (check_id, evaluated_at DESC) WHERE NOT passed;

-- --- incidents --------------------------------------------------------------

CREATE TABLE datatrust.incidents (
    incident_id         UUID PRIMARY KEY,
    state               datatrust.incident_state NOT NULL DEFAULT 'DETECTED',
    severity            datatrust.severity NOT NULL,
    failure_category    datatrust.failure_category NOT NULL,
    dataset             VARCHAR(128)  NOT NULL,
    -- Affected scope. Partitions are dataset-specific (a date, a Kafka
    -- partition, a product id) so they stay opaque text here.
    affected_partitions TEXT[]        NOT NULL DEFAULT '{}',
    affected_record_count BIGINT      NOT NULL DEFAULT 0,
    quality_check_id    VARCHAR(64)   REFERENCES datatrust.quality_checks(check_id),
    evidence            JSONB         NOT NULL DEFAULT '[]'::jsonb,
    -- Set by policy at triage time. NULL means "not yet assessed".
    recovery_eligible   BOOLEAN,
    recovery_strategy   VARCHAR(64),
    recovery_attempts   SMALLINT      NOT NULL DEFAULT 0,
    scenario_id         VARCHAR(64),
    detected_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ,
    verification_passed BOOLEAN,
    verification_run_id UUID,
    notes               TEXT,

    CONSTRAINT incidents_attempts_nonneg CHECK (recovery_attempts >= 0),
    CONSTRAINT incidents_resolved_has_timestamp CHECK (
        (state = 'RESOLVED') = (resolved_at IS NOT NULL)
    ),
    -- The invariant that keeps the dashboard honest.
    CONSTRAINT incidents_resolved_requires_verification CHECK (
        state <> 'RESOLVED' OR verification_passed IS TRUE
    ),
    CONSTRAINT incidents_affected_count_nonneg CHECK (affected_record_count >= 0)
);

COMMENT ON CONSTRAINT incidents_resolved_requires_verification ON datatrust.incidents IS
    'A finished recovery job is not a resolution. Verification must have passed.';

CREATE INDEX incidents_open_idx ON datatrust.incidents (detected_at DESC)
    WHERE state NOT IN ('RESOLVED', 'FAILED');
CREATE INDEX incidents_dataset_idx ON datatrust.incidents (dataset, detected_at DESC);
CREATE INDEX incidents_category_idx ON datatrust.incidents (failure_category, detected_at DESC);
CREATE INDEX incidents_scenario_idx ON datatrust.incidents (scenario_id)
    WHERE scenario_id IS NOT NULL;

-- --- state transition audit --------------------------------------------------

CREATE TABLE datatrust.incident_transitions (
    transition_id       BIGSERIAL PRIMARY KEY,
    incident_id         UUID          NOT NULL REFERENCES datatrust.incidents(incident_id)
                                      ON DELETE CASCADE,
    from_state          datatrust.incident_state,
    to_state            datatrust.incident_state NOT NULL,
    actor               VARCHAR(64)   NOT NULL,
    reason              TEXT          NOT NULL,
    occurred_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT transitions_actually_move CHECK (from_state IS DISTINCT FROM to_state)
);

CREATE INDEX incident_transitions_incident_idx
    ON datatrust.incident_transitions (incident_id, occurred_at);

-- Every state change must leave a trail. Enforced here so that a stray UPDATE
-- from psql cannot quietly rewrite incident history.
CREATE OR REPLACE FUNCTION datatrust.require_transition_audit() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        IF NOT EXISTS (
            SELECT 1 FROM datatrust.incident_transitions t
            WHERE t.incident_id = NEW.incident_id
              AND t.to_state = NEW.state
              AND t.occurred_at >= now() - INTERVAL '5 seconds'
        ) THEN
            RAISE EXCEPTION
                'incident % moved %->% without an audit row',
                NEW.incident_id, OLD.state, NEW.state
                USING ERRCODE = 'restrict_violation';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- --- recovery runs -----------------------------------------------------------

CREATE TABLE datatrust.recovery_runs (
    run_id              UUID PRIMARY KEY,
    incident_id         UUID          NOT NULL REFERENCES datatrust.incidents(incident_id)
                                      ON DELETE CASCADE,
    strategy            VARCHAR(64)   NOT NULL,
    attempt             SMALLINT      NOT NULL CHECK (attempt >= 1),
    succeeded           BOOLEAN,
    records_replayed    BIGINT        NOT NULL DEFAULT 0,
    records_repaired    BIGINT        NOT NULL DEFAULT 0,
    records_skipped     BIGINT        NOT NULL DEFAULT 0,
    duplicates_prevented BIGINT       NOT NULL DEFAULT 0,
    error_type          VARCHAR(128),
    error_message       TEXT,
    started_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    duration_ms         INTEGER,

    CONSTRAINT recovery_runs_finished_has_outcome CHECK (
        (finished_at IS NULL) = (succeeded IS NULL)
    ),
    CONSTRAINT recovery_runs_counts_nonneg CHECK (
        records_replayed >= 0 AND records_repaired >= 0
        AND records_skipped >= 0 AND duplicates_prevented >= 0
    ),
    CONSTRAINT recovery_runs_attempt_uniq UNIQUE (incident_id, attempt)
);

CREATE INDEX recovery_runs_incident_idx ON datatrust.recovery_runs (incident_id, attempt);

-- --- verification runs -------------------------------------------------------

-- Verification is deliberately a separate table from recovery_runs: the whole
-- point is that it is an *independent* assessment of the data, which can be
-- re-run without re-running the repair.
CREATE TABLE datatrust.verification_runs (
    verification_id     UUID PRIMARY KEY,
    incident_id         UUID          NOT NULL REFERENCES datatrust.incidents(incident_id)
                                      ON DELETE CASCADE,
    recovery_run_id     UUID          REFERENCES datatrust.recovery_runs(run_id)
                                      ON DELETE SET NULL,
    passed              BOOLEAN       NOT NULL,
    -- Both levels are required: matching row counts with wrong sums is still
    -- broken data, and correct sums with missing rows is too.
    record_level_passed BOOLEAN       NOT NULL,
    aggregate_level_passed BOOLEAN    NOT NULL,
    checks_run          INTEGER       NOT NULL DEFAULT 0,
    checks_failed       INTEGER       NOT NULL DEFAULT 0,
    evidence            JSONB         NOT NULL DEFAULT '[]'::jsonb,
    verified_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    duration_ms         INTEGER,

    CONSTRAINT verification_passed_requires_both_levels CHECK (
        passed = (record_level_passed AND aggregate_level_passed)
    ),
    CONSTRAINT verification_failed_has_evidence CHECK (
        passed OR jsonb_array_length(evidence) > 0
    ),
    CONSTRAINT verification_checks_sane CHECK (checks_failed <= checks_run)
);

COMMENT ON CONSTRAINT verification_passed_requires_both_levels ON datatrust.verification_runs IS
    'Record-level and aggregate-level correctness must both hold.';

ALTER TABLE datatrust.incidents
    ADD CONSTRAINT incidents_verification_run_fk
    FOREIGN KEY (verification_run_id)
    REFERENCES datatrust.verification_runs(verification_id)
    ON DELETE SET NULL;

CREATE INDEX verification_runs_incident_idx
    ON datatrust.verification_runs (incident_id, verified_at DESC);

-- Attach the audit trigger now that every dependency exists.
CREATE TRIGGER incidents_require_audit
    BEFORE UPDATE ON datatrust.incidents
    FOR EACH ROW EXECUTE FUNCTION datatrust.require_transition_audit();
