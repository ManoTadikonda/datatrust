-- ---------------------------------------------------------------------------
-- 003_analytics.sql — the curated star schema.
--
-- Grain is documented per table and enforced by a primary key. Every fact
-- table carries the originating idempotency_key with a UNIQUE constraint:
-- that is the mechanism that makes replaying a Kafka batch a no-op rather
-- than a double-count.
-- ---------------------------------------------------------------------------

SET search_path TO analytics, datatrust, public;

-- --- dimensions -------------------------------------------------------------

-- Grain: one row per calendar day.
CREATE TABLE analytics.dim_date (
    date_day        DATE PRIMARY KEY,
    day_of_week     SMALLINT NOT NULL,
    day_name        VARCHAR(9) NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    month_number    SMALLINT NOT NULL,
    month_name      VARCHAR(9) NOT NULL,
    quarter         SMALLINT NOT NULL,
    year_number     SMALLINT NOT NULL,
    is_weekend      BOOLEAN  NOT NULL
);

-- Grain: one row per customer. SCD-1 — last write wins by source_updated_at.
CREATE TABLE analytics.dim_customers (
    customer_id       VARCHAR(64) PRIMARY KEY,
    email             VARCHAR(255) NOT NULL,
    country           CHAR(2)      NOT NULL,
    signup_date       DATE         NOT NULL,
    segment           VARCHAR(32)  NOT NULL DEFAULT 'standard',
    -- Event-time of the update that produced this row. Used to reject
    -- out-of-order dimension updates rather than letting a late event win.
    source_updated_at TIMESTAMPTZ  NOT NULL,
    idempotency_key   CHAR(64)     NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Grain: one row per product.
CREATE TABLE analytics.dim_products (
    product_id      VARCHAR(64) PRIMARY KEY,
    product_name    VARCHAR(128) NOT NULL,
    category        VARCHAR(64)  NOT NULL,
    list_price      NUMERIC(12,2) NOT NULL CHECK (list_price >= 0),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- --- facts --------------------------------------------------------------------

-- Grain: one row per order. Money is NUMERIC, never float.
CREATE TABLE analytics.fact_orders (
    order_id          VARCHAR(64) PRIMARY KEY,
    customer_id       VARCHAR(64) NOT NULL,
    order_timestamp   TIMESTAMPTZ NOT NULL,
    order_date        DATE        NOT NULL,
    currency          CHAR(3)     NOT NULL,
    status            VARCHAR(16) NOT NULL,
    channel           VARCHAR(16) NOT NULL DEFAULT 'web',
    line_count        SMALLINT    NOT NULL CHECK (line_count >= 1),
    gross_amount      NUMERIC(14,2) NOT NULL CHECK (gross_amount >= 0),
    schema_version    SMALLINT    NOT NULL,
    idempotency_key   CHAR(64)    NOT NULL,
    -- Provenance: which recovery run, if any, produced or corrected this row.
    recovery_run_id   UUID,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fact_orders_idempotency_uniq UNIQUE (idempotency_key)
);

COMMENT ON CONSTRAINT fact_orders_idempotency_uniq ON analytics.fact_orders IS
    'Replaying an order event cannot increase its financial effect.';

CREATE INDEX fact_orders_date_idx ON analytics.fact_orders (order_date);
CREATE INDEX fact_orders_customer_idx ON analytics.fact_orders (customer_id);

-- Grain: one row per order line.
CREATE TABLE analytics.fact_order_lines (
    order_id        VARCHAR(64) NOT NULL,
    line_number     SMALLINT    NOT NULL,
    product_id      VARCHAR(64) NOT NULL,
    quantity        INTEGER     NOT NULL CHECK (quantity >= 1),
    unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_total      NUMERIC(14,2) NOT NULL CHECK (line_total >= 0),

    PRIMARY KEY (order_id, line_number),
    CONSTRAINT fact_order_lines_order_fk FOREIGN KEY (order_id)
        REFERENCES analytics.fact_orders(order_id) ON DELETE CASCADE
);

CREATE INDEX fact_order_lines_product_idx ON analytics.fact_order_lines (product_id);

-- Grain: one row per payment.
CREATE TABLE analytics.fact_payments (
    payment_id        VARCHAR(64) PRIMARY KEY,
    order_id          VARCHAR(64) NOT NULL,
    customer_id       VARCHAR(64) NOT NULL,
    captured_at       TIMESTAMPTZ NOT NULL,
    captured_date     DATE        NOT NULL,
    currency          CHAR(3)     NOT NULL,
    status            VARCHAR(16) NOT NULL,
    amount            NUMERIC(14,2) NOT NULL CHECK (amount >= 0),
    idempotency_key   CHAR(64)    NOT NULL,
    recovery_run_id   UUID,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fact_payments_idempotency_uniq UNIQUE (idempotency_key)
);

CREATE INDEX fact_payments_order_idx ON analytics.fact_payments (order_id);
CREATE INDEX fact_payments_date_idx ON analytics.fact_payments (captured_date);

-- Grain: one row per refund.
CREATE TABLE analytics.fact_refunds (
    refund_id         VARCHAR(64) PRIMARY KEY,
    payment_id        VARCHAR(64) NOT NULL,
    order_id          VARCHAR(64) NOT NULL,
    refunded_at       TIMESTAMPTZ NOT NULL,
    refunded_date     DATE        NOT NULL,
    currency          CHAR(3)     NOT NULL,
    amount            NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    reason            VARCHAR(120) NOT NULL DEFAULT 'unspecified',
    idempotency_key   CHAR(64)    NOT NULL,
    recovery_run_id   UUID,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fact_refunds_idempotency_uniq UNIQUE (idempotency_key)
);

CREATE INDEX fact_refunds_payment_idx ON analytics.fact_refunds (payment_id);
CREATE INDEX fact_refunds_date_idx ON analytics.fact_refunds (refunded_date);

-- Grain: one row per inventory movement.
CREATE TABLE analytics.fact_inventory_movements (
    movement_id       VARCHAR(64) PRIMARY KEY,
    product_id        VARCHAR(64) NOT NULL,
    location_id       VARCHAR(64) NOT NULL,
    moved_at          TIMESTAMPTZ NOT NULL,
    moved_date        DATE        NOT NULL,
    quantity_delta    INTEGER     NOT NULL CHECK (quantity_delta <> 0),
    on_hand_after     INTEGER     NOT NULL CHECK (on_hand_after >= 0),
    idempotency_key   CHAR(64)    NOT NULL,
    recovery_run_id   UUID,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT fact_inventory_idempotency_uniq UNIQUE (idempotency_key)
);

CREATE INDEX fact_inventory_product_idx ON analytics.fact_inventory_movements (product_id, moved_date);

-- --- trust status --------------------------------------------------------------

-- One row per curated dataset. Written by the lineage layer from open
-- incidents on the dataset's ancestors, so the dashboard reads trust from
-- system state rather than computing it in the UI.
CREATE TABLE analytics.dataset_trust (
    dataset           VARCHAR(128) PRIMARY KEY,
    trust_status      datatrust.trust_status NOT NULL DEFAULT 'TRUSTED',
    open_incidents    INTEGER      NOT NULL DEFAULT 0 CHECK (open_incidents >= 0),
    last_incident_id  UUID,
    last_verified_at  TIMESTAMPTZ,
    row_count         BIGINT,
    freshness_lag_seconds INTEGER,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT dataset_trust_degraded_has_incident CHECK (
        trust_status = 'TRUSTED' OR open_incidents > 0 OR last_incident_id IS NOT NULL
    )
);

-- --- incremental watermarks ------------------------------------------------------

-- Incremental transformations read from here instead of rescanning history.
CREATE TABLE analytics.transform_watermarks (
    transform_name    VARCHAR(128) PRIMARY KEY,
    source_dataset    VARCHAR(128) NOT NULL,
    -- Highest event_timestamp already folded into the target.
    high_water_mark   TIMESTAMPTZ  NOT NULL,
    rows_processed    BIGINT       NOT NULL DEFAULT 0,
    last_run_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
