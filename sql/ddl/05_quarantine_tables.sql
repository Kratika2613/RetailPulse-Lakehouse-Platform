-- ==============================================================================
-- RetailPulse - Quarantine Layer DDL (Dead-Letter Queue / Bad Records)
-- Schema: retailpulse_catalog.silver
-- ==============================================================================

USE CATALOG retailpulse_catalog;
USE SCHEMA silver;

-- Quarantine Table: Captures rejected rows with failure reason codes
CREATE TABLE IF NOT EXISTS quarantine_orders_dlq (
    quarantine_id BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate audit identifier',
    source_table STRING NOT NULL COMMENT 'Origin raw table (e.g. bronze.raw_orders)',
    record_payload STRING NOT NULL COMMENT 'Raw unparsed record in JSON format for debugging',
    failed_rule STRING NOT NULL COMMENT 'Constraint violated (e.g. NEGATIVE_AMOUNT, ORPHAN_CUSTOMER_ID)',
    failure_details STRING COMMENT 'Human-readable explanation of rejection',
    quarantined_at TIMESTAMP NOT NULL COMMENT 'System audit timestamp when row was rejected'
)
USING DELTA
COMMENT 'Dead-Letter Queue (DLQ) isolating anomalous transactions to preserve silver integrity';

