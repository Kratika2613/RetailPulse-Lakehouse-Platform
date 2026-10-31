-- ==============================================================================
-- RetailPulse Lakehouse Platform - Infrastructure & Governance Setup
-- Unity Catalog: Catalog, Schemas (Medallion Layers), and Volumes Definition
-- ==============================================================================

-- 1. Create or verify root catalog for RetailPulse
CREATE CATALOG IF NOT EXISTS retailpulse_catalog
COMMENT 'Unity Catalog root container for RetailPulse Lakehouse Platform';

USE CATALOG retailpulse_catalog;

-- 2. Create Medallion Architecture Schemas
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingestion layer holding unaltered data copies directly from sources';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, validated, deduplicated, and conformed domain data layer';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready dimensional models (Star Schema) and analytical KPI marts';

-- 3. Create Databricks Volumes for Batch and Streaming Landing Zones
-- Note: Volumes provide cloud object storage paths managed by Unity Catalog
CREATE VOLUME IF NOT EXISTS bronze.batch_landing
COMMENT 'Staging area for incoming batch files (customers, products, orders CSV)';

CREATE VOLUME IF NOT EXISTS bronze.streaming_landing
COMMENT 'High-velocity landing zone for incoming streaming NDJSON event files';

CREATE VOLUME IF NOT EXISTS bronze.checkpoints
COMMENT 'Storage path for Auto Loader and Structured Streaming checkpoint offsets';

