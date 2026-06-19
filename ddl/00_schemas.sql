-- Bootstrap schemas + meta tables for the warehouse.
-- Run once:  psql "$WAREHOUSE_URL" -f ddl/00_schemas.sql
-- (the ETL also creates target schemas on demand; this documents the layout)

-- one schema per topic DOMAIN; table = {origin}_{dataset}, e.g.
--   transport.nyc_collisions_person, environment.th_hii_rain_24_hist
-- (fine-grained `topic` and `origin` are tags/prefixes, not schemas)
CREATE SCHEMA IF NOT EXISTS environment;    -- disaster, weather, air, IoT, nature, sanitation
CREATE SCHEMA IF NOT EXISTS transport;      -- traffic, taxi, parking, mobility
CREATE SCHEMA IF NOT EXISTS public_safety;  -- crime, enforcement, adjudication
CREATE SCHEMA IF NOT EXISTS urban;          -- buildings, housing, land, property
CREATE SCHEMA IF NOT EXISTS society;        -- population, poverty, civic 311, government, business

-- operational metadata
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.watermarks (
    source_name text PRIMARY KEY,
    watermark   text,
    updated_at  timestamptz
);

CREATE TABLE IF NOT EXISTS meta.etl_runs (
    batch_id    text PRIMARY KEY,
    source_name text,
    status      text,
    rows        bigint,
    started_at  timestamptz,
    finished_at timestamptz,
    message     text
);
