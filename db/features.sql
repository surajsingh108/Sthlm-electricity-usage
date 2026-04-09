-- Sweden Energy Pipeline — Phase 4 feature tables
-- Run after init.sql. All statements are idempotent.

-- ---------------------------------------------------------------------------
-- features_hourly
-- One row per zone per hour. Single source of truth for the dashboard.
-- greenness_score, low_carbon_mw, total_gen_mw, wind_mw, hydro_mw, nuclear_mw,
-- solar_mw are NULL for SE1/SE2/SE4 — generation data is SE3 only.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features_hourly (
    id                  SERIAL PRIMARY KEY,
    hour                TIMESTAMPTZ NOT NULL,
    zone                TEXT NOT NULL,
    price_eur_mwh       NUMERIC,
    rolling_avg_6h      NUMERIC,
    rolling_avg_24h     NUMERIC,
    price_level         TEXT,              -- 'low', 'medium', 'high'
    temperature_c       NUMERIC,
    windspeed_ms        NUMERIC,
    radiation_wm2       NUMERIC,
    greenness_score     NUMERIC,           -- 0–100; NULL for non-SE3 zones
    low_carbon_mw       NUMERIC,
    total_gen_mw        NUMERIC,
    wind_mw             NUMERIC,           -- B18 + B19
    hydro_mw            NUMERIC,           -- B11 + B12
    nuclear_mw          NUMERIC,           -- B20
    solar_mw            NUMERIC,           -- B16
    appliance_signal    TEXT,              -- 'run_now', 'wait', 'avoid'
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (hour, zone)
);

CREATE INDEX IF NOT EXISTS idx_features_hourly_hour
    ON features_hourly (hour DESC);

CREATE INDEX IF NOT EXISTS idx_features_hourly_zone
    ON features_hourly (zone);

-- ---------------------------------------------------------------------------
-- features_correlation
-- Pre-computed Pearson r for key metric pairs, one row per zone per pair.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features_correlation (
    id              SERIAL PRIMARY KEY,
    zone            TEXT NOT NULL,
    metric_a        TEXT NOT NULL,         -- e.g. 'windspeed_ms'
    metric_b        TEXT NOT NULL,         -- e.g. 'price_eur_mwh'
    pearson_r       NUMERIC,               -- -1.0 to 1.0
    sample_size     INTEGER,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (zone, metric_a, metric_b)
);

-- ---------------------------------------------------------------------------
-- features_best_hours
-- Best hours to run appliances by zone, averaged across all collected data.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features_best_hours (
    id              SERIAL PRIMARY KEY,
    zone            TEXT NOT NULL,
    hour_of_day     INTEGER NOT NULL,      -- 0–23 UTC
    avg_price       NUMERIC,
    avg_greenness   NUMERIC,
    combined_score  NUMERIC,               -- higher = better time to run appliances
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (zone, hour_of_day)
);
