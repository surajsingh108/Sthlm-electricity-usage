-- Sweden Energy Pipeline — database schema
-- All timestamps stored in UTC. Run order is idempotent (IF NOT EXISTS / OR REPLACE).

-- ---------------------------------------------------------------------------
-- raw_weather
-- One row per source per city per forecast hour.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_weather (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL,          -- 'openmeteo' | 'met_norway'
    city            TEXT NOT NULL,          -- 'stockholm' | 'malmo' | 'sundsvall' | 'lulea'
    forecast_time   TIMESTAMPTZ NOT NULL,   -- the hour this row refers to (UTC)
    ingested_at     TIMESTAMPTZ NOT NULL,
    temperature_c   NUMERIC,
    windspeed_ms    NUMERIC,
    radiation_wm2   NUMERIC,
    UNIQUE (source, city, forecast_time)
);

-- ---------------------------------------------------------------------------
-- raw_prices
-- One row per bidding zone per hour. 15-min ENTSO-E data aggregated to hourly
-- average before insert.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_prices (
    id            SERIAL PRIMARY KEY,
    zone          TEXT NOT NULL,            -- 'SE1' | 'SE2' | 'SE3' | 'SE4'
    price_time    TIMESTAMPTZ NOT NULL,     -- UTC, truncated to hour
    ingested_at   TIMESTAMPTZ NOT NULL,
    price_eur_mwh NUMERIC,
    UNIQUE (zone, price_time)
);

-- ---------------------------------------------------------------------------
-- raw_generation
-- One row per production type per hour. 15-min ENTSO-E data summed to hourly
-- before insert.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_generation (
    id              SERIAL PRIMARY KEY,
    document_type   TEXT NOT NULL,          -- 'A75' (actual) | 'A69' (forecast)
    psr_type        TEXT NOT NULL,          -- B-code: 'B12', 'B16', 'B19', 'B20', etc.
    gen_time        TIMESTAMPTZ NOT NULL,   -- UTC, truncated to hour
    ingested_at     TIMESTAMPTZ NOT NULL,
    quantity_mw     NUMERIC,
    UNIQUE (document_type, psr_type, gen_time)
);

-- ---------------------------------------------------------------------------
-- pipeline_runs
-- Health log — one row per run_pipeline.py execution.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            SERIAL PRIMARY KEY,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status        TEXT NOT NULL,            -- 'success' | 'partial' | 'failed'
    sources_ok    INTEGER,
    sources_total INTEGER,
    notes         TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_run_at
    ON pipeline_runs (run_at DESC);

-- ---------------------------------------------------------------------------
-- hourly_energy  (view)
-- Joins SE3 prices, Stockholm weather (openmeteo), and SE3 actual generation
-- (A75). Computes a greenness score from the low-carbon generation fraction.
--
-- Low-carbon PSR types for Sweden:
--   B11 Hydro Run-of-river   B12 Hydro Reservoir   B16 Solar
--   B18 Wind Offshore        B19 Wind Onshore       B20 Nuclear
-- Also present in SE3 data: B04 Fossil Gas, B14 Fossil Hard Coal
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW hourly_energy AS
SELECT
    p.price_time                                            AS hour,
    p.zone,
    p.price_eur_mwh,
    w.temperature_c,
    w.windspeed_ms,
    w.radiation_wm2,
    w.source                                                AS weather_source,
    SUM(CASE WHEN g.psr_type IN ('B11','B12','B16','B18','B19','B20')
             THEN g.quantity_mw ELSE 0 END)                 AS low_carbon_mw,
    SUM(g.quantity_mw)                                      AS total_gen_mw,
    ROUND(
        SUM(CASE WHEN g.psr_type IN ('B11','B12','B16','B18','B19','B20')
                 THEN g.quantity_mw ELSE 0 END)
        / NULLIF(SUM(g.quantity_mw), 0) * 100
    , 1)                                                    AS greenness_score
FROM raw_prices p
LEFT JOIN raw_weather w
    ON  w.forecast_time = p.price_time
    AND w.city          = 'stockholm'
    AND w.source        = 'openmeteo'
LEFT JOIN raw_generation g
    ON  g.gen_time      = p.price_time
    AND g.document_type = 'A75'
WHERE p.zone = 'SE3'
GROUP BY
    p.price_time, p.zone, p.price_eur_mwh,
    w.temperature_c, w.windspeed_ms, w.radiation_wm2, w.source
ORDER BY p.price_time;
