-- ============================================================
--  WildGuard Chile – Data Lake PostgreSQL
--  Schemas: bronze (raw), silver (procesado), gold (analítico)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ════════════════════════════════════════════════════════════
-- BRONZE: Datos crudos e inmutables tal como llegan
-- Equivale a Amazon S3 Bronze Layer del pipeline
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bronze.iot_readings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source        VARCHAR(20) NOT NULL DEFAULT 'IOT_SENSOR',
    device_id     VARCHAR(60),
    zone          VARCHAR(80),
    sensor_type   VARCHAR(20),
    stream_id     VARCHAR(120),
    raw_payload   JSONB NOT NULL,
    layer         VARCHAR(10) DEFAULT 'BRONZE',
    file_path     VARCHAR(200)  -- simula ruta S3 /iot/sensor_tipo=.../fecha=.../
);

CREATE TABLE IF NOT EXISTS bronze.api_readings (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source        VARCHAR(30) NOT NULL,   -- 'WEATHER' | 'AIR_QUALITY' | 'GPS'
    api_name      VARCHAR(60),
    endpoint      VARCHAR(120),
    zone          VARCHAR(80),
    stream_id     VARCHAR(120),
    raw_payload   JSONB NOT NULL,
    http_status   INTEGER,
    layer         VARCHAR(10) DEFAULT 'BRONZE',
    file_path     VARCHAR(200)  -- simula ruta S3 /apis/api_nombre=.../fecha=.../
);

CREATE INDEX idx_bronze_iot_zone        ON bronze.iot_readings(zone);
CREATE INDEX idx_bronze_iot_received    ON bronze.iot_readings(received_at DESC);
CREATE INDEX idx_bronze_iot_stype       ON bronze.iot_readings(sensor_type);
CREATE INDEX idx_bronze_api_source      ON bronze.api_readings(source);
CREATE INDEX idx_bronze_api_received    ON bronze.api_readings(received_at DESC);

-- ════════════════════════════════════════════════════════════
-- SILVER: Datos limpios, validados, estructurados
-- Equivale a Amazon S3 Silver Layer del pipeline
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS silver.processed_readings (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bronze_id         UUID,
    processed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    etl_job_id        VARCHAR(60),          -- simula Glue job ID
    source            VARCHAR(30) NOT NULL,
    zone              VARCHAR(80),
    reserva           VARCHAR(80),
    sensor_type       VARCHAR(20),
    -- Sensores IoT
    temperature_c     NUMERIC(7,2),
    humidity_pct      NUMERIC(6,2),
    co2_ppm           NUMERIC(9,2),
    wind_kmh          NUMERIC(7,2),
    wind_direction    VARCHAR(5),
    smoke_density     NUMERIC(6,3),
    -- GPS pseudonimizado
    gps_hash_id       VARCHAR(64),          -- SHA-256+salt, no reversible
    altitude_m        NUMERIC(8,2),
    gps_precision_m   NUMERIC(6,2),
    -- Clima (Open-Meteo)
    weather_temp_c    NUMERIC(7,2),
    weather_humidity  NUMERIC(6,2),
    weather_wind_ms   NUMERIC(7,2),
    weather_wind_dir  NUMERIC(6,1),
    weather_code      INTEGER,
    precipitation_mm  NUMERIC(7,2),
    -- Calidad de aire
    aqi               NUMERIC(6,1),
    pm25              NUMERIC(7,2),
    pm10              NUMERIC(7,2),
    -- Calidad del dato
    quality_score     NUMERIC(4,3),
    anomaly_detected  BOOLEAN DEFAULT FALSE,
    duplicate_flag    BOOLEAN DEFAULT FALSE,
    -- Riesgo (FWI)
    fwi_score         NUMERIC(5,3),
    risk_level        VARCHAR(10),          -- LOW|MEDIUM|HIGH|CRITICAL
    layer             VARCHAR(10) DEFAULT 'SILVER'
);

CREATE INDEX idx_silver_zone          ON silver.processed_readings(zone);
CREATE INDEX idx_silver_processed_at  ON silver.processed_readings(processed_at DESC);
CREATE INDEX idx_silver_risk          ON silver.processed_readings(risk_level);
CREATE INDEX idx_silver_anomaly       ON silver.processed_readings(anomaly_detected);
CREATE INDEX idx_silver_fwi           ON silver.processed_readings(fwi_score DESC);

-- ════════════════════════════════════════════════════════════
-- GOLD: KPIs, alertas y vistas analíticas
-- Equivale a Redshift Serverless + QuickSight del pipeline
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS gold.kpi_snapshots (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_minutes      INTEGER DEFAULT 5,
    -- Volumetría
    total_iot           BIGINT DEFAULT 0,
    total_api           BIGINT DEFAULT 0,
    total_alerts        BIGINT DEFAULT 0,
    critical_alerts     BIGINT DEFAULT 0,
    -- Promedios IoT
    avg_temp            NUMERIC(7,2),
    avg_humidity        NUMERIC(6,2),
    avg_co2             NUMERIC(9,2),
    avg_wind            NUMERIC(7,2),
    -- Promedios clima
    avg_weather_temp    NUMERIC(7,2),
    avg_weather_hum     NUMERIC(6,2),
    avg_weather_wind    NUMERIC(7,2),
    -- Riesgo
    avg_fwi             NUMERIC(5,3),
    max_fwi             NUMERIC(5,3),
    -- Pipeline
    sla_compliance_pct  NUMERIC(5,2),
    avg_latency_ms      NUMERIC(8,2),
    -- JSON vistas
    top_risk_zones      JSONB,
    risk_distribution   JSONB,
    by_sensor_type      JSONB,
    layer               VARCHAR(10) DEFAULT 'GOLD'
);

CREATE TABLE IF NOT EXISTS gold.fire_alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_id        VARCHAR(40) UNIQUE,
    zone            VARCHAR(80),
    risk_level      VARCHAR(10),
    fwi_score       NUMERIC(5,3),
    ml_probability  NUMERIC(5,3),
    temperature_c   NUMERIC(7,2),
    humidity_pct    NUMERIC(6,2),
    wind_kmh        NUMERIC(7,2),
    factors         JSONB,
    destinations    JSONB,          -- brigadas, CONAF, bomberos
    acknowledged    BOOLEAN DEFAULT FALSE,
    silver_id       UUID REFERENCES silver.processed_readings(id),
    layer           VARCHAR(10) DEFAULT 'GOLD'
);

CREATE TABLE IF NOT EXISTS gold.ml_predictions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    predicted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    zone            VARCHAR(80),
    model_version   VARCHAR(60),
    fire_probability NUMERIC(5,3),
    risk_class      VARCHAR(20),
    confidence      NUMERIC(5,3),
    shap_top_feature VARCHAR(40),
    features        JSONB,
    silver_id       UUID REFERENCES silver.processed_readings(id)
);

CREATE INDEX idx_gold_alerts_zone   ON gold.fire_alerts(zone);
CREATE INDEX idx_gold_alerts_level  ON gold.fire_alerts(risk_level);
CREATE INDEX idx_gold_alerts_at     ON gold.fire_alerts(alert_at DESC);
CREATE INDEX idx_gold_ml_zone       ON gold.ml_predictions(zone);

-- Permisos
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO wildguard;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO wildguard;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold   TO wildguard;
GRANT USAGE ON SCHEMA bronze TO wildguard;
GRANT USAGE ON SCHEMA silver TO wildguard;
GRANT USAGE ON SCHEMA gold   TO wildguard;
