-- ============================================================
--  WildGuard Chile – Migration 002
--  fire_risk_current y fire_risk_history para validación
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fire_risk_current (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    location VARCHAR(255),
    temperature NUMERIC,
    humidity NUMERIC,
    wind_speed NUMERIC,
    fwi NUMERIC,
    risk_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold.fire_risk_history (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    location VARCHAR(255),
    temperature NUMERIC,
    humidity NUMERIC,
    wind_speed NUMERIC,
    fwi NUMERIC,
    risk_level VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);
