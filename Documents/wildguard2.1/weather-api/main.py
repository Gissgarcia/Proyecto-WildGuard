"""
WildGuard – Weather API Fetcher (Open-Meteo)
============================================================
Etapa 1 del pipeline: FUENTES DE DATOS → 2B. INGESTA APIs EXTERNAS

Consulta REAL a Open-Meteo (https://open-meteo.com) — API gratuita, sin key.

Endpoints utilizados:
  1. Forecast API  (https://api.open-meteo.com/v1/forecast)
     · current:  temperatura, humedad, viento, precipitación,
                 código WMO, UV index, presión, nubosidad,
                 temperatura suelo, evapotranspiración
     · hourly:   próximas 6h para tendencia de riesgo

  2. Air Quality API  (https://air-quality-api.open-meteo.com/v1/air-quality)
     · current:  PM2.5, PM10, CO, NO2, SO2, O3, AQI europeo/EEUU

Zonas monitoreadas: reservas naturales de Chile (coordenadas reales).
Cache TTL=5min en memoria (simula Amazon DynamoDB caché).

Streams destino:
  wg:raw:weather     → Bronze Layer → ETL → Silver
  wg:raw:airquality  → Bronze Layer → ETL → Silver
"""

import os
import json
import uuid
import time
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

import redis
from telemetry import init_telemetry, get_tracer, get_logger
from datadog import statsd

metrics = init_telemetry("weather-api")
tracer  = get_tracer()
log     = get_logger("weather-api")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [weather-api] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("weather-api")

# ── Config ────────────────────────────────────────────────────
REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
FETCH_INTERVAL = int(os.getenv("WEATHER_FETCH_INTERVAL_MS", 60000)) / 1000
WEATHER_URL    = os.getenv("WEATHER_API_URL",
                           "https://api.open-meteo.com/v1/forecast")
AQ_URL         = os.getenv("WEATHER_AIRQUALITY_URL",
                           "https://air-quality-api.open-meteo.com/v1/air-quality")
DD_AGENT       = os.getenv("DD_AGENT_HOST", "localhost")


# ── Zonas (coordenadas reales de reservas chilenas) ───────────
ZONES = [
    {"name": "malalcahuello", "lat": -38.4522, "lon": -71.5789, "region": "Araucanía",  "alt": 1200},
    {"name": "nahuelbuta",    "lat": -37.8108, "lon": -72.9723, "region": "Biobío",     "alt":  900},
    {"name": "villarrica",    "lat": -39.4213, "lon": -71.9328, "region": "Araucanía",  "alt": 1500},
    {"name": "cani",          "lat": -38.6601, "lon": -71.5234, "region": "Araucanía",  "alt": 1800},
    {"name": "radal7tazas",   "lat": -35.4743, "lon": -70.9801, "region": "Maule",      "alt":  700},
]

# Cache en memoria (simula DynamoDB caché de respuestas)
_cache: dict  = {}
CACHE_TTL_SEC = 300   # 5 minutos — equivale a TTL de DynamoDB

# ── Variables de Open-Meteo ───────────────────────────────────
CURRENT_VARS = (
    "temperature_2m,"
    "relative_humidity_2m,"
    "apparent_temperature,"
    "precipitation,"
    "weather_code,"
    "wind_speed_10m,"
    "wind_direction_10m,"
    "wind_gusts_10m,"
    "surface_pressure,"
    "cloud_cover,"
    "uv_index,"
    "et0_fao_evapotranspiration,"
    "soil_temperature_0cm,"
    "soil_moisture_0_to_1cm"
)

HOURLY_VARS = (
    "temperature_2m,"
    "relative_humidity_2m,"
    "precipitation_probability,"
    "wind_speed_10m,"
    "weather_code"
)

AQ_CURRENT_VARS = (
    "pm10,"
    "pm2_5,"
    "carbon_monoxide,"
    "nitrogen_dioxide,"
    "sulphur_dioxide,"
    "ozone,"
    "european_aqi,"
    "us_aqi"
)

# Descripción de códigos WMO (para logs legibles)
WMO_DESC = {
    0: "Despejado", 1: "Mayormente despejado", 2: "Parcialmente nublado",
    3: "Nublado", 45: "Niebla", 48: "Niebla con escarcha",
    51: "Llovizna leve", 53: "Llovizna moderada", 55: "Llovizna densa",
    61: "Lluvia leve", 63: "Lluvia moderada", 65: "Lluvia fuerte",
    71: "Nevada leve", 73: "Nevada moderada", 75: "Nevada fuerte",
    77: "Granizo", 80: "Chubascos leves", 81: "Chubascos moderados",
    82: "Chubascos fuertes", 95: "Tormenta", 96: "Tormenta con granizo",
    99: "Tormenta con granizo fuerte",
}


# ── Redis ─────────────────────────────────────────────────────
def connect_redis() -> redis.Redis:
    for attempt in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            log.info("✓ Redis conectado en %s:%s", REDIS_HOST, REDIS_PORT)
            return r
        except Exception as e:
            log.warning("Redis no disponible (%d/15): %s", attempt + 1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def init_streams(r: redis.Redis):
    for stream in ("wg:raw:weather", "wg:raw:airquality"):
        try:
            r.xgroup_create(stream, "bronze-ingestion", id="0", mkstream=True)
            log.info("Stream '%s' listo", stream)
        except redis.ResponseError:
            pass


# ── Open-Meteo: Forecast API ──────────────────────────────────
def fetch_weather(zone: dict) -> Optional[dict]:
    """
    Consulta Open-Meteo Forecast API.
    Retorna variables actuales + pronóstico próximas 6 horas.
    """
    cache_key = f"weather:{zone['name']}"
    cached    = _cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < CACHE_TTL_SEC:
        log.debug("Cache hit (DynamoDB simulado) para %s", zone["name"])
        return cached["data"]

    params = {
        "latitude":      zone["lat"],
        "longitude":     zone["lon"],
        "current":       CURRENT_VARS,
        "hourly":        HOURLY_VARS,
        "forecast_hours": 6,                  # próximas 6h para tendencia de riesgo
        "timezone":      "America/Santiago",
        "wind_speed_unit": "kmh",
    }

    try:
        resp = requests.get(WEATHER_URL, params=params, timeout=12)
        resp.raise_for_status()
        body = resp.json()
        cur  = body.get("current", {})
        hrly = body.get("hourly", {})

        # Pronóstico horario próximas 6h
        forecast_6h = []
        times = hrly.get("time", [])
        for i in range(min(6, len(times))):
            forecast_6h.append({
                "time":         times[i],
                "tempC":        hrly.get("temperature_2m",            [None]*6)[i],
                "humidityPct":  hrly.get("relative_humidity_2m",      [None]*6)[i],
                "precipProb":   hrly.get("precipitation_probability",  [None]*6)[i],
                "windKmh":      hrly.get("wind_speed_10m",            [None]*6)[i],
                "weatherCode":  hrly.get("weather_code",              [None]*6)[i],
            })

        wmo_code = cur.get("weather_code")
        data = {
            # Variables actuales
            "temperatureC":          cur.get("temperature_2m"),
            "apparentTempC":         cur.get("apparent_temperature"),
            "humidityPct":           cur.get("relative_humidity_2m"),
            "windKmh":               cur.get("wind_speed_10m"),
            "windDirectionDeg":      cur.get("wind_direction_10m"),
            "windGustsKmh":          cur.get("wind_gusts_10m"),
            "precipitationMm":       cur.get("precipitation"),
            "weatherCode":           wmo_code,
            "weatherDescription":    WMO_DESC.get(wmo_code, f"Código {wmo_code}"),
            "cloudCoverPct":         cur.get("cloud_cover"),
            "surfacePressureHpa":    cur.get("surface_pressure"),
            "uvIndex":               cur.get("uv_index"),
            "evapotranspirationMm":  cur.get("et0_fao_evapotranspiration"),
            "soilTempC":             cur.get("soil_temperature_0cm"),
            "soilMoisture":          cur.get("soil_moisture_0_to_1cm"),
            # Metadatos
            "dataSource":            "open-meteo.com",
            "apiVersion":            "v1",
            "forecastHours":         6,
            "forecast6h":            forecast_6h,
            "httpStatus":            resp.status_code,
            "fetchedAt":             datetime.now(timezone.utc).isoformat(),
        }

        _cache[cache_key] = {"data": data, "ts": time.time()}
        statsd.increment("wg.weather.fetch_ok", tags=[f"zone:{zone['name']}"])
        return data

    except requests.exceptions.Timeout:
        log.error("Timeout consultando Open-Meteo para %s", zone["name"])
        statsd.increment("wg.weather.fetch_timeout", tags=[f"zone:{zone['name']}"])
        return None
    except requests.exceptions.HTTPError as e:
        log.error("HTTP error Open-Meteo %s: %s", zone["name"], e)
        statsd.increment("wg.weather.fetch_error", tags=[f"zone:{zone['name']}"])
        return None
    except Exception as e:
        log.error("Error inesperado Open-Meteo %s: %s", zone["name"], e)
        return None


# ── Open-Meteo: Air Quality API ───────────────────────────────
def fetch_air_quality(zone: dict) -> Optional[dict]:
    """
    Consulta Open-Meteo Air Quality API.
    Retorna PM2.5, PM10, gases, AQI europeo y de EEUU.
    """
    cache_key = f"aq:{zone['name']}"
    cached    = _cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < CACHE_TTL_SEC:
        return cached["data"]

    params = {
        "latitude":  zone["lat"],
        "longitude": zone["lon"],
        "current":   AQ_CURRENT_VARS,
        "timezone":  "America/Santiago",
    }

    try:
        resp = requests.get(AQ_URL, params=params, timeout=12)
        resp.raise_for_status()
        cur = resp.json().get("current", {})

        aqi_eu = cur.get("european_aqi")
        aqi_us = cur.get("us_aqi")

        # Clasificación AQI
        def aqi_level(v):
            if v is None:  return "N/A"
            if v <= 20:    return "Bueno"
            if v <= 40:    return "Justo"
            if v <= 60:    return "Moderado"
            if v <= 80:    return "Pobre"
            if v <= 100:   return "Muy Pobre"
            return "Extremadamente Pobre"

        data = {
            "pm25":              cur.get("pm2_5"),
            "pm10":              cur.get("pm10"),
            "carbonMonoxidePpb": cur.get("carbon_monoxide"),
            "nitrogenDioxidePpb": cur.get("nitrogen_dioxide"),
            "sulphurDioxidePpb": cur.get("sulphur_dioxide"),
            "ozonePpb":          cur.get("ozone"),
            "europeanAqi":       aqi_eu,
            "europeanAqiLevel":  aqi_level(aqi_eu),
            "usAqi":             aqi_us,
            "dataSource":        "open-meteo.com/air-quality",
            "httpStatus":        resp.status_code,
            "fetchedAt":         datetime.now(timezone.utc).isoformat(),
        }

        _cache[cache_key] = {"data": data, "ts": time.time()}
        statsd.increment("wg.airquality.fetch_ok", tags=[f"zone:{zone['name']}"])
        return data

    except Exception as e:
        log.error("Error Air Quality API para %s: %s", zone["name"], e)
        statsd.increment("wg.airquality.fetch_error", tags=[f"zone:{zone['name']}"])
        return None


# ── Publicar en Redis Stream ──────────────────────────────────
def publish(r: redis.Redis, stream: str, source: str,
            zone: dict, payload: dict):
    """Publica evento en Redis Stream (equivale a Kinesis Firehose)."""
    event = {
        "eventId":   str(uuid.uuid4()),
        "source":    source,
        "apiName":   "open-meteo",
        "endpoint":  WEATHER_URL if source == "WEATHER" else AQ_URL,
        "zone":      zone["name"],
        "region":    zone.get("region", ""),
        "latitude":  str(zone["lat"]),
        "longitude": str(zone["lon"]),
        "altitude":  str(zone.get("alt", 0)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload":   json.dumps(payload),
    }
    r.xadd(stream, {k: str(v) for k, v in event.items()}, maxlen=3000)


# ── Main ──────────────────────────────────────────────────────
def main():
    log.info("════════════════════════════════════════════════════")
    log.info("  WildGuard Weather API – Open-Meteo iniciando")
    log.info("  Forecast:    %s", WEATHER_URL)
    log.info("  Air Quality: %s", AQ_URL)
    log.info("  Zonas: %d | Intervalo: %.0fs | Cache TTL: %ds",
             len(ZONES), FETCH_INTERVAL, CACHE_TTL_SEC)
    log.info("════════════════════════════════════════════════════")

    r = connect_redis()
    init_streams(r)

    cycle = 0
    while True:
        cycle += 1
        ok_weather = ok_aq = errors = 0

        for zone in ZONES:
            # ── Forecast (clima actual + próximas 6h) ──────────
            weather = fetch_weather(zone)
            if weather:
                publish(r, "wg:raw:weather", "WEATHER", zone, weather)
                ok_weather += 1

                uv     = weather.get("uvIndex") or 0
                temp   = weather.get("temperatureC") or 0
                hum    = weather.get("humidityPct") or 0
                wind   = weather.get("windKmh") or 0
                gusts  = weather.get("windGustsKmh") or 0
                wdesc  = weather.get("weatherDescription", "-")
                et0    = weather.get("evapotranspirationMm") or 0
                soil_t = weather.get("soilTempC") or 0

                log.info(
                    "[ciclo %04d] WEATHER %-16s | "
                    "temp=%.1f°C hum=%.0f%% viento=%.1fkm/h ráfagas=%.1fkm/h "
                    "UV=%.1f ET0=%.2fmm suelo=%.1f°C | %s",
                    cycle, zone["name"],
                    temp, hum, wind, gusts,
                    uv, et0, soil_t, wdesc
                )

                # Emitir alerta si UV extremo o temperatura muy alta
                if uv >= 11:
                    log.warning("⚠ UV EXTREMO en %s: %.1f — riesgo de incendio elevado",
                                zone["name"], uv)
                    statsd.event("WildGuard UV Extremo",
                                 f"UV={uv} en {zone['name']}", alert_type="warning")
            else:
                errors += 1

            # ── Air Quality ────────────────────────────────────
            aq = fetch_air_quality(zone)
            if aq:
                publish(r, "wg:raw:airquality", "AIR_QUALITY", zone, aq)
                ok_aq += 1
                log.info(
                    "[ciclo %04d] AIR_QUALITY %-16s | "
                    "PM2.5=%.1f PM10=%.1f CO=%.0fppb AQI-EU=%s (%s)",
                    cycle, zone["name"],
                    aq.get("pm25") or 0,
                    aq.get("pm10") or 0,
                    aq.get("carbonMonoxidePpb") or 0,
                    aq.get("europeanAqi") or "-",
                    aq.get("europeanAqiLevel") or "-",
                )
            else:
                errors += 1

            time.sleep(1.5)   # throttle entre zonas

        # Métricas Datadog
        statsd.increment("wg.weather.fetched",    ok_weather)
        statsd.increment("wg.airquality.fetched", ok_aq)
        statsd.increment("wg.weather.errors",     errors)
        statsd.gauge("wg.stream.weather.len",    r.xlen("wg:raw:weather"))
        statsd.gauge("wg.stream.airquality.len", r.xlen("wg:raw:airquality"))
        statsd.gauge("wg.cache.size",            len(_cache))

        log.info("[ciclo %04d] ── Resumen: weather=%d aq=%d errors=%d | "
                 "cache_entries=%d",
                 cycle, ok_weather, ok_aq, errors, len(_cache))

        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
