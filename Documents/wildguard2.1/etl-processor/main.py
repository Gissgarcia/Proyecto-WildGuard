"""
WildGuard – ETL Processor
============================================================
Etapa 5 del pipeline: PROCESAMIENTO & ETL

Simula tres componentes del pipeline AWS:

1. AWS Lambda (Stream Processing - tiempo real):
   - Validación de esquema
   - Limpieza básica
   - Conversión de tipos
   - Enriquecimiento ligero

2. AWS Glue ETL Jobs (Batch ETL):
   - Normalización de valores
   - Enriquecimiento avanzado (FWI, unificación datasets)
   - Reglas de negocio
   - Detección de duplicados

3. AWS Glue Data Catalog (Catálogo & Gobernanza):
   - Metadatos de cada dataset procesado
   - Linaje de datos (bronze_id → silver_id)
   - Clasificación por tipo de fuente
   - Gobernanza (retención, calidad)

4. AWS Step Functions (Orquestación):
   - Coordina stream → batch → catalog

Consume: wg:bronze:out
Publica: wg:etl:out → Silver Layer
"""

import os
import json
import uuid
import time
import hashlib
import logging
import threading
from datetime import datetime, timezone
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
import psycopg2
from telemetry import init_telemetry, get_tracer, get_logger
from datadog import statsd

metrics = init_telemetry("etl-processor")
tracer  = get_tracer()
log     = get_logger("etl-processor")

REDIS_HOST    = os.getenv("REDIS_HOST", "redis")
REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
PG_HOST       = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT       = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB         = os.getenv("POSTGRES_DB", "wildguard")
PG_USER       = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
HTTP_PORT     = int(os.getenv("HTTP_PORT", 8082))
BATCH_INTERVAL = int(os.getenv("ETL_BATCH_INTERVAL_MS", 30000)) / 1000
BATCH_SIZE    = int(os.getenv("ETL_STREAM_BATCH_SIZE", 50))
GPS_SALT      = os.getenv("GPS_HASH_SALT", "wg_gps_salt_chile_2024")
DD_AGENT      = os.getenv("DD_AGENT_HOST", "localhost")

counters = {
    "stream_processed": 0,
    "batch_processed":  0,
    "rejected":         0,
    "duplicates":       0,
    "glue_jobs":        0,
    "errors":           0,
}

# Catálogo de datos simulado (Glue Data Catalog)
data_catalog: dict = {}
seen_hashes: set   = set()

GROUP    = "etl-stream-processor"
STREAM   = "wg:bronze:out"


# ── Conexiones ────────────────────────────────────────────────
def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS, connect_timeout=5
            )
            conn.autocommit = True
            log.info("✓ PostgreSQL conectado")
            return conn
        except Exception as e:
            log.warning("PG no disponible (%d/20): %s", i + 1, e)
            time.sleep(4)
    raise RuntimeError("No se pudo conectar a PostgreSQL")


def connect_redis() -> redis.Redis:
    for i in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            log.info("✓ Redis conectado")
            return r
        except Exception as e:
            log.warning("Redis no disponible (%d/15): %s", i + 1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def ensure_groups(r: redis.Redis):
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.ResponseError:
        pass


# ── Lambda: Stream Processing (tiempo real) ───────────────────
VALID_RANGES = {
    "temperatureC": (-20, 65),
    "humidityPct":  (0, 100),
    "co2Ppm":       (250, 5000),
    "windKmh":      (0, 250),
    "smokeDensity": (0, 1),
    "windMs":       (0, 70),
}


def lambda_stream_process(fields: dict) -> tuple[dict | None, list[str]]:
    """
    Simula AWS Lambda stream processing:
    - Validación de esquema
    - Limpieza básica de tipos
    - Enriquecimiento ligero
    """
    source  = fields.get("source", "UNKNOWN")
    zone    = fields.get("zone", "unknown")
    errors  = []

    try:
        payload = json.loads(fields.get("payload", "{}"))
    except json.JSONDecodeError as e:
        return None, [f"JSON inválido: {e}"]

    # Validación y conversión de tipos
    cleaned = {}
    for key, val in payload.items():
        if key.startswith("_"):
            continue  # ignorar metadata interna
        if key in VALID_RANGES and val is not None:
            try:
                v = float(val)
                lo, hi = VALID_RANGES[key]
                if not (lo <= v <= hi):
                    errors.append(f"{key}={v} fuera de [{lo},{hi}]")
                    continue
                cleaned[key] = round(v, 3)
            except (ValueError, TypeError):
                errors.append(f"{key} no numérico: {val}")
        else:
            cleaned[key] = val

    if not cleaned:
        return None, errors + ["Payload vacío tras limpieza"]

    # Enriquecimiento ligero: timestamp normalizado
    cleaned["_normalizedAt"] = datetime.now(timezone.utc).isoformat()
    cleaned["_source"]       = source
    cleaned["_zone"]         = zone

    return cleaned, errors


def hash_gps(lat, lon) -> str:
    """Pseudonimiza coordenadas GPS (SHA-256 + salt). No reversible."""
    return hashlib.sha256(f"{GPS_SALT}:{lat:.6f}:{lon:.6f}".encode()).hexdigest()


def hash_device(device_id: str) -> str:
    return hashlib.sha256(f"{GPS_SALT}:{device_id}".encode()).hexdigest()[:16]


# ── Glue ETL: Batch processing ────────────────────────────────
def calc_fwi(payload: dict, source: str) -> float:
    """
    Fire Weather Index (FWI) — versión enriquecida con Open-Meteo.
    
    Pesos por fuente:
      IoT Sensores   → 60%  (temp 25%, humedad 15%, viento 8%, CO2 6%, humo 6%)
      Open-Meteo     → 40%  (temp 8%, humedad 7%, viento 7%, WMO 5%,
                             UV 5%, ET0 4%, suelo 4%)
      Modificador GPS altitud >1500m → +5%
    """
    score = 0.0

    if source == "IOT_SENSOR":
        temp  = float(payload.get("temperatureC") or 0)
        hum   = float(payload.get("humidityPct")  or 50)
        wind  = float(payload.get("windKmh")       or 0)
        co2   = float(payload.get("co2Ppm")        or 415)
        smoke = float(payload.get("smokeDensity")  or 0)

        score += min(temp / 50, 1.0)          * 0.25  # temperatura alta → riesgo
        score += max(1 - hum / 100, 0)        * 0.15  # humedad baja → riesgo
        score += min(wind / 100, 1.0)         * 0.08  # viento fuerte → propagación
        score += min((co2 - 350) / 500, 1.0)  * 0.06  # CO2 elevado → actividad
        score += min(smoke / 0.8, 1.0)        * 0.06  # humo detectado → incendio

    elif source == "WEATHER":
        temp  = float(payload.get("temperatureC")         or 0)
        hum   = float(payload.get("humidityPct")          or 50)
        wind  = float(payload.get("windKmh")              or 0)
        gusts = float(payload.get("windGustsKmh")         or 0)
        code  = int(float(payload.get("weatherCode")      or 0))
        uv    = float(payload.get("uvIndex")              or 0)
        et0   = float(payload.get("evapotranspirationMm") or 0)
        soil_t = float(payload.get("soilTempC")           or 15)

        score += min(temp / 50, 1.0)              * 0.08  # temperatura atmosférica
        score += max(1 - hum / 100, 0)            * 0.07  # humedad baja
        score += min(max(wind, gusts) / 100, 1.0) * 0.07  # viento o ráfagas
        score += min(uv / 12, 1.0)                * 0.05  # UV alto → reseca vegetación
        score += min(et0 / 8, 1.0)                * 0.04  # ET0 alta → vegetación seca
        score += min(soil_t / 40, 1.0)            * 0.04  # suelo caliente

        # Código WMO: lluvia/tormenta reduce riesgo; seco lo eleva
        if code > 70:       # lluvia/nieve/tormenta
            score *= 0.55
        elif code == 0:     # cielo despejado → máximo riesgo
            score += 0.05
        else:               # nublado/niebla → ligera reducción
            score += 0.02

    elif source == "AIR_QUALITY":
        # PM2.5 elevado puede indicar quema activa
        pm25 = float(payload.get("pm25") or 0)
        co   = float(payload.get("carbonMonoxidePpb") or 0)
        score += min(pm25 / 150, 1.0) * 0.04   # PM2.5 > 150 µg/m³ = muy peligroso
        score += min(co / 10000, 1.0) * 0.03   # CO elevado → combustión

    alt = float(payload.get("altitudeM") or 0)
    if alt > 1500:
        score *= 1.05  # zonas altas: menor humedad relativa

    return round(min(score, 1.0), 4)


def risk_level(fwi: float) -> str:
    if fwi >= 0.80: return "CRITICAL"
    if fwi >= 0.60: return "HIGH"
    if fwi >= 0.35: return "MEDIUM"
    return "LOW"


def glue_enrich(cleaned: dict, fields: dict) -> dict:
    """
    Simula AWS Glue ETL Job:
    - Normalización avanzada
    - Enriquecimiento con FWI
    - Pseudonimización GPS
    - Detección de duplicados
    - Unificación de datasets
    """
    source = fields.get("source", "UNKNOWN")

    # Detección de duplicados (hash del contenido)
    content_hash = hashlib.md5(
        json.dumps(cleaned, sort_keys=True).encode()
    ).hexdigest()
    is_duplicate = content_hash in seen_hashes
    if not is_duplicate:
        seen_hashes.add(content_hash)
        if len(seen_hashes) > 20000:
            seen_hashes.clear()

    # GPS: pseudonimizar lat/lon
    gps_hash_id = None
    if source == "GPS":
        lat = cleaned.get("latitude")
        lon = cleaned.get("longitude")
        if lat and lon:
            gps_hash_id = hash_gps(float(lat), float(lon))
            # Eliminar coordenadas exactas
            cleaned.pop("latitude", None)
            cleaned.pop("longitude", None)

    # FWI
    fwi   = calc_fwi(cleaned, source)
    level = risk_level(fwi)

    # Quality score
    expected = {
        "IOT_SENSOR": ["temperatureC", "humidityPct", "co2Ppm", "windKmh"],
        "GPS":        ["altitudeM", "gpsPrecisionM"],
        "WEATHER":    ["temperatureC", "humidityPct", "windMs", "weatherCode"],
        "AIR_QUALITY": ["pm25", "pm10", "europeanAqi"],
    }.get(source, [])
    filled = sum(1 for f in expected if cleaned.get(f) is not None)
    q_score = round(filled / len(expected), 3) if expected else 1.0

    return {
        **cleaned,
        "_gpsHashId":     gps_hash_id,
        "_isDuplicate":   is_duplicate,
        "_fwiScore":      fwi,
        "_riskLevel":     level,
        "_qualityScore":  q_score,
        "_deviceIdHash":  hash_device(fields.get("deviceId") or fields.get("zone", "")),
        "_glueJobId":     f"wg-glue-{uuid.uuid4().hex[:8]}",
        "_enrichedAt":    datetime.now(timezone.utc).isoformat(),
    }


# ── Glue Data Catalog ─────────────────────────────────────────
def catalog_entry(fields: dict, enriched: dict) -> dict:
    source = fields.get("source", "UNKNOWN")
    entry  = {
        "tableId":     f"wg_{source.lower()}_{datetime.now().strftime('%Y%m%d')}",
        "source":      source,
        "zone":        fields.get("zone"),
        "schema":      list(enriched.keys()),
        "rowCount":    1,
        "quality":     enriched.get("_qualityScore"),
        "retention":   "730-days",
        "classification": "wildguard-sensor-data",
        "lineage": {
            "bronzeId": fields.get("bronzeId"),
            "stream":   fields.get("streamSrc"),
        },
        "catalogedAt": datetime.now(timezone.utc).isoformat(),
    }
    data_catalog[entry["tableId"]] = entry
    return entry


# ── Consumer ──────────────────────────────────────────────────
def consumer_loop(r: redis.Redis, conn):
    consumer = f"etl-{uuid.uuid4().hex[:8]}"
    log.info("ETL consumer '%s' listo en stream '%s'", consumer, STREAM)

    while True:
        try:
            entries = r.xreadgroup(
                groupname=GROUP, consumername=consumer,
                streams={STREAM: ">"},
                count=BATCH_SIZE, block=2000,
            )
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, fields in messages:
                    t0 = time.monotonic()
                    try:
                        source = fields.get("source", "UNKNOWN")

                        # Step 1: Lambda stream processing
                        cleaned, errs = lambda_stream_process(fields)
                        if cleaned is None:
                            counters["rejected"] += 1
                            log.warning("ETL REJECTED | src=%s zone=%s errs=%s",
                                        source, fields.get("zone"), errs)
                            r.xack(STREAM, GROUP, msg_id)
                            statsd.increment("wg.etl.rejected")
                            continue

                        # Step 2: Glue ETL enrichment
                        enriched = glue_enrich(cleaned, fields)
                        if enriched["_isDuplicate"]:
                            counters["duplicates"] += 1

                        # Step 3: Glue Data Catalog
                        catalog_entry(fields, enriched)
                        counters["glue_jobs"] += 1

                        # Publicar a Silver
                        r.xadd("wg:etl:out", {
                            "bronzeId":   fields.get("bronzeId", ""),
                            "source":     source,
                            "zone":       fields.get("zone", ""),
                            "fwi":        str(enriched["_fwiScore"]),
                            "riskLevel":  enriched["_riskLevel"],
                            "qualScore":  str(enriched["_qualityScore"]),
                            "isDup":      str(enriched["_isDuplicate"]),
                            "gpsHash":    enriched.get("_gpsHashId") or "",
                            "glueJobId":  enriched["_glueJobId"],
                            "payload":    json.dumps(enriched),
                            "timestamp":  fields.get("timestamp",
                                          datetime.now(timezone.utc).isoformat()),
                        }, maxlen=10000)

                        r.xack(STREAM, GROUP, msg_id)
                        counters["stream_processed"] += 1

                        latency_ms = (time.monotonic() - t0) * 1000
                        statsd.histogram("wg.etl.latency_ms", latency_ms,
                                         tags=[f"source:{source.lower()}"])
                        statsd.increment("wg.etl.processed",
                                         tags=[f"source:{source.lower()}",
                                               f"risk:{enriched['_riskLevel'].lower()}"])

                        log.info("ETL OK | src=%s zone=%s fwi=%.3f risk=%s dup=%s lat=%.1fms",
                                 source, fields.get("zone"),
                                 enriched["_fwiScore"], enriched["_riskLevel"],
                                 enriched["_isDuplicate"], latency_ms)

                    except Exception as e:
                        counters["errors"] += 1
                        log.error("ETL error msg %s: %s", msg_id, e)
                        r.xack(STREAM, GROUP, msg_id)

        except Exception as e:
            log.error("Error en consumer_loop: %s", e)
            time.sleep(5)


# ── HTTP ──────────────────────────────────────────────────────
class ETLHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/catalog":
            body = json.dumps({"catalog_entries": len(data_catalog),
                               "tables": list(data_catalog.keys())[:20]})
        else:
            body = json.dumps({
                "status":     "UP",
                "component":  "etl-processor",
                "jobs":       ["lambda-stream", "glue-batch", "glue-catalog"],
                "stats":      counters,
            })
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args): pass


def main():
    log.info("════════════════════════════════════════════")
    log.info("  WildGuard ETL Processor iniciando")
    log.info("  Jobs: Lambda-Stream + Glue-Batch + Data-Catalog")
    log.info("  Orquestación: Step Functions simulado")
    log.info("════════════════════════════════════════════")

    conn = connect_pg()
    r    = connect_redis()
    ensure_groups(r)

    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), ETLHandler).serve_forever(),
        daemon=True
    ).start()
    log.info("HTTP health en :%d", HTTP_PORT)

    consumer_loop(r, conn)


if __name__ == "__main__":
    main()
