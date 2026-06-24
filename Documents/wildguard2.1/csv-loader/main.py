import os
import json
import csv
import uuid
import time
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("csv-loader")
tracer  = get_tracer()
log     = get_logger("csv-loader")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8090))
CSV_PATH   = os.getenv("CSV_PATH", "/data/test_fire_data.csv")
DELAY_S    = int(os.getenv("CSV_DELAY_S", 2))

DATA_SOURCE = "open-meteo.com"
STREAM      = "wg:raw:weather"

counters = {
    "processed":  0,
    "published":  0,
    "validation_errors": 0,
    "errors":     0,
}


def connect_redis() -> redis.Redis:
    for attempt in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            log.info("Redis conectado en %s:%s", REDIS_HOST, REDIS_PORT)
            return r
        except Exception as e:
            log.warning("Redis no disponible (%d/15): %s", attempt + 1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def ensure_stream(r: redis.Redis):
    try:
        r.xgroup_create(STREAM, "bronze-ingestion", id="0", mkstream=True)
        log.info("Stream '%s' listo", STREAM)
    except redis.ResponseError:
        pass


def parse_float(val, field_name):
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def validate_row(row, idx):
    with tracer.trace("csv.validate", resource=f"row:{idx}") as span:
        span.set_tag("row", str(idx))

        temp = parse_float(row.get("temperature"), "temperature")
        hum  = parse_float(row.get("humidity"), "humidity")
        wind = parse_float(row.get("wind_speed"), "wind_speed")
        co2  = parse_float(row.get("co2"), "co2")
        smoke = parse_float(row.get("smoke"), "smoke")
        lat  = parse_float(row.get("gps_lat"), "gps_lat")
        lon  = parse_float(row.get("gps_lon"), "gps_lon")

        errors = []
        if temp is not None and (temp < -20 or temp > 65):
            errors.append(f"temperature={temp} fuera de rango [-20, 65]")
        if hum is not None and (hum < 0 or hum > 100):
            errors.append(f"humidity={hum} fuera de rango [0, 100]")
        if wind is not None and (wind < 0 or wind > 250):
            errors.append(f"wind_speed={wind} fuera de rango [0, 250]")
        if co2 is not None and (co2 < 250 or co2 > 5000):
            errors.append(f"co2={co2} fuera de rango [250, 5000]")
        if smoke is not None and (smoke < 0 or smoke > 1):
            errors.append(f"smoke={smoke} fuera de rango [0, 1]")

        if errors:
            span.set_tag("error", True)
            span.set_tag("validation_errors", "; ".join(errors))
            log.warning("Validation failed | row=%d errors=%s", idx, errors)
            return None, errors

        log.info("Validation successful | row=%d", idx)
        return {
            "temperatureC":  temp,
            "humidityPct":   hum,
            "windKmh":       wind,
            "co2Ppm":        co2,
            "smokeDensity":  smoke,
            "gpsLat":        str(lat) if lat else "",
            "gpsLon":        str(lon) if lon else "",
            "dataSource":    DATA_SOURCE,
            "fetchedAt":     datetime.now(timezone.utc).isoformat(),
            "httpStatus":    200,
        }, []


def publish_event(r: redis.Redis, payload: dict):
    event = {
        "eventId":   str(uuid.uuid4()),
        "source":    "WEATHER",
        "apiName":   "open-meteo",
        "endpoint":  "csv-loader",
        "zone":      "csv_ingest",
        "region":    "",
        "latitude":  payload.get("gpsLat", ""),
        "longitude": payload.get("gpsLon", ""),
        "altitude":  "0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload":   json.dumps(payload),
    }
    r.xadd(STREAM, {k: str(v) for k, v in event.items()}, maxlen=3000)
    log.info("Record published to Redis Stream | stream=%s eventId=%s",
             STREAM, event["eventId"])


def process_csv(r: redis.Redis):
    log.info("Iniciando carga CSV desde: %s", CSV_PATH)
    try:
        with open(CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        log.error("Archivo CSV no encontrado: %s", CSV_PATH)
        return
    except Exception as e:
        log.error("Error leyendo CSV: %s", e)
        return

    log.info("CSV cargado: %d registros", len(rows))

    for idx, row in enumerate(rows, start=1):
        with tracer.trace("csv.process_record", resource=f"row:{idx}") as span:
            span.set_tag("row", str(idx))

            log.info("CSV record received | row=%d", idx)

            payload, errs = validate_row(row, idx)
            if payload is None:
                counters["validation_errors"] += 1
                span.set_tag("error", True)
                r.xadd("wg:csv:rejected", {
                    "row":       str(idx),
                    "errors":    json.dumps(errs),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, maxlen=1000)
                time.sleep(DELAY_S)
                continue

            try:
                publish_event(r, payload)
                counters["published"] += 1
            except Exception as e:
                counters["errors"] += 1
                span.set_tag("error", True)
                log.error("Error publicando row %d: %s", idx, e)
                time.sleep(DELAY_S)
                continue

            counters["processed"] += 1

        time.sleep(DELAY_S)

    log.info("Carga CSV finalizada | total=%d publicados=%d errores_validacion=%d errores=%d",
             len(rows), counters["published"], counters["validation_errors"], counters["errors"])


class CSVLoaderHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({
                "status":    "UP",
                "component": "csv-loader",
                "stats":     counters,
            })
        else:
            body = json.dumps({"error": "not found"})
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body.encode())
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args):
        pass


def main():
    log.info("WildGuard CSV Loader iniciando")
    log.info("CSV_PATH=%s | DELAY=%ds | PUERTO=%d", CSV_PATH, DELAY_S, HTTP_PORT)

    r = connect_redis()
    ensure_stream(r)

    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), CSVLoaderHandler).serve_forever(),
        daemon=True,
    ).start()

    process_csv(r)

    log.info("CSV Loader — todos los registros procesados, manteniendo health endpoint activo")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
