"""
WildGuard – Silver Layer (con telemetría Datadog completa)
"""
import os, json, uuid, time, logging, threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis, psycopg2
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("silver-layer")
tracer  = get_tracer()
log     = get_logger("silver-layer")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PG_HOST    = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT    = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB      = os.getenv("POSTGRES_DB", "wildguard")
PG_USER    = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS    = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8083))
GROUP  = "silver-ingestion"
STREAM = "wg:etl:out"
counters = {"stored": 0, "dup": 0, "errors": 0, "high_risk": 0}


def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASS, connect_timeout=5)
            conn.autocommit = True
            metrics.service_check("postgres.connection", 0, "OK")
            log.info("PostgreSQL conectado")
            return conn
        except Exception as e:
            log.warning("PG no disponible %d/20: %s", i+1, e)
            time.sleep(4)
    raise RuntimeError("No se pudo conectar a PostgreSQL")


def connect_redis():
    for i in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            metrics.service_check("redis.connection", 0, "OK")
            log.info("Redis conectado")
            return r
        except Exception as e:
            log.warning("Redis no disponible %d/15: %s", i+1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def sf(v):
    try: return float(v) if v is not None else None
    except: return None

def si(v):
    try: return int(float(v)) if v is not None else None
    except: return None


def insert_silver(conn, fields):
    with tracer.trace("silver.db.insert", resource="silver.processed_readings") as span:
        payload  = json.loads(fields.get("payload", "{}"))
        source   = fields.get("source", "UNKNOWN")
        zone     = fields.get("zone", "unknown")
        fwi      = float(fields.get("fwi", 0) or 0)
        level    = fields.get("riskLevel", "LOW")
        is_dup   = fields.get("isDup", "False") == "True"
        q        = float(fields.get("qualScore", 1.0) or 1.0)

        span.set_tag("source", source)
        span.set_tag("zone",   zone)
        span.set_tag("risk",   level)
        span.set_metric("fwi", fwi)

        sql = """INSERT INTO silver.processed_readings (
            bronze_id, etl_job_id, source, zone, reserva, sensor_type,
            temperature_c, humidity_pct, co2_ppm, wind_kmh, wind_direction,
            smoke_density, gps_hash_id, altitude_m, gps_precision_m,
            weather_temp_c, weather_humidity, weather_wind_ms,
            weather_wind_dir, weather_code, precipitation_mm,
            aqi, pm25, pm10,
            quality_score, anomaly_detected, duplicate_flag, fwi_score, risk_level
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id"""

        with conn.cursor() as cur:
            cur.execute(sql, (
                fields.get("bronzeId"), fields.get("glueJobId"),
                source, zone, zone, payload.get("sensorType"),
                sf(payload.get("temperatureC")), sf(payload.get("humidityPct")),
                sf(payload.get("co2Ppm")), sf(payload.get("windKmh")),
                payload.get("windDirection"),
                sf(payload.get("smokeDensity")),
                fields.get("gpsHash") or None,
                sf(payload.get("altitudeM")), sf(payload.get("gpsPrecisionM")),
                sf(payload.get("temperatureC")) if source == "WEATHER" else None,
                sf(payload.get("humidityPct"))  if source == "WEATHER" else None,
                sf(payload.get("windMs")), sf(payload.get("windDirectionDeg")),
                si(payload.get("weatherCode")), sf(payload.get("precipitationMm")),
                sf(payload.get("europeanAqi") or payload.get("usAqi")),
                sf(payload.get("pm25")), sf(payload.get("pm10")),
                q, fwi >= 0.70, is_dup, fwi, level,
            ))
            return cur.fetchone()[0]


def consumer_loop(r, conn):
    consumer = f"silver-{uuid.uuid4().hex[:8]}"
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        for out in ("wg:silver:out",):
            for grp in ("gold-aggregator","ml-inference","alert-evaluator"):
                try: r.xgroup_create(out, grp, id="0", mkstream=True)
                except redis.ResponseError: pass
    except redis.ResponseError:
        pass

    log.info("Silver consumer '%s' listo", consumer)

    while True:
        try:
            entries = r.xreadgroup(groupname=GROUP, consumername=consumer,
                                   streams={STREAM: ">"}, count=40, block=2000)
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, fields in messages:
                    t0     = time.monotonic()
                    source = fields.get("source", "UNKNOWN")
                    zone   = fields.get("zone", "unknown")
                    fwi    = float(fields.get("fwi", 0) or 0)
                    level  = fields.get("riskLevel", "LOW")

                    with tracer.trace("silver.process", resource=source) as span:
                        span.set_tag("source", source)
                        span.set_tag("zone",   zone)
                        span.set_tag("risk",   level)
                        span.set_metric("fwi", fwi)
                        try:
                            silver_id = insert_silver(conn, fields)
                            r.xadd("wg:silver:out", {
                                "silverId": str(silver_id), "bronzeId": fields.get("bronzeId",""),
                                "source": source, "zone": zone, "fwi": str(fwi),
                                "riskLevel": level, "qualScore": fields.get("qualScore","1.0"),
                                "glueJobId": fields.get("glueJobId",""),
                                "payload": fields.get("payload","{}"),
                                "timestamp": fields.get("timestamp",
                                             datetime.now(timezone.utc).isoformat()),
                            }, maxlen=8000)
                            r.xack(STREAM, GROUP, msg_id)

                            latency_ms = (time.monotonic() - t0) * 1000
                            span.set_metric("latency_ms", latency_ms)
                            counters["stored"] += 1
                            if level in ("HIGH","CRITICAL"): counters["high_risk"] += 1

                            metrics.record_fwi(fwi, level, zone)
                            metrics.record_latency("silver.process", latency_ms, source, zone)
                            metrics.increment("silver.stored",
                                             tags=[f"source:{source.lower()}",f"risk:{level.lower()}"])

                            log.info("SILVER STORED | src=%s zone=%s fwi=%.3f risk=%s lat=%.1fms",
                                     source, zone, fwi, level, latency_ms,
                                     extra={"zone": zone, "source": source,
                                            "fwi": fwi, "risk_level": level,
                                            "latency_ms": latency_ms})
                        except Exception as e:
                            counters["errors"] += 1
                            span.set_tag("error", True)
                            log.error("Silver error msg %s: %s", msg_id, e,
                                      extra={"zone": zone, "source": source})
                            r.xack(STREAM, GROUP, msg_id)

            metrics.service_check("silver.consumer", 0, f"OK stored={counters['stored']}")

        except Exception as e:
            log.error("Error en silver consumer_loop: %s", e)
            metrics.service_check("silver.consumer", 2, str(e))
            time.sleep(5)


class SilverHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"status":"UP","layer":"SILVER","stats":counters}).encode()
        self.send_response(200); self.send_header("Content-Type","application/json"); self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass


def main():
    log.info("WildGuard Silver Layer iniciando")
    conn = connect_pg(); r = connect_redis()
    threading.Thread(target=lambda: HTTPServer(("0.0.0.0",HTTP_PORT),SilverHandler).serve_forever(),daemon=True).start()
    consumer_loop(r, conn)

if __name__ == "__main__":
    main()
