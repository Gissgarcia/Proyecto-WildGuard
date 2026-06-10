"""
WildGuard – Bronze Layer (con telemetría Datadog completa)
============================================================
Consume Redis Streams, persiste raw en PostgreSQL bronze schema.
Telemetría: APM spans por mensaje, métricas de throughput y latencia,
service checks de Redis y PostgreSQL.
"""

import os, json, uuid, time, logging, threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis, psycopg2
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("bronze-layer")
tracer  = get_tracer()
log     = get_logger("bronze-layer")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PG_HOST    = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT    = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB      = os.getenv("POSTGRES_DB", "wildguard")
PG_USER    = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS    = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8081))

counters = {"iot_ingested": 0, "api_ingested": 0, "errors": 0, "total": 0}
IOT_STREAMS = ["wg:raw:iot", "wg:raw:gps", "wg:raw:alerts"]
API_STREAMS = ["wg:raw:weather", "wg:raw:airquality"]
GROUP       = "bronze-ingestion"


def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                    user=PG_USER, password=PG_PASS, connect_timeout=5)
            conn.autocommit = True
            metrics.service_check("postgres.connection", 0, "PostgreSQL connected")
            log.info("PostgreSQL conectado")
            return conn
        except Exception as e:
            log.warning("PG no disponible %d/20: %s", i + 1, e)
            metrics.service_check("postgres.connection", 2, str(e))
            time.sleep(4)
    raise RuntimeError("No se pudo conectar a PostgreSQL")


def connect_redis() -> redis.Redis:
    for i in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            metrics.service_check("redis.connection", 0, "Redis connected")
            log.info("Redis conectado")
            return r
        except Exception as e:
            log.warning("Redis no disponible %d/15: %s", i + 1, e)
            metrics.service_check("redis.connection", 2, str(e))
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def ensure_groups(r):
    for stream in IOT_STREAMS + API_STREAMS:
        try:
            r.xgroup_create(stream, GROUP, id="0", mkstream=True)
        except redis.ResponseError:
            pass


def s3_path(source, payload, zone):
    today = datetime.now().strftime("%Y-%m-%d")
    if source == "IOT_SENSOR":
        stype = payload.get("sensorType", "UNKNOWN") if isinstance(payload, dict) else "UNKNOWN"
        return f"/iot/sensor_tipo={stype}/fecha={today}/region={zone}/archivos.json"
    return f"/apis/api_nombre={source.lower()}/fecha={today}/endpoint=response/respuestas.json"


def insert_record(conn, source, fields, msg_id):
    with tracer.trace("bronze.db.insert", resource=f"bronze.{source.lower()}") as span:
        span.set_tag("db.type",       "postgresql")
        span.set_tag("db.schema",     "bronze")
        span.set_tag("source",        source)
        span.set_tag("zone",          fields.get("zone", ""))

        try:
            payload_str = fields.get("payload", "{}")
            payload     = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
            zone        = fields.get("zone", "unknown")

            if source in ("IOT_SENSOR", "GPS", "ALERT"):
                sql = """INSERT INTO bronze.iot_readings
                         (source, device_id, zone, sensor_type, stream_id, raw_payload, file_path)
                         VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id"""
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        source,
                        fields.get("deviceId"),
                        zone,
                        payload.get("sensorType") if isinstance(payload, dict) else None,
                        msg_id,
                        json.dumps({**payload if isinstance(payload, dict) else {},
                                    "_meta": {"eventId": fields.get("eventId"),
                                              "timestamp": fields.get("timestamp"),
                                              "immutable": True, "sse": "SSE-S3-KMS"}}),
                        s3_path(source, payload, zone),
                    ))
                    rec_id = cur.fetchone()[0]
            else:
                sql = """INSERT INTO bronze.api_readings
                         (source, api_name, endpoint, zone, stream_id, raw_payload, http_status, file_path)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id"""
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        source, fields.get("apiName", source.lower()),
                        fields.get("endpoint", ""),
                        zone, msg_id,
                        json.dumps({**payload if isinstance(payload, dict) else {},
                                    "_meta": {"immutable": True, "sse": "SSE-S3-KMS"}}),
                        int(fields.get("httpStatus", 200) or 200),
                        s3_path(source, payload, zone),
                    ))
                    rec_id = cur.fetchone()[0]

            span.set_tag("bronze.id", str(rec_id)[:8])
            return rec_id
        except Exception as e:
            span.set_tag("error", True)
            raise


def consumer_loop(r, conn):
    consumer  = f"bronze-{uuid.uuid4().hex[:8]}"
    all_streams = IOT_STREAMS + API_STREAMS
    log.info("Bronze consumer '%s' iniciado, streams=%d", consumer, len(all_streams))

    while True:
        try:
            with tracer.trace("bronze.consumer.read", resource="xreadgroup") as span:
                entries = r.xreadgroup(groupname=GROUP, consumername=consumer,
                                       streams={s: ">" for s in all_streams},
                                       count=40, block=2000)
            if not entries:
                continue

            for stream_name, messages in entries:
                is_iot = stream_name in IOT_STREAMS
                for msg_id, fields in messages:
                    t0     = time.monotonic()
                    source = fields.get("source", "UNKNOWN")
                    zone   = fields.get("zone", "unknown")

                    with tracer.trace("bronze.process_message", resource=source) as span:
                        span.set_tag("stream",  stream_name)
                        span.set_tag("source",  source)
                        span.set_tag("zone",    zone)
                        try:
                            bronze_id = insert_record(conn, source, fields, msg_id)

                            # Forward a ETL
                            r.xadd("wg:bronze:out", {
                                "bronzeId":  str(bronze_id),
                                "source":    source,
                                "zone":      zone,
                                "streamSrc": stream_name,
                                "payload":   fields.get("payload", "{}"),
                                "timestamp": fields.get("timestamp",
                                             datetime.now(timezone.utc).isoformat()),
                            }, maxlen=10000)

                            r.xack(stream_name, GROUP, msg_id)

                            latency_ms = (time.monotonic() - t0) * 1000
                            span.set_metric("latency_ms", latency_ms)

                            if is_iot:
                                counters["iot_ingested"] += 1
                            else:
                                counters["api_ingested"] += 1
                            counters["total"] += 1

                            metrics.record_ingestion(source, zone, success=True)
                            metrics.record_latency("bronze.ingest", latency_ms, source, zone)
                            metrics.record_stream_len(stream_name,
                                                      r.xlen(stream_name))

                            log.info("BRONZE STORED | src=%s zone=%s lat=%.1fms id=%s",
                                     source, zone, latency_ms, str(bronze_id)[:8],
                                     extra={"zone": zone, "source": source,
                                            "latency_ms": latency_ms})

                        except Exception as e:
                            counters["errors"] += 1
                            span.set_tag("error", True)
                            metrics.record_ingestion(source, zone, success=False)
                            log.error("Error bronze msg %s: %s", msg_id, e,
                                      extra={"zone": zone, "source": source})
                            r.xack(stream_name, GROUP, msg_id)

            # Service check periódico
            if counters["total"] % 100 == 0 and counters["total"] > 0:
                metrics.service_check("bronze.consumer", 0,
                                      f"OK total={counters['total']}")
                log.info("Bronze stats | iot=%d api=%d errors=%d",
                         counters["iot_ingested"], counters["api_ingested"],
                         counters["errors"])

        except Exception as e:
            log.error("Error en consumer_loop: %s", e)
            metrics.service_check("bronze.consumer", 2, str(e))
            time.sleep(5)


class BronzeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"status": "UP", "layer": "BRONZE",
                           "features": ["immutable","SSE-S3-KMS","versioned"],
                           "stats": counters}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass


def main():
    log.info("WildGuard Bronze Layer iniciando")
    conn = connect_pg()
    r    = connect_redis()
    ensure_groups(r)
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), BronzeHandler).serve_forever(),
        daemon=True).start()
    consumer_loop(r, conn)


if __name__ == "__main__":
    main()
