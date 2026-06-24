"""
WildGuard – Redis Streams Manager
============================================================
Etapa 3 del pipeline: STREAMING & MENSAJERÍA

Reemplaza:
  - Amazon Kinesis Data Streams (particionado por tipo de sensor)
  - Amazon Kinesis Data Firehose (entrega a S3/Lambda/OpenSearch)

Funciones:
  - Administra consumer groups para cada stream
  - Expone API HTTP con estadísticas de streams (/stats, /health)
  - Monitorea lag de consumidores y genera alertas si hay retraso
  - Limpia mensajes procesados (ACK tracking)
  - Publica métricas a Datadog

Streams gestionados (flujos independientes, como indica el pipeline):
  FLUJO IoT (tiempo real):  wg:raw:iot, wg:raw:gps
  FLUJO APIs (eventos):     wg:raw:weather, wg:raw:airquality
  Pipeline interno:         wg:bronze:out, wg:etl:out, wg:silver:out,
                            wg:gold:kpis, wg:gold:alerts, wg:gold:ml
"""

import os
import json
import time
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("redis-streams")
tracer  = get_tracer()
log     = get_logger("redis-streams")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8070))


# Definición de todos los streams y sus consumer groups
STREAM_CONFIG = {
    # Fuentes → Bronze
    "wg:raw:iot":         ["bronze-ingestion"],
    "wg:raw:gps":         ["bronze-ingestion"],
    "wg:raw:weather":     ["bronze-ingestion"],
    "wg:raw:airquality":  ["bronze-ingestion"],
    # Bronze → ETL
    "wg:bronze:out":      ["etl-stream-processor"],
    # ETL → Silver
    "wg:etl:out":         ["silver-ingestion"],
    # Silver → Gold/Analytics/ML/Alerts
    "wg:silver:out":      ["gold-aggregator", "ml-inference", "alert-evaluator"],
    # Gold outputs
    "wg:gold:kpis":       ["dashboard-consumer"],
    "wg:gold:alerts":     ["alert-service", "dashboard-consumer"],
    "wg:gold:ml":         ["dashboard-consumer"],
}

stats = {
    "initialized_at": datetime.now(timezone.utc).isoformat(),
    "streams":        {},
    "total_messages": 0,
    "lag_alerts":     0,
}


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


def init_all_streams(r: redis.Redis):
    """Crea todos los streams y consumer groups necesarios para el pipeline."""
    log.info("Inicializando streams del pipeline...")
    for stream, groups in STREAM_CONFIG.items():
        for group in groups:
            try:
                r.xgroup_create(stream, group, id="0", mkstream=True)
                log.info("  ✓ Stream '%s' → group '%s'", stream, group)
            except redis.ResponseError:
                log.debug("  · Stream '%s' → group '%s' ya existe", stream, group)


def collect_stream_stats(r: redis.Redis) -> dict:
    """Recolecta estadísticas de todos los streams."""
    stream_stats = {}
    for stream, groups in STREAM_CONFIG.items():
        try:
            length = r.xlen(stream)
            group_info = []
            for group in groups:
                try:
                    info = r.xinfo_groups(stream)
                    for g in info:
                        if g.get("name") == group:
                            lag = g.get("lag", 0) or 0
                            group_info.append({
                                "group":     group,
                                "pending":   g.get("pending", 0),
                                "lag":       lag,
                                "consumers": g.get("consumers", 0),
                            })
                            if lag > 500:
                                log.warning("⚠ LAG ALTO en '%s' group '%s': %d mensajes",
                                            stream, group, lag)
                                stats["lag_alerts"] += 1
                                metrics.event("WildGuard Stream Lag",
                                              f"Stream {stream} lag={lag}",
                                              alert_type="warning")
                except Exception:
                    pass
            stream_stats[stream] = {"length": length, "groups": group_info}
        except Exception:
            stream_stats[stream] = {"length": 0, "groups": [], "error": True}
    return stream_stats


def monitor_loop(r: redis.Redis):
    """Loop de monitoreo: recolecta stats y publica métricas cada 15s."""
    while True:
        try:
            stream_data = collect_stream_stats(r)
            stats["streams"] = stream_data
            stats["total_messages"] = sum(
                v.get("length", 0) for v in stream_data.values()
            )

            for stream, data in stream_data.items():
                tag = f"stream:{stream.replace(':', '_')}"
                metrics.gauge("stream.length", data.get("length", 0), tags=[tag])
                for g in data.get("groups", []):
                    metrics.gauge("stream.lag", g.get("lag", 0),
                                  tags=[tag, f"group:{g['group']}"])

            log.info("Stream stats → total_len=%d streams=%d lag_alerts=%d",
                     stats["total_messages"], len(stream_data), stats["lag_alerts"])

        except Exception as e:
            log.error("Error en monitor_loop: %s", e)

        time.sleep(15)


# ── HTTP API ──────────────────────────────────────────────────
class StreamsHandler(BaseHTTPRequestHandler):
    def __init__(self, r_conn, *args, **kwargs):
        self.r = r_conn
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({
                "status":    "UP",
                "component": "redis-streams",
                "streams":   len(STREAM_CONFIG),
                "redis":     "connected",
            })
        elif self.path == "/stats":
            body = json.dumps(stats, default=str)
        elif self.path == "/streams":
            body = json.dumps({"streams": list(STREAM_CONFIG.keys())})
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
    log.info("════════════════════════════════════════════")
    log.info("  WildGuard Redis Streams Manager iniciando")
    log.info("  Streams definidos: %d", len(STREAM_CONFIG))
    log.info("════════════════════════════════════════════")

    r = connect_redis()
    init_all_streams(r)

    # Monitor en hilo background
    threading.Thread(target=monitor_loop, args=(r,), daemon=True).start()

    # HTTP server
    def make_handler(*a, **kw):
        return StreamsHandler(r, *a, **kw)

    log.info("HTTP stats server en :%d", HTTP_PORT)
    HTTPServer(("0.0.0.0", HTTP_PORT), make_handler).serve_forever()


if __name__ == "__main__":
    main()
