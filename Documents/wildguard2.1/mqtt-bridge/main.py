"""
WildGuard – MQTT Bridge
============================================================
Equivale a: AWS IoT Core + IoT Rules Engine + Amazon Timestream

Función:
  - Suscribe a todos los tópicos MQTT de Mosquitto
  - Aplica reglas de enrutamiento (IoT Rules Engine simulado)
  - Reenvía mensajes a los Redis Streams correspondientes
  - Registra series temporales (Timestream simulado en Redis)
  - Expone API HTTP con estadísticas de tópicos activos

Tópicos MQTT suscritos:
  wildguard/sensors/iot/#    → wg:raw:iot
  wildguard/sensors/gps/#   → wg:raw:gps
  wildguard/alerts/fire      → wg:raw:alerts (stream directo a gold)

QoS de suscripción: 1 (at least once)

Reglas simuladas (IoT Rules Engine):
  RULE-01: sensor_type=SMOKE & smokeDensity>0.3 → también a wg:raw:alerts
  RULE-02: source=GPS → wg:raw:gps
  RULE-03: todos los IoT → wg:raw:iot
"""

import os
import json
import uuid
import time
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import defaultdict

import paho.mqtt.client as mqtt
import redis
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("mqtt-bridge")
tracer  = get_tracer()
log     = get_logger("mqtt-bridge")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [mqtt-bridge] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("mqtt-bridge")

# ── Config ────────────────────────────────────────────────────
MQTT_HOST  = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT  = int(os.getenv("MQTT_PORT", 1883))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8075))


# Tópicos a suscribir
SUBSCRIPTIONS = [
    ("wildguard/sensors/iot/#",  1),
    ("wildguard/sensors/gps/#",  1),
    ("wildguard/alerts/fire",    2),
]

# Contadores y estadísticas
counters = {
    "received":   0,
    "forwarded":  0,
    "alerts":     0,
    "errors":     0,
    "by_topic":   defaultdict(int),
}

# Timestream simulado: últimas lecturas por dispositivo (en memoria)
timestream: dict = {}   # device_id → {ts, value}

# Redis compartido entre callbacks MQTT (hilo paho) y HTTP
_redis_client: redis.Redis | None = None


# ── Redis ─────────────────────────────────────────────────────
def connect_redis() -> redis.Redis:
    for i in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            log.info("✓ Redis conectado (%s:%s)", REDIS_HOST, REDIS_PORT)
            return r
        except Exception as e:
            log.warning("Redis no disponible (%d/15): %s", i + 1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def ensure_groups(r: redis.Redis):
    for stream in ("wg:raw:iot", "wg:raw:gps", "wg:raw:alerts"):
        try:
            r.xgroup_create(stream, "bronze-ingestion", id="0", mkstream=True)
        except redis.ResponseError:
            pass


# ── IoT Rules Engine simulado ─────────────────────────────────
def apply_rules(topic: str, event: dict) -> list[tuple[str, dict]]:
    """
    Evalúa reglas y retorna lista de (stream, payload) a publicar.
    Equivale a AWS IoT Rules Engine.
    """
    routes  = []
    payload = event.get("payload", {})
    source  = event.get("source", "")

    # RULE-03: Sensores IoT → stream bronze iot
    if "sensors/iot" in topic or source == "IOT_SENSOR":
        routes.append(("wg:raw:iot", event))

        # RULE-01: Humo crítico → también stream de alertas directas
        if isinstance(payload, dict):
            smoke = float(payload.get("smokeDensity") or 0)
            if smoke >= 0.3:
                alert_event = {
                    **event,
                    "ruleTriggered": "RULE-01-SMOKE",
                    "alertLevel":    "HIGH" if smoke >= 0.5 else "MEDIUM",
                }
                routes.append(("wg:raw:alerts", alert_event))
                log.warning("⚡ IoT Rule RULE-01 activada | smoke=%.3f zona=%s",
                            smoke, event.get("zone"))

    # RULE-02: GPS → stream bronze gps
    elif "sensors/gps" in topic or source == "GPS":
        routes.append(("wg:raw:gps", event))

    # Alertas directas desde sensores
    elif topic == "wildguard/alerts/fire":
        routes.append(("wg:raw:alerts", event))
        counters["alerts"] += 1

    return routes


def record_timestream(event: dict):
    """
    Registra la última lectura de cada dispositivo.
    Simula Amazon Timestream (series temporales).
    """
    device_id = event.get("deviceId", "unknown")
    payload   = event.get("payload", {})
    timestream[device_id] = {
        "ts":    event.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "zone":  event.get("zone"),
        "value": payload if isinstance(payload, dict) else {},
    }
    # Mantener solo últimos 500 dispositivos en memoria
    if len(timestream) > 500:
        oldest = next(iter(timestream))
        del timestream[oldest]


# ── Callbacks MQTT ────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("✓ Conectado a Mosquitto (%s:%d)", MQTT_HOST, MQTT_PORT)
        for topic, qos in SUBSCRIPTIONS:
            client.subscribe(topic, qos=qos)
            log.info("  Suscrito a '%s' (QoS=%d)", topic, qos)
    else:
        log.error("Fallo conexión MQTT: rc=%d", rc)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning("Desconexión inesperada de Mosquitto (rc=%d) — reconectando...", rc)


def on_message(client, userdata, msg):
    global _redis_client
    topic   = msg.topic
    counters["received"]        += 1
    counters["by_topic"][topic] += 1

    try:
        raw  = json.loads(msg.payload.decode("utf-8"))
        zone = raw.get("zone", topic.split("/")[-2] if "/" in topic else "unknown")

        # Asegurar que payload sea dict
        if isinstance(raw.get("payload"), str):
            try:
                raw["payload"] = json.loads(raw["payload"])
            except Exception:
                pass

        # Registrar en Timestream simulado
        record_timestream(raw)

        # Aplicar reglas de enrutamiento
        routes = apply_rules(topic, raw)

        for stream, event in routes:
            flat_event = {
                "eventId":   event.get("eventId", str(uuid.uuid4())),
                "source":    event.get("source", "IOT_SENSOR"),
                "deviceId":  event.get("deviceId", ""),
                "zone":      zone,
                "timestamp": event.get("timestamp", datetime.now(timezone.utc).isoformat()),
                "mqttTopic": topic,
                "payload":   json.dumps(event.get("payload", {})),
            }
            # Campos extra si los tiene
            if "ruleTriggered" in event:
                flat_event["ruleTriggered"] = event["ruleTriggered"]
                flat_event["alertLevel"]    = event.get("alertLevel", "")

            _redis_client.xadd(stream, flat_event, maxlen=8000)
            counters["forwarded"] += 1

            statsd.increment("wg.mqtt_bridge.forwarded",
                             tags=[f"stream:{stream.replace(':', '_')}",
                                   f"topic:{topic.split('/')[2] if '/' in topic else topic}"])

        log.info("MQTT MSG | topic=%-40s zone=%-15s → %d stream(s)",
                 topic, zone, len(routes))

    except Exception as e:
        counters["errors"] += 1
        log.error("Error procesando msg MQTT topic='%s': %s", topic, e)

    # Métricas Datadog
    if counters["received"] % 50 == 0:
        statsd.gauge("wg.mqtt_bridge.received",  counters["received"])
        statsd.gauge("wg.mqtt_bridge.forwarded", counters["forwarded"])
        statsd.gauge("wg.timestream.devices",    len(timestream))


# ── HTTP API ──────────────────────────────────────────────────
class BridgeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({
                "status":       "UP",
                "component":    "mqtt-bridge",
                "equivalentTo": "AWS IoT Core + IoT Rules Engine + Timestream",
                "broker":       f"{MQTT_HOST}:{MQTT_PORT}",
                "subscriptions": [t for t, _ in SUBSCRIPTIONS],
                "stats":        {**counters,
                                 "by_topic": dict(counters["by_topic"])},
            })
        elif self.path == "/timestream":
            # Últimas lecturas por dispositivo (Timestream simulado)
            body = json.dumps({
                "devices": len(timestream),
                "data":    dict(list(timestream.items())[:50]),
            }, default=str)
        elif self.path == "/topics":
            body = json.dumps({
                "active_topics": dict(counters["by_topic"]),
                "subscriptions": [{"topic": t, "qos": q}
                                  for t, q in SUBSCRIPTIONS],
            })
        else:
            body = json.dumps({"error": "not found"})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args): pass


# ── Main ──────────────────────────────────────────────────────
def main():
    global _redis_client

    log.info("════════════════════════════════════════════════════")
    log.info("  WildGuard MQTT Bridge iniciando")
    log.info("  Equivale a: AWS IoT Core + Rules Engine + Timestream")
    log.info("  Broker MQTT: %s:%d", MQTT_HOST, MQTT_PORT)
    log.info("════════════════════════════════════════════════════")

    _redis_client = connect_redis()
    ensure_groups(_redis_client)

    # Cliente MQTT
    client = mqtt.Client(client_id=f"wg-mqtt-bridge-{uuid.uuid4().hex[:8]}")
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    # Reintentos de conexión al broker
    for attempt in range(15):
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            log.info("Conectando a Mosquitto (intento %d)...", attempt + 1)
            break
        except Exception as e:
            log.warning("Mosquitto no disponible (%d/15): %s", attempt + 1, e)
            time.sleep(3)
    else:
        raise RuntimeError("No se pudo conectar a Mosquitto")

    # HTTP stats server en hilo separado
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), BridgeHandler).serve_forever(),
        daemon=True
    ).start()
    log.info("HTTP stats en :%d (/health /timestream /topics)", HTTP_PORT)

    # Loop MQTT bloqueante (maneja reconexión automática)
    client.loop_forever(retry_first_connection=True)


if __name__ == "__main__":
    main()
