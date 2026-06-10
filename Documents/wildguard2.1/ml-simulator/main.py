"""
WildGuard – ML Simulator
============================================================
Simula Amazon SageMaker: inferencia de modelo ML de riesgo de incendio.

Modelo simulado: clasificador binario FWI Predictor v1.2
  - Input: features IoT + clima + GPS altitud
  - Output: fire_probability (0-1), risk_class, confidence, SHAP
  - AUC simulado: ~0.82 (como definido en el proyecto)

Consume:  wg:silver:out  (group: ml-inference)
Publica:  wg:gold:ml → Dashboard / Alert Service
Persiste: gold.ml_predictions (PostgreSQL)
"""

import os
import json
import uuid
import time
import random
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
import psycopg2
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("ml-simulator")
tracer  = get_tracer()
log     = get_logger("ml-simulator")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ml-simulator] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("ml-simulator")

REDIS_HOST    = os.getenv("REDIS_HOST", "redis")
REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
PG_HOST       = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT       = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB         = os.getenv("POSTGRES_DB", "wildguard")
PG_USER       = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
HTTP_PORT     = int(os.getenv("HTTP_PORT", 8085))
INTERVAL_S    = int(os.getenv("ML_INFERENCE_INTERVAL_MS", 20000)) / 1000
MODEL_VER     = os.getenv("ML_MODEL_VERSION", "wg-fwi-classifier-v1.2")
THRESHOLD     = float(os.getenv("ML_THRESHOLD", 0.50))

counters = {"inferences": 0, "fire_risk": 0, "no_risk": 0, "errors": 0}
GROUP  = "ml-inference"
STREAM = "wg:silver:out"


def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS, connect_timeout=5
            )
            conn.autocommit = True
            log.info("✓ PostgreSQL conectado (SageMaker → gold.ml_predictions)")
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


def simulate_inference(payload: dict, fwi: float) -> dict:
    """
    Simula inferencia SageMaker. Modelo: clasificador binario de riesgo.
    Reproduce comportamiento de un modelo entrenado con datos históricos CONAF.
    
    Features y pesos (SHAP simplificado):
      - temperature_c  → 35% de importancia
      - humidity_pct   → 25%
      - fwi_score      → 20%
      - wind_kmh       → 10%
      - co2_ppm        →  5%
      - smoke_density  →  5%
    """
    temp  = float(payload.get("temperatureC") or 0)
    hum   = float(payload.get("humidityPct")  or 50)
    wind  = float(payload.get("windKmh")       or 0)
    co2   = float(payload.get("co2Ppm")        or 415)
    smoke = float(payload.get("smokeDensity")  or 0)
    alt   = float(payload.get("altitudeM")     or 0)

    # Probabilidad base (función determinista del modelo)
    shap_values = {
        "temperature_c":  0.35 * min(temp / 45, 1.0),
        "humidity_pct":   0.25 * max(1 - hum / 80, 0),
        "fwi_score":      0.20 * fwi,
        "wind_kmh":       0.10 * min(wind / 80, 1.0),
        "co2_ppm":        0.05 * min((co2 - 380) / 300, 1.0),
        "smoke_density":  0.05 * min(smoke / 0.5, 1.0),
    }
    base_prob = sum(shap_values.values())

    # Modificador altitud (zonas altas tienen menor humedad)
    if alt > 1500:
        base_prob *= 1.05

    # Ruido gaussiano: simula varianza del modelo real
    noise = random.gauss(0, 0.04)
    prob  = max(0.0, min(1.0, base_prob + noise))

    # Confianza: mayor cuando está lejos del umbral 0.5
    conf = 0.50 + abs(prob - 0.50) * 0.82 + random.gauss(0, 0.02)
    conf = round(min(0.99, max(0.51, conf)), 3)

    top_feature = max(shap_values, key=shap_values.get)

    return {
        "modelVersion":    MODEL_VER,
        "inferenceId":     str(uuid.uuid4()),
        "inferredAt":      datetime.now(timezone.utc).isoformat(),
        "fireProbability": round(prob, 4),
        "riskClass":       "FIRE_RISK" if prob >= THRESHOLD else "NO_RISK",
        "confidence":      conf,
        "shapTopFeature":  top_feature,
        "shapValues":      {k: round(v, 4) for k, v in shap_values.items()},
        "features":        {
            "temperatureC": temp, "humidityPct": hum,
            "windKmh":      wind, "co2Ppm":      co2,
            "smokeDensity": smoke, "fwiScore":   fwi,
        },
        "threshold":       THRESHOLD,
        "simulatedAUC":    0.82,
    }


def persist_prediction(conn, silver_id: str, zone: str, pred: dict):
    sql = """
        INSERT INTO gold.ml_predictions
            (zone, model_version, fire_probability, risk_class,
             confidence, shap_top_feature, features, silver_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            zone,
            pred["modelVersion"],
            pred["fireProbability"],
            pred["riskClass"],
            pred["confidence"],
            pred["shapTopFeature"],
            json.dumps(pred["features"]),
            silver_id if silver_id else None,
        ))


def consumer_loop(r: redis.Redis, conn):
    consumer = f"ml-{uuid.uuid4().hex[:8]}"
    log.info("ML consumer '%s' listo en '%s'", consumer, STREAM)

    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.ResponseError:
        pass

    while True:
        try:
            entries = r.xreadgroup(
                groupname=GROUP, consumername=consumer,
                streams={STREAM: ">"},
                count=30, block=int(INTERVAL_S * 1000),
            )
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, fields in messages:
                    t0 = time.monotonic()
                    try:
                        silver_id = fields.get("silverId", "")
                        zone      = fields.get("zone", "unknown")
                        fwi       = float(fields.get("fwi", 0) or 0)
                        payload   = json.loads(fields.get("payload", "{}"))

                        pred = simulate_inference(payload, fwi)

                        # Persistir
                        persist_prediction(conn, silver_id, zone, pred)

                        # Publicar en gold:ml stream
                        r.xadd("wg:gold:ml", {
                            "silverId":        silver_id,
                            "zone":            zone,
                            "fireProbability": str(pred["fireProbability"]),
                            "riskClass":       pred["riskClass"],
                            "confidence":      str(pred["confidence"]),
                            "shapTopFeature":  pred["shapTopFeature"],
                            "modelVersion":    MODEL_VER,
                            "prediction":      json.dumps(pred),
                            "timestamp":       datetime.now(timezone.utc).isoformat(),
                        }, maxlen=2000)

                        r.xack(STREAM, GROUP, msg_id)
                        counters["inferences"] += 1

                        if pred["riskClass"] == "FIRE_RISK":
                            counters["fire_risk"] += 1
                        else:
                            counters["no_risk"] += 1

                        latency_ms = (time.monotonic() - t0) * 1000
                        statsd.histogram("wg.ml.latency_ms", latency_ms)
                        statsd.gauge("wg.ml.fire_probability",
                                     pred["fireProbability"],
                                     tags=[f"zone:{zone}"])
                        statsd.increment("wg.ml.inferences",
                                         tags=[f"class:{pred['riskClass'].lower()}"])

                        log.info("ML INFERENCE | zone=%s prob=%.4f class=%s "
                                 "conf=%.3f feat=%s lat=%.1fms",
                                 zone, pred["fireProbability"], pred["riskClass"],
                                 pred["confidence"], pred["shapTopFeature"], latency_ms)

                    except Exception as e:
                        counters["errors"] += 1
                        log.error("ML error msg %s: %s", msg_id, e)
                        r.xack(STREAM, GROUP, msg_id)

        except Exception as e:
            log.error("Error en ML consumer_loop: %s", e)
            time.sleep(5)


class MLHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            "status":       "UP",
            "component":    "ml-simulator",
            "model":        MODEL_VER,
            "simulatedAUC": 0.82,
            "threshold":    THRESHOLD,
            "features":     ["temperature_c","humidity_pct","fwi_score",
                             "wind_kmh","co2_ppm","smoke_density"],
            "stats":        counters,
        })
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args): pass


def main():
    log.info("════════════════════════════════════════════")
    log.info("  WildGuard ML Simulator iniciando")
    log.info("  Modelo: %s | AUC=0.82 | Threshold=%.2f", MODEL_VER, THRESHOLD)
    log.info("════════════════════════════════════════════")

    conn = connect_pg()
    r    = connect_redis()

    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), MLHandler).serve_forever(),
        daemon=True
    ).start()
    log.info("HTTP health en :%d", HTTP_PORT)

    # Esperar datos en pipeline
    log.info("Esperando 25s para que el pipeline tenga datos...")
    time.sleep(25)

    consumer_loop(r, conn)


if __name__ == "__main__":
    main()
