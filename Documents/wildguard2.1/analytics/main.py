"""
WildGuard – Analytics Service
============================================================
Etapa 7 del pipeline: ANALÍTICA & CONSUMO

Simula cuatro componentes del pipeline:

1. Amazon Athena → DuckDB/SQL queries sobre Silver
   - Consultas SQL serverless sobre los datos
   - Agrupaciones por zona, tipo sensor, riesgo

2. Amazon Redshift Serverless → PostgreSQL gold schema
   - Data Warehouse analítico
   - Materializa KPI snapshots cada 30s
   - Vistas para BI/ML/Data Science

3. Amazon OpenSearch Service → Elasticsearch local
   - Búsqueda y análisis de logs de actividad
   - Indexación de alertas y eventos

4. Amazon QuickSight → API REST de métricas
   - Expone dashboards y visualizaciones
   - Acceso: BI / ML / Data Science / Reporting

Consume: wg:silver:out (group: gold-aggregator)
Publica: wg:gold:kpis
"""

import os
import json
import time
import uuid
import logging
import threading
import requests
from collections import defaultdict
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
import psycopg2
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("analytics")
tracer  = get_tracer()
log     = get_logger("analytics")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [analytics] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("analytics")

REDIS_HOST  = os.getenv("REDIS_HOST", "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
PG_HOST     = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT     = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB       = os.getenv("POSTGRES_DB", "wildguard")
PG_USER     = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS     = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
ES_URL      = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
HTTP_PORT   = int(os.getenv("HTTP_PORT", 8084))
KPI_WINDOW  = 30  # segundos

# Buffer de ventana para KPIs
kpi_buffer: list[dict] = []
kpi_lock = threading.Lock()
latest_kpi: dict = {}
counters = {"processed": 0, "kpi_snapshots": 0, "es_indexed": 0, "errors": 0}

GROUP  = "analytics-consumer"
STREAM = "wg:gold:kpis"


# ── Conexiones ────────────────────────────────────────────────
def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS, connect_timeout=5
            )
            conn.autocommit = True
            log.info("✓ PostgreSQL conectado (Redshift simulado)")
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


def ensure_es_index():
    """Crea índice Elasticsearch para logs y alertas WildGuard."""
    try:
        resp = requests.put(
            f"{ES_URL}/wildguard-events",
            json={"settings": {"number_of_shards": 1, "number_of_replicas": 0},
                  "mappings": {"properties": {
                      "timestamp": {"type": "date"},
                      "zone":      {"type": "keyword"},
                      "source":    {"type": "keyword"},
                      "riskLevel": {"type": "keyword"},
                      "fwi":       {"type": "float"},
                  }}},
            timeout=5
        )
        if resp.status_code in (200, 400):
            log.info("✓ Elasticsearch index 'wildguard-events' listo")
    except Exception as e:
        log.warning("Elasticsearch no disponible: %s — continuando sin ES", e)


def index_to_es(doc: dict):
    """Indexa evento en Elasticsearch (OpenSearch simulado)."""
    try:
        resp = requests.post(
            f"{ES_URL}/wildguard-events/_doc",
            json={**doc, "@timestamp": datetime.now(timezone.utc).isoformat()},
            timeout=3
        )
        if resp.status_code == 201:
            counters["es_indexed"] += 1
    except Exception:
        pass  # ES opcional


# ── Redshift: KPI aggregation ─────────────────────────────────
def compute_kpi(records: list[dict]) -> dict:
    """Simula consulta Redshift analítica (Athena SQL sobre Silver)."""
    if not records:
        return {}

    temps, hums, co2s, winds = [], [], [], []
    w_temps, w_hums, w_winds = [], [], []
    fwis   = []
    zone_fwi  = defaultdict(list)
    risk_dist = defaultdict(int)
    stype_dist = defaultdict(int)
    alert_count = crit_count = 0

    for rec in records:
        fwi   = float(rec.get("fwi", 0))
        level = rec.get("riskLevel", "LOW")
        zone  = rec.get("zone", "unknown")
        source = rec.get("source", "")

        fwis.append(fwi)
        zone_fwi[zone].append(fwi)
        risk_dist[level] += 1

        if level in ("HIGH", "CRITICAL"): alert_count += 1
        if level == "CRITICAL":           crit_count  += 1

        payload = json.loads(rec.get("payload", "{}"))
        if source == "IOT_SENSOR":
            stype = payload.get("sensorType", "UNKNOWN")
            stype_dist[stype] += 1
            if t := payload.get("temperatureC"): temps.append(float(t))
            if h := payload.get("humidityPct"):  hums.append(float(h))
            if c := payload.get("co2Ppm"):        co2s.append(float(c))
            if w := payload.get("windKmh"):        winds.append(float(w))
        elif source == "WEATHER":
            if t := payload.get("temperatureC"): w_temps.append(float(t))
            if h := payload.get("humidityPct"):  w_hums.append(float(h))
            if w := payload.get("windMs"):        w_winds.append(float(w))

    def avg(lst): return round(sum(lst)/len(lst), 2) if lst else None

    top_zones = sorted(
        [{"zone": z, "avgFwi": round(sum(v)/len(v), 3)}
         for z, v in zone_fwi.items()],
        key=lambda x: x["avgFwi"], reverse=True
    )[:5]

    n = len(records)
    kpi = {
        "kpiId":           str(uuid.uuid4()),
        "generatedAt":     datetime.now(timezone.utc).isoformat(),
        "windowSeconds":   KPI_WINDOW,
        "totalRecords":    n,
        "totalIot":        sum(1 for r in records if r.get("source") == "IOT_SENSOR"),
        "totalApi":        sum(1 for r in records if r.get("source") in ("WEATHER","AIR_QUALITY","GPS")),
        "totalAlerts":     alert_count,
        "criticalAlerts":  crit_count,
        # Promedios IoT
        "avgTemperature":  avg(temps),
        "avgHumidity":     avg(hums),
        "avgCo2Ppm":       avg(co2s),
        "avgWindKmh":      avg(winds),
        # Promedios clima
        "avgWeatherTemp":  avg(w_temps),
        "avgWeatherHum":   avg(w_hums),
        "avgWeatherWind":  avg(w_winds),
        # Riesgo
        "avgFwi":          avg(fwis),
        "maxFwi":          round(max(fwis), 3) if fwis else 0,
        # Vistas analíticas (QuickSight simulado)
        "topRiskZones":    top_zones,
        "riskDistribution": dict(risk_dist),
        "bySensorType":    dict(stype_dist),
        "dataSources":     ["IoT-Sensores", "GPS", "Open-Meteo", "Air-Quality"],
        "accessProfiles":  ["BI", "ML", "DataScience", "Reporting"],
        "slaCompliance":   round((n - counters["errors"]) / n * 100, 2) if n else 100.0,
    }
    return kpi


def persist_kpi(conn, kpi: dict):
    """Persiste KPI en gold.kpi_snapshots (Redshift simulado)."""
    sql = """
        INSERT INTO gold.kpi_snapshots (
            window_minutes, total_iot, total_api, total_alerts, critical_alerts,
            avg_temp, avg_humidity, avg_co2, avg_wind,
            avg_weather_temp, avg_weather_hum, avg_weather_wind,
            avg_fwi, max_fwi, sla_compliance_pct,
            top_risk_zones, risk_distribution, by_sensor_type
        ) VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            1,
            kpi.get("totalIot"), kpi.get("totalApi"),
            kpi.get("totalAlerts"), kpi.get("criticalAlerts"),
            kpi.get("avgTemperature"), kpi.get("avgHumidity"),
            kpi.get("avgCo2Ppm"), kpi.get("avgWindKmh"),
            kpi.get("avgWeatherTemp"), kpi.get("avgWeatherHum"), kpi.get("avgWeatherWind"),
            kpi.get("avgFwi"), kpi.get("maxFwi"), kpi.get("slaCompliance"),
            json.dumps(kpi.get("topRiskZones", [])),
            json.dumps(kpi.get("riskDistribution", {})),
            json.dumps(kpi.get("bySensorType", {})),
        ))


def kpi_loop(conn, r: redis.Redis):
    """Loop de materialización de KPIs cada KPI_WINDOW segundos."""
    while True:
        time.sleep(KPI_WINDOW)
        with kpi_lock:
            snapshot = list(kpi_buffer)
            kpi_buffer.clear()

        if not snapshot:
            log.info("Analytics KPI: buffer vacío")
            continue

        kpi = compute_kpi(snapshot)
        try:
            persist_kpi(conn, kpi)
            counters["kpi_snapshots"] += 1
        except Exception as e:
            log.error("Error persistiendo KPI: %s", e)

        global latest_kpi
        latest_kpi = kpi

        # Publicar KPI en Redis y ES
        r.set("wg:latest:kpi", json.dumps(kpi))
        r.xadd("wg:gold:kpis", {"kpi": json.dumps(kpi)}, maxlen=500)
        index_to_es({"type": "kpi", **{k: str(v)[:200]
                                        for k, v in kpi.items()
                                        if not isinstance(v, (list, dict))}})

        statsd.gauge("wg.gold.fwi_avg",    kpi.get("avgFwi") or 0)
        statsd.gauge("wg.gold.alerts",     kpi.get("totalAlerts") or 0)
        statsd.gauge("wg.gold.critical",   kpi.get("criticalAlerts") or 0)
        statsd.gauge("wg.gold.sla_pct",    kpi.get("slaCompliance") or 100)

        log.info("ANALYTICS KPI | records=%d alerts=%d crit=%d avgFwi=%.3f "
                 "maxFwi=%.3f topZone=%s",
                 kpi.get("totalRecords", 0), kpi.get("totalAlerts", 0),
                 kpi.get("criticalAlerts", 0), kpi.get("avgFwi") or 0,
                 kpi.get("maxFwi") or 0,
                 (kpi.get("topRiskZones") or [{}])[0].get("zone", "-"))


def consumer_loop(r: redis.Redis, conn):
    """
    Consume KPIs materializados desde Gold Layer (wg:gold:kpis).
    También consume alertas Gold (wg:gold:alerts) para indexar en OpenSearch.
    Analytics SOLO lee desde Gold — respeta la arquitectura Medallion.
    """
    consumer = f"analytics-{uuid.uuid4().hex[:8]}"
    log.info("Analytics consumer '%s' listo | Gold streams: %s, wg:gold:alerts", consumer, STREAM)

    for stream in (STREAM, "wg:gold:alerts"):
        try:
            r.xgroup_create(stream, GROUP, id="0", mkstream=True)
        except redis.ResponseError:
            pass

    global latest_kpi

    while True:
        try:
            entries = r.xreadgroup(
                groupname=GROUP, consumername=consumer,
                streams={STREAM: ">", "wg:gold:alerts": ">"},
                count=20, block=2000,
            )
            if not entries:
                continue

            for stream_name, messages in entries:
                for msg_id, fields in messages:
                    with tracer.trace("analytics.consume_gold",
                                      resource=stream_name.split(":")[-1]) as span:
                        try:
                            if stream_name == STREAM:
                                # KPI completo desde Gold Layer
                                kpi_raw = fields.get("kpi", "{}")
                                kpi_obj = json.loads(kpi_raw) if kpi_raw else {}
                                if kpi_obj:
                                    latest_kpi = kpi_obj
                                    counters["processed"] += 1
                                    span.set_metric("records", kpi_obj.get("totalRecords",0))
                                    span.set_metric("avg_fwi", kpi_obj.get("avgFwi") or 0)
                                    # Indexar KPI en Elasticsearch (OpenSearch simulado)
                                    index_to_es({
                                        "type":          "gold-kpi",
                                        "totalRecords":  kpi_obj.get("totalRecords"),
                                        "avgFwi":        str(kpi_obj.get("avgFwi") or 0),
                                        "totalAlerts":   kpi_obj.get("totalAlerts"),
                                        "criticalAlerts": kpi_obj.get("criticalAlerts"),
                                        "slaCompliance": kpi_obj.get("slaCompliance"),
                                        "timestamp":     kpi_obj.get("generatedAt"),
                                    })
                                    metrics.increment("analytics.gold_kpi_received")
                                    log.info("Analytics KPI Gold | records=%s avgFwi=%s alerts=%s",
                                             kpi_obj.get("totalRecords"),
                                             kpi_obj.get("avgFwi"),
                                             kpi_obj.get("totalAlerts"),
                                             extra={"source": "GOLD",
                                                    "fwi": kpi_obj.get("avgFwi") or 0})

                            elif stream_name == "wg:gold:alerts":
                                # Alerta desde Gold Layer → indexar en ES
                                index_to_es({
                                    "type":      "gold-alert",
                                    "zone":      fields.get("zone"),
                                    "riskLevel": fields.get("riskLevel"),
                                    "fwi":       fields.get("fwi"),
                                    "message":   fields.get("message"),
                                    "layer":     fields.get("layer", "GOLD"),
                                    "timestamp": fields.get("firedAt"),
                                })
                                metrics.increment("analytics.gold_alert_indexed",
                                                  tags=[f"level:{fields.get('riskLevel','').lower()}"])
                                log.info("Analytics indexó alerta Gold | zone=%s level=%s",
                                         fields.get("zone"), fields.get("riskLevel"),
                                         extra={"zone": fields.get("zone"),
                                                "risk_level": fields.get("riskLevel")})

                            r.xack(stream_name, GROUP, msg_id)

                        except Exception as e:
                            counters["errors"] += 1
                            span.set_tag("error", True)
                            log.error("Analytics error msg %s: %s", msg_id, e)
                            r.xack(stream_name, GROUP, msg_id)

            metrics.service_check("analytics.consumer", 0,
                                  f"OK processed={counters['processed']}")

        except Exception as e:
            log.error("Error en analytics consumer_loop: %s", e)
            metrics.service_check("analytics.consumer", 2, str(e))
            time.sleep(5)


# ── HTTP: QuickSight API simulada ─────────────────────────────
class AnalyticsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/kpi" or self.path == "/":
            body = json.dumps(latest_kpi or {"message": "sin datos aún"})
        elif self.path == "/health":
            body = json.dumps({
                "status": "UP",
                "components": {
                    "athena":   "simulated (DuckDB/SQL)",
                    "redshift": "simulated (PostgreSQL gold)",
                    "opensearch": "elasticsearch:9200",
                    "quicksight": "this HTTP API",
                },
                "stats": counters,
            })
        else:
            body = json.dumps({"error": "not found"})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args): pass


def main():
    log.info("════════════════════════════════════════════")
    log.info("  WildGuard Analytics Service iniciando")
    log.info("  Athena + Redshift + OpenSearch + QuickSight")
    log.info("════════════════════════════════════════════")

    conn = connect_pg()
    r    = connect_redis()

    ensure_es_index()

    threading.Thread(target=kpi_loop, args=(conn, r), daemon=True).start()
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), AnalyticsHandler).serve_forever(),
        daemon=True
    ).start()
    log.info("QuickSight API en :%d", HTTP_PORT)

    consumer_loop(r, conn)


if __name__ == "__main__":
    main()
