"""
WildGuard – Gold Layer
============================================================
Etapa 6→7 del pipeline: SILVER → DATA LAKE GOLD

Equivale a:
  - Amazon Redshift Serverless  → materialización de KPIs
  - Amazon QuickSight            → vistas ejecutivas
  - AWS Step Functions           → orquesta ventanas de agregación
  - Amazon EventBridge           → dispara alertas Gold desde reglas

El Gold Layer es el único punto de verdad para:
  ✓ KPI snapshots ejecutivos  (gold.kpi_snapshots)
  ✓ Alertas de incendio       (gold.fire_alerts)
  ✓ Predicciones ML           (gold.ml_predictions)
  ✓ Streams para dashboard    (wg:gold:kpis, wg:gold:alerts, wg:gold:ml)

Flujo corregido del pipeline:
  wg:silver:out
       │
       ├─► gold-layer (ESTE COMPONENTE)
       │     ├─ Agrega ventana 30s → gold.kpi_snapshots
       │     ├─ Detecta riesgo → gold.fire_alerts
       │     └─ Publica → wg:gold:kpis / wg:gold:alerts
       │
       ├─► ml-simulator → gold.ml_predictions → wg:gold:ml
       └─► alert-service (SNS notificaciones) → wg:gold:alerts

Consumer groups del stream wg:silver:out:
  - gold-aggregator   (este componente)
  - ml-inference      (ml-simulator)
  - alert-evaluator   (alert-service)

HTTP API en :8087
  GET /health   → estado del componente
  GET /kpi      → último KPI materializado
  GET /alerts   → últimas alertas Gold
  GET /stats    → contadores internos
"""

import os
import json
import uuid
import time
import logging
import threading
from collections import defaultdict
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
import psycopg2
from telemetry import init_telemetry, get_tracer, get_logger

# ── Telemetría Datadog ────────────────────────────────────────
metrics = init_telemetry("gold-layer")
tracer  = get_tracer()
log     = get_logger("gold-layer")

# ── Config ────────────────────────────────────────────────────
REDIS_HOST   = os.getenv("REDIS_HOST", "redis")
REDIS_PORT   = int(os.getenv("REDIS_PORT", 6379))
PG_HOST      = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT      = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB        = os.getenv("POSTGRES_DB", "wildguard")
PG_USER      = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS      = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
HTTP_PORT    = int(os.getenv("HTTP_PORT", 8087))
KPI_WINDOW_S = int(os.getenv("GOLD_KPI_WINDOW_S", 30))

GROUP  = "gold-aggregator"
STREAM = "wg:silver:out"

# ── Estado en memoria ─────────────────────────────────────────
kpi_buffer: list[dict] = []
kpi_lock = threading.Lock()
latest_kpi: dict       = {}
recent_alerts: list    = []
counters = {
    "silver_consumed": 0,
    "kpi_snapshots":   0,
    "alerts_generated": 0,
    "ml_enriched":     0,
    "errors":          0,
}

# Deduplicación de alertas Gold (zona → timestamp)
active_alert_zones: dict = {}

# Umbrales Gold para generar alertas propias
GOLD_ALERT_RULES = [
    {
        "id":        "GOLD-FWI-CRITICAL",
        "condition": lambda fwi, p: fwi >= 0.80,
        "level":     "CRITICAL",
        "message":   "🔴 GOLD ALERT: FWI crítico — riesgo máximo de incendio",
    },
    {
        "id":        "GOLD-FWI-HIGH",
        "condition": lambda fwi, p: 0.60 <= fwi < 0.80,
        "level":     "HIGH",
        "message":   "🟠 GOLD ALERT: FWI alto — activar monitoreo intensivo",
    },
    {
        "id":        "GOLD-TEMP-CRITICAL",
        "condition": lambda fwi, p: float(p.get("temperatureC") or 0) >= 44,
        "level":     "HIGH",
        "message":   "🌡 GOLD ALERT: Temperatura extrema en zona de reserva",
    },
    {
        "id":        "GOLD-SMOKE-DETECTED",
        "condition": lambda fwi, p: float(p.get("smokeDensity") or 0) >= 0.45,
        "level":     "CRITICAL",
        "message":   "💨 GOLD ALERT: Densidad de humo crítica — incendio probable",
    },
]


# ── Conexiones ────────────────────────────────────────────────
def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS, connect_timeout=5
            )
            conn.autocommit = True
            metrics.service_check("postgres.connection", 0, "PostgreSQL connected")
            log.info("PostgreSQL conectado (Gold schema)")
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


def ensure_groups(r: redis.Redis):
    # Consumer group propio en silver:out
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        log.info("Consumer group '%s' listo en '%s'", GROUP, STREAM)
    except redis.ResponseError:
        pass
    # Streams de salida Gold
    for stream, groups in {
        "wg:gold:kpis":   ["dashboard-consumer"],
        "wg:gold:alerts": ["dashboard-consumer", "alert-service"],
        "wg:gold:ml":     ["dashboard-consumer"],
    }.items():
        for grp in groups:
            try:
                r.xgroup_create(stream, grp, id="0", mkstream=True)
            except redis.ResponseError:
                pass


# ── Agregación KPI (Redshift simulado) ───────────────────────
def aggregate_kpi(records: list[dict]) -> dict:
    """
    Materializa KPI snapshot de la ventana actual.
    Equivale a una query Redshift/Athena sobre la capa Silver.
    """
    if not records:
        return {}

    temps, hums, co2s, winds, smokes = [], [], [], [], []
    w_temps, w_hums, w_winds         = [], [], []
    fwis   = []
    zone_fwi   = defaultdict(list)
    risk_dist  = defaultdict(int)
    stype_dist = defaultdict(int)
    source_dist = defaultdict(int)
    alert_count = crit_count = 0
    latencies   = []

    for rec in records:
        fwi    = float(rec.get("fwi", 0) or 0)
        level  = rec.get("riskLevel", "LOW")
        zone   = rec.get("zone", "unknown")
        source = rec.get("source", "UNKNOWN")

        fwis.append(fwi)
        zone_fwi[zone].append(fwi)
        risk_dist[level]   += 1
        source_dist[source] += 1

        if level in ("HIGH", "CRITICAL"): alert_count += 1
        if level == "CRITICAL":           crit_count  += 1

        payload = {}
        try:
            payload = json.loads(rec.get("payload", "{}"))
        except Exception:
            pass

        if source == "IOT_SENSOR":
            stype = payload.get("sensorType") or payload.get("_normalizedAt") and "MULTI" or "UNKNOWN"
            stype_dist[stype] += 1
            if t := payload.get("temperatureC"): temps.append(float(t))
            if h := payload.get("humidityPct"):  hums.append(float(h))
            if c := payload.get("co2Ppm"):        co2s.append(float(c))
            if w := payload.get("windKmh"):        winds.append(float(w))
            if s := payload.get("smokeDensity"):   smokes.append(float(s))
        elif source == "WEATHER":
            if t := payload.get("temperatureC"): w_temps.append(float(t))
            if h := payload.get("humidityPct"):  w_hums.append(float(h))
            if w := payload.get("windKmh"):       w_winds.append(float(w))

    def avg(lst): return round(sum(lst) / len(lst), 3) if lst else None
    def mx(lst):  return round(max(lst), 3) if lst else None

    top_zones = sorted(
        [{"zone": z, "avgFwi": round(sum(v) / len(v), 4)}
         for z, v in zone_fwi.items()],
        key=lambda x: x["avgFwi"], reverse=True
    )[:5]

    n = len(records)
    return {
        "kpiId":              str(uuid.uuid4()),
        "generatedAt":        datetime.now(timezone.utc).isoformat(),
        "layer":              "GOLD",
        "windowSeconds":      KPI_WINDOW_S,
        "totalRecords":       n,
        "totalIot":           source_dist.get("IOT_SENSOR", 0),
        "totalWeather":       source_dist.get("WEATHER", 0),
        "totalGps":           source_dist.get("GPS", 0),
        "totalAirQuality":    source_dist.get("AIR_QUALITY", 0),
        "totalAlerts":        alert_count,
        "criticalAlerts":     crit_count,
        # FWI
        "avgFwi":             avg(fwis),
        "maxFwi":             mx(fwis),
        "minFwi":             round(min(fwis), 3) if fwis else None,
        # IoT
        "avgTemperature":     avg(temps),
        "maxTemperature":     mx(temps),
        "avgHumidity":        avg(hums),
        "avgCo2Ppm":          avg(co2s),
        "avgWindKmh":         avg(winds),
        "avgSmokeDensity":    avg(smokes),
        "maxSmokeDensity":    mx(smokes),
        # Clima
        "avgWeatherTemp":     avg(w_temps),
        "avgWeatherHumidity": avg(w_hums),
        "avgWeatherWindKmh":  avg(w_winds),
        # Vistas analíticas
        "topRiskZones":       top_zones,
        "riskDistribution":   dict(risk_dist),
        "bySensorType":       dict(stype_dist),
        "bySource":           dict(source_dist),
        "slaCompliance":      round((n - counters["errors"]) / n * 100, 2) if n else 100.0,
        "dataSources":        ["IoT-Sensores", "GPS", "Open-Meteo", "Air-Quality"],
    }


def persist_kpi(conn, kpi: dict):
    """Persiste KPI en gold.kpi_snapshots."""
    with tracer.trace("gold.db.kpi_snapshot", resource="gold.kpi_snapshots") as span:
        span.set_metric("records",    kpi.get("totalRecords", 0))
        span.set_metric("avg_fwi",    kpi.get("avgFwi") or 0)
        span.set_metric("alerts",     kpi.get("totalAlerts", 0))

        sql = """
            INSERT INTO gold.kpi_snapshots (
                window_minutes, total_iot, total_api, total_alerts, critical_alerts,
                avg_temp, avg_humidity, avg_co2, avg_wind,
                avg_weather_temp, avg_weather_hum, avg_weather_wind,
                avg_fwi, max_fwi, sla_compliance_pct,
                top_risk_zones, risk_distribution, by_sensor_type
            ) VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s)
            RETURNING id
        """
        with conn.cursor() as cur:
            cur.execute(sql, (
                1,
                kpi.get("totalIot"),
                kpi.get("totalWeather", 0) + kpi.get("totalGps", 0) + kpi.get("totalAirQuality", 0),
                kpi.get("totalAlerts"), kpi.get("criticalAlerts"),
                kpi.get("avgTemperature"), kpi.get("avgHumidity"),
                kpi.get("avgCo2Ppm"),   kpi.get("avgWindKmh"),
                kpi.get("avgWeatherTemp"), kpi.get("avgWeatherHumidity"),
                kpi.get("avgWeatherWindKmh"),
                kpi.get("avgFwi"), kpi.get("maxFwi"),
                kpi.get("slaCompliance"),
                json.dumps(kpi.get("topRiskZones", [])),
                json.dumps(kpi.get("riskDistribution", {})),
                json.dumps(kpi.get("bySensorType", {})),
            ))
            return cur.fetchone()[0]


def persist_alert(conn, alert: dict, silver_id: str) -> str:
    """Persiste alerta Gold en gold.fire_alerts."""
    with tracer.trace("gold.db.fire_alert", resource="gold.fire_alerts") as span:
        alert_id = str(uuid.uuid4())
        span.set_tag("zone",       alert["zone"])
        span.set_tag("risk_level", alert["riskLevel"])
        span.set_metric("fwi",     alert.get("fwiScore", 0))

        sql = """
            INSERT INTO gold.fire_alerts
                (alert_id, zone, risk_level, fwi_score,
                 temperature_c, humidity_pct, wind_kmh,
                 factors, destinations, silver_id)
            VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s)
            ON CONFLICT (alert_id) DO NOTHING
            RETURNING id
        """
        p = alert.get("payload", {})
        with conn.cursor() as cur:
            cur.execute(sql, (
                alert_id,
                alert["zone"],
                alert["riskLevel"],
                alert.get("fwiScore"),
                p.get("temperatureC"), p.get("humidityPct"), p.get("windKmh"),
                json.dumps({"ruleId": alert.get("ruleId"),
                            "message": alert.get("message"),
                            "layer": "GOLD"}),
                json.dumps(["CONAF", "Brigadas-Forestales", "Bomberos"]
                           if alert["riskLevel"] == "CRITICAL"
                           else ["CONAF", "Equipos-Regionales"]),
                silver_id if silver_id else None,
            ))
        return alert_id


# ── Evaluación de alertas Gold ────────────────────────────────
def evaluate_gold_alerts(fields: dict, conn, r: redis.Redis):
    """
    Evalúa reglas Gold sobre cada registro Silver.
    Genera alertas independientes del alert-service (capa Gold propia).
    """
    with tracer.trace("gold.alert.evaluate", resource="gold_rules") as span:
        zone      = fields.get("zone", "unknown")
        fwi       = float(fields.get("fwi", 0) or 0)
        silver_id = fields.get("silverId", "")
        payload   = {}
        try:
            payload = json.loads(fields.get("payload", "{}"))
        except Exception:
            pass

        span.set_tag("zone", zone)
        span.set_metric("fwi", fwi)

        for rule in GOLD_ALERT_RULES:
            try:
                if not rule["condition"](fwi, payload):
                    continue
            except Exception:
                continue

            # Deduplicación por zona: no repetir misma alerta en 5 min
            last = active_alert_zones.get(f"{zone}:{rule['id']}")
            if last and (time.time() - last) < 300:
                continue

            level = rule["level"]
            alert = {
                "alertId":   str(uuid.uuid4()),
                "zone":      zone,
                "riskLevel": level,
                "ruleId":    rule["id"],
                "message":   rule["message"],
                "fwiScore":  fwi,
                "payload":   payload,
                "layer":     "GOLD",
                "firedAt":   datetime.now(timezone.utc).isoformat(),
            }

            # Persistir en gold.fire_alerts
            try:
                alert_id = persist_alert(conn, alert, silver_id)
                alert["alertId"] = alert_id
            except Exception as e:
                log.error("Error persistiendo alerta Gold: %s", e,
                          extra={"zone": zone, "risk_level": level})
                continue

            # Publicar en wg:gold:alerts
            r.xadd("wg:gold:alerts", {
                "alertId":   alert["alertId"],
                "zone":      zone,
                "riskLevel": level,
                "fwi":       str(fwi),
                "message":   rule["message"],
                "layer":     "GOLD",
                "ruleId":    rule["id"],
                "firedAt":   alert["firedAt"],
            }, maxlen=1000)

            active_alert_zones[f"{zone}:{rule['id']}"] = time.time()
            recent_alerts.insert(0, alert)
            if len(recent_alerts) > 200:
                recent_alerts.pop()

            counters["alerts_generated"] += 1
            span.set_tag("alert.level",   level)
            span.set_tag("alert.rule_id", rule["id"])

            metrics.record_alert(level, zone, rule["id"])
            log.warning(
                "GOLD ALERT | level=%s zone=%s fwi=%.3f rule=%s",
                level, zone, fwi, rule["id"],
                extra={"zone": zone, "risk_level": level,
                       "fwi": fwi, "source": "GOLD"}
            )
            break  # una alerta por registro (la de mayor severidad)


# ── KPI loop (ventana temporal) ───────────────────────────────
def kpi_loop(conn, r: redis.Redis):
    """
    Materializa KPI snapshot cada KPI_WINDOW_S segundos.
    Equivale a una query programada en Redshift/Step Functions.
    """
    while True:
        time.sleep(KPI_WINDOW_S)

        with kpi_lock:
            snapshot = list(kpi_buffer)
            kpi_buffer.clear()

        if not snapshot:
            log.info("Gold KPI: ventana vacía, esperando datos Silver")
            metrics.service_check("gold.kpi_loop", 1, "Empty window")
            continue

        with tracer.trace("gold.kpi.materialize", resource="kpi_snapshot") as span:
            span.set_metric("window.records", len(snapshot))

            kpi = aggregate_kpi(snapshot)

            try:
                kpi_id = persist_kpi(conn, kpi)
                counters["kpi_snapshots"] += 1
                span.set_tag("kpi_id", str(kpi_id)[:8])
            except Exception as e:
                log.error("Error persistiendo KPI Gold: %s", e)
                span.set_tag("error", True)
                continue

            global latest_kpi
            latest_kpi = {**kpi, "kpiDbId": str(kpi_id)}

            # Publicar en stream wg:gold:kpis
            r.set("wg:latest:gold:kpi", json.dumps(kpi))
            r.xadd("wg:gold:kpis", {"kpi": json.dumps(kpi)}, maxlen=500)

            # Métricas Datadog
            metrics.gauge("gold.fwi.avg",            kpi.get("avgFwi") or 0)
            metrics.gauge("gold.fwi.max",            kpi.get("maxFwi") or 0)
            metrics.gauge("gold.alerts.total",       kpi.get("totalAlerts", 0))
            metrics.gauge("gold.alerts.critical",    kpi.get("criticalAlerts", 0))
            metrics.gauge("gold.records.window",     kpi.get("totalRecords", 0))
            metrics.gauge("gold.sla_compliance",     kpi.get("slaCompliance", 100))
            metrics.increment("gold.kpi.snapshots")
            metrics.record_stream_len("wg:gold:kpis", r.xlen("wg:gold:kpis"))

            top_zone = (kpi.get("topRiskZones") or [{}])[0].get("zone", "-")
            log.info(
                "GOLD KPI MATERIALIZADO | records=%d alerts=%d crit=%d "
                "avgFwi=%.3f maxFwi=%.3f sla=%.1f%% topZone=%s",
                kpi.get("totalRecords", 0),
                kpi.get("totalAlerts",  0),
                kpi.get("criticalAlerts", 0),
                kpi.get("avgFwi") or 0,
                kpi.get("maxFwi") or 0,
                kpi.get("slaCompliance", 100),
                top_zone,
                extra={"zone": top_zone, "fwi": kpi.get("avgFwi") or 0,
                       "records": kpi.get("totalRecords", 0)}
            )
            metrics.service_check("gold.kpi_loop", 0,
                                  f"OK snapshots={counters['kpi_snapshots']}")


# ── Consumer Silver → Gold ────────────────────────────────────
def consumer_loop(r: redis.Redis, conn):
    consumer = f"gold-{uuid.uuid4().hex[:8]}"
    log.info("Gold consumer '%s' listo en '%s'", consumer, STREAM)

    while True:
        try:
            entries = r.xreadgroup(
                groupname=GROUP,
                consumername=consumer,
                streams={STREAM: ">"},
                count=50,
                block=2000,
            )
            if not entries:
                continue

            for _, messages in entries:
                for msg_id, fields in messages:
                    t0     = time.monotonic()
                    source = fields.get("source", "UNKNOWN")
                    zone   = fields.get("zone",   "unknown")
                    fwi    = float(fields.get("fwi", 0) or 0)
                    level  = fields.get("riskLevel", "LOW")

                    with tracer.trace("gold.consume_silver",
                                      resource=source) as span:
                        span.set_tag("source", source)
                        span.set_tag("zone",   zone)
                        span.set_tag("risk",   level)
                        span.set_metric("fwi", fwi)

                        try:
                            # 1. Buffer para KPI
                            with kpi_lock:
                                kpi_buffer.append(fields)

                            # 2. Evaluar alertas Gold
                            evaluate_gold_alerts(fields, conn, r)

                            r.xack(STREAM, GROUP, msg_id)
                            counters["silver_consumed"] += 1

                            latency_ms = (time.monotonic() - t0) * 1000
                            span.set_metric("latency_ms", latency_ms)

                            metrics.increment(
                                "gold.silver_consumed",
                                tags=[f"source:{source.lower()}",
                                      f"risk:{level.lower()}"]
                            )
                            metrics.record_latency(
                                "gold.consume_silver", latency_ms, source, zone
                            )

                            log.info(
                                "GOLD CONSUMED | src=%s zone=%s fwi=%.3f risk=%s lat=%.1fms",
                                source, zone, fwi, level, latency_ms,
                                extra={"zone": zone, "source": source,
                                       "fwi": fwi, "risk_level": level,
                                       "latency_ms": latency_ms}
                            )

                        except Exception as e:
                            counters["errors"] += 1
                            span.set_tag("error", True)
                            log.error("Gold error msg %s: %s", msg_id, e,
                                      extra={"zone": zone, "source": source})
                            r.xack(STREAM, GROUP, msg_id)

            # Service check cada 200 registros
            if counters["silver_consumed"] % 200 == 0 and counters["silver_consumed"] > 0:
                metrics.service_check(
                    "gold.consumer", 0,
                    f"OK consumed={counters['silver_consumed']} "
                    f"kpis={counters['kpi_snapshots']} "
                    f"alerts={counters['alerts_generated']}"
                )
                metrics.gauge("gold.total_consumed",   counters["silver_consumed"])
                metrics.gauge("gold.total_kpis",       counters["kpi_snapshots"])
                metrics.gauge("gold.total_alerts",     counters["alerts_generated"])

        except Exception as e:
            log.error("Error en Gold consumer_loop: %s", e)
            metrics.service_check("gold.consumer", 2, str(e))
            time.sleep(5)


# ── HTTP API (QuickSight simulado) ────────────────────────────
class GoldHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/kpi":
            body = json.dumps(latest_kpi or {"message": "sin KPI aún"})
        elif self.path == "/alerts":
            body = json.dumps({
                "recent":   recent_alerts[:20],
                "active_zones": {k: v for k, v in active_alert_zones.items()
                                 if (time.time() - v) < 300},
            }, default=str)
        elif self.path == "/stats":
            body = json.dumps({
                "counters": counters,
                "buffer_size": len(kpi_buffer),
                "kpi_window_s": KPI_WINDOW_S,
            })
        elif self.path == "/health":
            body = json.dumps({
                "status":    "UP",
                "layer":     "GOLD",
                "component": "gold-layer",
                "equivalentTo": [
                    "Amazon Redshift Serverless",
                    "Amazon QuickSight",
                    "AWS Step Functions (KPI window)",
                    "Amazon EventBridge (Gold alerts)",
                ],
                "streams_out": ["wg:gold:kpis", "wg:gold:alerts"],
                "tables": ["gold.kpi_snapshots", "gold.fire_alerts"],
                "stats": counters,
            })
        else:
            body = json.dumps({"error": "not found"})

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *args):
        pass


# ── Main ──────────────────────────────────────────────────────
def main():
    log.info(
        "WildGuard Gold Layer iniciando | ventana_kpi=%ds",
        KPI_WINDOW_S,
        extra={"source": "GOLD", "zone": "all"}
    )

    conn = connect_pg()
    r    = connect_redis()
    ensure_groups(r)

    # KPI loop en hilo background
    threading.Thread(
        target=kpi_loop,
        args=(conn, r),
        daemon=True,
        name="gold-kpi-loop",
    ).start()
    log.info("KPI loop iniciado (ventana=%ds)", KPI_WINDOW_S)

    # HTTP API
    threading.Thread(
        target=lambda: HTTPServer(("0.0.0.0", HTTP_PORT), GoldHandler).serve_forever(),
        daemon=True,
        name="gold-http",
    ).start()
    log.info("HTTP API Gold en :%d (/health /kpi /alerts /stats)", HTTP_PORT)

    # Consumer principal (bloqueante)
    consumer_loop(r, conn)


if __name__ == "__main__":
    main()
