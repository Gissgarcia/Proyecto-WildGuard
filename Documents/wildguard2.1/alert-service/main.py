"""
WildGuard – Alert Service (con telemetría Datadog completa)
EventBridge + Lambda + SNS simulado.
"""
import os, json, uuid, time, logging, threading, requests
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import defaultdict

import redis, psycopg2
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("alert-service")
tracer  = get_tracer()
log     = get_logger("alert-service")

REDIS_HOST    = os.getenv("REDIS_HOST", "redis")
REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
PG_HOST       = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT       = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB         = os.getenv("POSTGRES_DB", "wildguard")
PG_USER       = os.getenv("POSTGRES_USER", "wildguard")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "wg_secret_2024")
HTTP_PORT     = int(os.getenv("HTTP_PORT", 8086))
DASHBOARD_URL = os.getenv("ALERT_WEBHOOK_URL", "http://dashboard:8080/api/alerts/ingest")
GROUP  = "alert-evaluator"
STREAM = "wg:silver:out"

counters = {"evaluated":0,"fired":0,"high":0,"critical":0,"suppressed":0,"errors":0}
active_alerts: dict = {}
alert_history: list = []

ALERT_RULES = [
    {"id":"RULE-FWI-CRITICAL",  "condition": lambda p,fwi: fwi>=0.80,
     "level":"CRITICAL", "message":"🔴 RIESGO CRÍTICO DE INCENDIO — Activar brigadas"},
    {"id":"RULE-FWI-HIGH",      "condition": lambda p,fwi: 0.60<=fwi<0.80,
     "level":"HIGH",     "message":"🟠 RIESGO ALTO DE INCENDIO — Alerta preventiva"},
    {"id":"RULE-TEMP-EXTREME",  "condition": lambda p,fwi: float(p.get("temperatureC") or 0)>=42,
     "level":"HIGH",     "message":"🌡 TEMPERATURA EXTREMA detectada"},
    {"id":"RULE-SMOKE",         "condition": lambda p,fwi: float(p.get("smokeDensity") or 0)>=0.4,
     "level":"CRITICAL", "message":"💨 HUMO DETECTADO — Posible incendio activo"},
    {"id":"RULE-WIND",          "condition": lambda p,fwi: float(p.get("windKmh") or 0)>=70 and fwi>=0.5,
     "level":"HIGH",     "message":"🌬 VIENTO PELIGROSO con riesgo elevado"},
]
DESTINATIONS = {
    "CRITICAL": ["CONAF","Brigadas-Forestales","Bomberos","Equipos-Regionales"],
    "HIGH":     ["CONAF","Brigadas-Forestales","Equipos-Regionales"],
    "MEDIUM":   ["CONAF"], "LOW": [],
}


def connect_pg():
    for i in range(20):
        try:
            conn = psycopg2.connect(host=PG_HOST,port=PG_PORT,dbname=PG_DB,
                                    user=PG_USER,password=PG_PASS,connect_timeout=5)
            conn.autocommit = True
            metrics.service_check("postgres.connection",0,"OK")
            return conn
        except Exception as e:
            time.sleep(4)
    raise RuntimeError("No se pudo conectar a PostgreSQL")


def connect_redis():
    for i in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST,port=REDIS_PORT,decode_responses=True)
            r.ping()
            metrics.service_check("redis.connection",0,"OK")
            return r
        except Exception:
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def evaluate_rules(payload, fwi):
    for rule in ALERT_RULES:
        try:
            if rule["condition"](payload, fwi):
                return rule["level"], rule["id"], rule["message"]
        except Exception:
            pass
    return None, None, None


def persist_alert(conn, alert, silver_id):
    alert_id = str(uuid.uuid4())
    sql = """INSERT INTO gold.fire_alerts
        (alert_id,zone,risk_level,fwi_score,temperature_c,humidity_pct,wind_kmh,factors,destinations,silver_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (alert_id) DO NOTHING"""
    p = alert.get("payload",{})
    with conn.cursor() as cur:
        cur.execute(sql,(alert_id,alert["zone"],alert["riskLevel"],alert.get("fwiScore"),
                        p.get("temperatureC"),p.get("humidityPct"),p.get("windKmh"),
                        json.dumps({"ruleId":alert.get("ruleId"),"message":alert.get("message")}),
                        json.dumps(alert.get("destinations",[])),
                        silver_id if silver_id else None))
    return alert_id


def simulate_sns(alert):
    level = alert["riskLevel"]; zone = alert["zone"]
    dests = DESTINATIONS.get(level, [])
    for dest in dests:
        log.warning("SNS → %s | %s | zona=%s fwi=%.3f",
                    dest, level, zone, alert.get("fwiScore",0),
                    extra={"zone": zone, "risk_level": level,
                           "fwi": alert.get("fwiScore",0)})
        metrics.increment("sns.notification_sent",
                          tags=[f"dest:{dest.lower()}",f"level:{level.lower()}"])
    try:
        requests.post(DASHBOARD_URL, json=alert, timeout=2)
    except Exception:
        pass
    return dests


def consumer_loop(r, conn):
    consumer = f"alert-{uuid.uuid4().hex[:8]}"
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        r.xgroup_create("wg:gold:alerts", "dashboard-consumer", id="0", mkstream=True)
    except redis.ResponseError:
        pass
    log.info("Alert consumer '%s' listo", consumer)

    while True:
        try:
            entries = r.xreadgroup(groupname=GROUP, consumername=consumer,
                                   streams={STREAM:">"}, count=30, block=2000)
            if not entries:
                continue
            for _, messages in entries:
                for msg_id, fields in messages:
                    t0     = time.monotonic()
                    zone   = fields.get("zone","unknown")
                    fwi    = float(fields.get("fwi",0) or 0)
                    sid    = fields.get("silverId","")
                    payload= json.loads(fields.get("payload","{}"))
                    counters["evaluated"] += 1

                    with tracer.trace("alert.evaluate", resource="rules_engine") as span:
                        span.set_tag("zone", zone)
                        span.set_metric("fwi", fwi)
                        level, rule_id, message = evaluate_rules(payload, fwi)

                        if level is None:
                            r.xack(STREAM, GROUP, msg_id)
                            continue

                        # Deduplicación 5 min
                        last = active_alerts.get(zone)
                        if last and (time.time()-last["ts"]) < 300:
                            counters["suppressed"] += 1
                            r.xack(STREAM, GROUP, msg_id)
                            continue

                        span.set_tag("alert.level",   level)
                        span.set_tag("alert.rule_id", rule_id)

                        dests = DESTINATIONS.get(level,[])
                        alert = {"alertId":str(uuid.uuid4()),"zone":zone,"riskLevel":level,
                                 "ruleId":rule_id,"message":message,"fwiScore":fwi,
                                 "payload":payload,"destinations":dests,
                                 "firedAt":datetime.now(timezone.utc).isoformat()}

                        alert_id = persist_alert(conn, alert, sid)
                        alert["alertId"] = alert_id
                        simulate_sns(alert)
                        active_alerts[zone] = {"ts":time.time(),"level":level}
                        alert_history.insert(0, alert)
                        if len(alert_history) > 200: alert_history.pop()

                        r.xadd("wg:gold:alerts",{"alertId":alert_id,"zone":zone,
                               "riskLevel":level,"fwi":str(fwi),"message":message,
                               "dests":json.dumps(dests),
                               "firedAt":alert["firedAt"]},maxlen=1000)
                        r.xack(STREAM, GROUP, msg_id)

                        counters["fired"] += 1
                        if level=="CRITICAL": counters["critical"] += 1
                        elif level=="HIGH":   counters["high"]     += 1

                        latency_ms = (time.monotonic()-t0)*1000
                        metrics.record_alert(level, zone, rule_id)
                        metrics.record_latency("alert.eval", latency_ms, zone=zone)
                        span.set_metric("latency_ms", latency_ms)

                        log.warning("ALERTA %s | zona=%s fwi=%.3f rule=%s dests=%s lat=%.1fms",
                                    level, zone, fwi, rule_id, dests, latency_ms,
                                    extra={"zone":zone,"risk_level":level,
                                           "fwi":fwi,"latency_ms":latency_ms})

            metrics.service_check("alert.consumer", 0,
                                  f"OK fired={counters['fired']}")
        except Exception as e:
            log.error("Error en alert consumer_loop: %s", e)
            metrics.service_check("alert.consumer", 2, str(e))
            time.sleep(5)


class AlertHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=="/alerts":
            body=json.dumps({"active":list(active_alerts.items()),
                             "history":alert_history[:20],"stats":counters},default=str)
        else:
            body=json.dumps({"status":"UP","component":"alert-service","stats":counters})
        self.send_response(200); self.send_header("Content-Type","application/json"); self.end_headers()
        self.wfile.write(body.encode())
    def do_POST(self):
        l=int(self.headers.get("Content-Length",0))
        self.rfile.read(l)
        self.send_response(202); self.end_headers(); self.wfile.write(b'{"accepted":true}')
    def log_message(self,*a): pass


def main():
    log.info("WildGuard Alert Service iniciando (EventBridge+Lambda+SNS)")
    conn=connect_pg(); r=connect_redis()
    threading.Thread(target=lambda:HTTPServer(("0.0.0.0",HTTP_PORT),AlertHandler).serve_forever(),daemon=True).start()
    consumer_loop(r,conn)

if __name__=="__main__":
    main()
