"""
WildGuard – Alert Service con canales múltiples
============================================================
EventBridge + Lambda + SNS multi-canal.

Canales de notificación:
  - Telegram (bot API)
  - Slack (Incoming Webhook)
  - Email (SMTP)
  - Webhook (HTTP POST genérico)

Cada canal se activa si su variable de entorno está configurada.
"""
import os, json, uuid, time, logging, threading, requests, smtplib, ssl
from email.message import EmailMessage
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
DASHBOARD_URL = os.getenv("ALERT_WEBHOOK_URL", "http://dashboard:8088/api/alerts/ingest")
EXTERNAL_HOOK = os.getenv("ALERT_WEBHOOK_EXTERNAL", "")
GROUP  = "alert-evaluator"
STREAM = "wg:silver:out"

# ── Config canales de notificación ─────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
SLACK_URL      = os.getenv("SLACK_WEBHOOK_URL", "")
SMTP_HOST      = os.getenv("SMTP_HOST", "")
SMTP_PORT      = int(os.getenv("SMTP_PORT", 587))
SMTP_USER      = os.getenv("SMTP_USER", "")
SMTP_PASS      = os.getenv("SMTP_PASS", "")
SMTP_FROM      = os.getenv("SMTP_FROM", "wildguard@wildguard.cl")
SMTP_TO        = os.getenv("SMTP_TO_ALERTS", "")

# ── Contadores y estado ────────────────────────────────────────
counters = {"evaluated":0,"fired":0,"high":0,"critical":0,"suppressed":0,"errors":0,
            "telegram_sent":0,"slack_sent":0,"email_sent":0,"webhook_sent":0}
active_alerts: dict = {}
alert_history: list = []

ALERT_RULES = [
    {"id":"RULE-FWI-CRITICAL",  "condition": lambda p,fwi: fwi>=0.80,
     "level":"CRITICAL", "message":"RIESGO CRITICO DE INCENDIO — Activar brigadas"},
    {"id":"RULE-FWI-HIGH",      "condition": lambda p,fwi: 0.60<=fwi<0.80,
     "level":"HIGH",     "message":"RIESGO ALTO DE INCENDIO — Alerta preventiva"},
    {"id":"RULE-TEMP-EXTREME",  "condition": lambda p,fwi: float(p.get("temperatureC") or 0)>=42,
     "level":"HIGH",     "message":"TEMPERATURA EXTREMA detectada"},
    {"id":"RULE-SMOKE",         "condition": lambda p,fwi: float(p.get("smokeDensity") or 0)>=0.4,
     "level":"CRITICAL", "message":"HUMO DETECTADO — Posible incendio activo"},
    {"id":"RULE-WIND",          "condition": lambda p,fwi: float(p.get("windKmh") or 0)>=70 and fwi>=0.5,
     "level":"HIGH",     "message":"VIENTO PELIGROSO con riesgo elevado"},
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


# ── Canales de notificación ────────────────────────────────────

def send_telegram(alert: dict) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    try:
        text = (
            f"*WildGuard Alerta*\n"
            f"Nivel: {alert['riskLevel']}\n"
            f"Zona: {alert['zone']}\n"
            f"FWI: {alert.get('fwiScore', 0):.3f}\n"
            f"Mensaje: {alert.get('message', '')}\n"
            f"Hora: {alert.get('firedAt', '')}"
        )
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT,
            "text": text,
            "parse_mode": "Markdown",
        }, timeout=5)
        ok = resp.status_code == 200
        if ok:
            counters["telegram_sent"] += 1
            metrics.increment("alert.channel.telegram", tags=[f"level:{alert['riskLevel'].lower()}"])
        return ok
    except Exception as e:
        log.error("Error enviando Telegram: %s", e)
        return False


def send_slack(alert: dict) -> bool:
    if not SLACK_URL:
        return False
    try:
        color = "#f87171" if alert["riskLevel"] == "CRITICAL" else "#fb923c"
        payload = {
            "attachments": [{
                "color": color,
                "title": f"WildGuard Alerta — {alert['riskLevel']}",
                "text": alert.get("message", ""),
                "fields": [
                    {"title": "Zona", "value": alert["zone"], "short": True},
                    {"title": "FWI", "value": f"{alert.get('fwiScore', 0):.3f}", "short": True},
                    {"title": "Hora", "value": alert.get("firedAt", ""), "short": False},
                ],
                "footer": "WildGuard Chile",
                "ts": time.time(),
            }]
        }
        resp = requests.post(SLACK_URL, json=payload, timeout=5)
        ok = resp.status_code == 200
        if ok:
            counters["slack_sent"] += 1
            metrics.increment("alert.channel.slack", tags=[f"level:{alert['riskLevel'].lower()}"])
        return ok
    except Exception as e:
        log.error("Error enviando Slack: %s", e)
        return False


def send_email(alert: dict) -> bool:
    if not SMTP_HOST or not SMTP_TO:
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = f"WildGuard {alert['riskLevel']} — {alert['zone']}"
        msg["From"] = SMTP_FROM
        msg["To"] = SMTP_TO
        body = (
            f"ALERTA WILDGUARD\n"
            f"{'='*40}\n"
            f"Nivel: {alert['riskLevel']}\n"
            f"Zona: {alert['zone']}\n"
            f"FWI: {alert.get('fwiScore', 0):.3f}\n"
            f"Temperatura: {alert.get('payload', {}).get('temperatureC', 'N/A')} °C\n"
            f"Humedad: {alert.get('payload', {}).get('humidityPct', 'N/A')} %\n"
            f"Viento: {alert.get('payload', {}).get('windKmh', 'N/A')} km/h\n"
            f"Mensaje: {alert.get('message', '')}\n"
            f"Hora: {alert.get('firedAt', '')}\n"
            f"{'='*40}\n"
            f"WildGuard Chile — Sistema de Monitoreo de Incendios Forestales"
        )
        msg.set_content(body)
        ctx = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls(context=ctx)
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        counters["email_sent"] += 1
        metrics.increment("alert.channel.email", tags=[f"level:{alert['riskLevel'].lower()}"])
        return True
    except Exception as e:
        log.error("Error enviando Email: %s", e)
        return False


def send_webhook(alert: dict) -> bool:
    ok = False
    for url in [DASHBOARD_URL, EXTERNAL_HOOK]:
        if not url:
            continue
        try:
            resp = requests.post(url, json=alert, timeout=3)
            if resp.status_code in (200, 202):
                ok = True
        except Exception:
            pass
    if ok:
        counters["webhook_sent"] += 1
        metrics.increment("alert.channel.webhook", tags=[f"level:{alert['riskLevel'].lower()}"])
    return ok


def notify_all_channels(alert: dict):
    """Envía la alerta por todos los canales configurados."""
    channels = []
    if send_telegram(alert):      channels.append("telegram")
    if send_slack(alert):         channels.append("slack")
    if send_email(alert):         channels.append("email")
    if send_webhook(alert):       channels.append("webhook")
    if channels:
        log.info("Alerta enviada por: %s", ", ".join(channels),
                 extra={"zone": alert["zone"], "risk_level": alert["riskLevel"]})
    return channels


def simulate_sns(alert):
    """Simula SNS: escribe log + notifica canales + métrica."""
    level = alert["riskLevel"]; zone = alert["zone"]
    dests = DESTINATIONS.get(level, [])
    for dest in dests:
        log.warning("SNS → %s | %s | zona=%s fwi=%.3f",
                    dest, level, zone, alert.get("fwiScore",0),
                    extra={"zone": zone, "risk_level": level,
                           "fwi": alert.get("fwiScore",0)})
        metrics.increment("sns.notification_sent",
                          tags=[f"dest:{dest.lower()}",f"level:{level.lower()}"])
    # Notificar por todos los canales configurados
    notify_all_channels(alert)
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
    def is_telegram_configured(self):
        if os.getenv("TELEGRAM_ENABLED","").lower()=="true": return True
        return bool(TELEGRAM_TOKEN and TELEGRAM_CHAT)
    def is_slack_configured(self):
        if os.getenv("SLACK_ENABLED","").lower()=="true": return True
        return bool(SLACK_URL)
    def is_email_configured(self):
        if os.getenv("EMAIL_ENABLED","").lower()=="true": return True
        return bool(SMTP_HOST and SMTP_TO)

    def do_GET(self):
        tg = self.is_telegram_configured()
        sl = self.is_slack_configured()
        em = self.is_email_configured()
        if self.path=="/alerts":
            body=json.dumps({"active":list(active_alerts.items()),
                             "history":alert_history[:20],"stats":counters},default=str)
        elif self.path=="/channels":
            body=json.dumps({
                "telegram": tg,
                "slack": sl,
                "email": em,
                "webhook": bool(DASHBOARD_URL),
                "webhook_external": bool(EXTERNAL_HOOK),
                "counts": {k: counters[k] for k in ("telegram_sent","slack_sent","email_sent","webhook_sent")},
            })
        else:
            body=json.dumps({"status":"UP","component":"alert-service","stats":counters,"channels":{
                "telegram":tg,"slack":sl,"email":em,"webhook":bool(DASHBOARD_URL)}})
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
