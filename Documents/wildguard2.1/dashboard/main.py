"""
WildGuard – Dashboard (Cliente Final)
============================================================
Etapa 9 del pipeline: CLIENTE FINAL

Dashboard WildGuard que expone:
  - Mapa de sensores en tiempo real (API REST)
  - Alertas activas
  - KPIs ambientales
  - Tendencias y reportes
  - Históricos

También actúa como:
  - API de consumo para integraciones externas (APIs/Feed)
  - Webhook receiver para alertas del alert-service
  - APIs externas / Open Data

Puerto: 8080
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

metrics = init_telemetry("dashboard")
tracer  = get_tracer()
log     = get_logger("dashboard")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [dashboard] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("dashboard")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8080))


# Estado en memoria del dashboard
state = {
    "kpi":          {},
    "alerts":       [],
    "ml_events":    [],
    "initialized":  datetime.now(timezone.utc).isoformat(),
    "requests":     0,
}


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


def poll_streams(r: redis.Redis):
    """Consume streams Gold para mantener el estado del dashboard actualizado."""
    kpi_last    = "$"
    alert_last  = "$"
    ml_last     = "$"

    while True:
        try:
            # KPIs
            kpi_entries = r.xread({"wg:gold:kpis": kpi_last}, count=5, block=0)
            if kpi_entries:
                for _, msgs in kpi_entries:
                    for mid, fields in msgs:
                        kpi_last = mid
                        try:
                            kpi_data = json.loads(fields.get("kpi", "{}"))
                            state["kpi"] = kpi_data
                            log.info("Dashboard KPI actualizado | records=%s fwi=%s",
                                     kpi_data.get("totalRecords"),
                                     kpi_data.get("avgFwi"))
                        except Exception:
                            pass

        except Exception as e:
            log.debug("Poll KPIs: %s", e)
            time.sleep(2)

        try:
            # Alertas
            alert_entries = r.xread({"wg:gold:alerts": alert_last}, count=10, block=0)
            if alert_entries:
                for _, msgs in alert_entries:
                    for mid, fields in msgs:
                        alert_last = mid
                        alert = {
                            "alertId":   fields.get("alertId"),
                            "zone":      fields.get("zone"),
                            "riskLevel": fields.get("riskLevel"),
                            "fwi":       fields.get("fwi"),
                            "message":   fields.get("message"),
                            "firedAt":   fields.get("firedAt"),
                        }
                        state["alerts"].insert(0, alert)
                        if len(state["alerts"]) > 50:
                            state["alerts"].pop()
                        statsd.increment("wg.dashboard.alerts_received")
                        log.warning("Dashboard ALERTA | %s zona=%s fwi=%s",
                                    alert["riskLevel"], alert["zone"], alert["fwi"])
        except Exception as e:
            log.debug("Poll alerts: %s", e)
            time.sleep(2)

        try:
            # ML predictions
            ml_entries = r.xread({"wg:gold:ml": ml_last}, count=10, block=0)
            if ml_entries:
                for _, msgs in ml_entries:
                    for mid, fields in msgs:
                        ml_last = mid
                        ml_event = {
                            "zone":            fields.get("zone"),
                            "fireProbability": fields.get("fireProbability"),
                            "riskClass":       fields.get("riskClass"),
                            "confidence":      fields.get("confidence"),
                            "shapTopFeature":  fields.get("shapTopFeature"),
                            "timestamp":       fields.get("timestamp"),
                        }
                        state["ml_events"].insert(0, ml_event)
                        if len(state["ml_events"]) > 30:
                            state["ml_events"].pop()
        except Exception as e:
            log.debug("Poll ML: %s", e)
            time.sleep(2)

        time.sleep(1)


# ── HTML Dashboard ────────────────────────────────────────────
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="10">
<title>WildGuard Chile – Dashboard</title>
<style>
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:'Segoe UI',sans-serif; background:#0a1628; color:#e2e8f0; }
  header { background:#1b3a6b; padding:20px 30px; display:flex;
           justify-content:space-between; align-items:center; }
  header h1 { font-size:22px; color:#fff; }
  header .badge { font-size:11px; background:#00897b; color:#fff;
                  padding:4px 10px; border-radius:20px; }
  .grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; padding:20px; }
  .card { background:#1e2d40; border-radius:10px; padding:16px; }
  .card h3 { font-size:11px; color:#94a3b8; text-transform:uppercase;
             letter-spacing:.05em; margin-bottom:8px; }
  .card .val { font-size:28px; font-weight:600; color:#38bdf8; }
  .card .sub { font-size:12px; color:#64748b; margin-top:4px; }
  .section { padding:0 20px 20px; }
  .section h2 { font-size:14px; color:#94a3b8; margin-bottom:10px;
                text-transform:uppercase; letter-spacing:.05em; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th { background:#1b3a6b; color:#93c5fd; padding:8px 12px; text-align:left; }
  td { padding:8px 12px; border-bottom:1px solid #1e2d40; }
  tr:hover td { background:#1e2d40; }
  .CRITICAL { color:#f87171; font-weight:700; }
  .HIGH     { color:#fb923c; font-weight:600; }
  .MEDIUM   { color:#fbbf24; }
  .LOW      { color:#4ade80; }
  .FIRE_RISK { color:#f87171; }
  .NO_RISK   { color:#4ade80; }
  footer { text-align:center; padding:16px; color:#334155; font-size:12px; }
</style>
</head>
<body>
<header>
  <h1>🌲 WildGuard Chile – Sistema de Monitoreo de Incendios Forestales</h1>
  <span class="badge">● LIVE</span>
</header>

<div class="grid">
  <div class="card">
    <h3>Registros totales</h3>
    <div class="val">{total_records}</div>
    <div class="sub">Última ventana 30s</div>
  </div>
  <div class="card">
    <h3>FWI promedio</h3>
    <div class="val">{avg_fwi}</div>
    <div class="sub">Fire Weather Index</div>
  </div>
  <div class="card">
    <h3>Alertas activas</h3>
    <div class="val" style="color:#fb923c">{total_alerts}</div>
    <div class="sub">Críticas: {critical_alerts}</div>
  </div>
  <div class="card">
    <h3>SLA Compliance</h3>
    <div class="val" style="color:#4ade80">{sla}%</div>
    <div class="sub">Objetivo &gt;99%</div>
  </div>
  <div class="card">
    <h3>Temp. promedio IoT</h3>
    <div class="val">{avg_temp}°C</div>
    <div class="sub">Sensores activos</div>
  </div>
  <div class="card">
    <h3>Humedad promedio</h3>
    <div class="val">{avg_hum}%</div>
    <div class="sub">Sensores activos</div>
  </div>
  <div class="card">
    <h3>CO₂ promedio</h3>
    <div class="val">{avg_co2}</div>
    <div class="sub">ppm</div>
  </div>
  <div class="card">
    <h3>FWI máximo</h3>
    <div class="val" style="color:#f87171">{max_fwi}</div>
    <div class="sub">Zona más crítica</div>
  </div>
</div>

<div class="section">
  <h2>🚨 Alertas Recientes (EventBridge + SNS)</h2>
  <table>
    <tr><th>Hora</th><th>Zona</th><th>Nivel</th><th>FWI</th><th>Mensaje</th></tr>
    {alerts_rows}
  </table>
</div>

<div class="section">
  <h2>🤖 Predicciones ML (SageMaker simulado)</h2>
  <table>
    <tr><th>Zona</th><th>Probabilidad</th><th>Clase</th><th>Confianza</th><th>Feature Top</th></tr>
    {ml_rows}
  </table>
</div>

<div class="section">
  <h2>📊 Top Zonas de Riesgo (Redshift / QuickSight)</h2>
  <table>
    <tr><th>Zona</th><th>FWI Promedio</th></tr>
    {zones_rows}
  </table>
</div>

<footer>
  WildGuard Chile &nbsp;|&nbsp; Pipeline: Sensor → Redis Streams → Bronze → ETL → Silver → Gold → Dashboard
  &nbsp;|&nbsp; Actualizado: {now}
</footer>
</body></html>"""


def render_html(state: dict) -> str:
    kpi = state.get("kpi", {})

    # Alertas
    alerts_rows = ""
    for a in state["alerts"][:15]:
        level = a.get("riskLevel", "LOW")
        ts    = a.get("firedAt", "")[:19].replace("T", " ")
        alerts_rows += (
            f'<tr><td>{ts}</td><td>{a.get("zone","-")}</td>'
            f'<td class="{level}">{level}</td>'
            f'<td>{float(a.get("fwi",0)):.3f}</td>'
            f'<td>{a.get("message","-")}</td></tr>'
        )
    if not alerts_rows:
        alerts_rows = '<tr><td colspan="5" style="color:#4ade80">Sin alertas activas</td></tr>'

    # ML
    ml_rows = ""
    for m in state["ml_events"][:10]:
        cls = m.get("riskClass", "")
        ml_rows += (
            f'<tr><td>{m.get("zone","-")}</td>'
            f'<td>{float(m.get("fireProbability",0)):.3%}</td>'
            f'<td class="{cls}">{cls}</td>'
            f'<td>{float(m.get("confidence",0)):.1%}</td>'
            f'<td>{m.get("shapTopFeature","-")}</td></tr>'
        )
    if not ml_rows:
        ml_rows = '<tr><td colspan="5">Sin predicciones aún</td></tr>'

    # Zonas
    zones_rows = ""
    for z in kpi.get("topRiskZones", []):
        zones_rows += (
            f'<tr><td>{z.get("zone","-")}</td>'
            f'<td>{z.get("avgFwi",0):.3f}</td></tr>'
        )
    if not zones_rows:
        zones_rows = '<tr><td colspan="2">Sin datos de zonas aún</td></tr>'

    return HTML_TEMPLATE.format(
        total_records  = kpi.get("totalRecords", "-"),
        avg_fwi        = f'{kpi.get("avgFwi") or 0:.3f}',
        total_alerts   = kpi.get("totalAlerts", 0),
        critical_alerts = kpi.get("criticalAlerts", 0),
        sla            = f'{kpi.get("slaCompliance") or 100:.1f}',
        avg_temp       = f'{kpi.get("avgTemperature") or 0:.1f}',
        avg_hum        = f'{kpi.get("avgHumidity") or 0:.1f}',
        avg_co2        = f'{kpi.get("avgCo2Ppm") or 0:.0f}',
        max_fwi        = f'{kpi.get("maxFwi") or 0:.3f}',
        alerts_rows    = alerts_rows,
        ml_rows        = ml_rows,
        zones_rows     = zones_rows,
        now            = datetime.now(timezone.utc).isoformat()[:19] + "Z",
    )


# ── HTTP ──────────────────────────────────────────────────────
class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        state["requests"] += 1
        statsd.increment("wg.dashboard.requests")

        if self.path == "/" or self.path == "/dashboard":
            body    = render_html(state).encode()
            ctype   = "text/html; charset=utf-8"
            code    = 200
        elif self.path == "/api/kpi":
            body  = json.dumps(state["kpi"]).encode()
            ctype = "application/json"
            code  = 200
        elif self.path == "/api/alerts":
            body  = json.dumps(state["alerts"]).encode()
            ctype = "application/json"
            code  = 200
        elif self.path == "/api/ml":
            body  = json.dumps(state["ml_events"]).encode()
            ctype = "application/json"
            code  = 200
        elif self.path == "/health":
            body  = json.dumps({"status": "UP", "component": "dashboard",
                                 "requests": state["requests"]}).encode()
            ctype = "application/json"
            code  = 200
        else:
            body  = b'{"error":"not found"}'
            ctype = "application/json"
            code  = 404

        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        """Recibe alertas vía webhook del alert-service."""
        if self.path == "/api/alerts/ingest":
            length = int(self.headers.get("Content-Length", 0))
            data   = self.rfile.read(length)
            try:
                alert = json.loads(data)
                state["alerts"].insert(0, alert)
                if len(state["alerts"]) > 50:
                    state["alerts"].pop()
                log.info("Dashboard WEBHOOK alerta recibida | zona=%s nivel=%s",
                         alert.get("zone"), alert.get("riskLevel"))
            except Exception as e:
                log.error("Error en webhook: %s", e)
            self.send_response(202)
            self.end_headers()
            self.wfile.write(b'{"accepted":true}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args): pass


def main():
    log.info("════════════════════════════════════════════")
    log.info("  WildGuard Dashboard iniciando")
    log.info("  Puerto: %d", HTTP_PORT)
    log.info("  URL: http://localhost:%d", HTTP_PORT)
    log.info("════════════════════════════════════════════")

    r = connect_redis()

    # Poll streams en background
    threading.Thread(target=poll_streams, args=(r,), daemon=True).start()

    log.info("Dashboard disponible en http://localhost:%d", HTTP_PORT)
    HTTPServer(("0.0.0.0", HTTP_PORT), DashboardHandler).serve_forever()


if __name__ == "__main__":
    main()
