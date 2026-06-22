"""
WildGuard – Dashboard (Flask + Chart.js + SSE)
============================================================
Etapa 9 del pipeline: CLIENTE FINAL

Características:
  - Flask con SSE para actualización en tiempo real
  - Chart.js (FWI trend, risk distribution, zone bars)
  - API REST para KPIs, alertas, predicciones ML
  - Webhook receiver para alertas del alert-service
"""
import os, json, time, logging, threading
from datetime import datetime, timezone

import redis
from flask import Flask, Response, jsonify, render_template_string
from telemetry import init_telemetry, get_tracer, get_logger

metrics = init_telemetry("dashboard")
tracer  = get_tracer()
log     = get_logger("dashboard")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
HTTP_PORT  = int(os.getenv("HTTP_PORT", 8088))

state = {
    "kpi":          {},
    "alerts":       [],
    "ml_events":    [],
    "fwi_history":  [],
    "initialized":  datetime.now(timezone.utc).isoformat(),
    "requests":     0,
}
state_lock = threading.Lock()

app = Flask(__name__)


def connect_redis():
    for i in range(15):
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            r.ping()
            log.info("Redis conectado")
            return r
        except Exception as e:
            log.warning("Redis no disponible (%d/15): %s", i + 1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Redis")


def poll_streams(r: redis.Redis):
    kpi_last = "$"
    alert_last = "$"
    ml_last = "$"

    while True:
        try:
            kpi_entries = r.xread({"wg:gold:kpis": kpi_last}, count=5, block=2000)
            if kpi_entries:
                for _, msgs in kpi_entries:
                    for mid, fields in msgs:
                        kpi_last = mid
                        kpi_data = json.loads(fields.get("kpi", "{}"))
                        with state_lock:
                            state["kpi"] = kpi_data
                            fwi = kpi_data.get("avgFwi")
                            if fwi is not None:
                                state["fwi_history"].append({
                                    "t": datetime.now().isoformat(),
                                    "v": fwi
                                })
                                if len(state["fwi_history"]) > 50:
                                    state["fwi_history"] = state["fwi_history"][-50:]
        except Exception as e:
            log.debug("Poll KPIs: %s", e)

        try:
            alert_entries = r.xread({"wg:gold:alerts": alert_last}, count=10, block=2000)
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
                        with state_lock:
                            state["alerts"].insert(0, alert)
                            if len(state["alerts"]) > 50:
                                state["alerts"].pop()
        except Exception as e:
            log.debug("Poll alerts: %s", e)

        try:
            ml_entries = r.xread({"wg:gold:ml": ml_last}, count=10, block=2000)
            if ml_entries:
                for _, msgs in ml_entries:
                    for mid, fields in msgs:
                        ml_last = mid
                        ml_event = {
                            "zone": fields.get("zone"),
                            "fireProbability": fields.get("fireProbability"),
                            "riskClass": fields.get("riskClass"),
                            "confidence": fields.get("confidence"),
                            "shapTopFeature": fields.get("shapTopFeature"),
                            "timestamp": fields.get("timestamp"),
                        }
                        with state_lock:
                            state["ml_events"].insert(0, ml_event)
                            if len(state["ml_events"]) > 30:
                                state["ml_events"].pop()
        except Exception as e:
            log.debug("Poll ML: %s", e)

        time.sleep(1)


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WildGuard Chile – Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:'Segoe UI',sans-serif; background:#0a1628; color:#e2e8f0; }
  header { background:linear-gradient(135deg,#1b3a6b,#0f1f3d); padding:20px 30px; display:flex;
           justify-content:space-between; align-items:center; border-bottom:2px solid #2a4a7f; }
  header h1 { font-size:20px; color:#fff; }
  header .badge { font-size:11px; background:#00897b; color:#fff; padding:4px 10px; border-radius:20px; }
  header .badge.warning { background:#e65100; }
  .grid { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; padding:20px; }
  .card { background:#1e2d40; border-radius:10px; padding:16px; border:1px solid #2a3d55; }
  .card h3 { font-size:11px; color:#94a3b8; text-transform:uppercase;
             letter-spacing:.05em; margin-bottom:4px; }
  .card .val { font-size:28px; font-weight:600; color:#38bdf8; }
  .card .sub { font-size:12px; color:#64748b; margin-top:4px; }
  .chart-row { display:grid; grid-template-columns:1fr 1fr; gap:12px; padding:0 20px 20px; }
  .chart-card { background:#1e2d40; border-radius:10px; padding:16px; border:1px solid #2a3d55; }
  .chart-card h3 { font-size:13px; color:#94a3b8; margin-bottom:8px; }
  .chart-card canvas { max-height:250px; }
  .section { padding:0 20px 20px; }
  .section h2 { font-size:14px; color:#94a3b8; margin-bottom:10px;
                text-transform:uppercase; letter-spacing:.05em; display:flex; align-items:center; gap:8px; }
  .section h2 .count { font-size:11px; background:#2a3d55; padding:2px 8px; border-radius:12px; color:#94a3b8; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th { background:#1b3a6b; color:#93c5fd; padding:8px 12px; text-align:left; font-weight:500; }
  td { padding:8px 12px; border-bottom:1px solid #1e2d40; }
  tr:hover td { background:#1e2d40; }
  .CRITICAL { color:#f87171; font-weight:700; }
  .HIGH     { color:#fb923c; font-weight:600; }
  .MEDIUM   { color:#fbbf24; }
  .LOW      { color:#4ade80; }
  footer { text-align:center; padding:16px; color:#334155; font-size:12px; }
  .updated { font-size:11px; color:#64748b; padding:0 20px; }
  @media (max-width:900px) { .grid { grid-template-columns:1fr 1fr; } .chart-row { grid-template-columns:1fr; } }
  @media (max-width:500px) { .grid { grid-template-columns:1fr; } }
  .loading { color:#64748b; padding:12px; }
</style>
</head>
<body>
<header>
  <h1>WildGuard Chile – Monitoreo de Incendios Forestales</h1>
  <div><span class="badge" id="live-badge">LIVE</span></div>
</header>

<div class="grid" id="kpi-grid">
  <div class="card"><h3>Registros totales</h3>
    <div class="val" id="total-records">--</div>
    <div class="sub">Última ventana 30s</div></div>
  <div class="card"><h3>FWI promedio</h3>
    <div class="val" id="avg-fwi">--</div>
    <div class="sub">Fire Weather Index</div></div>
  <div class="card"><h3>Alertas activas</h3>
    <div class="val" id="total-alerts" style="color:#fb923c">--</div>
    <div class="sub">Críticas: <span id="critical-alerts">0</span></div></div>
  <div class="card"><h3>SLA Compliance</h3>
    <div class="val" id="sla" style="color:#4ade80">--</div>
    <div class="sub">Objetivo &gt;99%</div></div>
  <div class="card"><h3>Temp. promedio IoT</h3>
    <div class="val" id="avg-temp">--</div>
    <div class="sub">Sensores activos</div></div>
  <div class="card"><h3>Humedad promedio</h3>
    <div class="val" id="avg-hum">--</div>
    <div class="sub">Sensores activos</div></div>
  <div class="card"><h3>CO&#8322; promedio</h3>
    <div class="val" id="avg-co2">--</div>
    <div class="sub">ppm</div></div>
  <div class="card"><h3>FWI máximo</h3>
    <div class="val" id="max-fwi" style="color:#f87171">--</div>
    <div class="sub" id="top-zone">Zona más crítica</div></div>
</div>

<div class="chart-row">
  <div class="chart-card">
    <h3>FWI Trend (últimos puntos)</h3>
    <canvas id="fwiChart"></canvas>
  </div>
  <div class="chart-card">
    <h3>Distribución de Riesgo</h3>
    <canvas id="riskChart"></canvas>
  </div>
</div>

<div class="section">
  <h2>Alertas Recientes <span class="count" id="alert-count">0</span></h2>
  <table id="alerts-table">
    <tr><th>Hora</th><th>Zona</th><th>Nivel</th><th>FWI</th><th>Mensaje</th></tr>
  </table>
</div>

<div class="section">
  <h2>Predicciones ML <span class="count" id="ml-count">0</span></h2>
  <table id="ml-table">
    <tr><th>Zona</th><th>Probabilidad</th><th>Clase</th><th>Confianza</th><th>Feature Top</th></tr>
  </table>
</div>

<div class="section">
  <h2>Top Zonas de Riesgo</h2>
  <table id="zones-table">
    <tr><th>Zona</th><th>FWI Promedio</th></tr>
  </table>
</div>

<footer>
  WildGuard Chile | Pipeline: Sensor &rarr; Redis Streams &rarr; Bronze &rarr; ETL &rarr; Silver &rarr; Gold &rarr; Dashboard
  | Actualizado: <span id="updated-at">--</span>
</footer>

<script>
let fwiChart = null;
let riskChart = null;
let alertHistory = [];
let mlHistory = [];

function initCharts() {
  const ctx1 = document.getElementById('fwiChart').getContext('2d');
  fwiChart = new Chart(ctx1, {
    type: 'line',
    data: { labels: [], datasets: [{
      label: 'FWI',
      data: [],
      borderColor: '#38bdf8',
      backgroundColor: 'rgba(56,189,248,0.1)',
      fill: true,
      tension: 0.3,
      pointRadius: 3,
    }]},
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#64748b', maxTicksLimit: 10 }, grid: { color: '#1e2d40' } },
        y: { min: 0, max: 1, ticks: { color: '#64748b' }, grid: { color: '#1e2d40' } }
      }
    }
  });

  const ctx2 = document.getElementById('riskChart').getContext('2d');
  riskChart = new Chart(ctx2, {
    type: 'doughnut',
    data: {
      labels: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'],
      datasets: [{
        data: [1, 0, 0, 0],
        backgroundColor: ['#4ade80', '#fbbf24', '#fb923c', '#f87171'],
        borderWidth: 0,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { color: '#94a3b8', padding: 12 } }
      }
    }
  });
}

function updateDashboard() {
  fetch('/api/state')
    .then(r => r.json())
    .then(data => {
      const kpi = data.kpi || {};
      const alerts = data.alerts || [];
      const ml = data.ml || [];
      const fwiHist = data.fwiHistory || [];
      alertHistory = alerts;
      mlHistory = ml;

      document.getElementById('total-records').textContent = kpi.totalRecords ?? '--';
      document.getElementById('avg-fwi').textContent = kpi.avgFwi ? kpi.avgFwi.toFixed(3) : '--';
      document.getElementById('total-alerts').textContent = kpi.totalAlerts ?? 0;
      document.getElementById('critical-alerts').textContent = kpi.criticalAlerts ?? 0;
      document.getElementById('sla').textContent = kpi.slaCompliance ? kpi.slaCompliance.toFixed(1) + '%' : '--';
      document.getElementById('avg-temp').textContent = kpi.avgTemperature ? kpi.avgTemperature.toFixed(1) + '\u00b0C' : '--';
      document.getElementById('avg-hum').textContent = kpi.avgHumidity ? kpi.avgHumidity.toFixed(1) + '%' : '--';
      document.getElementById('avg-co2').textContent = kpi.avgCo2Ppm ? kpi.avgCo2Ppm.toFixed(0) : '--';
      document.getElementById('max-fwi').textContent = kpi.maxFwi ? kpi.maxFwi.toFixed(3) : '--';
      document.getElementById('updated-at').textContent = new Date().toISOString().slice(0,19) + 'Z';

      const topZone = (kpi.topRiskZones && kpi.topRiskZones.length > 0) ? kpi.topRiskZones[0].zone : '--';
      document.getElementById('top-zone').textContent = 'Máx: ' + topZone;

      const liveBadge = document.getElementById('live-badge');
      liveBadge.className = 'badge' + (kpi.criticalAlerts > 0 ? ' warning' : '');
      liveBadge.textContent = kpi.criticalAlerts > 0 ? 'CRITICAL' : 'LIVE';

      // FWI Chart
      if (fwiChart && fwiHist.length > 0) {
        const labels = fwiHist.map((p, i) => i);
        const values = fwiHist.map(p => p.v);
        fwiChart.data.labels = labels;
        fwiChart.data.datasets[0].data = values;
        fwiChart.update('none');
      }

      // Risk distribution chart
      if (riskChart && kpi.riskDistribution) {
        const rd = kpi.riskDistribution;
        riskChart.data.datasets[0].data = [
          rd.LOW || 0, rd.MEDIUM || 0, rd.HIGH || 0, rd.CRITICAL || 0
        ];
        riskChart.update('none');
      }

      // Alerts table
      const at = document.getElementById('alerts-table');
      let alertRows = '<tr><th>Hora</th><th>Zona</th><th>Nivel</th><th>FWI</th><th>Mensaje</th></tr>';
      alerts.slice(0, 15).forEach(a => {
        const ts = (a.firedAt || '').slice(0,19).replace('T', ' ');
        alertRows += '<tr><td>' + ts + '</td><td>' + (a.zone||'') + '</td>' +
          '<td class="' + (a.riskLevel||'') + '">' + (a.riskLevel||'') + '</td>' +
          '<td>' + parseFloat(a.fwi||0).toFixed(3) + '</td>' +
          '<td>' + (a.message||'') + '</td></tr>';
      });
      if (alerts.length === 0) {
        alertRows += '<tr><td colspan="5" style="color:#4ade80">Sin alertas activas</td></tr>';
      }
      at.innerHTML = alertRows;
      document.getElementById('alert-count').textContent = alerts.length;

      // ML table
      const mt = document.getElementById('ml-table');
      let mlRows = '<tr><th>Zona</th><th>Probabilidad</th><th>Clase</th><th>Confianza</th><th>Feature Top</th></tr>';
      ml.slice(0, 10).forEach(m => {
        mlRows += '<tr><td>' + (m.zone||'') + '</td>' +
          '<td>' + parseFloat(m.fireProbability||0).toFixed(1) + '%</td>' +
          '<td class="' + (m.riskClass||'') + '">' + (m.riskClass||'') + '</td>' +
          '<td>' + parseFloat(m.confidence||0).toFixed(1) + '%</td>' +
          '<td>' + (m.shapTopFeature||'-') + '</td></tr>';
      });
      if (ml.length === 0) mlRows += '<tr><td colspan="5">Sin predicciones</td></tr>';
      mt.innerHTML = mlRows;
      document.getElementById('ml-count').textContent = ml.length;

      // Zones table
      const zt = document.getElementById('zones-table');
      let zoneRows = '<tr><th>Zona</th><th>FWI Promedio</th></tr>';
      (kpi.topRiskZones || []).forEach(z => {
        zoneRows += '<tr><td>' + (z.zone||'') + '</td><td>' + (z.avgFwi||0).toFixed(3) + '</td></tr>';
      });
      if (!kpi.topRiskZones || kpi.topRiskZones.length === 0) {
        zoneRows += '<tr><td colspan="2">Sin datos de zonas</td></tr>';
      }
      zt.innerHTML = zoneRows;
    })
    .catch(err => console.error('Error fetching state:', err));
}

document.addEventListener('DOMContentLoaded', function() {
  initCharts();
  updateDashboard();
  setInterval(updateDashboard, 3000);
});
</script>
</body></html>"""


# ── Rutas Flask ────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/kpi")
def api_kpi():
    with state_lock:
        return jsonify(state["kpi"])


@app.route("/api/alerts")
def api_alerts():
    with state_lock:
        return jsonify(state["alerts"])


@app.route("/api/ml")
def api_ml():
    with state_lock:
        return jsonify(state["ml_events"])


@app.route("/api/state")
def api_state():
    with state_lock:
        return jsonify({
            "kpi": state["kpi"],
            "alerts": state["alerts"][:20],
            "ml": state["ml_events"][:10],
            "fwiHistory": state["fwi_history"][-30:],
        })


@app.route("/api/events")
def sse_events():
    def event_stream():
        last_kpi = ""
        last_alerts = 0
        while True:
            with state_lock:
                kpi_json = json.dumps(state["kpi"], default=str)
                alert_count = len(state["alerts"])
            if kpi_json != last_kpi or alert_count != last_alerts:
                last_kpi = kpi_json
                last_alerts = alert_count
                with state_lock:
                    payload = json.dumps({
                        "kpi": state["kpi"],
                        "alerts": state["alerts"][:5],
                        "fwiHistory": state["fwi_history"][-30:],
                    }, default=str)
                yield f"data: {payload}\n\n"
            time.sleep(2)
    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-store", "Connection": "keep-alive"})


@app.route("/api/alerts/ingest", methods=["POST"])
def webhook_ingest():
    from flask import request
    data = request.get_json(silent=True)
    if data:
        with state_lock:
            state["alerts"].insert(0, data)
            if len(state["alerts"]) > 50:
                state["alerts"].pop()
        log.info("Dashboard webhook alerta recibida | zona=%s nivel=%s",
                 data.get("zone"), data.get("riskLevel"))
    return jsonify({"accepted": True}), 202


@app.route("/health")
def health():
    with state_lock:
        return jsonify({
            "status": "UP",
            "component": "dashboard",
            "requests": state["requests"],
            "alerts": len(state["alerts"]),
        })


def main():
    log.info("WildGuard Dashboard (Flask+Chart.js) iniciando en puerto %d", HTTP_PORT)
    r = connect_redis()
    threading.Thread(target=poll_streams, args=(r,), daemon=True).start()
    app.run(host="0.0.0.0", port=HTTP_PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
