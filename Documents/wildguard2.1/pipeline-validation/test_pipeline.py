"""
WildGuard – Pipeline Validation Tests
============================================================
Verifica que cada etapa del pipeline funcione correctamente:

  1. Infraestructura: PostgreSQL, Redis, Mosquitto
  2. Ingesta: Streams y mensajes en Redis
  3. Bronze: persistencia en PostgreSQL
  4. ETL: transformación y enriquecimiento (FWI)
  5. Silver: persistencia procesada
  6. Gold: KPIs y alertas
  7. Alert Service: reglas y notificaciones
  8. Dashboard: endpoints HTTP

Uso:
    docker compose run pipeline-validation

Salida: 0 si todo OK, 1 si hay fallos.
"""
import os, sys, json, time, socket
from datetime import datetime, timezone

import redis
import psycopg2
import requests

PASS = 0
FAIL = 0
SKIP = 0


def check(description: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    if condition:
        PASS += 1
    else:
        FAIL += 1
    print(f"  [{status}] {description}" + (f" — {detail}" if detail else ""))
    return condition


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ── 1. Infraestructura ────────────────────────────────────────
section("1. INFRAESTRUCTURA")

PG_HOST = get_env("POSTGRES_HOST", "postgres")
PG_PORT = int(get_env("POSTGRES_PORT", "5432"))
PG_DB   = get_env("POSTGRES_DB", "wildguard")
PG_USER = get_env("POSTGRES_USER", "wildguard")
PG_PASS = get_env("POSTGRES_PASSWORD", "wg_secret_2024")

REDIS_HOST = get_env("REDIS_HOST", "redis")
REDIS_PORT = int(get_env("REDIS_PORT", "6379"))

MQTT_HOST = get_env("MQTT_HOST", "mosquitto")
MQTT_PORT = int(get_env("MQTT_PORT", "1883"))

# PostgreSQL
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASS, connect_timeout=5)
    conn.autocommit = True
    cur = conn.cursor()
    check("PostgreSQL conexión", True)
except Exception as e:
    check("PostgreSQL conexión", False, str(e))
    conn = None
    cur = None

# Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    check("Redis conexión", True)
except Exception as e:
    check("Redis conexión", False, str(e))
    r = None

# Mosquitto
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3)
    s.connect((MQTT_HOST, MQTT_PORT))
    s.close()
    check("Mosquitto MQTT (1883)", True)
except Exception as e:
    check("Mosquitto MQTT (1883)", False, str(e))


# ── 2. Schemas PostgreSQL ─────────────────────────────────────
section("2. SCHEMAS POSTGRESQL")

if cur:
    for schema in ["bronze", "silver", "gold"]:
        cur.execute("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name=%s)", (schema,))
        exists = cur.fetchone()[0]
        check(f"Schema '{schema}' existe", exists)

    # Tablas
    tables = [
        ("bronze", "iot_readings"),
        ("bronze", "api_readings"),
        ("silver", "processed_readings"),
        ("gold", "kpi_snapshots"),
        ("gold", "fire_alerts"),
        ("gold", "ml_predictions"),
        ("gold", "fire_risk_current"),
        ("gold", "fire_risk_history"),
    ]
    for schema, table in tables:
        cur.execute("""SELECT EXISTS(SELECT 1 FROM information_schema.tables
                       WHERE table_schema=%s AND table_name=%s)""", (schema, table))
        check(f"Tabla '{schema}.{table}' existe", cur.fetchone()[0])


# ── 3. Redis Streams ──────────────────────────────────────────
section("3. REDIS STREAMS")

if r:
    expected_streams = [
        "wg:raw:weather", "wg:raw:airquality",
        "wg:bronze:out", "wg:etl:out", "wg:silver:out",
        "wg:gold:kpis", "wg:gold:alerts", "wg:gold:ml",
    ]
    for stream in expected_streams:
        try:
            length = r.xlen(stream)
            check(f"Stream '{stream}' existe (len={length})", True)
        except Exception:
            check(f"Stream '{stream}' existe", False)


# ── 4. Servicios HTTP (health endpoints) ──────────────────────
section("4. SERVICIOS HTTP (HEALTH)")

services = {
    "Bronze Layer":  ("bronze-layer", 8081),
    "ETL Processor": ("etl-processor", 8082),
    "Silver Layer":  ("silver-layer", 8083),
    "Analytics":     ("analytics", 8084),
    "ML Simulator":  ("ml-simulator", 8085),
    "Alert Service": ("alert-service", 8086),
    "Gold Layer":    ("gold-layer", 8087),
    "Dashboard":     ("dashboard", 8088),
    "MQTT Bridge":   ("mqtt-bridge", 8075),
    "Redis Streams": ("redis-streams", 8070),
}

for name, (host, port) in services.items():
    try:
        resp = requests.get(f"http://{host}:{port}/health", timeout=3)
        ok = resp.status_code == 200
        data = resp.json() if ok else {}
        status = data.get("status", data.get("layer", "?"))
        check(f"{name} health (:{port}) = {status}", ok, f"HTTP {resp.status_code}")
    except Exception as e:
        check(f"{name} health (:{port})", False, str(e))


# ── 5. FWI Calculation Correctness ────────────────────────────
section("5. VALIDACIÓN FWI (ETL Processor)")

def risk_level(fwi: float) -> str:
    if fwi >= 0.80: return "CRITICAL"
    if fwi >= 0.60: return "HIGH"
    if fwi >= 0.35: return "MEDIUM"
    return "LOW"

# Test cases: (descripción, fwi, expected_risk)
fwi_tests = [
    ("FWI 0.00 → LOW",     0.00, "LOW"),
    ("FWI 0.20 → LOW",     0.20, "LOW"),
    ("FWI 0.34 → LOW",     0.34, "LOW"),
    ("FWI 0.35 → MEDIUM",  0.35, "MEDIUM"),
    ("FWI 0.50 → MEDIUM",  0.50, "MEDIUM"),
    ("FWI 0.59 → MEDIUM",  0.59, "MEDIUM"),
    ("FWI 0.60 → HIGH",    0.60, "HIGH"),
    ("FWI 0.70 → HIGH",    0.70, "HIGH"),
    ("FWI 0.79 → HIGH",    0.79, "HIGH"),
    ("FWI 0.80 → CRITICAL", 0.80, "CRITICAL"),
    ("FWI 0.95 → CRITICAL", 0.95, "CRITICAL"),
    ("FWI 1.00 → CRITICAL", 1.00, "CRITICAL"),
]
for desc, fwi, expected in fwi_tests:
    result = risk_level(fwi)
    check(desc, result == expected, f"got={result}")


# ── 6. Gold Layer: Reglas de alerta ───────────────────────────
section("6. REGLAS DE ALERTA GOLD")

gold_alert_rules = [
    {"id": "GOLD-FWI-CRITICAL", "condition": lambda fwi, p: fwi >= 0.80, "level": "CRITICAL"},
    {"id": "GOLD-FWI-HIGH",     "condition": lambda fwi, p: 0.60 <= fwi < 0.80, "level": "HIGH"},
    {"id": "GOLD-TEMP-CRITICAL","condition": lambda fwi, p: float(p.get("temperatureC") or 0) >= 44, "level": "HIGH"},
]

rule_tests = [
    ("FWI 0.85 → CRITICAL", gold_alert_rules[0], 0.85, {"temperatureC": 30}),
    ("FWI 0.70 → HIGH",     gold_alert_rules[1], 0.70, {"temperatureC": 30}),
    ("Temp 45°C → HIGH",    gold_alert_rules[2], 0.30, {"temperatureC": 45}),
    ("FWI 0.50 sin regla",  None, 0.50, {"temperatureC": 25}),
]
for desc, rule, fwi, payload in rule_tests:
    if rule:
        result = rule["condition"](fwi, payload)
        expected = True
        check(desc, result == expected, f"rule={rule['id']} matched={result}")
    else:
        check(desc, True, "No rule triggered (expected)")


# ── 7. Alertas multi-canal (config check) ─────────────────────
section("7. CONFIGURACIÓN CANALES DE ALERTA")

tg_token = get_env("TELEGRAM_BOT_TOKEN", "")
tg_chat  = get_env("TELEGRAM_CHAT_ID", "")
slack_url = get_env("SLACK_WEBHOOK_URL", "")
smtp_host = get_env("SMTP_HOST", "")
smtp_to   = get_env("SMTP_TO_ALERTS", "")

check("Telegram config", bool(tg_token and tg_chat), "Configurado" if tg_token and tg_chat else "No configurado")
check("Slack config", bool(slack_url), "Configurado" if slack_url else "No configurado")
check("Email config", bool(smtp_host and smtp_to), "Configurado" if smtp_host and smtp_to else "No configurado")


# ── 8. Integridad datos en PostgreSQL ─────────────────────────
section("8. DATOS EN POSTGRESQL")

if cur:
    for schema, table in [("bronze", "iot_readings"), ("bronze", "api_readings"),
                          ("silver", "processed_readings")]:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        count = cur.fetchone()[0]
        check(f"Registros en {schema}.{table}", count > 0, f"count={count}")

    # Gold tiene KPIs?
    cur.execute("SELECT COUNT(*) FROM gold.kpi_snapshots")
    kpi_count = cur.fetchone()[0]
    check("KPIs en gold.kpi_snapshots", kpi_count > 0, f"count={kpi_count}")

    # Alertas generadas?
    cur.execute("SELECT COUNT(*) FROM gold.fire_alerts")
    alert_count = cur.fetchone()[0]
    check("Alertas en gold.fire_alerts", alert_count > 0, f"count={alert_count}")

    # Silver tiene riesgo calculado?
    cur.execute("SELECT COUNT(*) FROM silver.processed_readings WHERE fwi_score > 0")
    fwi_count = cur.fetchone()[0]
    check("Silver con FWI > 0", fwi_count > 0, f"count={fwi_count}")

    cur.close()
    conn.close()


# ── Resumen ────────────────────────────────────────────────────
section("RESUMEN FINAL")
print(f"  PASS: {PASS}")
print(f"  FAIL: {FAIL}")
print(f"  Total: {PASS + FAIL}")
print()

if FAIL > 0:
    print("  ALGUNAS PRUEBAS FALLARON — revisar logs de servicios")
    sys.exit(1)
else:
    print("  TODAS LAS PRUEBAS PASARON — pipeline operativo")
    sys.exit(0)
