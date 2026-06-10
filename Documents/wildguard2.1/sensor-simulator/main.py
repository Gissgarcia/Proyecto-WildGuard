"""
WildGuard – Sensor Simulator (MQTT + Datadog Telemetría)
============================================================
Publica lecturas de sensores IoT y GPS vía MQTT a Mosquitto.
Telemetría completa: APM traces, métricas, logs JSON, service checks.
"""

import os, json, uuid, time, random, logging
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from telemetry import init_telemetry, get_tracer, get_logger

# ── Telemetría Datadog ────────────────────────────────────────
metrics = init_telemetry("sensor-simulator")
tracer  = get_tracer()
log     = get_logger("sensor-simulator")

# ── Config ────────────────────────────────────────────────────
MQTT_HOST     = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT     = int(os.getenv("MQTT_PORT", 1883))
EMIT_INTERVAL = int(os.getenv("SENSOR_EMIT_INTERVAL_MS", 5000)) / 1000
SENSOR_COUNT  = int(os.getenv("SENSOR_COUNT", 12))
ZONES         = os.getenv("SENSOR_ZONES",
    "malalcahuello,nahuelbuta,villarrica,cani,radal7tazas").split(",")

ZONE_META = {
    "malalcahuello": {"lat": -38.4522, "lon": -71.5789, "alt": 1200},
    "nahuelbuta":    {"lat": -37.8108, "lon": -72.9723, "alt":  900},
    "villarrica":    {"lat": -39.4213, "lon": -71.9328, "alt": 1500},
    "cani":          {"lat": -38.6601, "lon": -71.5234, "alt": 1800},
    "radal7tazas":   {"lat": -35.4743, "lon": -70.9801, "alt":  700},
}
SENSOR_TYPES = ["TEMPERATURE", "HUMIDITY", "CO2", "WIND", "SMOKE", "MULTI"]

counters = {"published": 0, "errors": 0, "alerts_fired": 0}


def gen_iot_payload(sensor_type: str, zone: str) -> dict:
    p = {"sensorType": sensor_type, "zone": zone, "protocol": "MQTT", "tlsVersion": "1.2"}
    if sensor_type in ("TEMPERATURE", "MULTI"):
        p["temperatureC"] = round(random.gauss(30, 9), 2)
    if sensor_type in ("HUMIDITY", "MULTI"):
        p["humidityPct"]  = round(max(5.0, min(95.0, random.gauss(38, 18))), 2)
    if sensor_type in ("CO2", "MULTI"):
        p["co2Ppm"]       = round(random.gauss(420, 35), 2)
    if sensor_type in ("WIND", "MULTI"):
        p["windKmh"]       = round(abs(random.gauss(22, 14)), 2)
        p["windDirection"] = random.choice(["N","NE","E","SE","S","SO","O","NO"])
    if sensor_type in ("SMOKE", "MULTI"):
        p["smokeDensity"]  = round(max(0.0, random.gauss(0.05, 0.08)), 3)
    return p


def gen_gps_payload(zone: str) -> dict:
    meta = ZONE_META.get(zone, {"lat": -38.0, "lon": -71.5, "alt": 800})
    return {
        "latitude":      round(meta["lat"] + random.uniform(-0.025, 0.025), 6),
        "longitude":     round(meta["lon"] + random.uniform(-0.025, 0.025), 6),
        "altitudeM":     round(meta["alt"] + random.uniform(-80, 80), 1),
        "gpsPrecisionM": round(random.uniform(1.2, 9.5), 2),
        "speedKmh":      round(abs(random.gauss(0, 1.5)), 2),
        "zone":          zone,
    }


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Conectado a Mosquitto broker", extra={"broker": f"{MQTT_HOST}:{MQTT_PORT}"})
        metrics.service_check("mqtt.broker", 0, "Mosquitto connected")
    else:
        log.error("Fallo conexión MQTT rc=%d", rc)
        metrics.service_check("mqtt.broker", 2, f"Connection failed rc={rc}")


def on_publish(client, userdata, mid):
    pass


def connect_mqtt() -> mqtt.Client:
    client = mqtt.Client(client_id=f"wg-sensor-{uuid.uuid4().hex[:8]}")
    client.on_connect = on_connect
    client.on_publish = on_publish
    for attempt in range(15):
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_start()
            time.sleep(1)
            if client.is_connected():
                return client
        except Exception as e:
            log.warning("Mosquitto no disponible intento %d/15: %s", attempt + 1, e)
            time.sleep(3)
    raise RuntimeError("No se pudo conectar a Mosquitto")


def publish_mqtt(client: mqtt.Client, topic: str, payload: dict,
                 qos: int = 1, zone: str = "", source: str = "") -> bool:
    with tracer.trace("mqtt.publish", resource=topic) as span:
        span.set_tag("mqtt.topic", topic)
        span.set_tag("mqtt.qos",   qos)
        span.set_tag("zone",       zone)
        span.set_tag("source",     source)
        try:
            msg    = json.dumps(payload, default=str)
            result = client.publish(topic, msg, qos=qos, retain=False)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                span.set_tag("error", True)
                metrics.increment("mqtt.publish_error",
                                  tags=[f"topic:{topic}", f"zone:{zone}"])
                counters["errors"] += 1
                return False
            counters["published"] += 1
            metrics.increment("mqtt.published",
                              tags=[f"source:{source}", f"zone:{zone}", f"qos:{qos}"])
            span.set_metric("payload.bytes", len(msg))
            return True
        except Exception as e:
            span.set_tag("error", True)
            log.error("Error publicando MQTT topic=%s: %s", topic, e)
            counters["errors"] += 1
            return False


def emit_cycle(client: mqtt.Client, cycle: int):
    """Un ciclo completo de emisión de sensores con traza APM."""
    with tracer.trace("sensor.emit_cycle", resource="all_zones") as span:
        span.set_tag("cycle", cycle)
        span.set_metric("sensor.count", SENSOR_COUNT)
        n_iot = n_gps = 0

        for i in range(SENSOR_COUNT):
            zone        = random.choice(ZONES)
            sensor_type = random.choice(SENSOR_TYPES)
            device_id   = f"WG-IOT-{zone[:4].upper()}-{i:03d}"
            iot_payload = gen_iot_payload(sensor_type, zone)
            event = {
                "eventId":   str(uuid.uuid4()),
                "source":    "IOT_SENSOR",
                "deviceId":  device_id,
                "zone":      zone,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload":   iot_payload,
            }
            topic = f"wildguard/sensors/iot/{zone}/{sensor_type.lower()}"
            if publish_mqtt(client, topic, event, qos=1, zone=zone, source="IOT_SENSOR"):
                n_iot += 1
                log.info("MQTT PUB IoT | topic=%s temp=%s hum=%s smoke=%s",
                         topic,
                         iot_payload.get("temperatureC", "-"),
                         iot_payload.get("humidityPct",  "-"),
                         iot_payload.get("smokeDensity", "-"),
                         extra={"zone": zone, "source": "IOT_SENSOR"})

            # Alerta si humo crítico
            if iot_payload.get("smokeDensity", 0) >= 0.5:
                alert = {**event, "alertType": "SMOKE_THRESHOLD",
                         "severity": "HIGH",
                         "message":  f"Humo crítico {zone}: {iot_payload['smokeDensity']:.3f}"}
                publish_mqtt(client, "wildguard/alerts/fire", alert, qos=2,
                             zone=zone, source="ALERT")
                counters["alerts_fired"] += 1
                metrics.record_alert("HIGH", zone, "RULE-SMOKE")
                log.warning("ALERTA SMOKE | zona=%s smoke=%.3f",
                            zone, iot_payload["smokeDensity"],
                            extra={"zone": zone, "fwi": 0.0, "risk_level": "HIGH"})

            # GPS
            if i % 3 == 0:
                gps_id      = f"WG-GPS-{zone[:4].upper()}-{i:03d}"
                gps_payload = gen_gps_payload(zone)
                gps_event   = {
                    "eventId":   str(uuid.uuid4()),
                    "source":    "GPS",
                    "deviceId":  gps_id,
                    "zone":      zone,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "payload":   gps_payload,
                }
                if publish_mqtt(client, f"wildguard/sensors/gps/{zone}",
                                gps_event, qos=1, zone=zone, source="GPS"):
                    n_gps += 1

        span.set_metric("iot.emitted",  n_iot)
        span.set_metric("gps.emitted",  n_gps)
        span.set_metric("alerts.fired", counters["alerts_fired"])

        metrics.gauge("sensor.cycle",         cycle)
        metrics.gauge("sensor.total_published", counters["published"])
        log.info("Ciclo %d completado | IoT=%d GPS=%d alerts=%d",
                 cycle, n_iot, n_gps, counters["alerts_fired"])
        return n_iot, n_gps


def main():
    log.info("WildGuard Sensor Simulator (MQTT) iniciando",
             extra={"broker": f"{MQTT_HOST}:{MQTT_PORT}",
                    "zones": ZONES, "sensor_count": SENSOR_COUNT})
    client = connect_mqtt()
    cycle  = 0
    while True:
        cycle += 1
        try:
            emit_cycle(client, cycle)
        except Exception as e:
            log.error("Error en ciclo %d: %s", cycle, e)
            metrics.service_check("sensor.emit_cycle", 2, str(e))
        time.sleep(EMIT_INTERVAL)


if __name__ == "__main__":
    main()
