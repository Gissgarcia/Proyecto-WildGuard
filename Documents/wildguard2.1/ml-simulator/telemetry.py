"""
WildGuard – Telemetría Datadog (módulo compartido)
============================================================
Centraliza toda la integración con Datadog:

  1. APM / Distributed Tracing (ddtrace)
     - Spans por operación (ingest, transform, publish, inference)
     - Propagación de contexto entre servicios
     - Tags estándar: env, service, version, zone, source

  2. Métricas Custom (DogStatsD)
     - Contadores, gauges, histogramas, timers
     - Naming convention: wg.<component>.<metric>
     - Tags enriquecidos automáticamente

  3. Logs estructurados (JSON)
     - Campos requeridos por Datadog Log Management:
       dd.trace_id, dd.span_id, dd.service, dd.env, dd.version
     - Niveles: DEBUG, INFO, WARNING, ERROR, CRITICAL
     - Correlación automática APM ↔ Logs

  4. Service Checks
     - Estado UP/DOWN de cada componente
     - Reportado cada 30s al Datadog Agent

  5. Runtime Metrics
     - CPU %, memoria RSS, threads activos, FD abiertos
     - Reportados cada 15s

Uso en cada componente:
    from telemetry import init_telemetry, get_tracer, get_logger, metrics

    telemetry = init_telemetry("bronze-layer")
    tracer    = get_tracer()
    log       = get_logger()

    with tracer.trace("bronze.ingest", resource="iot_sensor") as span:
        span.set_tag("zone", zone)
        span.set_metric("payload.size", len(payload))
        ...
"""

import os
import json
import time
import logging
import threading
import traceback
from datetime import datetime, timezone
from typing import Any

# Datadog
from datadog import initialize, statsd
from datadog.api import ServiceCheck

# ddtrace APM
try:
    from ddtrace import tracer, patch_all, config as dd_config
    from ddtrace.contrib.logging import patch as patch_logging
    DDTRACE_AVAILABLE = True
except ImportError:
    DDTRACE_AVAILABLE = False

# Runtime metrics
try:
    import resource
    RESOURCE_AVAILABLE = True
except ImportError:
    RESOURCE_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


# ── Configuración global ──────────────────────────────────────
DD_AGENT_HOST = os.getenv("DD_AGENT_HOST", "localhost")
DD_AGENT_PORT = int(os.getenv("DD_AGENT_PORT", 8125))
DD_APM_HOST   = os.getenv("DD_AGENT_HOST", "localhost")
DD_APM_PORT   = int(os.getenv("DD_APM_PORT", 8126))
DD_ENV        = os.getenv("DD_ENV", "wildguard-dev")
DD_VERSION    = os.getenv("DD_VERSION", "1.0.0")

_service_name: str = "wildguard"
_initialized:  bool = False


# ── Logging JSON estructurado para Datadog ────────────────────
class DatadogJsonFormatter(logging.Formatter):
    """
    Formatea logs en JSON con campos de correlación Datadog.
    Permite correlacionar logs con trazas APM en la UI de Datadog.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "level":      record.levelname,
            "message":    record.getMessage(),
            "logger":     record.name,
            "module":     record.module,
            "function":   record.funcName,
            "line":       record.lineno,
            # Tags Datadog obligatorios
            "dd.service": _service_name,
            "dd.env":     DD_ENV,
            "dd.version": DD_VERSION,
        }

        # Correlación APM: inyectar trace_id y span_id si hay span activo
        if DDTRACE_AVAILABLE:
            span = tracer.current_span()
            if span:
                log_entry["dd.trace_id"] = str(span.trace_id)
                log_entry["dd.span_id"]  = str(span.span_id)

        # Excepción si la hay
        if record.exc_info:
            log_entry["error.kind"]    = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
            log_entry["error.message"] = str(record.exc_info[1])
            log_entry["error.stack"]   = traceback.format_exception(*record.exc_info)[-1].strip()

        # Campos extra pasados con extra={}
        for key in ("zone", "source", "stream", "device_id", "fwi", "risk_level",
                    "latency_ms", "records", "job_id"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        return json.dumps(log_entry, default=str)


def get_logger(name: str = None) -> logging.Logger:
    """Retorna logger configurado con formato JSON para Datadog."""
    logger = logging.getLogger(name or _service_name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(DatadogJsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


# ── Métricas DogStatsD ────────────────────────────────────────
class WildGuardMetrics:
    """
    Wrapper sobre DogStatsD con naming convention y tags automáticos.
    Todos los métodos aceptan tags adicionales como lista.
    """
    def __init__(self, service: str):
        self.service = service
        self._base_tags = [
            f"service:{service}",
            f"env:{DD_ENV}",
            f"version:{DD_VERSION}",
        ]

    def _tags(self, extra: list[str] = None) -> list[str]:
        return self._base_tags + (extra or [])

    # ── Contadores ────────────────────────────────────────────
    def increment(self, metric: str, value: int = 1, tags: list[str] = None):
        statsd.increment(f"wg.{metric}", value, tags=self._tags(tags))

    def decrement(self, metric: str, value: int = 1, tags: list[str] = None):
        statsd.decrement(f"wg.{metric}", value, tags=self._tags(tags))

    # ── Gauges ────────────────────────────────────────────────
    def gauge(self, metric: str, value: float, tags: list[str] = None):
        statsd.gauge(f"wg.{metric}", value, tags=self._tags(tags))

    # ── Histogramas / Timers ──────────────────────────────────
    def histogram(self, metric: str, value: float, tags: list[str] = None):
        statsd.histogram(f"wg.{metric}", value, tags=self._tags(tags))

    def timing(self, metric: str, value_ms: float, tags: list[str] = None):
        statsd.timing(f"wg.{metric}", value_ms, tags=self._tags(tags))

    # ── Eventos ───────────────────────────────────────────────
    def event(self, title: str, text: str,
               alert_type: str = "info", tags: list[str] = None):
        statsd.event(title, text,
                     alert_type=alert_type,
                     tags=self._tags(tags))

    # ── Service Check ─────────────────────────────────────────
    def service_check(self, name: str, status: int,
                      message: str = "", tags: list[str] = None):
        """
        status: 0=OK, 1=WARNING, 2=CRITICAL, 3=UNKNOWN
        """
        statsd.service_check(
            f"wg.{name}",
            status,
            message=message,
            tags=self._tags(tags),
        )

        # ── Pipeline stage throughput (por minuto) ──────────────
    def record_throughput(self, stage: str, count: int = 1, tags: list[str] = None):
        extra = [f"stage:{stage}"] + (tags or [])
        self.increment("pipeline.throughput", count, tags=extra)

    def gauge_throughput_rpm(self, stage: str, rpm: float, tags: list[str] = None):
        extra = [f"stage:{stage}"] + (tags or [])
        self.gauge("pipeline.throughput_rpm", rpm, tags=extra)

    # ── End-to-end latency ─────────────────────────────────
    def record_e2e_latency(self, latency_ms: float, source: str, zone: str = ""):
        extra = [f"source:{source}"]
        if zone: extra.append(f"zone:{zone}")
        self.histogram("pipeline.e2e_latency_ms", latency_ms, tags=extra)

    # ── Component health ───────────────────────────────────
    def record_component_health(self, component: str, status: str):
        self.gauge(f"health.{component}", 1 if status == "UP" else 0,
                   tags=[f"status:{status}", f"component:{component}"])

    # ── Métricas de pipeline ──────────────────────────────────
    def record_ingestion(self, source: str, zone: str, success: bool = True):
        status = "ok" if success else "error"
        self.increment("pipeline.ingested",
                       tags=[f"source:{source}", f"zone:{zone}", f"status:{status}"])

    def record_latency(self, stage: str, latency_ms: float,
                       source: str = "", zone: str = ""):
        extra = [f"stage:{stage}"]
        if source: extra.append(f"source:{source}")
        if zone:   extra.append(f"zone:{zone}")
        self.histogram("pipeline.latency_ms", latency_ms, tags=extra)

    def record_fwi(self, fwi: float, risk_level: str, zone: str):
        self.gauge("fwi.score",  fwi,         tags=[f"zone:{zone}", f"risk:{risk_level}"])
        self.increment("fwi.computed", tags=[f"risk:{risk_level}", f"zone:{zone}"])

    def record_alert(self, level: str, zone: str, rule_id: str = ""):
        extra = [f"level:{level}", f"zone:{zone}"]
        if rule_id: extra.append(f"rule:{rule_id}")
        self.increment("alert.fired", tags=extra)
        if level == "CRITICAL":
            self.event(
                f"WildGuard CRITICAL Alert – {zone}",
                f"Riesgo crítico de incendio en {zone}",
                alert_type="error",
                tags=extra,
            )

    def record_stream_len(self, stream: str, length: int):
        clean = stream.replace(":", "_").replace("/", "_")
        self.gauge(f"stream.{clean}.length", length)

    def record_ml_inference(self, zone: str, prob: float,
                             risk_class: str, latency_ms: float):
        self.gauge("ml.fire_probability", prob,
                   tags=[f"zone:{zone}", f"class:{risk_class}"])
        self.histogram("ml.latency_ms", latency_ms, tags=[f"zone:{zone}"])
        self.increment("ml.inferences", tags=[f"class:{risk_class}"])


# ── Runtime metrics ───────────────────────────────────────────
def _collect_runtime_metrics(metrics: WildGuardMetrics):
    """
    Recolecta métricas de runtime del proceso y las envía a Datadog.
    Equivale a las runtime metrics automáticas del agente Datadog.
    """
    log = get_logger("telemetry.runtime")
    while True:
        try:
            if PSUTIL_AVAILABLE:
                import psutil, os
                proc = psutil.Process(os.getpid())

                # CPU
                cpu_pct = proc.cpu_percent(interval=1)
                metrics.gauge("runtime.cpu_percent", cpu_pct)

                # Memoria
                mem = proc.memory_info()
                metrics.gauge("runtime.memory.rss_mb",
                              mem.rss / 1024 / 1024)
                metrics.gauge("runtime.memory.vms_mb",
                              mem.vms / 1024 / 1024)

                # Threads y file descriptors
                metrics.gauge("runtime.threads",     proc.num_threads())
                metrics.gauge("runtime.open_fds",    proc.num_fds()
                              if hasattr(proc, "num_fds") else 0)

                log.debug("Runtime metrics enviadas: cpu=%.1f%% rss=%.1fMB threads=%d",
                          cpu_pct, mem.rss / 1024 / 1024, proc.num_threads())

            # Service check: el proceso está vivo
            metrics.service_check("process.alive", 0, "Process running OK")

        except Exception as e:
            log.warning("Error recolectando runtime metrics: %s", e)
            metrics.service_check("process.alive", 2, f"Error: {e}")

        time.sleep(15)


# ── APM Tracer ────────────────────────────────────────────────
def get_tracer():
    """Retorna el tracer ddtrace configurado, o un stub si no está disponible."""
    if DDTRACE_AVAILABLE:
        return tracer
    return _StubTracer()


class _StubSpan:
    """Span no-op cuando ddtrace no está disponible."""
    def set_tag(self, *a, **kw): pass
    def set_metric(self, *a, **kw): pass
    def set_error(self, *a, **kw): pass
    def finish(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): pass


class _StubTracer:
    def trace(self, *a, **kw): return _StubSpan()
    def current_span(self): return None
    def current_root_span(self): return None


# ── Init principal ────────────────────────────────────────────
def init_telemetry(service: str) -> "WildGuardMetrics":
    """
    Inicializa toda la telemetría Datadog para un componente.
    Llama una sola vez al inicio de cada microservicio.

    Args:
        service: nombre del componente (ej: "bronze-layer")

    Returns:
        WildGuardMetrics: instancia lista para usar
    """
    global _service_name, _initialized
    _service_name = service

    log = logging.getLogger(f"{service}.telemetry")

    # 1. DogStatsD
    initialize(
        statsd_host=DD_AGENT_HOST,
        statsd_port=DD_AGENT_PORT,
    )
    log.info("[telemetry] DogStatsD → %s:%d", DD_AGENT_HOST, DD_AGENT_PORT)

    # 2. APM / ddtrace
    if DDTRACE_AVAILABLE:
        os.environ.setdefault("DD_AGENT_HOST",   DD_APM_HOST)
        os.environ.setdefault("DD_TRACE_AGENT_PORT", str(DD_APM_PORT))
        os.environ.setdefault("DD_SERVICE",      service)
        os.environ.setdefault("DD_ENV",          DD_ENV)
        os.environ.setdefault("DD_VERSION",      DD_VERSION)

        dd_config.env     = DD_ENV
        dd_config.version = DD_VERSION

        # Parchear librerías automáticamente (redis, psycopg2, requests, logging)
        patch_all(
            redis=True,
            psycopg2=True,
            requests=True,
            logging=True,
        )
        log.info("[telemetry] APM ddtrace activado → %s:%d (service=%s)",
                 DD_APM_HOST, DD_APM_PORT, service)
    else:
        log.warning("[telemetry] ddtrace no disponible — APM deshabilitado")

    # 3. Logger JSON
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(DatadogJsonFormatter())
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # 4. Instancia de métricas
    metrics_instance = WildGuardMetrics(service)

    # 5. Runtime metrics en hilo background
    threading.Thread(
        target=_collect_runtime_metrics,
        args=(metrics_instance,),
        daemon=True,
        name=f"{service}-runtime-metrics",
    ).start()
    log.info("[telemetry] Runtime metrics iniciadas (intervalo=15s)")

    # 6. Evento de inicio en Datadog
    metrics_instance.event(
        f"WildGuard {service} iniciado",
        f"Componente {service} arrancó correctamente en env={DD_ENV}",
        alert_type="info",
    )

    _initialized = True
    log.info("[telemetry] ✓ Telemetría Datadog lista para '%s'", service)
    return metrics_instance
