"""
WildGuard – Microservicio de Clima (versión simple)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FastAPI · Un solo archivo · Listo para correr

Instalación:
    pip install fastapi uvicorn httpx

Correr:
    uvicorn main:app --reload
    Documentación: http://localhost:8000/docs
"""

import math
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field

app = FastAPI(
    title="WildGuard – Microservicio de Clima",
    description="Consulta clima, calcula riesgo de incendio y genera alertas.",
    version="1.0.0",
)

# ── Configuración ─────────────────────────────────────────────────────
API_KEY_VALIDA = "wildguard-2026"
CONAF_API_URL  = "https://api.conaf.cl/v1"       # reemplazar por URL real

# Umbrales de riesgo
TEMP_CRITICA     = 40.0   # °C
HUMEDAD_CRITICA  = 15.0   # %
VIENTO_CRITICO   = 60.0   # km/h


# ── Modelos ───────────────────────────────────────────────────────────

class NivelRiesgo(str, Enum):
    NORMAL  = "NORMAL"
    MEDIO   = "MEDIO"
    ALTO    = "ALTO"
    CRITICO = "CRITICO"


class DatoClima(BaseModel):
    zona:           str
    temperatura_c:  float = Field(..., ge=-50, le=80)
    humedad_pct:    float = Field(..., ge=0,   le=100)
    viento_kmh:     float = Field(..., ge=0,   le=300)
    indice_uv:      Optional[float] = None
    ts_utc:         datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RiesgoIncendio(BaseModel):
    zona:      str
    nivel:     NivelRiesgo
    score:     float
    factores:  list[str]
    mensaje:   str


class AlertaClima(BaseModel):
    zona:        str
    tipo:        str
    descripcion: str
    nivel:       NivelRiesgo
    ts_utc:      datetime


# ── Lógica de riesgo ──────────────────────────────────────────────────

def calcular_riesgo(dato: DatoClima) -> RiesgoIncendio:
    """
    IRMI simplificado:
        score = 0.35×(100-humedad) + 0.35×temp_norm + 0.30×viento_norm
    """
    factores = []

    # Componentes normalizados a [0, 100]
    hum_score    = max(0.0, 100.0 - dato.humedad_pct)
    temp_norm    = min(100.0, max(0.0, (dato.temperatura_c - 10) / 50 * 100))
    viento_norm  = min(100.0, dato.viento_kmh / 80 * 100)

    score = round(0.35 * hum_score + 0.35 * temp_norm + 0.30 * viento_norm, 1)

    # Factores que disparan la alerta
    if dato.temperatura_c >= TEMP_CRITICA:
        factores.append(f"Temperatura alta: {dato.temperatura_c}°C")
    if dato.humedad_pct <= HUMEDAD_CRITICA:
        factores.append(f"Humedad crítica: {dato.humedad_pct}%")
    if dato.viento_kmh >= VIENTO_CRITICO:
        factores.append(f"Viento fuerte: {dato.viento_kmh} km/h")

    # Clasificación
    if score >= 75:
        nivel   = NivelRiesgo.CRITICO
        mensaje = "⚠️ Riesgo extremo. Activar brigadas de emergencia de inmediato."
    elif score >= 50:
        nivel   = NivelRiesgo.ALTO
        mensaje = "🔶 Riesgo alto. Aumentar vigilancia y preparar recursos."
    elif score >= 25:
        nivel   = NivelRiesgo.MEDIO
        mensaje = "🟡 Riesgo moderado. Mantener patrullaje frecuente."
    else:
        nivel   = NivelRiesgo.NORMAL
        mensaje = "✅ Condiciones normales. Vigilancia rutinaria."

    return RiesgoIncendio(zona=dato.zona, nivel=nivel, score=score,
                          factores=factores, mensaje=mensaje)


def generar_alertas(dato: DatoClima) -> list[AlertaClima]:
    """Genera alertas para cada parámetro que supere su umbral."""
    alertas = []
    ahora   = datetime.now(timezone.utc)

    checks = [
        (dato.temperatura_c >= TEMP_CRITICA,
         "TEMPERATURA_CRITICA",
         f"Temperatura de {dato.temperatura_c}°C supera el umbral crítico de {TEMP_CRITICA}°C",
         NivelRiesgo.CRITICO),

        (dato.humedad_pct <= HUMEDAD_CRITICA,
         "HUMEDAD_CRITICA",
         f"Humedad de {dato.humedad_pct}% está por debajo del umbral crítico de {HUMEDAD_CRITICA}%",
         NivelRiesgo.CRITICO),

        (dato.viento_kmh >= VIENTO_CRITICO,
         "VIENTO_FUERTE",
         f"Viento de {dato.viento_kmh} km/h supera el umbral crítico de {VIENTO_CRITICO} km/h",
         NivelRiesgo.ALTO),
    ]

    for condicion, tipo, descripcion, nivel in checks:
        if condicion:
            alertas.append(AlertaClima(
                zona=dato.zona, tipo=tipo,
                descripcion=descripcion, nivel=nivel, ts_utc=ahora,
            ))

    return alertas


# ── Helper de autenticación ───────────────────────────────────────────

def verificar_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY_VALIDA:
        raise HTTPException(status_code=401, detail="API Key inválida.")
    return x_api_key


# ── Endpoints ─────────────────────────────────────────────────────────

@app.get("/health", tags=["Sistema"])
def health():
    """Verifica que el servicio está activo."""
    return {"status": "ok", "ts": datetime.now(timezone.utc)}


@app.post("/clima/riesgo", response_model=RiesgoIncendio, tags=["Clima"])
def evaluar_riesgo(dato: DatoClima, api_key: str = Header(..., alias="X-API-Key")):
    """
    Recibe una lectura meteorológica y calcula el nivel de riesgo de incendio.

    Ejemplo de body:
    ```json
    {
      "zona": "peñuelas",
      "temperatura_c": 42.0,
      "humedad_pct": 12.0,
      "viento_kmh": 55.0
    }
    ```
    """
    verificar_api_key(api_key)
    return calcular_riesgo(dato)


@app.post("/clima/alertas", response_model=list[AlertaClima], tags=["Clima"])
def evaluar_alertas(dato: DatoClima, api_key: str = Header(..., alias="X-API-Key")):
    """
    Evalúa si los datos meteorológicos superan los umbrales críticos
    y devuelve una lista de alertas generadas.
    """
    verificar_api_key(api_key)
    alertas = generar_alertas(dato)
    if not alertas:
        return []
    return alertas


@app.post("/clima/analizar", tags=["Clima"])
def analizar_completo(dato: DatoClima, api_key: str = Header(..., alias="X-API-Key")):
    """
    Análisis completo: riesgo + alertas en una sola llamada.
    """
    verificar_api_key(api_key)
    return {
        "dato":    dato,
        "riesgo":  calcular_riesgo(dato),
        "alertas": generar_alertas(dato),
    }


@app.get("/clima/actual/{zona}", tags=["Clima"])
async def clima_actual(zona: str, api_key: str = Header(..., alias="X-API-Key")):
    """
    Consulta el clima actual de una zona desde la API de CONAF.
    Devuelve el dato junto con el riesgo calculado.

    Nota: requiere configurar CONAF_API_URL y una API Key real.
    """
    verificar_api_key(api_key)
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{CONAF_API_URL}/estaciones/{zona}/actual")
            resp.raise_for_status()
            raw = resp.json()

        dato = DatoClima(
            zona=zona,
            temperatura_c=float(raw["temp_aire"]),
            humedad_pct=float(raw["hum_rel"]),
            viento_kmh=float(raw["vel_viento_kmh"]),
            indice_uv=raw.get("indice_uv"),
        )
        return {"dato": dato, "riesgo": calcular_riesgo(dato)}

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502,
                            detail=f"Error en API CONAF: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=503,
                            detail=f"No se pudo obtener clima para '{zona}': {str(e)}")
