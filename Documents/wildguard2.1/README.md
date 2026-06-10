# WildGuard Chile – Sistema de Monitoreo de Incendios Forestales
## Arquitectura de Diseño – Tercera Nota (DAY1101)

---

## Pipeline Implementado

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              WILDGUARD CHILE – ARQUITECTURA DE DATOS (PIPELINE)             │
├───────────────┬──────────────┬─────────────┬─────────────────┬─────────────┤
│ 1. FUENTES    │ 3. STREAMING │ 4. BRONZE   │ 5. ETL          │ 6. SILVER   │
│               │ (Redis)      │ (Raw-Crudo) │ (Lambda+Glue)   │ (Procesado) │
│ sensor-sim ───┤              │             │                 │             │
│ (IoT MQTT)    │ wg:raw:iot   │             │ Stream Process  │             │
│               │ wg:raw:gps   │ bronze.     │ (validación,    │ silver.     │
│ weather-api ──┤ wg:raw:      │ iot_readings│  limpieza)      │ processed_  │
│ (Open-Meteo)  │ weather      │             │                 │ readings    │
│ (Air Quality) │ wg:raw:      │ bronze.     │ Batch ETL       │             │
│               │ airquality   │ api_readings│ (FWI, GPS-hash, │             │
│               │              │             │  dedup, catalog)│             │
├───────────────┴──────────────┴─────────────┴─────────────────┴─────────────┤
│  ↓ wg:silver:out (fan-out a 3 consumers)                                    │
├─────────────────┬──────────────────┬─────────────────────────────────────── │
│ 7. ANALÍTICA    │ 8. ALERTAS       │ ML SIMULATOR          9. DASHBOARD     │
│                 │ (EventBridge     │ (SageMaker)           (Cliente Final)  │
│ Athena (SQL)    │  + SNS)          │                                        │
│ Redshift (DWH)  │                  │ FWI Classifier        KPIs en tiempo   │
│ OpenSearch (ES) │ Reglas de alerta │ AUC=0.82              real, alertas,   │
│ QuickSight API  │ CONAF/Brigadas/  │ SHAP values           tendencias,      │
│                 │ Bomberos         │                       históricos        │
│ → gold.kpis     │ → gold.alerts    │ → gold.ml_predictions                  │
└─────────────────┴──────────────────┴────────────────────────────────────────┘
```

---

## Estructura del Proyecto

```
wildguard/
├── .env                          ← Variables de entorno (raíz del proyecto)
├── docker-compose.yml            ← Orquestación de 13 contenedores
├── README.md
│
├── postgres-init/
│   └── 01-schemas.sql            ← Schemas bronze, silver, gold
│
├── sensor-simulator/             ← Simula MQTT IoT (temp/hum/CO₂/viento/humo/GPS)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── weather-api/                  ← APIs externas (Open-Meteo clima + calidad aire)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── redis-streams/                ← Reemplaza Kinesis DataStreams + Firehose
│   ├── Dockerfile                  Gestiona consumer groups, monitorea lag
│   ├── requirements.txt
│   └── main.py
│
├── bronze-layer/                 ← Data Lake Bronze (raw, inmutable, SSE simulado)
│   ├── Dockerfile                  S3 paths simulados
│   ├── requirements.txt
│   └── main.py
│
├── etl-processor/                ← Lambda (stream) + Glue ETL (batch) + Data Catalog
│   ├── Dockerfile                  + Step Functions (orquestación)
│   ├── requirements.txt
│   └── main.py
│
├── silver-layer/                 ← Data Lake Silver (Parquet, validado, trazable)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── analytics/                    ← Athena + Redshift + OpenSearch + QuickSight
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── ml-simulator/                 ← SageMaker simulado (clasificador FWI v1.2, AUC=0.82)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
├── alert-service/                ← EventBridge + Lambda + SNS
│   ├── Dockerfile                  → CONAF, Brigadas, Bomberos, Equipos Regionales
│   ├── requirements.txt
│   └── main.py
│
├── dashboard/                    ← Cliente Final: Dashboard HTML + API REST
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
│
└── datadog-agent/                ← Observabilidad (logs, métricas, APM)
    ├── Dockerfile
    └── conf.d/
        └── wildguard.yaml
```

---

## Fuentes de Datos

| Componente      | Tipo       | Protocolo    | Datos                                |
|-----------------|------------|--------------|--------------------------------------|
| sensor-simulator | Simulado  | MQTT/TLS 1.2 | Temp, humedad, CO₂, viento, humo, GPS |
| weather-api     | Real       | HTTPS REST   | Clima + calidad de aire (Open-Meteo) |
| ml-simulator    | Simulado   | Interno      | Inferencia ML (SageMaker)            |

> ⚠️ **Sin cámaras, videos ni imágenes.**

---

## Streams Redis (reemplaza Kinesis)

| Stream               | Descripción                        |
|----------------------|------------------------------------|
| `wg:raw:iot`         | Lecturas sensores IoT              |
| `wg:raw:gps`         | Posiciones GPS                     |
| `wg:raw:weather`     | Datos clima Open-Meteo             |
| `wg:raw:airquality`  | Calidad de aire                    |
| `wg:bronze:out`      | Bronze → ETL                       |
| `wg:etl:out`         | ETL → Silver                       |
| `wg:silver:out`      | Silver → Gold/ML/Alerts (fan-out)  |
| `wg:gold:kpis`       | KPIs materializados                |
| `wg:gold:alerts`     | Alertas de incendio                |
| `wg:gold:ml`         | Predicciones ML                    |

---

## Uso

### Iniciar el stack completo

```bash
# 1. Configurar credenciales
cp .env .env.local
# Editar .env: reemplazar DD_API_KEY con tu clave Datadog

# 2. Levantar todo
docker compose up --build

# 3. Acceder al Dashboard
open http://localhost:8080
```

### Verificar servicios

```bash
# Health checks
curl http://localhost:8070/health    # Redis Streams manager
curl http://localhost:8081/health    # Bronze Layer
curl http://localhost:8082/health    # ETL Processor
curl http://localhost:8083/health    # Silver Layer
curl http://localhost:8084/health    # Analytics (QuickSight API)
curl http://localhost:8084/kpi       # KPIs ejecutivos
curl http://localhost:8085/health    # ML Simulator
curl http://localhost:8086/health    # Alert Service
curl http://localhost:8086/alerts    # Alertas activas
curl http://localhost:8080/          # Dashboard HTML
curl http://localhost:8080/api/kpi   # KPIs API

# Streams Redis
docker exec wg-redis redis-cli XLEN wg:raw:iot
docker exec wg-redis redis-cli XLEN wg:silver:out

# PostgreSQL
docker exec wg-postgres psql -U wildguard -d wildguard \
  -c "SELECT risk_level, COUNT(*) FROM silver.processed_readings GROUP BY 1;"

docker exec wg-postgres psql -U wildguard -d wildguard \
  -c "SELECT zone, risk_level, fwi_score, alert_at FROM gold.fire_alerts \
      ORDER BY alert_at DESC LIMIT 10;"

# Logs por componente
docker compose logs -f sensor-simulator
docker compose logs -f etl-processor
docker compose logs -f alert-service
```

---

## Cálculo FWI (Fire Weather Index)

| Feature          | Fuente     | Peso |
|------------------|------------|------|
| Temperatura      | IoT        | 30%  |
| Humedad inversa  | IoT        | 20%  |
| Viento           | IoT        | 10%  |
| CO₂              | IoT        |  5%  |
| Humo             | IoT        |  5%  |
| Temperatura      | Open-Meteo |  8%  |
| Humedad          | Open-Meteo |  7%  |
| Viento           | Open-Meteo |  7%  |
| Código WMO       | Open-Meteo |  8%  |
| Altitud >1500m   | GPS        | +5%  |

---

## Puertos expuestos

| Contenedor        | Puerto |
|-------------------|--------|
| Dashboard         | 8080   |
| Redis Streams API | 8070   |
| Bronze Layer      | 8081   |
| ETL Processor     | 8082   |
| Silver Layer      | 8083   |
| Analytics         | 8084   |
| ML Simulator      | 8085   |
| Alert Service     | 8086   |
| PostgreSQL        | 5432   |
| Redis             | 6379   |
| Elasticsearch     | 9200   |
| Datadog APM       | 8126   |
| Datadog StatsD    | 8125   |

---

## Tecnologías

| Componente      | Equivalente AWS       | Local                   |
|-----------------|-----------------------|-------------------------|
| Kinesis Streams | Redis Streams         | Redis 7                 |
| S3 Bronze/Silver| PostgreSQL schemas    | PostgreSQL 15           |
| Glue ETL        | etl-processor         | Python 3.11             |
| Lambda          | etl-processor         | Python 3.11             |
| Step Functions  | etl-processor         | Python threads          |
| Glue Catalog    | etl-processor         | Dict en memoria         |
| Redshift        | analytics             | PostgreSQL gold schema  |
| Athena          | analytics             | SQL queries             |
| OpenSearch      | analytics             | Elasticsearch 8         |
| QuickSight      | dashboard             | HTML + REST API         |
| SageMaker       | ml-simulator          | Python + numpy          |
| EventBridge     | alert-service         | Rule engine Python      |
| SNS             | alert-service         | HTTP webhooks + logs    |
| Datadog         | CloudWatch            | Datadog Agent 7         |
