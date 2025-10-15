# Lambdas de Métricas

## Panorama general
- Conjunto de funciones Lambda que orquestan la ingesta, validación y explotación analítica de eventos de negocio.
- Cada etapa escribe en buckets de S3 particionados y agrega metadatos para facilitar auditoría y trazabilidad.
- La capa de analítica consulta datos curados mediante Athena para exponer KPIs vía API Gateway.

## Flujo de datos
1. **Ingesta (`tp-ingest-events`)**: recibe eventos JSON (API Gateway o invocación directa), realiza validaciones mínimas, enriquece con metadatos y persiste el payload en el bucket *raw*.
2. **Validación (`tp-validate-events`)**: se dispara con notificaciones de S3, revalida la estructura según el tipo de evento, normaliza formatos y distribuye los datos entre buckets *curated* o *invalid*.
3. **KPIs (`tp-kpi-backend`)**: expone endpoints REST que construyen métricas sobre los datos curados empleando consultas a Athena.

## Lambdas

### tp-ingest-events (`lambdas/ingest/tp-ingest-events.py`)
- **Objetivo**: actuar como endpoint de entrada único para todos los eventos operativos.
- **Entrada**: JSON con campos mínimos `type` y `ts`; soporta distintos tipos (`reserva_creada`, `vuelo_cancelado`, `pago_rechazado`, `usuario_registrado`, etc.).
  - Compatibilidad con eventos `SearchMetric` que pueden enviar `timestamp`; se traduce automáticamente a `ts` y se asume `type = search_metric` si no se especifica.
  - Soporta eventos `catalogo` (inventario de vuelos) que derivan `ts` desde `despegue` si no viene informado y completan el tipo automáticamente cuando se detecta la estructura.
- **Validaciones clave**:
  - Presencia de campos requeridos globales y opcionales por tipo.
  - Normalización de `ts` a ISO 8601 en UTC (`...Z`).
  - Cálculo de latencia de ingesta, tamaño del evento y completitud de campos.
- **Salida**: guarda un archivo JSON enriquecido en S3 `RAW_BUCKET`, particionado por `year=/month=/day=/type=`. Retorna `202 Accepted` con `eventId`.
- **Uso típico**: se publica detrás de API Gateway; los datos generados alimentan la etapa de validación.

### tp-validate-events (`lambdas/validate/tp-validate-events.py`)
- **Objetivo**: depurar y normalizar los eventos del bucket raw.
- **Disparador**: eventos de S3 que notifican nuevos archivos crudos.
- **Validaciones**:
  - Presencia de `type`, `ts`, `eventId`.
  - Esquemas específicos por tipo (campos requeridos/opcionales, coerción de tipos, constraints de negocio), incluyendo los eventos `search_metric` y `catalogo`.
  - Detección de payloads corruptos o tipos no reconocidos (genera *warnings*).
- **Salida**:
  - Eventos válidos → bucket `CURATED_BUCKET` en formato Parquet (fallback JSON) y particionados igual que su origen; agrega bloque `validation`.
  - Eventos inválidos → bucket `INVALID_BUCKET` como JSON con detalle de errores y advertencias.
- **Notas**:
  - Remueve `type` del cuerpo antes de escribir al bucket curado para evitar conflictos con la partición.
  - Añade metadatos en S3 para facilitar búsquedas posteriores (`validation-status`, `event-type`, etc.).

### tp-kpi-backend (`lambdas/kpis/tp-kpi-backend.py`)
- **Objetivo**: servir KPIs y datos analíticos a dashboards o clientes externos.
- **Entrada**: solicitudes HTTP GET (principalmente) mapeadas por API Gateway hacia Athena.
- **Comportamiento**:
  - Valida optionalmente un API Key (`API_KEY`) además del plan de uso del API Gateway.
  - Ejecuta consultas Athena sobre la tabla curada (`CURATED_TABLE`) y devuelve JSON normalizado (incluye métricas basadas en los eventos `search_metric` y `catalogo`).
  - Calcula métricas de funnel, revenue, tasas de validación/pago/cancelación, distribución de reservas por hora, origen de usuarios, LTV, salud del inventario de vuelos (status operacionales, disponibilidad por aerolínea/ruta/tipo de avión), entre otras.
- **Salida**: respuestas HTTP con cabeceras CORS, status 200 y estructura JSON lista para frontends.
- **Resiliencia**: reintenta el estado de ejecución de Athena hasta 30 segundos; captura fallos y devuelve 500 con contexto.

## Variables de entorno relevantes

| Lambda | Variables requeridas | Descripción |
| ------ | ------------------- | ----------- |
| `tp-ingest-events` | `RAW_BUCKET`, `AWS_REGION` (opcional) | Bucket raw destino y región para metadatos. |
| `tp-validate-events` | `RAW_BUCKET`, `CURATED_BUCKET`, `INVALID_BUCKET` | Buckets origen/destino para el pipeline de validación. |
| `tp-kpi-backend` | `ATHENA_DATABASE`, `CURATED_TABLE`, `ATHENA_OUTPUT_BUCKET`, `API_KEY` (opcional) | Parámetros de conexión para Athena y autenticación del endpoint. |

## Consideraciones operativas
- Mantener sincronizados los esquemas de eventos entre ingesta y validación; nuevos tipos requieren actualizar ambos módulos.
- Verificar tamaños y formatos de archivos en `RAW_BUCKET` para evitar fallos por payloads no JSON.
- Asegurar que el bucket de resultados de Athena tenga políticas que permitan escritura desde la Lambda de KPIs.
- Monitorizar metadatos de validación en S3 para detectar tendencias de errores o advertencias.
- Revisar los endpoints listados en `tp-kpi-backend` al publicar nuevas visualizaciones o dashboards.

## Endpoints disponibles en la capa de KPIs
- `/analytics/health`, `/analytics/summary`, `/analytics/recent`, `/analytics/daily`, `/analytics/events`, `/analytics/events-by-type`, `/analytics/validation-stats`, `/analytics/revenue`
- Nuevos KPIs: `/analytics/funnel`, `/analytics/avg-fare`, `/analytics/revenue-monthly`, `/analytics/ltv`, `/analytics/revenue-per-user`, `/analytics/popular-airlines`, `/analytics/user-origins`, `/analytics/booking-hours`, `/analytics/payment-success`, `/analytics/cancellation-rate`, `/analytics/anticipation`, `/analytics/time-to-complete`, `/analytics/search-metrics` (basado en eventos `search_metric`), `/analytics/catalog/airline-summary`, `/analytics/catalog/status`, `/analytics/catalog/aircraft`, `/analytics/catalog/routes` (insights del evento `catalogo`)
