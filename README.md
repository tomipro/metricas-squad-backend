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
- **Entrada**: JSON con campos mínimos `type` y `ts`; soporta los eventos históricos (`reserva_creada`, `pago_aprobado`, etc.) y los nuevos formatos canonizados (`search.search.performed`, `search.cart.item.added`, `reservations.reservation.created`, `flights.flight.created`, `payments.payment.status_updated`, `users.user.created`, etc.).
  - Cuando el payload trae marcas de tiempo específicas (`performedAt`, `reservedAt`, `departureAt`, `updatedAt`, `createdAt`, etc.) la Lambda deriva automáticamente el `ts` para mantener consistencia temporal.
  - Se mantiene compatibilidad con eventos `search_metric` y `catalogo`, autocompletando `type` y `ts` cuando la estructura coincide con esos payloads.
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
  - Esquemas específicos por tipo (campos requeridos/opcionales, coerción de tipos, constraints de negocio) para todos los eventos de negocio: búsquedas (`search.search.performed`, `search.cart.item.added`), reservas (`reservations.reservation.created/updated`), vuelos (`flights.flight.created/updated`, `flights.aircraft_or_airline.updated`), pagos (`payments.payment.status_updated`), usuarios (`users.user.created`), catálogo y métricas históricas.
  - Normalización de timestamps, `currency` en ISO, enumeraciones (`status`, `newStatus`) y advertencias cuando se detectan anomalías (por ejemplo, `arrivalAt` anterior al `departureAt`).
- **Salida**:
  - Eventos válidos → bucket `CURATED_BUCKET` en un formato homogéneo: columnas fijas (`eventType`, `ts`, `eventId`, `payload_json`, `validation_json`, etc.) para simplificar los crawlers y las consultas de Athena.
  - Eventos inválidos → bucket `INVALID_BUCKET` como JSON con detalle de errores y advertencias.
- **Notas**:
  - El esquema unificado evita columnas duplicadas con la partición `type=...` y mantiene la trazabilidad (`eventType`, `metadata_json`, `validation_json`, `payload_json`).
  - Al agregar nuevos tipos basta con ampliar el diccionario `EVENT_SCHEMAS` y la sección de normalización.

### tp-kpi-backend (`lambdas/kpis/tp-kpi-backend.py`)
- **Objetivo**: servir KPIs y datos analíticos a dashboards o clientes externos.
- **Entrada**: solicitudes HTTP GET (principalmente) mapeadas por API Gateway hacia Athena.
- **Comportamiento**:
  - Valida optionalmente un API Key (`API_KEY`) además del plan de uso del API Gateway.
  - Ejecuta consultas Athena sobre la tabla curada (`CURATED_TABLE`) y devuelve JSON normalizado. Todas las consultas trabajan sobre `payload_json`/`validation_json`, por lo que nuevos campos quedan disponibles sin cambiar el esquema físico.
  - Entrega métricas para todo el dominio: búsqueda (`search.search.performed`, `search.cart.item.added`), catálogo/vuelos (`catalogo`, `flights.flight.created/updated`, `flights.aircraft_or_airline.updated`), reservas (`reservations.reservation.created/updated`), pagos (`payments.payment.status_updated`), usuarios (`users.user.created`) y los eventos legacy.
- **Salida**: respuestas HTTP con cabeceras CORS, status 200 y estructura JSON lista para frontends.
- **Resiliencia**: reintenta el estado de ejecución de Athena hasta 30 segundos; captura fallos y devuelve 500 con contexto.

### Eventos soportados

| Dominio | Tipo (`type`) | Campos clave normalizados |
| ------- | ------------- | ------------------------ |
| Búsquedas | `search.search.performed` | `performedAt`, `origin`, `destination`, `departDate`, `returnDate`, `paxCount`, `category` |
| Búsquedas | `search.cart.item.added` | `flightId`, `userId`, `addedAt` |
| Reservas | `reservations.reservation.created` | `reservationId`, `flightId`, `amount`, `currency`, `reservedAt` |
| Reservas | `reservations.reservation.updated` | `reservationId`, `newStatus`, `reservationDate`, `flightDate` |
| Pagos | `payments.payment.status_updated` | `paymentId`, `reservationId`, `userId`, `status`, `amount`, `currency`, `updatedAt` |
| Vuelos | `flights.flight.created` | `flightNumber`, `origin`, `destination`, `aircraftModel`, `departureAt`, `arrivalAt`, `status`, `price`, `currency` |
| Vuelos | `flights.flight.updated` | `flightId`, `newStatus`, `newDepartureAt`, `newArrivalAt` |
| Vuelos | `flights.aircraft_or_airline.updated` | `aircraftId`, `airlineBrand`, `capacity`, `seatMapId` |
| Usuarios | `users.user.created` | `userId`, `nationalityOrOrigin`, `roles`, `createdAt` |
| Legacy / métricas | `search_metric`, `catalogo`, `reserva_creada`, `pago_aprobado`, `pago_rechazado`, etc. | Se mantienen para compatibilidad y se consultan junto a los eventos nuevos. |

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
- Métricas de conversión y revenue: `/analytics/funnel`, `/analytics/avg-fare`, `/analytics/revenue-monthly`, `/analytics/ltv`, `/analytics/revenue-per-user`, `/analytics/payment-success`, `/analytics/time-to-complete`
- Catálogo y vuelos: `/analytics/catalog/airline-summary`, `/analytics/catalog/status`, `/analytics/catalog/aircraft`, `/analytics/catalog/routes`, `/analytics/flights/updates`, `/analytics/flights/aircraft`
- Búsquedas: `/analytics/search-metrics`, `/analytics/search/cart`
- Reservas y usuarios: `/analytics/booking-hours`, `/analytics/cancellation-rate`, `/analytics/reservations/updates`, `/analytics/user-origins`, `/analytics/anticipation`
- Pagos: `/analytics/payments/status`

## Postman
- Se incluye la colección `postman/metrics-squad.postman_collection.json` y el entorno `postman/metrics-squad.postman_environment.json` con ejemplos de ingesta de eventos y llamadas a todos los endpoints de analítica. Ajustá `ingest_base`, `ingest_api_key`, `api_base` y `analytics_api_key` antes de ejecutar.
- Para ejecutar lotes mockeados desde el Runner importá la colección `postman/Metrics Squad.postman_collection.json`, seleccioná la carpeta **Mocked Events (Runner)** y usa los CSV de `postman/data/` siguiendo este orden sugerido: `search_search_performed` → `search_metric` → `search_cart_item_added` → `users_user_created` → `flights_flight_created` → `catalogo` → `reservations_reservation_created` → `payments_payment_status_updated` → `reservations_reservation_updated` → `flights_flight_updated` → `flights_aircraft_or_airline_updated`.
