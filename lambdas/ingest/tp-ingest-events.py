import json
import os
import uuid
import boto3
import logging
import base64
from datetime import datetime, timezone
from typing import Dict, Any, List, Union

# ---------------------------
# Configuración / clientes
# ---------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "")
if not RAW_BUCKET:
    raise ValueError("RAW_BUCKET environment variable is required")

# ---------------------------
# Esquemas y reglas
# ---------------------------
REQUIRED_FIELDS = ["type", "ts"]

EVENT_SCHEMAS: Dict[str, List[str]] = {
    "reserva_creada": ["reservaId", "vueloId", "precio", "userId"],
    "vuelo_cancelado": ["vueloId", "motivo"],
    "pago_rechazado": ["pagoId", "monto", "razon"],
    "usuario_registrado": ["userId", "canal"],
    "search_metric": [
        "flightsFrom",
        "flightsTo",
        "dateFrom",
        "dateTo",
        "resultsCount",
        "userId",
    ],
    "catalogo": [
        "id",
        "id_vuelo",
        "aerolinea",
        "origen",
        "destino",
        "precio",
        "moneda",
        "despegue",
        "aterrizaje_local",
        "estado_vuelo",
        "capacidadAvion",
        "tipoAvion",
    ],
    "search.cart.item.added": ["userId", "flightId", "addedAt"],
    "search.search.performed": ["searchId", "userId", "origin", "destination", "category", "performedAt"],
    "reservations.reservation.created": ["reservationId", "userId", "flightId", "amount", "currency", "reservedAt"],
    "reservations.reservation.updated": ["reservationId", "newStatus"],
    "flights.flight.created": ["flightId", "flightNumber", "origin", "destination", "aircraftModel", "departureAt", "arrivalAt", "status", "price", "currency"],
    "flights.flight.updated": ["flightId", "newStatus"],
    "flights.aircraft_or_airline.updated": ["airlineBrand", "aircraftId", "capacity"],
    "payments.payment.status_updated": ["paymentId", "reservationId", "userId", "status", "amount", "currency", "updatedAt"],
    "users.user.created": ["userId", "nationalityOrOrigin", "roles", "createdAt"],
}

TS_FIELD_MAP: Dict[str, Union[str, List[str]]] = {
    "search_metric": ["timestamp", "ts"],
    "catalogo": ["despegue", "ts"],
    "search.cart.item.added": ["addedAt"],
    "search.search.performed": ["performedAt"],
    "reservations.reservation.created": ["reservedAt", "occurredAt", "eventTime"],
    "reservations.reservation.updated": ["reservationDate", "flightDate", "occurredAt", "eventTime"],
    "flights.flight.created": ["departureAt", "occurredAt", "eventTime"],
    "flights.flight.updated": ["newDepartureAt", "newArrivalAt", "occurredAt", "eventTime"],
    "flights.aircraft_or_airline.updated": ["occurredAt", "eventTime"],
    "payments.payment.status_updated": ["updatedAt", "occurredAt", "eventTime"],
    "users.user.created": ["createdAt", "occurredAt", "eventTime"],
    "reserva_creada": ["ts", "occurredAt", "eventTime"],
    "pago_rechazado": ["ts", "occurredAt", "eventTime"],
    "usuario_registrado": ["ts", "occurredAt", "eventTime"],
    "vuelo_cancelado": ["ts", "occurredAt", "eventTime"],
}

# ---------------------------
# Helpers
# ---------------------------
def _normalize_ts(value: str) -> str:
    """Devuelve ISO-8601 en UTC con sufijo Z, sin microsegundos."""
    if not isinstance(value, str):
        raise ValueError("ts must be a string")
    v = value.strip().replace(' ', 'T')
    # Aceptar 'Z' o cualquier offset
    if v.endswith('Z'):
        v = v.replace('Z', '+00:00')
    dt = datetime.fromisoformat(v)
    # Si viene naive, asumimos UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Forzar UTC y sin microsegundos
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')

def _get_request_id(event, context) -> str:
    if isinstance(event, dict) and "requestContext" in event:
        return event["requestContext"].get("requestId", "unknown")
    elif hasattr(context, "aws_request_id"):
        return context.aws_request_id
    else:
        return "unknown"

def _response(status: int, body: Dict[str, Any]):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, ensure_ascii=False),
    }

def _has_all_optional_fields(body: Dict[str, Any], event_type: str) -> bool:
    if event_type not in EVENT_SCHEMAS:
        return True
    return all(field in body for field in EVENT_SCHEMAS[event_type])

def _looks_like_search_metric(body: Dict[str, Any]) -> bool:
    required = {
        "flightsFrom",
        "flightsTo",
        "dateFrom",
        "dateTo",
        "resultsCount",
        "timestamp",
        "userId",
    }
    return required.issubset(body.keys())

def _looks_like_catalog_event(body: Dict[str, Any]) -> bool:
    required = {
        "id",
        "id_vuelo",
        "aerolinea",
        "origen",
        "destino",
        "precio",
        "moneda",
        "despegue",
        "aterrizaje_local",
        "estado_vuelo",
        "capacidadAvion",
        "tipoAvion",
    }
    return required.issubset(body.keys())

def _ensure_ts_field(body: Dict[str, Any]) -> None:
    """Completa ts desde aliases conocidos si falta; si no encuentra, usa ingestion time."""
    if "ts" in body and body["ts"]:
        return
    event_type = body.get("type")
    candidates: List[str] = []
    mapping = TS_FIELD_MAP.get(event_type) if event_type else None
    if isinstance(mapping, str):
        candidates = [mapping]
    elif isinstance(mapping, list):
        candidates = mapping or []
    # Buscar en candidatos
    for candidate in candidates:
        val = body.get(candidate)
        if val:
            body["ts"] = str(val)
            return
    # Fallback
    body["ts"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')

def _get_first(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _flatten(d: Dict[str, Any], parent_key: str = "", out: Dict[str, Any] = None) -> Dict[str, Any]:
    if out is None:
        out = {}
    for k, v in d.items():
        nk = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            _flatten(v, nk, out)
        else:
            out[nk] = v
    return out

def _derive_reservation_update_fields(body: Dict[str, Any]) -> None:
    """Intenta poblar reservationId y newStatus desde estructuras comunes y diffs."""
    if body.get("type") != "reservations.reservation.updated":
        return

    flat = _flatten(body)

    rid = _get_first(body, ["reservationId"]) or _get_first(flat, [
        "reservation.id", "entity.id", "data.reservation.id", "after.reservation.id", "id"
    ])

    new = _get_first(body, ["newStatus"]) or _get_first(flat, [
        "after.status", "reservation.status", "entity.newStatus", "data.after.status",
        "toStatus", "targetStatus", "status"
    ])

    if not new:
        changes = body.get("changes") or body.get("diff") or []
        if isinstance(changes, list):
            for ch in changes:
                f = (ch.get("field") or ch.get("name") or "").lower()
                if "status" in f:
                    new = ch.get("new") or ch.get("to") or ch.get("newValue")
                    break

    if rid and "reservationId" not in body:
        body["reservationId"] = rid
    if new and "newStatus" not in body:
        body["newStatus"] = new

def _parse_event_body(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Soporta:
      - API Gateway (body string o dict, base64)
      - Invocación directa (event es el body)
      - Rechaza batch (SNS/SQS)
    """
    if isinstance(event, dict) and "Records" in event:
        logger.warning("Received batch event - not supported")
        return {"error": "Batch events not supported. Use single event format."}

    if isinstance(event, dict) and "body" in event:
        raw = event.get("body")
        if raw is None:
            return {"error": "Missing request body"}

        if event.get("isBase64Encoded"):
            try:
                raw = base64.b64decode(raw).decode("utf-8")
            except Exception as e:
                logger.error(f"Base64 decode failed: {e}")
                return {"error": "Invalid base64 body"}

        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in request body: {e}")
                return {"error": "Invalid JSON format"}
        elif isinstance(raw, dict):
            return raw
        else:
            return {"error": "Unexpected request body format"}

    if isinstance(event, dict) and any(field in event for field in REQUIRED_FIELDS):
        # Invocación directa
        return event

    return {"error": "Invalid event format. Expected API Gateway proxy or direct invocation."}

# ---------------------------
# Handler principal
# ---------------------------
def lambda_handler(event, context):
    try:
        request_id = _get_request_id(event, context)
        logger.info(json.dumps({"event": "ingest_start", "requestId": request_id}))

        body = _parse_event_body(event)
        if isinstance(body, dict) and "error" in body:
            return _response(400, body)

        # Compatibilidad: inferir type si falta
        if "type" not in body or not body.get("type"):
            if _looks_like_search_metric(body):
                body["type"] = "search_metric"
            elif _looks_like_catalog_event(body):
                body["type"] = "catalogo"

        # Completar ts si falta
        _ensure_ts_field(body)

        # Validación mínima
        missing_required = [f for f in REQUIRED_FIELDS if f not in body or body[f] in (None, "")]
        if missing_required:
            logger.warning(json.dumps({"msg": "missing required", "missing": missing_required}))
            return _response(400, {"error": f"Missing fields: {missing_required}"})

        # Normalizar timestamp
        try:
            body["ts"] = _normalize_ts(body["ts"])
        except Exception:
            logger.error(f"Invalid timestamp format: {body.get('ts')}")
            return _response(400, {"error": "Invalid timestamp format. Use ISO 8601 format (e.g., '2024-01-15T10:30:00Z')"})

        # Derivar campos para tipos problemáticos
        _derive_reservation_update_fields(body)

        # Validación opcional (suave para algunos tipos)
        event_type = body.get("type") or "unknown"
        schema_missing: List[str] = []
        if event_type in EVENT_SCHEMAS:
            schema_missing = [f for f in EVENT_SCHEMAS[event_type] if f not in body or body[f] in (None, "")]
            if schema_missing:
                # En vez de 400, guardamos igual con metadata para auditoría
                logger.warning(json.dumps({
                    "msg": "schema missing (soft)",
                    "type": event_type,
                    "missing": schema_missing
                }))

        # Enriquecer y guardar
        event_id = str(uuid.uuid4())
        received_at = datetime.now(timezone.utc).replace(microsecond=0)

        # Latencia de ingesta
        ingestion_latency_ms = 0
        try:
            event_ts = datetime.fromisoformat(body["ts"].replace('Z', '+00:00'))
            ingestion_latency_ms = int((received_at - event_ts).total_seconds() * 1000)
        except Exception:
            pass

        enriched = {
            **body,
            "eventId": event_id,
            "receivedAt": received_at.isoformat().replace("+00:00", "Z"),
            "requestId": request_id,
            "metadata": {
                "source": "tp-ingest-events",
                "version": "1.1",
                "ingestionLatencyMs": ingestion_latency_ms,
                "eventSizeBytes": len(json.dumps(body, ensure_ascii=False)),
                "hasAllRequiredFields": len(missing_required) == 0,
                "hasAllOptionalFields": _has_all_optional_fields(body, event_type),
                "schemaMissing": schema_missing,
                "processingRegion": os.environ.get("AWS_REGION", "unknown"),
                "normalizedTs": True,
            },
        }

        # Particionamiento en S3 por fecha (UTC) y tipo
        now = received_at
        s3_key = (
            f"year={now.year}/month={now.month:02}/day={now.day:02}/"
            f"type={event_type or 'unknown'}/{event_id}.json"
        )

        try:
            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=s3_key,
                Body=json.dumps(enriched, ensure_ascii=False),
                ContentType="application/json",
            )
            logger.info(json.dumps({"msg": "stored_to_s3", "key": s3_key, "eventId": event_id}))
        except Exception as e:
            logger.error(f"Failed to store event to S3: {str(e)}")
            return _response(500, {"error": "Failed to store event"})

        # Si faltaron campos opcionales, lo marcamos en la respuesta pero no fallamos
        resp = {"ok": True, "eventId": event_id}
        if schema_missing:
            resp["warning"] = {"missingOptionalForType": schema_missing, "type": event_type}

        return _response(202, resp)

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return _response(500, {"error": "Internal server error"})
