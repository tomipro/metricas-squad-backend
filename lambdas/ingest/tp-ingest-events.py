import json
import os
import uuid
import boto3
import logging
from datetime import datetime
from typing import Dict, Any, List, Union

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

RAW_BUCKET = os.environ.get("RAW_BUCKET", "")

# Validate required environment variables
if not RAW_BUCKET:
    raise ValueError("RAW_BUCKET environment variable is required")

# Campos obligatorios globales
REQUIRED_FIELDS = ["type", "ts"]

# Reglas opcionales por tipo
EVENT_SCHEMAS = {
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
    "reservations.reservation.created": ["reservedAt"],
    "reservations.reservation.updated": ["reservationDate", "flightDate"],
    "flights.flight.created": ["departureAt"],
    "flights.flight.updated": ["newDepartureAt", "newArrivalAt"],
    "flights.aircraft_or_airline.updated": [],
    "payments.payment.status_updated": ["updatedAt"],
    "users.user.created": ["createdAt"],
    "reserva_creada": ["ts"],
    "pago_rechazado": ["ts"],
    "usuario_registrado": ["ts"],
    "vuelo_cancelado": ["ts"],
}

def lambda_handler(event, context):
    try:
        request_id = _get_request_id(event, context)
        logger.info(f"Processing event: {request_id}")
        
        # Parse event body - handle both proxy and direct invocation
        body = _parse_event_body(event)
        if isinstance(body, dict) and "error" in body:
            return _response(400, body)

        # Compatibilidad con eventos externos que pueden omitir `ts` o `type`
        if isinstance(body, dict):
            if "type" not in body:
                if _looks_like_search_metric(body):
                    body["type"] = "search_metric"
                elif _looks_like_catalog_event(body):
                    body["type"] = "catalogo"
            _ensure_ts_field(body)
        
        # Validación mínima
        missing = [f for f in REQUIRED_FIELDS if f not in body]
        if missing:
            logger.warning(f"Missing required fields: {missing}")
            return _response(400, {"error": f"Missing fields: {missing}"})

        # Validate and normalize timestamp format
        try:
            # Parse the timestamp to validate format
            original_ts = body["ts"]
            if original_ts.endswith('Z'):
                parsed_ts = datetime.fromisoformat(original_ts.replace('Z', '+00:00'))
            else:
                parsed_ts = datetime.fromisoformat(original_ts)
            
            # Normalize to UTC with Z suffix for consistency
            normalized_ts = parsed_ts.astimezone().astimezone().isoformat().replace('+00:00', 'Z')
            body["ts"] = normalized_ts
            
        except (ValueError, AttributeError) as e:
            logger.error(f"Invalid timestamp format: {body.get('ts')}")
            return _response(400, {"error": "Invalid timestamp format. Use ISO 8601 format (e.g., '2024-01-15T10:30:00Z')"})

        # Validación opcional por tipo
        event_type = body.get("type")
        if event_type in EVENT_SCHEMAS:
            extra_missing = [f for f in EVENT_SCHEMAS[event_type] if f not in body]
            if extra_missing:
                logger.warning(f"Missing fields for {event_type}: {extra_missing}")
                return _response(400, {"error": f"Missing fields for {event_type}: {extra_missing}"})

        # Enriquecer con metadatos útiles para métricas
        event_id = str(uuid.uuid4())
        received_at = datetime.utcnow()
        
        # Calculate ingestion latency if possible
        ingestion_latency_ms = 0
        try:
            event_timestamp = datetime.fromisoformat(body["ts"].replace('Z', '+00:00'))
            latency_delta = received_at - event_timestamp.replace(tzinfo=None)
            ingestion_latency_ms = int(latency_delta.total_seconds() * 1000)
        except:
            pass  # If calculation fails, keep default 0
        
        enriched = {
            **body,
            "eventId": event_id,
            "receivedAt": received_at.isoformat() + "Z",
            "requestId": request_id,
            # Metadatos útiles para el módulo de métricas
            "metadata": {
                "source": "tp-ingest-events",
                "version": "1.0",
                "ingestionLatencyMs": ingestion_latency_ms,
                "eventSizeBytes": len(json.dumps(body)),
                "hasAllRequiredFields": len(missing) == 0,
                "hasAllOptionalFields": _has_all_optional_fields(body, body.get("type")),
                "processingRegion": os.environ.get("AWS_REGION", "unknown")
            }
        }

        # Construir key en S3: partición por fecha y tipo
        ts = datetime.utcnow()
        s3_key = (
            f"year={ts.year}/month={ts.month:02}/day={ts.day:02}/"
            f"type={event_type or 'unknown'}/{event_id}.json"
        )

        # Guardar en S3 Raw
        try:
            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=s3_key,
                Body=json.dumps(enriched, ensure_ascii=False),
                ContentType="application/json"
            )
            logger.info(f"Successfully stored event {event_id} to S3: {s3_key}")
        except Exception as e:
            logger.error(f"Failed to store event to S3: {str(e)}")
            return _response(500, {"error": "Failed to store event"})

        return _response(202, {"ok": True, "eventId": event_id})

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return _response(500, {"error": "Internal server error"})

def _parse_event_body(event):
    """Parse event body handling both API Gateway proxy and direct invocation"""
    # API Gateway proxy integration
    if "body" in event:
        if event["body"] is None:
            logger.warning("Received null body in API Gateway event")
            return {"error": "Missing request body"}
        
        # Handle string body (typical API Gateway)
        if isinstance(event["body"], str):
            try:
                return json.loads(event["body"])
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in request body: {str(e)}")
                return {"error": "Invalid JSON format"}
        
        # Handle already parsed body (some integrations)
        elif isinstance(event["body"], dict):
            return event["body"]
        else:
            logger.error(f"Unexpected body type: {type(event['body'])}")
            return {"error": "Unexpected request body format"}
    
    # Direct invocation - event IS the body
    elif isinstance(event, dict) and any(field in event for field in REQUIRED_FIELDS):
        logger.info("Processing direct Lambda invocation")
        return event
    
    # SQS/SNS events (future extensibility)
    elif "Records" in event:
        logger.warning("Received batch event - this lambda handles single events only")
        return {"error": "Batch events not supported. Use single event format."}
    
    # Unknown format
    else:
        logger.error(f"Unknown event format. Keys: {list(event.keys()) if isinstance(event, dict) else 'not dict'}")
        return {"error": "Invalid event format. Expected API Gateway proxy or direct invocation."}

def _get_request_id(event, context):
    """Get request ID from various sources"""
    # API Gateway
    if "requestContext" in event:
        return event["requestContext"].get("requestId", "unknown")
    # Lambda context
    elif hasattr(context, "aws_request_id"):
        return context.aws_request_id
    else:
        return "unknown"

def _response(status, body):
    """Return appropriate response format"""
    # Check if this is an API Gateway request
    # If so, return full HTTP response; otherwise return simple response
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps(body)
    }

def _has_all_optional_fields(body, event_type):
    """Check if event has all optional fields for its type"""
    if event_type not in EVENT_SCHEMAS:
        return True  # No optional fields defined
    
    optional_fields = EVENT_SCHEMAS[event_type]
    return all(field in body for field in optional_fields)

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
    return required.issubset(set(body.keys()))

def _ensure_ts_field(body: Dict[str, Any]) -> None:
    """Populate ts from known fields when missing"""
    if "ts" in body:
        return
    event_type = body.get("type")
    if not event_type:
        return

    mapping = TS_FIELD_MAP.get(event_type)
    candidates: List[str] = []
    if isinstance(mapping, str):
        candidates = [mapping]
    elif isinstance(mapping, list):
        candidates = mapping

    for candidate in candidates:
        if not candidate:
            continue
        value = body.get(candidate)
        if value:
            body["ts"] = str(value)
            return

    # Fallback: use ingestion time to avoid rejection (ensures downstream handling)
    body["ts"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

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
    return required.issubset(set(body.keys()))
