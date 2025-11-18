import json
import os
import re
import boto3
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Union
from enum import Enum
from io import BytesIO
from urllib.parse import unquote_plus

# ---------- Logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- AWS Clients ----------
s3 = boto3.client("s3")

# ---------- Env Vars ----------
RAW_BUCKET = os.environ.get("RAW_BUCKET", "")
CURATED_BUCKET = os.environ.get("CURATED_BUCKET", "")
INVALID_BUCKET = os.environ.get("INVALID_BUCKET", "")

for name, val in {
    "RAW_BUCKET": RAW_BUCKET,
    "CURATED_BUCKET": CURATED_BUCKET,
    "INVALID_BUCKET": INVALID_BUCKET,
}.items():
    if not val:
        raise ValueError(f"{name} environment variable is required")

# ---------- Enum de estado de validación ----------
class ValidationStatus(Enum):
    VALID = "valid"
    INVALID = "invalid"
    CORRUPTED = "corrupted"

VALID_AIRCRAFT_TYPES = {"E190", "A330", "B737"}
FLIGHT_STATUS_CANONICAL = {
    "en hora": "En hora",
    "en_hora": "En hora",
    "on time": "En hora",
    "ontime": "En hora",
    "demorado": "Demorado",
    "delayed": "Demorado",
    "delay": "Demorado",
    "cancelado": "Cancelado",
    "cancelled": "Cancelado",
}

def _is_valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False

def _is_valid_currency(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{3}", value))

def _normalize_currency(value: str) -> str:
    return value.upper()

def _normalize_flight_status(value: str) -> Union[str, None]:
    normalized = FLIGHT_STATUS_CANONICAL.get(value.strip().lower())
    return normalized

def _is_valid_datetime(value: str) -> bool:
    try:
        _ = _normalize_iso_utc(str(value))
        return True
    except Exception:
        return False

# ---------- Esquemas de Validación ----------
EVENT_SCHEMAS = {
    # Reserva creada: agrego airlineCode/origin/destination/flightDate/searchId
    "reserva_creada": {
        "required": ["reservaId", "vueloId", "precio", "userId"],
        "optional": ["airlineCode", "origin", "destination", "flightDate", "searchId"],
        "types": {
            "reservaId": str,
            "vueloId": str,
            "precio": (int, float),
            "userId": str,
            "airlineCode": str,
            "origin": str,
            "destination": str,
            "flightDate": str,  # 'YYYY-MM-DD' recomendado
            "searchId": str
        },
        "constraints": {
            "precio": lambda x: float(x) > 0,
            "reservaId": lambda x: len(x) > 0,
            "vueloId": lambda x: len(x) > 0,
            "userId": lambda x: len(x) > 0
        }
    },

    # Pago aprobado: revenue real
    "pago_aprobado": {
        "required": ["paymentId", "reservaId", "userId", "amount"],
        "optional": [],
        "types": {
            "paymentId": str,
            "reservaId": str,
            "userId": str,
            "amount": (int, float)
        },
        "constraints": {
            "amount": lambda x: float(x) > 0
        }
    },

    # Pago rechazado: para tasa de éxito de pago
    "pago_rechazado": {
        "required": ["pagoId", "monto", "razon"],
        "optional": ["metodoPago", "intentos", "userId", "reservaId"],
        "types": {
            "pagoId": str,
            "monto": (int, float),
            "razon": str,
            "metodoPago": str,
            "intentos": int,
            "userId": str,
            "reservaId": str
        },
        "constraints": {
            "monto": lambda x: float(x) > 0,
            "razon": lambda x: len(x) > 0,
            "intentos": lambda x: int(x) >= 1 if x is not None else True
        }
    },

    # Reserva cancelada: para cancellation rate
    "reserva_cancelada": {
        "required": ["reservaId", "userId", "motivo"],
        "optional": [],
        "types": {
            "reservaId": str,
            "userId": str,
            "motivo": str
        },
        "constraints": {
            "reservaId": lambda x: len(x) > 0,
            "userId": lambda x: len(x) > 0,
            "motivo": lambda x: len(x) > 0
        }
    },

    # Operacional: se mantiene por si lo usan
    "vuelo_cancelado": {
        "required": ["vueloId", "motivo"],
        "optional": ["fechaCancelacion", "reembolso"],
        "types": {
            "vueloId": str,
            "motivo": str,
            "fechaCancelacion": str,
            "reembolso": bool
        },
        "constraints": {
            "vueloId": lambda x: len(x) > 0,
            "motivo": lambda x: len(x) > 0
        }
    },

    # Onboarding de usuarios (origen por país)
    "usuario_registrado": {
        "required": ["userId", "canal"],
        "optional": ["email", "fechaRegistro", "pais"],
        "types": {
            "userId": str,
            "canal": str,
            "email": str,
            "fechaRegistro": str,
            "pais": str
        },
        "constraints": {
            "userId": lambda x: len(x) > 0,
            "canal": lambda x: str(x) in ["web", "mobile", "api"],
            "email": lambda x: ("@" in x) if x else True
        }
    },

    # Métricas agregadas del módulo de búsquedas
    "search_metric": {
        "required": ["flightsFrom", "flightsTo", "dateFrom", "dateTo", "resultsCount", "userId"],
        "optional": [],
        "types": {
            "flightsFrom": str,
            "flightsTo": str,
            "dateFrom": str,
            "dateTo": str,
            "resultsCount": int,
            "userId": str
        },
        "constraints": {
            "flightsFrom": lambda x: len(str(x)) > 0,
            "flightsTo": lambda x: len(str(x)) > 0,
            "dateFrom": lambda x: _is_valid_date(str(x)),
            "dateTo": lambda x: _is_valid_date(str(x)),
            "resultsCount": lambda x: int(x) >= 0,
            "userId": lambda x: len(str(x)) > 0
        }
    },

    # Catálogo de vuelos disponibles
    "catalogo": {
        "required": [
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
            "tipoAvion"
        ],
        "optional": [],
        "types": {
            "id": int,
            "id_vuelo": str,
            "aerolinea": str,
            "origen": str,
            "destino": str,
            "precio": (int, float),
            "moneda": str,
            "despegue": str,
            "aterrizaje_local": str,
            "estado_vuelo": str,
            "capacidadAvion": int,
            "tipoAvion": str
        },
        "constraints": {
            "id": lambda x: int(x) > 0,
            "precio": lambda x: float(x) > 0,
            "moneda": lambda x: _is_valid_currency(str(x).upper()),
            "despegue": lambda x: bool(x),
            "aterrizaje_local": lambda x: bool(x),
            "capacidadAvion": lambda x: int(x) > 0
        }
    },

    "search.cart.item.added": {
        "required": ["userId", "flightId", "addedAt"],
        "optional": [],
        "types": {
            "userId": str,
            "flightId": str,
            "addedAt": str
        },
        "constraints": {
            "addedAt": lambda x: _is_valid_datetime(str(x))
        }
    },

    "search.search.performed": {
        "required": ["searchId", "userId", "origin", "destination", "category", "performedAt"],
        "optional": ["departDate", "returnDate", "paxCount"],
        "types": {
            "searchId": str,
            "userId": str,
            "origin": str,
            "destination": str,
            "category": str,
            "performedAt": str,
            "departDate": str,
            "returnDate": str,
            "paxCount": int
        },
        "constraints": {
            "performedAt": lambda x: _is_valid_datetime(str(x)),
            "departDate": lambda x: _is_valid_date(str(x)) if x else True,
            "returnDate": lambda x: _is_valid_date(str(x)) if x else True,
            "paxCount": lambda x: int(x) >= 1 if x is not None else True
        }
    },

    "reservations.reservation.created": {
        "required": ["reservationId", "userId", "flightId", "amount", "currency", "reservedAt"],
        "optional": [],
        "types": {
            "reservationId": str,
            "userId": str,
            "flightId": str,
            "amount": (int, float),
            "currency": str,
            "reservedAt": str
        },
        "constraints": {
            "amount": lambda x: float(x) > 0,
            "currency": lambda x: _is_valid_currency(str(x).upper()),
            "reservedAt": lambda x: _is_valid_datetime(str(x))
        }
    },

    "reservations.reservation.updated": {
        "required": ["reservationId", "newStatus"],
        "optional": ["reservationDate", "flightDate"],
        "types": {
            "reservationId": str,
            "newStatus": str,
            "reservationDate": str,
            "flightDate": str
        },
        "constraints": {
            "reservationDate": lambda x: _is_valid_datetime(str(x)) if x else True,
            "flightDate": lambda x: _is_valid_datetime(str(x)) if x else True,
            "newStatus": lambda x: len(str(x)) > 0
        }
    },

    "flights.flight.created": {
        "required": ["flightId", "flightNumber", "origin", "destination", "aircraftModel", "departureAt", "arrivalAt", "status", "price", "currency"],
        "optional": [],
        "types": {
            "flightId": str,
            "flightNumber": str,
            "origin": str,
            "destination": str,
            "aircraftModel": str,
            "departureAt": str,
            "arrivalAt": str,
            "status": str,
            "price": (int, float),
            "currency": str
        },
        "constraints": {
            "price": lambda x: float(x) >= 0,
            "currency": lambda x: _is_valid_currency(str(x).upper()),
            "departureAt": lambda x: _is_valid_datetime(str(x)),
            "arrivalAt": lambda x: _is_valid_datetime(str(x))
        }
    },

    "flights.flight.updated": {
        "required": ["flightId", "newStatus"],
        "optional": ["newDepartureAt", "newArrivalAt"],
        "types": {
            "flightId": str,
            "newStatus": str,
            "newDepartureAt": str,
            "newArrivalAt": str
        },
        "constraints": {
            "newDepartureAt": lambda x: _is_valid_datetime(str(x)) if x else True,
            "newArrivalAt": lambda x: _is_valid_datetime(str(x)) if x else True,
            "newStatus": lambda x: len(str(x)) > 0
        }
    },

    "flights.aircraft_or_airline.updated": {
        "required": ["airlineBrand", "aircraftId", "capacity"],
        "optional": ["seatMapId"],
        "types": {
            "airlineBrand": str,
            "aircraftId": str,
            "capacity": int,
            "seatMapId": str
        },
        "constraints": {
            "capacity": lambda x: int(x) > 0
        }
    },

    "payments.payment.status_updated": {
        "required": ["paymentId", "reservationId", "userId", "status", "amount", "currency", "updatedAt"],
        "optional": [],
        "types": {
            "paymentId": str,
            "reservationId": str,
            "userId": str,
            "status": str,
            "amount": (int, float),
            "currency": str,
            "updatedAt": str
        },
        "constraints": {
            "status": lambda x: str(x).upper() in {"SUCCESS", "FAILURE", "PENDING", "REFUND"},
            "amount": lambda x: float(x) >= 0,
            "currency": lambda x: _is_valid_currency(str(x).upper()),
            "updatedAt": lambda x: _is_valid_datetime(str(x))
        }
    },

    "users.user.created": {
        "required": ["userId", "nationalityOrOrigin", "roles", "createdAt"],
        "optional": [],
        "types": {
            "userId": str,
            "nationalityOrOrigin": str,
            "roles": list,
            "createdAt": str
        },
        "constraints": {
            "roles": lambda x: isinstance(x, list) and len(x) > 0,
            "createdAt": lambda x: _is_valid_datetime(str(x))
        }
    }
}

# ---------- Parquet (opcional) ----------
try:
    import pandas as pd
    import pyarrow as pa  # noqa
    import pyarrow.parquet as pq  # noqa
    PARQUET_AVAILABLE = True
    logger.info("Parquet libraries loaded successfully")
except Exception as e:
    PARQUET_AVAILABLE = False
    pd = None
    logger.warning(f"Parquet not available: {e}. Will fallback to JSON when needed.")

# ---------- Utils de tipos ----------
def _coerce_value(value: Any, expected: Union[type, Tuple[type, ...]]) -> Tuple[Any, bool]:
    def _try_one(v: Any, t: type) -> Tuple[Any, bool]:
        try:
            if t is int:
                if isinstance(v, bool):
                    return v, False
                return int(v), True
            if t is float:
                if isinstance(v, bool):
                    return v, False
                return float(v), True
            if t is bool:
                if isinstance(v, bool):
                    return v, True
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s in ("true", "1", "yes"):
                        return True, True
                    if s in ("false", "0", "no"):
                        return False, True
                return bool(v), True
            if t is str:
                return str(v), True
            return t(v), True
        except Exception:
            return v, False

    if isinstance(expected, tuple):
        for t in expected:
            new_v, ok = _try_one(value, t)
            if ok:
                return new_v, True
        return value, False
    else:
        return _try_one(value, expected)

def _type_names(expected: Union[type, Tuple[type, ...]]) -> str:
    if isinstance(expected, tuple):
        return ", ".join(t.__name__ for t in expected)
    return expected.__name__

def _normalize_iso_utc(ts: str) -> str:
    try:
        if ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts)
        return dt.astimezone().isoformat().replace("+00:00", "Z")
    except Exception:
        raise ValueError("Invalid timestamp format. Use ISO 8601 e.g. 2025-01-15T10:30:00Z")

# ---------- Handler ----------

def lambda_handler(event, context):
    try:
        request_id = _get_request_id(event, context)
        logger.info(f"Starting validation process: {request_id}")

        records = _parse_s3_event(event)
        results: List[Dict[str, Any]] = []

        for rec in records:
            try:
                results.append(_process_s3_object(rec))
            except Exception as e:
                logger.error(f"Error processing {rec}: {e}", exc_info=True)
                results.append({"status": "error", "record": rec, "error": str(e)})

        logger.info(f"Validation completed. Processed {len(results)} objects")
        return {"statusCode": 200, "body": json.dumps({"processed": len(results), "results": results})}

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": "Internal validation error"})}

# ---------- Core ----------

def _parse_s3_event(event: Dict[str, Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if isinstance(event, dict) and "Records" in event:
        for r in event["Records"]:
            if r.get("eventSource") == "aws:s3":
                b = r["s3"]["bucket"]["name"]
                k = unquote_plus(r["s3"]["object"]["key"])
                logger.info(f"Decoded S3 key: {k}")
                out.append({"bucket": b, "key": k})
    elif isinstance(event, dict) and {"bucket", "key"} <= set(event.keys()):
        out.append({"bucket": event["bucket"], "key": event["key"]})
    elif isinstance(event, dict) and "objects" in event:
        for o in event["objects"]:
            out.append({"bucket": o["bucket"], "key": o["key"]})
    if not out:
        raise ValueError("No valid S3 records found")
    return out

def _process_s3_object(record: Dict[str, str]) -> Dict[str, Any]:
    bucket, key = record["bucket"], record["key"]
    logger.info(f"Processing S3 object: s3://{bucket}/{key}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        event = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        return {"status": ValidationStatus.CORRUPTED.value, "bucket": bucket, "key": key, "error": f"read/parse: {e}"}

    # Validación + normalización
    result = _validate_and_normalize(event)

    # Enriquecer con metadatos de validación
    validated_event = {
        **result["event"],  # ya normalizado y con tipos coerced
        "validation": {
            "status": result["status"].value,
            "validatedAt": datetime.utcnow().isoformat() + "Z",
            "validatedBy": "tp-validate-events",
            "errors": result["errors"],
            "warnings": result["warnings"],
            "originalS3Key": key,
        },
    }

    if result["status"] == ValidationStatus.VALID:
        dest = _store_valid_event(validated_event, key)
        logger.info(f"Valid event stored at: {dest}")
    else:
        _store_invalid_event(validated_event, result)
        logger.warning(f"Invalid event stored for key: {key}")

    return {
        "status": result["status"].value,
        "bucket": bucket,
        "key": key,
        "errors": result["errors"],
        "warnings": result["warnings"],
        "eventId": validated_event.get("eventId", "unknown"),
    }

def _validate_and_normalize(event_data: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    if not isinstance(event_data, dict):
        return {"status": ValidationStatus.CORRUPTED, "event": {}, "errors": ["Payload is not JSON object"], "warnings": []}

    # Required básicos
    for f in ("type", "ts", "eventId"):
        if f not in event_data:
            errors.append(f"Missing required field: {f}")
    if errors:
        return {"status": ValidationStatus.INVALID, "event": event_data, "errors": errors, "warnings": warnings}

    # Normalizar timestamp (a Z)
    try:
        event_data["ts"] = _normalize_iso_utc(str(event_data["ts"]))
    except ValueError as e:
        errors.append(str(e))

    event_type = str(event_data.get("type"))
    schema = EVENT_SCHEMAS.get(event_type)
    if not schema:
        warnings.append(f"Unknown event type: {event_type}")
    else:
        # 1) Requeridos del tipo
        for f in schema["required"]:
            if f not in event_data:
                errors.append(f"Missing required field for {event_type}: {f}")

        # 2) Coerción de tipos + validación
        for field, expected in schema.get("types", {}).items():
            if field in event_data:
                coerced, ok = _coerce_value(event_data[field], expected)
                if ok:
                    event_data[field] = coerced
                if not isinstance(event_data[field], expected):
                    errors.append(f"Field {field} must be of type { _type_names(expected) }")

        # 3) Constraints
        for field, constraint in schema.get("constraints", {}).items():
            if field in event_data:
                try:
                    if not constraint(event_data[field]):
                        errors.append(f"Field {field} violates constraint")
                except Exception as e:
                    errors.append(f"Error validating constraint for {field}: {e}")

        if event_type == "catalogo":
            for ts_field in ("despegue", "aterrizaje_local"):
                if ts_field in event_data:
                    try:
                        event_data[ts_field] = _normalize_iso_utc(str(event_data[ts_field]))
                    except ValueError as e:
                        errors.append(f"{ts_field} invalid timestamp: {e}")
            currency = event_data.get("moneda")
            if currency is not None:
                if isinstance(currency, str):
                    normalized_currency = _normalize_currency(currency)
                    event_data["moneda"] = normalized_currency
                    if not _is_valid_currency(normalized_currency):
                        errors.append("moneda must be a valid ISO 4217 code (3 letras)")
                else:
                    errors.append("moneda must be a string")
            status_value = event_data.get("estado_vuelo")
            if status_value is not None:
                normalized_status = _normalize_flight_status(str(status_value))
                if normalized_status:
                    event_data["estado_vuelo"] = normalized_status
                else:
                    warnings.append(f"Unknown estado_vuelo: {status_value}")
            aircraft_type = event_data.get("tipoAvion")
            if aircraft_type is not None:
                normalized_type = str(aircraft_type).upper()
                event_data["tipoAvion"] = normalized_type
                if normalized_type not in VALID_AIRCRAFT_TYPES:
                    warnings.append(f"Unknown aircraft type: {normalized_type}")
        elif event_type == "search.cart.item.added":
            try:
                event_data["addedAt"] = _normalize_iso_utc(str(event_data["addedAt"]))
            except Exception as e:
                errors.append(f"addedAt invalid timestamp: {e}")
        elif event_type == "search.search.performed":
            try:
                event_data["performedAt"] = _normalize_iso_utc(str(event_data["performedAt"]))
            except Exception as e:
                errors.append(f"performedAt invalid timestamp: {e}")
            for date_field in ("departDate", "returnDate"):
                if date_field in event_data and event_data[date_field]:
                    if not _is_valid_date(str(event_data[date_field])):
                        warnings.append(f"{date_field} is not a valid date")
        elif event_type == "reservations.reservation.created":
            currency = event_data.get("currency")
            if currency:
                normalized_currency = _normalize_currency(currency)
                event_data["currency"] = normalized_currency
                if not _is_valid_currency(normalized_currency):
                    errors.append("currency must be a valid ISO 4217 code")
            try:
                event_data["reservedAt"] = _normalize_iso_utc(str(event_data["reservedAt"]))
            except Exception as e:
                errors.append(f"reservedAt invalid timestamp: {e}")
        elif event_type == "reservations.reservation.updated":
            for f in ("reservationDate", "flightDate"):
                if f in event_data and event_data[f]:
                    try:
                        event_data[f] = _normalize_iso_utc(str(event_data[f]))
                    except Exception as e:
                        errors.append(f"{f} invalid timestamp: {e}")
        elif event_type == "flights.flight.created":
            for f in ("departureAt", "arrivalAt"):
                try:
                    event_data[f] = _normalize_iso_utc(str(event_data[f]))
                except Exception as e:
                    errors.append(f"{f} invalid timestamp: {e}")
            currency = event_data.get("currency")
            if currency:
                normalized_currency = _normalize_currency(currency)
                event_data["currency"] = normalized_currency
                if not _is_valid_currency(normalized_currency):
                    errors.append("currency must be a valid ISO 4217 code")
            try:
                dep = datetime.fromisoformat(event_data["departureAt"].replace("Z", "+00:00"))
                arr = datetime.fromisoformat(event_data["arrivalAt"].replace("Z", "+00:00"))
                if arr < dep:
                    warnings.append("arrivalAt is earlier than departureAt")
            except Exception:
                pass
        elif event_type == "flights.flight.updated":
            for f in ("newDepartureAt", "newArrivalAt"):
                if f in event_data and event_data[f]:
                    try:
                        event_data[f] = _normalize_iso_utc(str(event_data[f]))
                    except Exception as e:
                        errors.append(f"{f} invalid timestamp: {e}")
        elif event_type == "flights.aircraft_or_airline.updated":
            capacity = event_data.get("capacity")
            if capacity is not None:
                try:
                    event_data["capacity"] = int(capacity)
                except Exception:
                    errors.append("capacity must be integer")
        elif event_type == "payments.payment.status_updated":
            currency = event_data.get("currency")
            if currency:
                normalized_currency = _normalize_currency(currency)
                event_data["currency"] = normalized_currency
                if not _is_valid_currency(normalized_currency):
                    errors.append("currency must be a valid ISO 4217 code")
            status_val = event_data.get("status")
            if status_val:
                event_data["status"] = str(status_val).upper()
            try:
                event_data["updatedAt"] = _normalize_iso_utc(str(event_data["updatedAt"]))
            except Exception as e:
                errors.append(f"updatedAt invalid timestamp: {e}")
        elif event_type == "users.user.created":
            try:
                event_data["createdAt"] = _normalize_iso_utc(str(event_data["createdAt"]))
            except Exception as e:
                errors.append(f"createdAt invalid timestamp: {e}")
            roles = event_data.get("roles")
            if isinstance(roles, list):
                event_data["roles"] = [str(role) for role in roles]

        # 4) Warnings por campos inesperados
        expected_fields = set(schema["required"] + schema.get("optional", []))
        system_fields = {"type", "ts", "eventId", "receivedAt", "requestId", "metadata"}
        allowed = expected_fields.union(system_fields)
        for f in list(event_data.keys()):
            if f not in allowed and f not in schema.get("types", {}):
                warnings.append(f"Unexpected field for {event_type}: {f}")

    # 5) Reglas de negocio simples
    try:
        event_time = datetime.fromisoformat(event_data["ts"].replace("Z", "+00:00"))
        now = datetime.utcnow().replace(tzinfo=event_time.tzinfo)
        delta = (event_time - now).total_seconds()
        if delta > 3600:
            warnings.append("Event timestamp is more than 1h in the future")
        elif delta < -86400 * 30:
            warnings.append("Event timestamp is more than 30 days old")
    except Exception:
        pass

    # Extra business examples
    if event_type == "reserva_creada":
        precio = event_data.get("precio")
        try:
            if precio is not None and float(precio) > 50000:
                warnings.append("Unusually high ticket price detected")
        except Exception:
            pass
    elif event_type == "pago_rechazado":
        monto = event_data.get("monto")
        try:
            if monto is not None and float(monto) > 100000:
                warnings.append("Large payment amount detected")
        except Exception:
            pass

    status = ValidationStatus.INVALID if errors else ValidationStatus.VALID
    return {"status": status, "event": event_data, "errors": errors, "warnings": warnings}

# ---------- Persistencia ----------

def _store_valid_event(event_data: Dict[str, Any], original_key: str) -> str:
    """
    Guarda el evento validado en CURATED como Parquet (si está disponible) o JSON.
    ✱ Importante: se utiliza un esquema homogéneo para todos los tipos de evento.
    """
    event_type = str(event_data.get("type", "unknown"))
    event_id = str(event_data.get("eventId", "unknown"))

    # Derivar particiones desde la key original (mantiene year=/month=/day=/)
    try:
        parts = original_key.split("/")
        y = next((p for p in parts if p.startswith("year=")), None)
        m = next((p for p in parts if p.startswith("month=")), None)
        d = next((p for p in parts if p.startswith("day=")), None)
        if y and m and d:
            key_out = f"{y}/{m}/{d}/type={event_type}/{event_id}.parquet"
        else:
            now = datetime.utcnow()
            key_out = f"year={now.year}/month={now.month:02}/day={now.day:02}/type={event_type}/{event_id}.parquet"
    except Exception:
        now = datetime.utcnow()
        key_out = f"year={now.year}/month={now.month:02}/day={now.day:02}/type={event_type}/{event_id}.parquet"

    # Construir payload homogéneo
    base_fields = {
        "type",
        "ts",
        "eventId",
        "receivedAt",
        "requestId",
        "metadata",
        "validation",
    }
    payload = {k: v for k, v in event_data.items() if k not in base_fields}
    record = {
        "eventType": event_type,
        "ts": event_data.get("ts"),
        "eventId": event_id,
        "requestId": event_data.get("requestId"),
        "receivedAt": event_data.get("receivedAt"),
        "metadata_json": json.dumps(event_data.get("metadata", {}), ensure_ascii=False),
        "validation_json": json.dumps(event_data.get("validation", {}), ensure_ascii=False),
        "payload_json": json.dumps(payload, ensure_ascii=False),
        "ingestedAt": datetime.utcnow().isoformat() + "Z",
    }

    # Parquet si se puede, sino JSON
    try:
        if not PARQUET_AVAILABLE:
            raise RuntimeError("Parquet not available")
        df = pd.DataFrame([record])
        buf = BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
        buf.seek(0)
        s3.put_object(
            Bucket=CURATED_BUCKET,
            Key=key_out,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
            Metadata={"validation-status": "valid", "original-key": original_key, "event-type": event_type, "format": "parquet"},
        )
        return key_out
    except Exception as e:
        logger.warning(f"Parquet failed, fallback to JSON: {e}")
        key_json = key_out.replace(".parquet", ".json")
        s3.put_object(
            Bucket=CURATED_BUCKET,
            Key=key_json,
            Body=json.dumps(record, ensure_ascii=False),
            ContentType="application/json",
            Metadata={"validation-status": "valid", "original-key": original_key, "event-type": event_type, "format": "json"},
        )
        return key_json

def _store_invalid_event(event_data: Dict[str, Any], result: Dict[str, Any]) -> None:
    """Guarda inválidos en INVALID_BUCKET como JSON (particionado por fecha y type)."""
    event_type = str(event_data.get("type", "unknown"))
    event_id = str(event_data.get("eventId", "unknown"))
    now = datetime.utcnow()
    key_out = f"year={now.year}/month={now.month:02}/day={now.day:02}/type={event_type}/{event_id}.json"

    invalid_doc = {
        **event_data,
        "validationResult": {
            "status": result["status"].value,
            "errors": result["errors"],
            "warnings": result["warnings"],
            "processedAt": datetime.utcnow().isoformat() + "Z",
            "processedBy": "tp-validate-events",
        },
    }
    s3.put_object(
        Bucket=INVALID_BUCKET,
        Key=key_out,
        Body=json.dumps(invalid_doc, ensure_ascii=False),
        ContentType="application/json",
        Metadata={
            "validation-status": "invalid",
            "error-count": str(len(result["errors"])),
            "warning-count": str(len(result["warnings"])),
            "event-type": event_type,
        },
    )

# ---------- Helpers ----------

def _get_request_id(event: Dict[str, Any], context) -> str:
    if hasattr(context, "aws_request_id"):
        return context.aws_request_id
    if isinstance(event, dict) and "Records" in event and event["Records"]:
        return event["Records"][0].get("responseElements", {}).get("x-amz-request-id", "unknown")
    return "unknown"
