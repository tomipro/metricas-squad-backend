import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')

# Environment variables
ATHENA_DATABASE = os.environ.get('ATHENA_DATABASE', 'tp_events_db')
ATHENA_OUTPUT_BUCKET = os.environ.get('ATHENA_OUTPUT_BUCKET', '')  # e.g. tp-athena-results-xxxx
CURATED_TABLE = os.environ.get('CURATED_TABLE', 'tp_curated_events_tprodan')
API_KEY = os.environ.get('API_KEY', None)

if not ATHENA_DATABASE or not CURATED_TABLE or not ATHENA_OUTPUT_BUCKET:
    raise ValueError("ATHENA_DATABASE, CURATED_TABLE, and ATHENA_OUTPUT_BUCKET env vars are required")

logger.info(f"Initialized with database: {ATHENA_DATABASE}, table: {CURATED_TABLE}")

def lambda_handler(event, context):
    try:
        # Opcional: API Key estática por env var (además de Usage Plan del API GW)
        if API_KEY:
            headers = event.get('headers', {}) or {}
            provided_key = headers.get('x-api-key') or headers.get('X-API-Key')
            if not provided_key or provided_key != API_KEY:
                return _resp(401, {"error": "Unauthorized", "message": "Invalid or missing API key"})

        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '')  # ejemplo: /analytics/funnel
        qs = event.get('queryStringParameters') or {}

        logger.info(f"Request {http_method} {path} qs={qs}")

        # Routing (mantengo los que ya tenías + agrego nuevos)
        if path.endswith('/analytics/health'):
            out = _health()
        elif path.endswith('/analytics/recent'):
            out = _recent(qs)
        elif path.endswith('/analytics/daily'):
            out = _daily(qs)
        elif path.endswith('/analytics/events-by-type'):
            out = _events_by_type(qs)
        elif path.endswith('/analytics/validation-stats'):
            out = _validation_stats(qs)
        elif path.endswith('/analytics/revenue'):
            out = _revenue_legacy(qs)
        elif path.endswith('/analytics/events'):
            out = _events_data(qs)
        elif path.endswith('/analytics/summary'):
            out = _summary(qs)
        elif path.endswith('/analytics/search-metrics'):
            out = _search_metrics(qs)
        elif path.endswith('/analytics/catalog/airline-summary'):
            out = _catalog_airline_summary(qs)
        elif path.endswith('/analytics/catalog/status'):
            out = _catalog_status(qs)
        elif path.endswith('/analytics/catalog/aircraft'):
            out = _catalog_aircraft(qs)
        elif path.endswith('/analytics/catalog/routes'):
            out = _catalog_routes(qs)
        elif path.endswith('/analytics/search/cart'):
            out = _search_cart_summary(qs)
        elif path.endswith('/analytics/reservations/updates'):
            out = _reservations_updates(qs)
        elif path.endswith('/analytics/payments/status'):
            out = _payments_status(qs)
        elif path.endswith('/analytics/flights/updates'):
            out = _flights_updates(qs)
        elif path.endswith('/analytics/flights/aircraft'):
            out = _aircraft_updates(qs)

        # ---- NUEVOS ENDPOINTS ----
        elif path.endswith('/analytics/funnel'):
            out = _funnel(qs)
        elif path.endswith('/analytics/avg-fare'):
            out = _avg_fare(qs)
        elif path.endswith('/analytics/revenue-monthly'):
            out = _revenue_monthly(qs)
        elif path.endswith('/analytics/ltv'):
            out = _ltv(qs)
        elif path.endswith('/analytics/revenue-per-user'):
            out = _revenue_per_user(qs)
        elif path.endswith('/analytics/popular-airlines'):
            out = _popular_airlines(qs)
        elif path.endswith('/analytics/user-origins'):
            out = _user_origins(qs)
        elif path.endswith('/analytics/booking-hours'):
            out = _booking_hours(qs)
        elif path.endswith('/analytics/payment-success'):
            out = _payment_success(qs)
        elif path.endswith('/analytics/cancellation-rate'):
            out = _cancellation_rate(qs)
        elif path.endswith('/analytics/anticipation'):
            out = _anticipation(qs)
        elif path.endswith('/analytics/time-to-complete'):
            out = _time_to_complete(qs)

        else:
            out = {
                "error": "Endpoint not found",
                "available_endpoints": [
                    "/analytics/health",
                    "/analytics/recent",
                    "/analytics/daily",
                    "/analytics/events-by-type",
                    "/analytics/validation-stats",
                    "/analytics/revenue",
                    "/analytics/events",
                    "/analytics/summary",
                    "/analytics/search-metrics",
                    "/analytics/catalog/airline-summary",
                    "/analytics/catalog/status",
                    "/analytics/catalog/aircraft",
                    "/analytics/catalog/routes",
                    "/analytics/search/cart",
                    "/analytics/reservations/updates",
                    "/analytics/payments/status",
                    "/analytics/flights/updates",
                    "/analytics/flights/aircraft",
                    # nuevos
                    "/analytics/funnel",
                    "/analytics/avg-fare",
                    "/analytics/revenue-monthly",
                    "/analytics/ltv",
                    "/analytics/revenue-per-user",
                    "/analytics/popular-airlines",
                    "/analytics/user-origins",
                    "/analytics/booking-hours",
                    "/analytics/payment-success",
                    "/analytics/cancellation-rate",
                    "/analytics/anticipation",
                    "/analytics/time-to-complete"
                ]
            }

        return _resp(200, out)

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return _resp(500, {"error": str(e), "type": "internal_error"})

# ---------- Helpers HTTP ----------

def _resp(code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, x-api-key"
        },
        "body": json.dumps(body, default=str)
    }

# ---------- Helpers Athena ----------

def _exec(query: str) -> List[tuple]:
    logger.info("Athena query (truncated): " + query.strip().replace("\n", " ")[:1000])
    qid = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': f's3://{ATHENA_OUTPUT_BUCKET}/athena-results/'}
    )['QueryExecutionId']

    # Espera simple
    for _ in range(60):  # hasta ~60*0.5s=30s
        st = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if st == 'SUCCEEDED':
            break
        if st in ('FAILED', 'CANCELLED'):
            reason = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            logger.error(f"Athena failed: {reason}")
            return []
        time.sleep(0.5)

    rows = athena.get_query_results(QueryExecutionId=qid)['ResultSet']['Rows']
    if not rows or len(rows) <= 1:
        return []
    out = []
    for r in rows[1:]:
        vals = []
        for c in r['Data']:
            v = c.get('VarCharValue')
            if v is None:
                vals.append(None)
            else:
                # casting mínimo
                try:
                    if '.' in v and all(p.isdigit() or (p.startswith('-') and p[1:].isdigit()) for p in v.split('.', 1)):
                        vals.append(float(v))
                    elif v.isdigit() or (v.startswith('-') and v[1:].isdigit()):
                        vals.append(int(v))
                    else:
                        vals.append(v)
                except Exception:
                    vals.append(v)
        out.append(tuple(vals))
    return out

def _days(qs: Dict[str, str], key: str, default_val: int) -> int:
    try:
        return max(1, int(qs.get(key, str(default_val))))
    except:
        return default_val

def _top(qs: Dict[str, str], key: str, default_val: int) -> int:
    try:
        return max(1, int(qs.get(key, str(default_val))))
    except:
        return default_val

def _currency_filter_clause(qs: Dict[str, str]) -> Tuple[str, Optional[str], bool]:
    currency = qs.get('currency')
    if not currency:
        return "", None, False
    sanitized = currency.strip().upper()
    if len(sanitized) == 3 and sanitized.isalpha():
        return f" AND upper(coalesce(json_extract_scalar(payload_json, '$.moneda'), json_extract_scalar(payload_json, '$.currency'))) = '{sanitized}'", sanitized, False
    return "", currency, True

# ---------- Endpoints existentes (compatibilidad) ----------

def _health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": "tp-analytics-backend",
        "api_gateway": "tp-analytics-api",
        "purpose": "Analytics, KPIs, and Dashboard data",
        "version": "1.1"
    }

def _recent(qs):
    limit = int(qs.get('limit', '10'))
    hours = int(qs.get('hours', '24'))
    q = f"""
    SELECT eventid,
           type,
           ts,
           receivedat,
           json_extract_scalar(validation_json, '$.status') AS validation_status
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('hour', -{hours}, now())
    ORDER BY from_iso8601_timestamp(ts) DESC
    LIMIT {limit}
    """
    res = _exec(q)
    return {
        "recent_events": [{"eventId":r[0],"type":r[1],"timestamp":r[2],"received_at":r[3],"validation_status":r[4]} for r in res],
        "count": len(res),
        "period_hours": hours
    }

def _daily(qs):
    date = qs.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
    y, m, d = date[:4], date[5:7], date[8:10]
    q = f"""
    SELECT type, COUNT(*) cnt, COUNT(DISTINCT eventid) uniq
    FROM {CURATED_TABLE}
    WHERE year='{y}' AND month='{m}' AND day='{d}'
    GROUP BY type ORDER BY cnt DESC
    """
    res = _exec(q)
    return {"date": date, "metrics": [{"type":r[0],"event_count":r[1],"unique_events":r[2]} for r in res],
            "total_events": sum((r[1] for r in res), 0)}

def _events_by_type(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT type, COUNT(*) cnt,
           AVG(
             CASE
               WHEN type='reserva_creada' THEN TRY_CAST(json_extract_scalar(payload_json, '$.precio') AS DOUBLE)
               WHEN type='reservations.reservation.created' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           ) avg_price
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY type ORDER BY cnt DESC
    """
    res = _exec(q)
    return {"period_days": days, "events_by_type": [{"type":r[0],"count":r[1],"avg_price":r[2]} for r in res]}

def _validation_stats(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT json_extract_scalar(validation_json, '$.status') AS status, COUNT(*)
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY json_extract_scalar(validation_json, '$.status')
    """
    res = _exec(q)
    total = sum(r[1] for r in res) if res else 0
    valid = next((r[1] for r in res if r[0]=='valid'), 0)
    rate = round((valid/total)*100, 2) if total>0 else 0
    return {"period_days": days, "validation_stats":[{"status":r[0],"count":r[1]} for r in res],
            "validation_rate_percent": rate, "total_events": total}

def _revenue_legacy(qs):
    # compat: revenue basado en reservas (precio)
    days = _days(qs, 'days', 30)
    q = f"""
    SELECT DATE(from_iso8601_timestamp(ts)) d,
           SUM(
             CASE
               WHEN type='reserva_creada' THEN TRY_CAST(json_extract_scalar(payload_json, '$.precio') AS DOUBLE)
               WHEN type='reservations.reservation.created' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           ),
           COUNT(*),
           AVG(
             CASE
               WHEN type='reserva_creada' THEN TRY_CAST(json_extract_scalar(payload_json, '$.precio') AS DOUBLE)
               WHEN type='reservations.reservation.created' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           )
    FROM {CURATED_TABLE}
    WHERE type IN ('reserva_creada', 'reservations.reservation.created')
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, current_date)
      AND (
        (type='reserva_creada' AND TRY_CAST(json_extract_scalar(payload_json, '$.precio') AS DOUBLE) > 0)
        OR (type='reservations.reservation.created' AND TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE) > 0)
      )
    GROUP BY DATE(from_iso8601_timestamp(ts))
    ORDER BY d DESC
    """
    res = _exec(q)
    total_rev = sum((r[1] or 0) for r in res)
    total_book = sum((r[2] or 0) for r in res)
    return {
        "period_days": days,
        "total_revenue": total_rev,
        "total_bookings": total_book,
        "avg_ticket_price": round(total_rev/total_book,2) if total_book>0 else 0,
        "daily_breakdown":[{"date":str(r[0]),"revenue":r[1] or 0,"bookings":r[2] or 0,"avg_price":round(r[3],2) if r[3] else 0} for r in res]
    }

def _events_data(qs):
    limit = int(qs.get('limit', '100'))
    event_type = qs.get('type')
    days = _days(qs, 'days', 7)
    type_filter = f"AND type='{event_type}'" if event_type else ""
    q = f"""
    SELECT eventid,
           type,
           ts,
           receivedat,
           payload_json,
           validation_json
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      {type_filter}
    ORDER BY from_iso8601_timestamp(ts) DESC
    LIMIT {limit}
    """
    res = _exec(q)
    events = []
    for r in res:
        payload = {}
        validation = {}
        try:
            payload = json.loads(r[4]) if r[4] else {}
        except Exception:
            payload = {"raw": r[4]}
        try:
            validation = json.loads(r[5]) if r[5] else {}
        except Exception:
            validation = {"raw": r[5]}
        events.append({
            "eventId": r[0],
            "type": r[1],
            "timestamp": r[2],
            "received_at": r[3],
            "payload": payload,
            "validation": validation
        })
    return {"events":events,
            "count": len(res),
            "filters":{"days":days,"type":event_type,"limit":limit}
    }

def _summary(qs):
    days = _days(qs, 'days', 7)
    # simple summary reutilizando funciones
    by_type = _events_by_type({"days": str(days)})
    val = _validation_stats({"days": str(days)})
    rev = _revenue_legacy({"days": str(days)})
    rec = _recent({"limit":"5"})
    return {
        "period_days": days,
        "summary": {
            "total_events": sum((i["count"] for i in by_type["events_by_type"]), 0),
            "total_revenue": rev["total_revenue"],
            "validation_rate": val["validation_rate_percent"],
            "total_bookings": rev["total_bookings"]
        },
        "events_by_type": by_type["events_by_type"],
        "validation_breakdown": val["validation_stats"],
        "recent_activity": rec["recent_events"]
    }

# ---------- Búsquedas ----------

def _search_metrics(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 10)
    q = f"""
    WITH unified AS (
      SELECT
        json_extract_scalar(payload_json, '$.flightsFrom') AS origin,
        json_extract_scalar(payload_json, '$.flightsTo') AS destination,
        TRY_CAST(json_extract_scalar(payload_json, '$.resultsCount') AS DOUBLE) AS results_count,
        try(date_diff(
              'day',
              from_iso8601_timestamp(concat(json_extract_scalar(payload_json, '$.dateFrom'), 'T00:00:00Z')),
              from_iso8601_timestamp(concat(json_extract_scalar(payload_json, '$.dateTo'), 'T00:00:00Z'))
        )) AS trip_length_days
      FROM {CURATED_TABLE}
      WHERE type = 'search_metric'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())

      UNION ALL

      SELECT
        json_extract_scalar(payload_json, '$.origin') AS origin,
        json_extract_scalar(payload_json, '$.destination') AS destination,
        NULL AS results_count,
        try(date_diff(
              'day',
              date_parse(json_extract_scalar(payload_json, '$.departDate'), '%Y-%m-%d'),
              date_parse(json_extract_scalar(payload_json, '$.returnDate'), '%Y-%m-%d')
        )) AS trip_length_days
      FROM {CURATED_TABLE}
      WHERE type = 'search.search.performed'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT origin, destination,
           COUNT(*) searches,
           AVG(results_count) avg_results,
           MAX(results_count) max_results,
           MIN(results_count) min_results,
           AVG(trip_length_days) trip_length_days
    FROM unified
    GROUP BY origin, destination
    ORDER BY searches DESC
    LIMIT {top}
    """
    res = _exec(q)

    return {
        "period_days": days,
        "top": top,
        "routes": [
            {
                "origin": row[0],
                "destination": row[1],
                "flightsFrom": row[0],
                "flightsTo": row[1],
                "searches": row[2],
                "avg_results": row[3],
                "max_results": row[4],
                "min_results": row[5],
                "avg_trip_length_days": row[6],
            }
            for row in res
        ],
        "total_routes": len(res),
        "total_searches": sum((row[2] or 0) for row in res),
    }

# ---------- Catálogo ----------

def _catalog_airline_summary(qs):
    days = _days(qs, 'days', 7)
    currency_clause, currency, invalid_currency = _currency_filter_clause(qs)
    q = f"""
    SELECT
        coalesce(
            json_extract_scalar(payload_json, '$.aerolinea'),
            json_extract_scalar(payload_json, '$.airlineBrand'),
            json_extract_scalar(payload_json, '$.flightNumber')
        ) AS aerolinea,
        COUNT(*) flights,
        AVG(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) avg_price,
        MIN(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) min_price,
        MAX(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) max_price,
        AVG(TRY_CAST(json_extract_scalar(payload_json, '$.capacidadAvion') AS DOUBLE)) avg_capacity,
        SUM(TRY_CAST(json_extract_scalar(payload_json, '$.capacidadAvion') AS DOUBLE)) total_capacity
    FROM {CURATED_TABLE}
    WHERE type IN ('catalogo','flights.flight.created')
      AND from_iso8601_timestamp(coalesce(json_extract_scalar(payload_json, '$.despegue'), json_extract_scalar(payload_json, '$.departureAt')))
          BETWEEN current_timestamp AND date_add('day', {days}, current_timestamp)
      {currency_clause}
    GROUP BY 1
    ORDER BY flights DESC
    """
    res = _exec(q)
    total_flights = sum((row[1] or 0) for row in res)
    return {
        "period_days": days,
        "currency": currency,
        "invalid_currency": invalid_currency,
        "airlines": [
            {
                "airline": row[0],
                "flights": row[1],
                "avg_price": row[2],
                "min_price": row[3],
                "max_price": row[4],
                "avg_capacity": row[5],
                "total_capacity": row[6],
            }
            for row in res
        ],
        "total_flights": total_flights,
    }

def _catalog_status(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT
        coalesce(
            json_extract_scalar(payload_json, '$.estado_vuelo'),
            json_extract_scalar(payload_json, '$.status')
        ) AS estado,
        COUNT(*) flights
    FROM {CURATED_TABLE}
    WHERE type IN ('catalogo','flights.flight.created','flights.flight.updated')
      AND (
        (type IN ('catalogo','flights.flight.created')
            AND from_iso8601_timestamp(coalesce(json_extract_scalar(payload_json, '$.despegue'), json_extract_scalar(payload_json, '$.departureAt')))
                BETWEEN current_timestamp AND date_add('day', {days}, current_timestamp))
        OR type='flights.flight.updated'
      )
    GROUP BY 1
    """
    res = _exec(q)
    total = sum((row[1] or 0) for row in res)
    on_time = next((row[1] for row in res if str(row[0]).lower() == 'en hora'), 0)
    return {
        "period_days": days,
        "status": [
            {"status": row[0], "flights": row[1]}
            for row in res
        ],
        "total_flights": total,
        "on_time_rate_percent": round((on_time / total) * 100, 2) if total else 0.0,
    }

def _catalog_aircraft(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT
        coalesce(
            json_extract_scalar(payload_json, '$.tipoAvion'),
            json_extract_scalar(payload_json, '$.aircraftModel')
        ) AS tipoavion,
        COUNT(*) flights,
        AVG(TRY_CAST(json_extract_scalar(payload_json, '$.capacidadAvion') AS DOUBLE)) avg_capacity,
        SUM(TRY_CAST(json_extract_scalar(payload_json, '$.capacidadAvion') AS DOUBLE)) total_capacity,
        AVG(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) avg_price
    FROM {CURATED_TABLE}
    WHERE type IN ('catalogo','flights.flight.created')
      AND from_iso8601_timestamp(coalesce(json_extract_scalar(payload_json, '$.despegue'), json_extract_scalar(payload_json, '$.departureAt')))
          BETWEEN current_timestamp AND date_add('day', {days}, current_timestamp)
    GROUP BY 1
    ORDER BY flights DESC
    """
    res = _exec(q)
    return {
        "period_days": days,
        "aircraft": [
            {
                "aircraftType": row[0],
                "flights": row[1],
                "avg_capacity": row[2],
                "total_capacity": row[3],
                "avg_price": row[4],
            }
            for row in res
        ],
    }

def _catalog_routes(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 10)
    currency_clause, currency, invalid_currency = _currency_filter_clause(qs)
    q = f"""
    SELECT
        coalesce(json_extract_scalar(payload_json, '$.origen'), json_extract_scalar(payload_json, '$.origin')) AS origen,
        coalesce(json_extract_scalar(payload_json, '$.destino'), json_extract_scalar(payload_json, '$.destination')) AS destino,
        COUNT(*) flights,
        AVG(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) avg_price,
        MIN(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) min_price,
        MAX(TRY_CAST(coalesce(json_extract_scalar(payload_json, '$.precio'), json_extract_scalar(payload_json, '$.price')) AS DOUBLE)) max_price,
        AVG(TRY_CAST(json_extract_scalar(payload_json, '$.capacidadAvion') AS DOUBLE)) avg_capacity
    FROM {CURATED_TABLE}
    WHERE type IN ('catalogo','flights.flight.created')
      AND from_iso8601_timestamp(coalesce(json_extract_scalar(payload_json, '$.despegue'), json_extract_scalar(payload_json, '$.departureAt')))
          BETWEEN current_timestamp AND date_add('day', {days}, current_timestamp)
      {currency_clause}
    GROUP BY 1, 2
    ORDER BY flights DESC
    LIMIT {top}
    """
    res = _exec(q)
    return {
        "period_days": days,
        "top": top,
        "currency": currency,
        "invalid_currency": invalid_currency,
        "routes": [
            {
                "origin": row[0],
                "destination": row[1],
                "flights": row[2],
                "avg_price": row[3],
                "min_price": row[4],
                "max_price": row[5],
                "avg_capacity": row[6],
            }
            for row in res
        ],
    }

# ---------- Operaciones ----------

def _search_cart_summary(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 10)
    q = f"""
    SELECT
        json_extract_scalar(payload_json, '$.flightId') AS flightId,
        COUNT(*) cnt,
        max(ts) last_added
    FROM {CURATED_TABLE}
    WHERE type='search.cart.item.added'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY json_extract_scalar(payload_json, '$.flightId')
    ORDER BY cnt DESC
    LIMIT {top}
    """
    res = _exec(q)
    total = sum((row[1] or 0) for row in res)
    return {
        "period_days": days,
        "top": top,
        "total_additions": total,
        "cart_items": [
            {
                "flightId": row[0],
                "additions": row[1],
                "last_added_at": row[2],
            }
            for row in res
        ],
    }

def _reservations_updates(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT upper(json_extract_scalar(payload_json, '$.newStatus')) AS status,
           COUNT(*) cnt
    FROM {CURATED_TABLE}
    WHERE type='reservations.reservation.updated'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY 1
    ORDER BY cnt DESC
    """
    res = _exec(q)

    recent_q = f"""
    SELECT
        json_extract_scalar(payload_json, '$.reservationId') AS reservationId,
        json_extract_scalar(payload_json, '$.newStatus') AS newStatus,
        json_extract_scalar(payload_json, '$.reservationDate') AS reservationDate,
        json_extract_scalar(payload_json, '$.flightDate') AS flightDate,
        ts
    FROM {CURATED_TABLE}
    WHERE type='reservations.reservation.updated'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ORDER BY from_iso8601_timestamp(ts) DESC
    LIMIT 20
    """
    recent = _exec(recent_q)
    return {
        "period_days": days,
        "status_counts": [{"status": row[0], "count": row[1]} for row in res],
        "recent_updates": [
            {
                "reservationId": row[0],
                "newStatus": row[1],
                "reservationDate": row[2],
                "flightDate": row[3],
                "updated_ts": row[4],
            }
            for row in recent
        ],
    }

def _payments_status(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH base AS (
      SELECT
        upper(json_extract_scalar(payload_json, '$.status')) AS status,
        TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE) AS amount
      FROM {CURATED_TABLE}
      WHERE type='payments.payment.status_updated'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT status,
           COUNT(*) cnt,
           SUM(CASE WHEN status='PAID' THEN amount ELSE NULL END) paid_amount
    FROM base
    GROUP BY status
    ORDER BY cnt DESC
    """
    res = _exec(q)
    total = sum((row[1] or 0) for row in res)
    paid_amount = sum((row[2] or 0) for row in res)
    return {
        "period_days": days,
        "total_events": total,
        "statuses": [
            {"status": row[0], "count": row[1], "paid_amount": row[2]}
            for row in res
        ],
        "total_paid_amount": paid_amount,
    }

def _flights_updates(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT
        upper(json_extract_scalar(payload_json, '$.newStatus')) AS status,
        COUNT(*) cnt,
        max(json_extract_scalar(payload_json, '$.newDepartureAt')) AS last_departure,
        max(json_extract_scalar(payload_json, '$.newArrivalAt')) AS last_arrival
    FROM {CURATED_TABLE}
    WHERE type='flights.flight.updated'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY 1
    ORDER BY cnt DESC
    """
    res = _exec(q)
    return {
        "period_days": days,
        "updates": [
            {
                "status": row[0],
                "count": row[1],
                "latest_departure": row[2],
                "latest_arrival": row[3]
            }
            for row in res
        ]
    }

def _aircraft_updates(qs):
    days = _days(qs, 'days', 30)
    q = f"""
    SELECT
        json_extract_scalar(payload_json, '$.aircraftId') AS aircraftId,
        json_extract_scalar(payload_json, '$.airlineBrand') AS airlineBrand,
        MAX(TRY_CAST(json_extract_scalar(payload_json, '$.capacity') AS DOUBLE)) AS capacity,
        COUNT(*) updates
    FROM {CURATED_TABLE}
    WHERE type='flights.aircraft_or_airline.updated'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY 1,2
    ORDER BY updates DESC
    """
    res = _exec(q)
    return {
        "period_days": days,
        "aircraft": [
            {
                "aircraftId": row[0],
                "airlineBrand": row[1],
                "capacity": row[2],
                "updates": row[3]
            }
            for row in res
        ]
    }

# ---------- NUEVOS KPIs ----------

def _funnel(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH searches AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type IN ('search_metric','search.search.performed')
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    carts AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='search.cart.item.added'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    reserves AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type IN ('reserva_creada','reservations.reservation.created')
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    pays AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE (
            type='pago_aprobado'
            OR (type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID')
          )
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT s.c, carts.c, r.c, p.c FROM searches s, carts, reserves r, pays p
    """
    r = _exec(q)
    s, carts_count, rsv, pay = (r[0] if r else (0,0,0,0))
    conv_sc = round((carts_count/s)*100,2) if s>0 else 0.0
    conv_cr = round((rsv/carts_count)*100,2) if carts_count>0 else 0.0
    conv_sr = round((rsv/s)*100,2) if s>0 else 0.0
    conv_rp = round((pay/rsv)*100,2) if rsv>0 else 0.0
    conv_sp = round((pay/s)*100,2) if s>0 else 0.0
    return {
        "period_days":days,
        "searches":s,
        "cart_adds":carts_count,
        "reservations":rsv,
        "payments":pay,
        "conversion":{
            "search_to_cart": conv_sc,
            "cart_to_reserve": conv_cr,
            "search_to_reserve": conv_sr,
            "reserve_to_pay": conv_rp,
            "search_to_pay": conv_sp
        }
    }

def _avg_fare(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT AVG(
      CASE
        WHEN type='reserva_creada' THEN TRY_CAST(json_extract_scalar(payload_json, '$.precio') AS DOUBLE)
        WHEN type='reservations.reservation.created' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
        ELSE NULL
      END
    )
    FROM {CURATED_TABLE}
    WHERE type IN ('reserva_creada','reservations.reservation.created')
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      AND (
        (type='reserva_creada' AND json_extract_scalar(payload_json, '$.precio') IS NOT NULL)
        OR (type='reservations.reservation.created' AND json_extract_scalar(payload_json, '$.amount') IS NOT NULL)
      )
    """
    r = _exec(q)
    return {"period_days":days,"avg_fare": (r[0][0] if r else None)}

def _revenue_monthly(qs):
    months = _days(qs, 'months', 6)
    q = f"""
    SELECT date_format(from_iso8601_timestamp(ts), '%Y-%m') ym,
           SUM(
             CASE
               WHEN type='pago_aprobado' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID'
                 THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           ) revenue,
           COUNT(*) payments
    FROM {CURATED_TABLE}
    WHERE (
          type='pago_aprobado'
          OR (type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status')) IN ('PAID','FAILED','PENDING'))
        )
      AND from_iso8601_timestamp(ts) >= date_add('month', -{months}, current_date)
    GROUP BY 1 ORDER BY 1 DESC
    """
    r = _exec(q)
    return {"months": months, "monthly": [{"ym":row[0],"revenue":row[1] or 0,"payments":row[2] or 0} for row in r]}

def _ltv(qs):
    top = _top(qs, 'top', 10)
    q = f"""
    SELECT json_extract_scalar(payload_json, '$.userId') AS userid,
           SUM(
             CASE
               WHEN type='pago_aprobado' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID'
                 THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           ) total_spend,
           SUM(
             CASE
               WHEN type='pago_aprobado' THEN 1
               WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID' THEN 1
               ELSE 0
             END
           ) payments
    FROM {CURATED_TABLE}
    WHERE type IN ('pago_aprobado','payments.payment.status_updated')
      AND json_extract_scalar(payload_json, '$.userId') IS NOT NULL
    GROUP BY json_extract_scalar(payload_json, '$.userId')
    ORDER BY total_spend DESC
    LIMIT {top}
    """
    r = _exec(q)
    return {"top": top, "ltv":[{"userId":row[0],"total_spend":row[1] or 0,"payments":row[2] or 0} for row in r]}

def _revenue_per_user(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 10)
    q = f"""
    SELECT json_extract_scalar(payload_json, '$.userId') AS userid,
           SUM(
             CASE
               WHEN type='pago_aprobado' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID'
                 THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           ) revenue,
           SUM(
             CASE
               WHEN type='pago_aprobado' THEN 1
               WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID' THEN 1
               ELSE 0
             END
           ) payments
    FROM {CURATED_TABLE}
    WHERE type IN ('pago_aprobado','payments.payment.status_updated')
      AND json_extract_scalar(payload_json, '$.userId') IS NOT NULL
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY json_extract_scalar(payload_json, '$.userId')
    ORDER BY revenue DESC
    LIMIT {top}
    """
    r = _exec(q)
    return {"period_days":days,"top": top,
            "revenue_per_user":[{"userId":row[0],"revenue":row[1] or 0,"payments":row[2] or 0} for row in r]}

def _popular_airlines(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 5)
    q = f"""
    SELECT json_extract_scalar(payload_json, '$.airlineCode') AS airlineCode,
           COUNT(*) cnt,
           ROUND(AVG(
             CASE
               WHEN type='reserva_creada' THEN TRY_CAST(json_extract_scalar(payload_json, '$.precio') AS DOUBLE)
               WHEN type='reservations.reservation.created' THEN TRY_CAST(json_extract_scalar(payload_json, '$.amount') AS DOUBLE)
               ELSE NULL
             END
           ),2) avg_price
    FROM {CURATED_TABLE}
    WHERE type IN ('reserva_creada','reservations.reservation.created')
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      AND json_extract_scalar(payload_json, '$.airlineCode') IS NOT NULL
    GROUP BY json_extract_scalar(payload_json, '$.airlineCode')
    ORDER BY cnt DESC
    LIMIT {top}
    """
    r = _exec(q)
    return {"period_days":days,"top":top,
            "popular_airlines":[{"airlineCode":row[0],"count":row[1],"avg_price":row[2]} for row in r]}

def _user_origins(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 10)
    q = f"""
    SELECT coalesce(
             json_extract_scalar(payload_json, '$.pais'),
             json_extract_scalar(payload_json, '$.nationalityOrOrigin')
           ) AS pais,
           COUNT(*) cnt
    FROM {CURATED_TABLE}
    WHERE type IN ('usuario_registrado','users.user.created')
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      AND coalesce(
            json_extract_scalar(payload_json, '$.pais'),
            json_extract_scalar(payload_json, '$.nationalityOrOrigin')
          ) IS NOT NULL
    GROUP BY 1
    ORDER BY cnt DESC
    LIMIT {top}
    """
    r = _exec(q)
    return {"period_days":days,"top":top,
            "user_origins":[{"country":row[0],"users":row[1]} for row in r]}

def _booking_hours(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT hour(from_iso8601_timestamp(ts)) as hour_utc, COUNT(*) cnt
    FROM {CURATED_TABLE}
    WHERE type IN ('reserva_creada','reservations.reservation.created')
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY 1 ORDER BY 1
    """
    r = _exec(q)
    return {"period_days":days,"histogram":[{"hour_utc":row[0],"count":row[1]} for row in r]}

def _payment_success(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH base AS (
      SELECT
        CASE
          WHEN type='pago_aprobado' THEN 'approved'
          WHEN type='pago_rechazado' THEN 'rejected'
          WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID' THEN 'approved'
          WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='FAILED' THEN 'rejected'
          WHEN type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PENDING' THEN 'pending'
          ELSE 'other'
        END AS bucket
      FROM {CURATED_TABLE}
      WHERE type IN ('pago_aprobado','pago_rechazado','payments.payment.status_updated')
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT
      SUM(CASE WHEN bucket='approved' THEN 1 ELSE 0 END) AS approved,
      SUM(CASE WHEN bucket='rejected' THEN 1 ELSE 0 END) AS rejected,
      SUM(CASE WHEN bucket='pending' THEN 1 ELSE 0 END) AS pending
    FROM base
    """
    r = _exec(q)
    approved, rejected, pending = (r[0] if r else (0,0,0))
    total = approved + rejected
    rate = round((approved/total)*100, 2) if total>0 else 0.0
    return {"period_days":days,"approved":approved,"rejected":rejected,"pending":pending,"success_rate_percent":rate}

def _cancellation_rate(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH created AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type IN ('reserva_creada','reservations.reservation.created')
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    canceled AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE (
            type='reserva_cancelada'
            OR (
              type='reservations.reservation.updated'
              AND upper(json_extract_scalar(payload_json, '$.newStatus')) IN ('CANCELLED','CANCELED')
            )
          )
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT created.c, canceled.c FROM created, canceled
    """
    r = _exec(q)
    created, canceled = (r[0] if r else (0,0))
    rate = round((canceled/created)*100, 2) if created>0 else 0.0
    return {"period_days":days,"created":created,"canceled":canceled,"cancellation_rate_percent":rate}

def _anticipation(qs):
    # días entre reservationDate y flightDate para reservas pagadas
    days = _days(qs, 'days', 90)
    q = f"""
    WITH paid_updates AS (
      SELECT
        from_iso8601_timestamp(json_extract_scalar(payload_json, '$.reservationDate')) AS reservation_ts,
        from_iso8601_timestamp(json_extract_scalar(payload_json, '$.flightDate')) AS flight_ts
      FROM {CURATED_TABLE}
      WHERE type = 'reservations.reservation.updated'
        AND upper(coalesce(json_extract_scalar(payload_json, '$.newStatus'), '')) = 'PAID'
        AND json_extract_scalar(payload_json, '$.reservationDate') IS NOT NULL
        AND json_extract_scalar(payload_json, '$.flightDate') IS NOT NULL
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT AVG(date_diff('day', reservation_ts, flight_ts))
    FROM paid_updates
    WHERE reservation_ts IS NOT NULL
      AND flight_ts IS NOT NULL
    """
    r = _exec(q)
    return {"period_days":days,"avg_anticipation_days": (r[0][0] if r else None)}

def _time_to_complete(qs):
    # tiempo (min) entre búsqueda → reserva → pago usando eventos search_metric y userId como vínculo
    days = _days(qs, 'days', 7)
    q = f"""
    WITH searches AS (
      SELECT json_extract_scalar(payload_json, '$.userId') AS userid,
             from_iso8601_timestamp(ts) AS s_ts
      FROM {CURATED_TABLE}
      WHERE type IN ('search_metric','search.search.performed')
        AND json_extract_scalar(payload_json, '$.userId') IS NOT NULL
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    reserve AS (
      SELECT coalesce(
               json_extract_scalar(payload_json, '$.reservaId'),
               json_extract_scalar(payload_json, '$.reservationId')
             ) AS reservaid,
             json_extract_scalar(payload_json, '$.userId') AS userid,
             from_iso8601_timestamp(ts) AS r_ts
      FROM {CURATED_TABLE}
      WHERE type IN ('reserva_creada','reservations.reservation.created')
        AND json_extract_scalar(payload_json, '$.userId') IS NOT NULL
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    pay AS (
      SELECT coalesce(
               json_extract_scalar(payload_json, '$.reservaId'),
               json_extract_scalar(payload_json, '$.reservationId')
             ) AS reservaid,
             from_iso8601_timestamp(ts) AS p_ts
      FROM {CURATED_TABLE}
      WHERE (
            type='pago_aprobado'
            OR (type='payments.payment.status_updated' AND upper(json_extract_scalar(payload_json, '$.status'))='PAID')
          )
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    linked AS (
      SELECT
        r.reservaid,
        r.userid,
        r.r_ts,
        p.p_ts,
        (
          SELECT max(s.s_ts)
          FROM searches s
          WHERE s.userid = r.userid
            AND s.s_ts <= r.r_ts
        ) AS s_ts
      FROM reserve r
      LEFT JOIN pay p ON r.reservaid = p.reservaid
    )
    SELECT
      AVG(date_diff('minute', s_ts, r_ts)) AS search_to_reserve_min,
      AVG(CASE WHEN p_ts IS NOT NULL THEN date_diff('minute', r_ts, p_ts) END) AS reserve_to_pay_min,
      AVG(CASE WHEN p_ts IS NOT NULL AND s_ts IS NOT NULL THEN date_diff('minute', s_ts, p_ts) END) AS search_to_pay_min
    FROM linked
    WHERE s_ts IS NOT NULL
    """
    r = _exec(q)
    if r:
        return {
            "period_days": days,
            "avg_minutes": {
                "search_to_reserve": r[0][0],
                "reserve_to_pay": r[0][1],
                "search_to_pay": r[0][2]
            }
        }
    return {"period_days": days, "avg_minutes": {}}
