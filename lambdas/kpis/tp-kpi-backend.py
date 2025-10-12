import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
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
    SELECT eventid, type, ts, receivedat, validation.status
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
           AVG(CASE WHEN type='reserva_creada' AND precio IS NOT NULL THEN CAST(precio AS DOUBLE) END) avg_price
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY type ORDER BY cnt DESC
    """
    res = _exec(q)
    return {"period_days": days, "events_by_type": [{"type":r[0],"count":r[1],"avg_price":r[2]} for r in res]}

def _validation_stats(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT validation.status, COUNT(*)
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY validation.status
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
    SELECT DATE(from_iso8601_timestamp(ts)) d, SUM(CAST(precio AS DOUBLE)), COUNT(*), AVG(CAST(precio AS DOUBLE))
    FROM {CURATED_TABLE}
    WHERE type='reserva_creada'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, current_date)
      AND precio IS NOT NULL AND CAST(precio AS DOUBLE) > 0
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
    SELECT eventid, type, ts, receivedat, precio, userid, reservaid, vueloid, validation.status
    FROM {CURATED_TABLE}
    WHERE from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      {type_filter}
    ORDER BY from_iso8601_timestamp(ts) DESC
    LIMIT {limit}
    """
    res = _exec(q)
    return {"events":[{"eventId":r[0],"type":r[1],"timestamp":r[2],"received_at":r[3],"precio":r[4],
                       "userId":r[5],"reservaId":r[6],"vueloId":r[7],"validation_status":r[8]} for r in res],
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

# ---------- NUEVOS KPIs ----------

def _funnel(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH searches AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='busqueda_realizada'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    reserves AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='reserva_creada'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    pays AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='pago_aprobado'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT s.c, r.c, p.c FROM searches s, reserves r, pays p
    """
    r = _exec(q)
    s, rsv, pay = (r[0] if r else (0,0,0))
    conv_sr = round((rsv/s)*100,2) if s>0 else 0.0
    conv_rp = round((pay/rsv)*100,2) if rsv>0 else 0.0
    conv_sp = round((pay/s)*100,2) if s>0 else 0.0
    return {"period_days":days,"searches":s,"reservations":rsv,"payments":pay,
            "conversion":{"search_to_reserve":conv_sr,"reserve_to_pay":conv_rp,"search_to_pay":conv_sp}
           }

def _avg_fare(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    SELECT AVG(CAST(precio AS DOUBLE))
    FROM {CURATED_TABLE}
    WHERE type='reserva_creada'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      AND precio IS NOT NULL
    """
    r = _exec(q)
    return {"period_days":days,"avg_fare": (r[0][0] if r else None)}

def _revenue_monthly(qs):
    months = _days(qs, 'months', 6)
    q = f"""
    SELECT date_format(from_iso8601_timestamp(ts), '%Y-%m') ym,
           SUM(CAST(amount AS DOUBLE)) revenue, COUNT(*) payments
    FROM {CURATED_TABLE}
    WHERE type='pago_aprobado'
      AND from_iso8601_timestamp(ts) >= date_add('month', -{months}, current_date)
    GROUP BY 1 ORDER BY 1 DESC
    """
    r = _exec(q)
    return {"months": months, "monthly": [{"ym":row[0],"revenue":row[1] or 0,"payments":row[2] or 0} for row in r]}

def _ltv(qs):
    top = _top(qs, 'top', 10)
    q = f"""
    SELECT userid, SUM(CAST(amount AS DOUBLE)) total_spend, COUNT(*) payments
    FROM {CURATED_TABLE}
    WHERE type='pago_aprobado' AND userid IS NOT NULL
    GROUP BY userid
    ORDER BY total_spend DESC
    LIMIT {top}
    """
    r = _exec(q)
    return {"top": top, "ltv":[{"userId":row[0],"total_spend":row[1] or 0,"payments":row[2] or 0} for row in r]}

def _revenue_per_user(qs):
    days = _days(qs, 'days', 7)
    top = _top(qs, 'top', 10)
    q = f"""
    SELECT userid, SUM(CAST(amount AS DOUBLE)) revenue, COUNT(*) payments
    FROM {CURATED_TABLE}
    WHERE type='pago_aprobado'
      AND userid IS NOT NULL
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY userid
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
    SELECT airlineCode, COUNT(*) cnt, ROUND(AVG(CAST(precio AS DOUBLE)),2) avg_price
    FROM {CURATED_TABLE}
    WHERE type='reserva_creada'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      AND airlineCode IS NOT NULL
    GROUP BY airlineCode
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
    SELECT pais, COUNT(*) cnt
    FROM {CURATED_TABLE}
    WHERE type='usuario_registrado'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      AND pais IS NOT NULL
    GROUP BY pais
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
    WHERE type='reserva_creada'
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    GROUP BY 1 ORDER BY 1
    """
    r = _exec(q)
    return {"period_days":days,"histogram":[{"hour_utc":row[0],"count":row[1]} for row in r]}

def _payment_success(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH ok AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='pago_aprobado' AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    bad AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='pago_rechazado' AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT ok.c, bad.c FROM ok, bad
    """
    r = _exec(q)
    ok, bad = (r[0] if r else (0,0))
    total = ok + bad
    rate = round((ok/total)*100, 2) if total>0 else 0.0
    return {"period_days":days,"approved":ok,"rejected":bad,"success_rate_percent":rate}

def _cancellation_rate(qs):
    days = _days(qs, 'days', 7)
    q = f"""
    WITH created AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='reserva_creada'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    ),
    canceled AS (
      SELECT COUNT(*) c FROM {CURATED_TABLE}
      WHERE type='reserva_cancelada'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    )
    SELECT created.c, canceled.c FROM created, canceled
    """
    r = _exec(q)
    created, canceled = (r[0] if r else (0,0))
    rate = round((canceled/created)*100, 2) if created>0 else 0.0
    return {"period_days":days,"created":created,"canceled":canceled,"cancellation_rate_percent":rate}

def _anticipation(qs):
    # días entre reserva.ts y flightDate (YYYY-MM-DD)
    days = _days(qs, 'days', 90)
    q = f"""
    SELECT AVG(date_diff('day',
               from_iso8601_timestamp(ts),
               from_iso8601_timestamp(concat(flightDate,'T00:00:00Z'))))
    FROM {CURATED_TABLE}
    WHERE type='reserva_creada'
      AND flightDate IS NOT NULL
      AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
    """
    r = _exec(q)
    return {"period_days":days,"avg_anticipation_days": (r[0][0] if r else None)}

def _time_to_complete(qs):
    # tiempo (min) entre búsqueda → reserva → pago (cuando hay linkage por searchId/reservaId)
    days = _days(qs, 'days', 7)
    q = f"""
    WITH reserve AS (
      SELECT reservaid, searchid, MIN(from_iso8601_timestamp(ts)) r_ts
      FROM {CURATED_TABLE}
      WHERE type='reserva_creada'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      GROUP BY reservaid, searchid
    ),
    search AS (
      SELECT searchid, MIN(from_iso8601_timestamp(ts)) s_ts
      FROM {CURATED_TABLE}
      WHERE type='busqueda_realizada'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      GROUP BY searchid
    ),
    pay AS (
      SELECT reservaid, MIN(from_iso8601_timestamp(ts)) p_ts
      FROM {CURATED_TABLE}
      WHERE type='pago_aprobado'
        AND from_iso8601_timestamp(ts) >= date_add('day', -{days}, now())
      GROUP BY reservaid
    )
    SELECT
      AVG(date_diff('minute', s.s_ts, r.r_ts)) as search_to_reserve_min,
      AVG(date_diff('minute', r.r_ts, p.p_ts)) as reserve_to_pay_min,
      AVG(date_diff('minute', s.s_ts, p.p_ts)) as search_to_pay_min
    FROM reserve r
    LEFT JOIN search s ON r.searchid = s.searchid
    LEFT JOIN pay p ON r.reservaid = p.reservaid
    """
    r = _exec(q)
    if r:
        return {
            "period_days":days,
            "avg_minutes": {
                "search_to_reserve": r[0][0],
                "reserve_to_pay": r[0][1],
                "search_to_pay": r[0][2]
            }
        }
    return {"period_days":days,"avg_minutes": {}}
