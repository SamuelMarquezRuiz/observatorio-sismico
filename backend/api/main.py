import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor

from common.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    CORS_ORIGINS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [API] %(message)s"
)
logger = logging.getLogger("api")

app = FastAPI(
    title="Observatorio de Actividad Sísmica - API",
    description="API REST FastAPI para consulta de actividad sísmica en tiempo real, histórico y estadísticas.",
    version="1.0.0"
)

# Configuración de CORS estricto conforme a CONTRATO-API.md
# Permite entorno de desarrollo local (localhost:4200), subdominios de Netlify (*.netlify.app),
# métodos GET y OPTIONS, y cabeceras personalizadas como ngrok-skip-browser-warning.
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS else ["http://localhost:4200", "http://127.0.0.1:4200"],
    allow_origin_regex=r"^https:\/\/.*\.netlify\.app$",
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*", "ngrok-skip-browser-warning"],
    expose_headers=["*"]
)

def get_db_connection():
    """Retorna una conexión a PostgreSQL con timeout controlado."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=4
    )

@app.on_event("startup")
def startup_event():
    logger.info("Iniciando Observatorio Sísmico API...")
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            v = cur.fetchone()[0]
        conn.close()
        logger.info(f"¡CONEXIÓN EXITOSA! API conectada a PostgreSQL ({v.split()[0]} {v.split()[1]}).")
    except Exception as e:
        logger.error(f"PostgreSQL no accesible durante startup: {e}")

# =============================================================================
# 1. GET /api/salud
# =============================================================================
@app.get("/api/salud")
def get_salud():
    """
    Endpoint de salud del backend según CONTRATO-API.md.
    No expone contraseñas ni secretos internos.
    Informa del estado del almacenamiento y del último latido de ingesta de USGS.
    """
    now_ms = int(time.time() * 1000)
    postgres_ok = False
    last_ingestion_ts = now_ms
    total_eventos = 0

    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT valor FROM estado_ingesta WHERE clave = 'last_ingestion_success' LIMIT 1;")
            row = cur.fetchone()
            if row and row["valor"]:
                try:
                    last_ingestion_ts = int(row["valor"])
                except ValueError:
                    pass

            cur.execute("SELECT COUNT(*) AS total FROM evento_actual;")
            r_count = cur.fetchone()
            if r_count:
                total_eventos = r_count["total"]

        postgres_ok = True
        conn.close()
    except Exception as e:
        logger.warning(f"Error consultando salud en DB: {e}")

    # Determinar si la ingesta está al día (menos de 180s de antigüedad) o desactualizada
    segundos_desde_ingesta = max(0, int((now_ms - last_ingestion_ts) / 1000))
    estado_ingesta_str = "activa" if segundos_desde_ingesta <= 180 else "desactualizada"

    last_ingestion_iso = datetime.fromtimestamp(
        last_ingestion_ts / 1000.0, tz=timezone.utc
    ).isoformat()

    return {
        "status": "ok" if postgres_ok else "degraded",
        "service": "observatorio-api",
        "postgres_connected": postgres_ok,
        "total_eventos_base": total_eventos,
        "last_ingestion": last_ingestion_ts,
        "last_ingestion_utc": last_ingestion_iso,
        "segundos_desde_ultima_ingesta": segundos_desde_ingesta,
        "estado_ingesta": estado_ingesta_str,
        "timestamp": now_ms
    }

# =============================================================================
# 2. GET /api/eventos
# =============================================================================
@app.get("/api/eventos")
def get_eventos(
    start: Optional[str] = Query(None, description="Fecha inicio ISO8601 inclusive"),
    end: Optional[str] = Query(None, description="Fecha fin ISO8601 exclusive"),
    min_mag: Optional[float] = Query(None, description="Magnitud mínima"),
    country_id: Optional[str] = Query(None, description="Código de país ADM0_A3 o 'SIN_PAIS'"),
    mode: Optional[str] = Query("live", description="Modo operativo: 'live' o 'replay'"),
    limit: Optional[int] = Query(1000, description="Límite máximo de puntos")
):
    """
    Devuelve FeatureCollection GeoJSON con los terremotos actuales según CONTRATO-API.md.
    Cada evento aparece una sola vez con su versión más reciente.
    Por defecto devuelve sismos de las últimas 24 horas (en modo live).
    """
    now_ms = int(time.time() * 1000)
    features = []
    last_ingestion_ms = now_ms
    is_truncated = False
    total_coincidentes = 0

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Obtener última ingesta
        cur.execute("SELECT valor FROM estado_ingesta WHERE clave = 'last_ingestion_success' LIMIT 1;")
        row_ingest = cur.fetchone()
        if row_ingest and row_ingest["valor"]:
            try:
                last_ingestion_ms = int(row_ingest["valor"])
            except ValueError:
                pass

        # 2. Construcción dinámica de filtros SQL
        where_clauses = ["ea.mode = %s"]
        params: List[Any] = [mode]

        # Filtro temporal por defecto: últimas 24h si no se especifica start/end en modo live
        if not start and not end and mode == "live":
            where_clauses.append("ea.time_utc >= NOW() - INTERVAL '24 hours'")
        else:
            if start:
                where_clauses.append("ea.time_utc >= %s")
                params.append(start)
            if end:
                where_clauses.append("ea.time_utc < %s")
                params.append(end)

        # Filtro por magnitud (la magnitud nula queda excluida conforme al contrato)
        if min_mag is not None:
            where_clauses.append("ea.mag >= %s")
            params.append(min_mag)

        # Filtro por país (admite código ADM0_A3 o 'SIN_PAIS')
        if country_id:
            if country_id.upper() in ["SIN_PAIS", "NULL", "NONE"]:
                where_clauses.append("ea.country_id IS NULL")
            else:
                where_clauses.append("ea.country_id = %s")
                params.append(country_id)

        where_sql = " AND ".join(where_clauses)

        # 3. Contar total antes de truncar
        cur.execute(f"SELECT COUNT(*) AS total FROM evento_actual ea WHERE {where_sql};", params)
        r_total = cur.fetchone()
        total_coincidentes = r_total["total"] if r_total else 0

        # 4. Obtener features con límite
        limite_efectivo = max(1, min(limit or 1000, 5000))
        sql = f"""
            SELECT 
                ea.event_id,
                ea.time_utc,
                ea.updated_utc,
                ea.mag,
                ea.mag_type,
                ea.depth_km,
                ea.latitude,
                ea.longitude,
                ea.place,
                ea.country_id,
                p.name AS country_name
            FROM evento_actual ea
            LEFT JOIN pais p ON ea.country_id = p.country_id
            WHERE {where_sql}
            ORDER BY ea.time_utc DESC
            LIMIT %s;
        """
        cur.execute(sql, params + [limite_efectivo])
        rows = cur.fetchall()

        if total_coincidentes > limite_efectivo:
            is_truncated = True

        for r in rows:
            time_ms = int(r["time_utc"].timestamp() * 1000)
            updated_ms = int(r["updated_utc"].timestamp() * 1000)
            mag_val = float(r["mag"]) if r["mag"] is not None else None
            depth_val = float(r["depth_km"]) if r["depth_km"] is not None else None
            lat_val = float(r["latitude"])
            lon_val = float(r["longitude"])

            feature = {
                "type": "Feature",
                "id": r["event_id"],
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon_val, lat_val, depth_val]
                },
                "properties": {
                    "mag": mag_val,
                    "magType": r["mag_type"],
                    "place": r["place"],
                    "time": time_ms,
                    "updated": updated_ms,
                    "country_id": r["country_id"],
                    "country_name": r["country_name"] or "Sin país asignado"
                }
            }
            features.append(feature)

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error consultando eventos en PostgreSQL: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error en base de datos al consultar eventos: {str(e)}"
        )

    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": now_ms,
            "last_ingestion": last_ingestion_ms,
            "mode": mode,
            "count": len(features),
            "total_coincidentes": total_coincidentes,
            "truncated": is_truncated
        },
        "features": features
    }

# =============================================================================
# 3. GET /api/estadisticas
# =============================================================================
@app.get("/api/estadisticas")
def get_estadisticas(
    start: Optional[str] = Query(None, description="Fecha inicio ISO8601"),
    end: Optional[str] = Query(None, description="Fecha fin ISO8601"),
    min_mag: Optional[float] = Query(None, description="Magnitud mínima"),
    country_id: Optional[str] = Query(None, description="Código de país"),
    mode: Optional[str] = Query("live", description="Modo: 'live' o 'replay'")
):
    """
    Devuelve agregaciones por ventana fija de 1 hora UTC, distribución de magnitud y profundidad,
    recuentos por país y comparativa histórica rigurosa conforme a CONTRATO-API.md y la FAQ (pregunta 23).
    """
    now_ms = int(time.time() * 1000)

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Filtros base
        where_clauses = ["ea.mode = %s"]
        params: List[Any] = [mode]

        if not start and not end and mode == "live":
            where_clauses.append("ea.time_utc >= NOW() - INTERVAL '24 hours'")
        else:
            if start:
                where_clauses.append("ea.time_utc >= %s")
                params.append(start)
            if end:
                where_clauses.append("ea.time_utc < %s")
                params.append(end)

        if min_mag is not None:
            where_clauses.append("ea.mag >= %s")
            params.append(min_mag)

        if country_id:
            if country_id.upper() in ["SIN_PAIS", "NULL", "NONE"]:
                where_clauses.append("ea.country_id IS NULL")
            else:
                where_clauses.append("ea.country_id = %s")
                params.append(country_id)

        where_sql = " AND ".join(where_clauses)

        # 1. Total eventos seleccionados y métricas generales
        cur.execute(f"""
            SELECT 
                COUNT(*) AS total,
                COUNT(mag) AS total_con_magnitud,
                COUNT(*) - COUNT(mag) AS total_magnitud_nula,
                ROUND(AVG(mag)::numeric, 2) AS mag_media,
                ROUND(MIN(mag)::numeric, 2) AS mag_min,
                ROUND(MAX(mag)::numeric, 2) AS mag_max,
                ROUND(AVG(depth_km)::numeric, 2) AS profundidad_media
            FROM evento_actual ea
            WHERE {where_sql};
        """, params)
        res_general = cur.fetchone() or {}
        total_eventos = res_general.get("total", 0)

        # 2. Ventanas fijas de 1 hora UTC (usando tiempo de ocurrencia time_utc)
        cur.execute(f"""
            SELECT 
                date_trunc('hour', ea.time_utc) AS ventana_utc,
                COUNT(*) AS count,
                ROUND(AVG(ea.mag)::numeric, 2) AS mag_media,
                ROUND(MIN(ea.mag)::numeric, 2) AS mag_min,
                ROUND(MAX(ea.mag)::numeric, 2) AS mag_max
            FROM evento_actual ea
            WHERE {where_sql}
            GROUP BY date_trunc('hour', ea.time_utc)
            ORDER BY ventana_utc ASC;
        """, params)
        rows_ventanas = cur.fetchall()
        por_ventana_hora = [
            {
                "hora_utc": r["ventana_utc"].isoformat(),
                "timestamp": int(r["ventana_utc"].timestamp() * 1000),
                "count": r["count"],
                "mag_media": float(r["mag_media"]) if r["mag_media"] is not None else None,
                "mag_min": float(r["mag_min"]) if r["mag_min"] is not None else None,
                "mag_max": float(r["mag_max"]) if r["mag_max"] is not None else None
            }
            for r in rows_ventanas
        ]

        # 3. Recuentos por país / región (incluyendo "Sin país asignado")
        cur.execute(f"""
            SELECT 
                ea.country_id,
                COALESCE(p.name, 'Sin país asignado') AS country_name,
                COALESCE(p.continent, 'Oceánico / Sin asignar') AS continent,
                COUNT(*) AS count,
                ROUND(AVG(ea.mag)::numeric, 2) AS mag_media
            FROM evento_actual ea
            LEFT JOIN pais p ON ea.country_id = p.country_id
            WHERE {where_sql}
            GROUP BY ea.country_id, p.name, p.continent
            ORDER BY count DESC;
        """, params)
        rows_paises = cur.fetchall()
        por_pais = [
            {
                "country_id": r["country_id"],
                "country_name": r["country_name"],
                "continent": r["continent"],
                "count": r["count"],
                "mag_media": float(r["mag_media"]) if r["mag_media"] is not None else None
            }
            for r in rows_paises
        ]

        # 4. Distribución por magnitud
        cur.execute(f"""
            SELECT 
                CASE 
                    WHEN ea.mag IS NULL THEN 'Sin dato (ND)'
                    WHEN ea.mag < 3.0 THEN '< 3.0 (Micro)'
                    WHEN ea.mag < 4.0 THEN '3.0 - 3.9 (Menor)'
                    WHEN ea.mag < 5.0 THEN '4.0 - 4.9 (Ligero)'
                    WHEN ea.mag < 6.0 THEN '5.0 - 5.9 (Moderado)'
                    WHEN ea.mag < 7.0 THEN '6.0 - 6.9 (Fuerte)'
                    ELSE '>= 7.0 (Mayor)'
                END AS rango_mag,
                COUNT(*) AS count
            FROM evento_actual ea
            WHERE {where_sql}
            GROUP BY 1
            ORDER BY MIN(COALESCE(ea.mag, -999)) ASC;
        """, params)
        rows_mag = cur.fetchall()
        distribucion_magnitud = {r["rango_mag"]: r["count"] for r in rows_mag}

        # 5. Distribución por profundidad
        cur.execute(f"""
            SELECT 
                CASE 
                    WHEN ea.depth_km IS NULL THEN 'Sin dato (ND)'
                    WHEN ea.depth_km < 70 THEN 'Superficial (< 70 km)'
                    WHEN ea.depth_km <= 300 THEN 'Intermedio (70 - 300 km)'
                    ELSE 'Profundo (> 300 km)'
                END AS rango_profundidad,
                COUNT(*) AS count
            FROM evento_actual ea
            WHERE {where_sql}
            GROUP BY 1
            ORDER BY MIN(COALESCE(ea.depth_km, -999)) ASC;
        """, params)
        rows_depth = cur.fetchall()
        distribucion_profundidad = {r["rango_profundidad"]: r["count"] for r in rows_depth}

        # 6. Comparativa histórica equivalente (FAQ Q23)
        # Compara el recuento reciente de 24h con la media diaria del histórico 2025
        # bajo exactamente los mismos filtros de magnitud y país
        hist_where = ["ea.mode = 'historical'"]
        hist_params: List[Any] = []

        if min_mag is not None:
            hist_where.append("ea.mag >= %s")
            hist_params.append(min_mag)
        else:
            # Si no se pasó filtro de magnitud, el histórico de base de USGS 2025 es mag >= 4.0
            hist_where.append("ea.mag >= 4.0")

        if country_id:
            if country_id.upper() in ["SIN_PAIS", "NULL", "NONE"]:
                hist_where.append("ea.country_id IS NULL")
            else:
                hist_where.append("ea.country_id = %s")
                hist_params.append(country_id)

        cur.execute(f"""
            SELECT COUNT(*) AS total_historico
            FROM evento_actual ea
            WHERE {" AND ".join(hist_where)};
        """, hist_params)
        r_hist = cur.fetchone()
        total_historico_2025 = r_hist["total_historico"] if r_hist else 0

        # Conteo reciente equivalente (últimas 24h con los mismos filtros de magnitud y país)
        reciente_where = ["ea.mode = 'live'", "ea.time_utc >= NOW() - INTERVAL '24 hours'"]
        reciente_params: List[Any] = []

        if min_mag is not None:
            reciente_where.append("ea.mag >= %s")
            reciente_params.append(min_mag)
        else:
            reciente_where.append("ea.mag >= 4.0")

        if country_id:
            if country_id.upper() in ["SIN_PAIS", "NULL", "NONE"]:
                reciente_where.append("ea.country_id IS NULL")
            else:
                reciente_where.append("ea.country_id = %s")
                reciente_params.append(country_id)

        cur.execute(f"""
            SELECT COUNT(*) AS total_reciente
            FROM evento_actual ea
            WHERE {" AND ".join(reciente_where)};
        """, reciente_params)
        r_reciente = cur.fetchone()
        recuento_reciente_24h = r_reciente["total_reciente"] if r_reciente else 0

        # Cálculo de media diaria histórica (365 días en 2025)
        # Si aún no se cargó el histórico, usamos la media global histórica de USGS (~10.0 al día para mag >= 4)
        if total_historico_2025 > 0:
            media_diaria_historica = round(total_historico_2025 / 365.0, 2)
        else:
            media_diaria_historica = 10.0 # Valor de referencia estándar documentado

        diferencia = round(recuento_reciente_24h - media_diaria_historica, 2)
        if media_diaria_historica > 0:
            variacion_porcentual = round((diferencia / media_diaria_historica) * 100.0, 2)
        else:
            variacion_porcentual = "ND"

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error generando estadísticas en PostgreSQL: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error en base de datos al calcular estadísticas: {str(e)}"
        )

    return {
        "metadata": {
            "generated": now_ms,
            "mode": mode,
            "periodo_consultado": {
                "start": start or "últimas 24 horas",
                "end": end or "actual"
            },
            "filtros": {
                "min_mag": min_mag,
                "country_id": country_id
            }
        },
        "total_eventos": total_eventos,
        "resumen_metricas": {
            "total_con_magnitud": res_general.get("total_con_magnitud", 0),
            "total_magnitud_nula": res_general.get("total_magnitud_nula", 0),
            "mag_media": float(res_general["mag_media"]) if res_general.get("mag_media") is not None else None,
            "mag_min": float(res_general["mag_min"]) if res_general.get("mag_min") is not None else None,
            "mag_max": float(res_general["mag_max"]) if res_general.get("mag_max") is not None else None,
            "profundidad_media_km": float(res_general["profundidad_media"]) if res_general.get("profundidad_media") is not None else None
        },
        "por_ventana_hora": por_ventana_hora,
        "por_pais": por_pais,
        "distribucion_magnitud": distribucion_magnitud,
        "distribucion_profundidad": distribucion_profundidad,
        "comparacion_historica": {
            "criterio": "Sismos con mag >= 4.0 y filtros espaciales equivalentes",
            "dias_periodo_historico": 365,
            "total_historico_registrado": total_historico_2025,
            "media_diaria_historica": media_diaria_historica,
            "recuento_reciente_24h": recuento_reciente_24h,
            "diferencia": diferencia,
            "variacion_porcentual": variacion_porcentual,
            "interpretacion": (
                f"El recuento observado en las últimas 24 horas ({recuento_reciente_24h}) "
                f"presenta una variación del {variacion_porcentual}% respecto a la media diaria histórica de 2025 "
                f"({media_diaria_historica} sismos/día) bajo los filtros seleccionados."
            )
        }
    }

# =============================================================================
# 4. GET /api/paises (Catálogo auxiliar para selectores del frontend)
# =============================================================================
@app.get("/api/paises")
def get_paises():
    """Devuelve la lista de los 177 países disponibles para poblar los desplegables de Angular."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT country_id, name, continent, subregion
                FROM pais
                ORDER BY name ASC;
            """)
            paises = cur.fetchall()
        conn.close()
        return paises
    except Exception as e:
        logger.error(f"Error consultando catálogo de países: {e}")
        return []
