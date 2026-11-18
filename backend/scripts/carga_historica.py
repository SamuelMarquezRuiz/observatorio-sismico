"""
Script de Carga Histórica: Catálogo USGS 2025 (Magnitud >= 4, tipo earthquake)
Descarga el año 2025 completo dividido por meses para respetar los límites de la API de USGS,
guarda los archivos GeoJSON para reproducibilidad y los inserta en PostgreSQL aplicando
las mismas reglas de calidad, enriquecimiento espacial y deduplicación.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Dict, Any

import httpx
import psycopg2
from psycopg2.extras import Json

# Asegurar importación de módulos comunes del backend
current_dir = Path(__file__).resolve().parent
backend_dir = current_dir.parent
sys.path.insert(0, str(backend_dir))

from common.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)
from common.geo import get_catalogo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [HISTORICO] %(message)s"
)
logger = logging.getLogger("carga_historica")

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}
DIAS_ES = {
    1: "Lunes", 2: "Martes", 3: "Miércoles", 4: "Jueves", 5: "Viernes", 6: "Sábado", 7: "Domingo"
}

def generar_intervalos_mensuales(anio: int = 2025) -> List[Tuple[str, str, str]]:
    """Genera los 12 intervalos [inicio, fin) para el año especificado."""
    intervalos = []
    for mes in range(1, 13):
        inicio = f"{anio}-{mes:02d}-01T00:00:00Z"
        if mes == 12:
            fin = f"{anio + 1}-01-01T00:00:00Z"
        else:
            fin = f"{anio}-{mes + 1:02d}-01T00:00:00Z"
        nombre_archivo = f"{anio}_mes_{mes:02d}.geojson"
        intervalos.append((inicio, fin, nombre_archivo))
    return intervalos

def descargar_mes(client: httpx.Client, inicio: str, fin: str, destino: Path, min_mag: float = 4.0) -> Dict[str, Any]:
    """Descarga un mes de la API de USGS con reintentos y control de límites."""
    if destino.exists() and destino.stat().st_size > 1000:
        logger.info(f"Reutilizando archivo local existente: {destino.name}")
        with open(destino, "r", encoding="utf-8") as f:
            return json.load(f)

    url = (
        f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
        f"format=geojson&starttime={inicio}&endtime={fin}&minmagnitude={min_mag}&eventtype=earthquake&orderby=time-asc"
    )
    logger.info(f"Descargando de USGS: {inicio} a {fin} (mag >= {min_mag})...")

    retries = 3
    for attempt in range(1, retries + 1):
        try:
            resp = client.get(url, timeout=45.0)
            if resp.status_code == 200:
                data = resp.json()
                total = len(data.get("features", []))
                if total >= 20000:
                    logger.warning(f"¡ATENCIÓN! La consulta ha alcanzado el límite máximo de 20.000 resultados en {inicio}")

                destino.parent.mkdir(parents=True, exist_ok=True)
                with open(destino, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                logger.info(f"Guardado exitosamente: {destino.name} ({total} terremotos)")
                return data
            else:
                logger.warning(f"HTTP {resp.status_code} en intento {attempt}: {resp.text[:150]}")
                time.sleep(3)
        except Exception as e:
            logger.warning(f"Error de conexión en intento {attempt}: {e}")
            time.sleep(3)

    raise RuntimeError(f"No se pudo descargar el intervalo {inicio} a {fin} tras {retries} intentos.")

def insertar_eventos_historicos(conn, features: List[Dict[str, Any]], catalogo) -> Tuple[int, int, int]:
    """
    Inserta una lista de features en PostgreSQL aplicando:
    - Validación y cuarentena.
    - Dimensión fecha.
    - Asignación espacial con STRtree.
    - Idempotencia en evento_version y evento_actual con mode='historical'.
    """
    insertados = 0
    actualizados = 0
    descartados = 0

    with conn.cursor() as cur:
        for feat in features:
            event_id = feat.get("id")
            props = feat.get("properties") or {}
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or []

            # 1. Validaciones
            if not event_id or props.get("type") != "earthquake":
                descartados += 1
                continue

            if not isinstance(coords, (list, tuple)) or len(coords) < 2:
                descartados += 1
                continue

            try:
                lon = float(coords[0])
                lat = float(coords[1])
                depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None
            except (ValueError, TypeError):
                descartados += 1
                continue

            if not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
                descartados += 1
                continue

            time_ms = props.get("time")
            updated_ms = props.get("updated")
            if time_ms is None or updated_ms is None:
                descartados += 1
                continue

            try:
                time_utc = datetime.fromtimestamp(float(time_ms) / 1000.0, tz=timezone.utc)
                updated_utc = datetime.fromtimestamp(float(updated_ms) / 1000.0, tz=timezone.utc)
                fecha_id = time_utc.date()
            except Exception:
                descartados += 1
                continue

            # 2. Integración espacial
            try:
                country_id, _ = catalogo.asignar_pais(lon, lat)
            except Exception:
                country_id = None

            raw_mag = props.get("mag")
            mag = float(raw_mag) if raw_mag is not None else None
            mag_type = props.get("magType")
            place = props.get("place")

            # 3. Dimensión fecha
            cur.execute("""
                INSERT INTO fecha (fecha_id, anio, mes, dia, dia_semana, nombre_mes, nombre_dia)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fecha_id) DO NOTHING;
            """, (
                fecha_id, fecha_id.year, fecha_id.month, fecha_id.day,
                fecha_id.isoweekday(), MESES_ES.get(fecha_id.month, ""), DIAS_ES.get(fecha_id.isoweekday(), "")
            ))

            # 4. Versión histórica inmutable
            cur.execute("""
                INSERT INTO evento_version (
                    event_id, updated_utc, received_at, mag, mag_type, depth_km,
                    latitude, longitude, mode, raw_payload
                )
                VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, 'historical', %s)
                ON CONFLICT (event_id, updated_utc) DO NOTHING;
            """, (
                event_id, updated_utc, mag, mag_type, depth, lat, lon, Json(feat)
            ))

            # 5. Estado actual con mode='historical'
            cur.execute("""
                INSERT INTO evento_actual (
                    event_id, time_utc, updated_utc, mag, mag_type, depth_km,
                    latitude, longitude, place, country_id, fecha_id, mode,
                    raw_properties, last_modified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'historical', %s, NOW())
                ON CONFLICT (event_id) DO UPDATE
                SET
                    time_utc = EXCLUDED.time_utc,
                    updated_utc = EXCLUDED.updated_utc,
                    mag = EXCLUDED.mag,
                    mag_type = EXCLUDED.mag_type,
                    depth_km = EXCLUDED.depth_km,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    place = EXCLUDED.place,
                    country_id = EXCLUDED.country_id,
                    fecha_id = EXCLUDED.fecha_id,
                    raw_properties = EXCLUDED.raw_properties,
                    last_modified_at = NOW()
                WHERE EXCLUDED.updated_utc > evento_actual.updated_utc;
            """, (
                event_id, time_utc, updated_utc, mag, mag_type, depth,
                lat, lon, place, country_id, fecha_id, Json(props)
            ))
            insertados += 1

    conn.commit()
    return insertados, actualizados, descartados

def main():
    parser = argparse.ArgumentParser(description="Carga histórica de terremotos USGS 2025.")
    parser.add_argument("--limit-months", type=int, default=12, help="Límite de meses a procesar (1 a 12). Default: 12")
    parser.add_argument("--min-mag", type=float, default=4.0, help="Magnitud mínima. Default: 4.0")
    parser.add_argument("--download-only", action="store_true", help="Solo descargar GeoJSON sin insertar en BD.")
    args = parser.parse_args()

    # Directorio de almacenamiento de datos históricos
    data_dir = Path("/app/datos/historico")
    if not data_dir.exists():
        data_dir = current_dir.parent.parent / "datos" / "historico"
    data_dir.mkdir(parents=True, exist_ok=True)

    intervalos = generar_intervalos_mensuales(2025)[:args.limit_months]
    catalogo = get_catalogo()

    conn = None
    if not args.download_only:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=10
            )
            logger.info("Conexión con PostgreSQL establecida para carga histórica.")
        except Exception as e:
            logger.error(f"No se pudo conectar a PostgreSQL: {e}")
            sys.exit(1)

    registro_extraccion = {
        "fecha_extraccion_utc": datetime.now(timezone.utc).isoformat(),
        "fuente": "USGS Earthquake Catalog FDSNWS API",
        "min_mag": args.min_mag,
        "anio": 2025,
        "meses": []
    }

    total_descargados = 0
    total_insertados = 0

    headers = {"User-Agent": "ObservatorioSismico/1.0 (IABD Big Data Historical Ingestion)"}
    with httpx.Client(headers=headers) as client:
        for inicio, fin, nombre_archivo in intervalos:
            archivo_path = data_dir / nombre_archivo
            data = descargar_mes(client, inicio, fin, archivo_path, min_mag=args.min_mag)
            features = data.get("features", [])
            count = len(features)
            total_descargados += count

            ins = 0
            if conn:
                logger.info(f"Insertando {count} eventos de {nombre_archivo} en base de datos...")
                ins, _, _ = insertar_eventos_historicos(conn, features, catalogo)
                total_insertados += ins

            registro_extraccion["meses"].append({
                "periodo": f"{inicio} - {fin}",
                "archivo": nombre_archivo,
                "registros": count,
                "insertados": ins
            })

    # Guardar metadatos de extracción para la memoria
    meta_path = data_dir / "registro_extraccion_2025.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(registro_extraccion, f, indent=2, ensure_ascii=False)

    if conn:
        conn.close()

    logger.info("=== CARGA HISTÓRICA COMPLETADA ===")
    logger.info(f"Total registros descargados: {total_descargados}")
    logger.info(f"Total registros procesados/insertados: {total_insertados}")
    logger.info(f"Metadatos de procedencia guardados en: {meta_path}")

if __name__ == "__main__":
    main()
