"""
Script de Carga Histórica: Catálogo USGS 2025 (Magnitud >= 4, tipo earthquake)
Descarga el año 2025 completo dividido por meses para respetar los límites de la API de USGS,
guarda los archivos GeoJSON para reproducibilidad y los inserta en PostgreSQL aplicando
las mismas reglas de calidad, enriquecimiento espacial y deduplicación.
"""

import argparse
import json
import logging
import math
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

USGS_LIMITE = 20000


def _get_json(client: httpx.Client, url: str, retries: int = 4) -> Dict[str, Any]:
    """GET con espera progresiva ante timeout, HTTP 429/5xx o JSON inválido."""
    espera = 2.0
    for intento in range(1, retries + 1):
        try:
            resp = client.get(url, timeout=60.0)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429 or resp.status_code >= 500:
                logger.warning(f"HTTP {resp.status_code} (intento {intento}/{retries})")
            else:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:150]}")
        except (httpx.HTTPError, ValueError) as e:
            logger.warning(f"Error de red/JSON (intento {intento}/{retries}): {e}")
        time.sleep(espera)
        espera *= 2
    raise RuntimeError(f"No se pudo completar la consulta tras {retries} intentos: {url}")


def descargar_subdividido(client: httpx.Client, inicio: str, fin: str, min_mag: float) -> List[Dict[str, Any]]:
    """
    Descarga [inicio, fin] y, si USGS devuelve 20.000 resultados (límite máximo), parte el
    intervalo en dos mitades y repite recursivamente. Nunca trunca en silencio.
    Los extremos de USGS pueden ser inclusivos: se deduplica por id al unir las mitades.
    """
    url = (
        "https://earthquake.usgs.gov/fdsnws/event/1/query?"
        f"format=geojson&starttime={inicio}&endtime={fin}&minmagnitude={min_mag}&eventtype=earthquake&orderby=time-asc"
    )
    data = _get_json(client, url)
    feats = data.get("features", [])
    if len(feats) < USGS_LIMITE:
        return feats

    t0 = datetime.strptime(inicio, "%Y-%m-%dT%H:%M:%SZ")
    t1 = datetime.strptime(fin, "%Y-%m-%dT%H:%M:%SZ")
    mitad = t0 + (t1 - t0) / 2
    mid = mitad.strftime("%Y-%m-%dT%H:%M:%SZ")
    if mid == inicio or mid == fin:
        raise RuntimeError(f"Intervalo indivisible con {len(feats)} resultados: {inicio} - {fin}")
    logger.warning(f"Límite de {USGS_LIMITE} alcanzado en {inicio} - {fin}: subdividiendo en {mid}")
    time.sleep(1.0)
    unidos = {f["id"]: f for f in descargar_subdividido(client, inicio, mid, min_mag)}
    time.sleep(1.0)
    for f in descargar_subdividido(client, mid, fin, min_mag):
        unidos.setdefault(f["id"], f)
    return sorted(unidos.values(), key=lambda f: f.get("properties", {}).get("time", 0))


def descargar_mes(client: httpx.Client, inicio: str, fin: str, destino: Path, min_mag: float = 4.0) -> Tuple[Dict[str, Any], str]:
    """Descarga un mes de la API de USGS con reintentos y control de límites."""
    url = (
        f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
        f"format=geojson&starttime={inicio}&endtime={fin}&minmagnitude={min_mag}&eventtype=earthquake&orderby=time-asc"
    )

    if destino.exists() and destino.stat().st_size > 1000:
        logger.info(f"Reutilizando archivo local existente: {destino.name}")
        with open(destino, "r", encoding="utf-8") as f:
            return json.load(f), url

    logger.info(f"Descargando de USGS: {inicio} a {fin} (mag >= {min_mag})...")

    retries = 4
    for attempt in range(1, retries + 1):
        try:
            resp = client.get(url, timeout=60.0)
            if resp.status_code == 200:
                data = resp.json()
                total = len(data.get("features", []))
                meta_count = data.get("metadata", {}).get("count", total)
                logger.info(f"Respuesta recibida: {meta_count} terremotos (metadata.count: {meta_count})")
                if meta_count >= USGS_LIMITE:
                    logger.warning(f"Límite de {USGS_LIMITE} alcanzado en {inicio}: se descarga subdividiendo el intervalo")
                    feats_completas = descargar_subdividido(client, inicio, fin, min_mag)
                    data = {"type": "FeatureCollection",
                            "metadata": {**data.get("metadata", {}), "count": len(feats_completas), "subdividido": True},
                            "features": feats_completas}
                    total = len(feats_completas)

                destino.parent.mkdir(parents=True, exist_ok=True)
                with open(destino, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                logger.info(f"Guardado exitosamente: {destino.name} ({total} terremotos)")
                time.sleep(1.0) # Pausa cortés entre peticiones
                return data, url
            elif resp.status_code == 429:
                wait_s = 5 * attempt
                logger.warning(f"HTTP 429 Too Many Requests. Esperando {wait_s}s antes de reintentar...")
                time.sleep(wait_s)
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
        def rechazar(event_id, feat, motivo):
            """Cuarentena: conserva el original y el motivo (mismas reglas que el consumidor)."""
            cur.execute(
                "INSERT INTO cuarentena (received_at, event_id, raw_payload, reason) VALUES (NOW(), %s, %s, %s);",
                (event_id, Json({"source": "usgs-historico", "mode": "historical", "event": feat}), motivo)
            )

        for feat in features:
            event_id = feat.get("id")
            props = feat.get("properties") or {}
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or []

            # 1. Validaciones (idénticas a consumer/main.py) con envío a cuarentena
            if not event_id:
                rechazar(None, feat, "Falta identificador 'id' en evento")
                descartados += 1
                continue

            if props.get("type") != "earthquake":
                rechazar(event_id, feat, f"Tipo no sismico excluido del analisis: '{props.get('type')}'")
                descartados += 1
                continue

            if geom.get("type") != "Point" or not isinstance(coords, (list, tuple)) or len(coords) < 2:
                rechazar(event_id, feat, f"Geometria o coordenadas invalidas: {coords}")
                descartados += 1
                continue

            try:
                lon = float(coords[0])
                lat = float(coords[1])
                depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None
            except (ValueError, TypeError) as e:
                rechazar(event_id, feat, f"Error de conversion numerica en coordenadas: {coords} ({e})")
                descartados += 1
                continue

            if not math.isfinite(lon) or not math.isfinite(lat) or not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
                rechazar(event_id, feat, f"Coordenadas fuera de rango geografico: lon={lon}, lat={lat}")
                descartados += 1
                continue

            time_ms = props.get("time")
            updated_ms = props.get("updated")
            if time_ms is None or updated_ms is None:
                rechazar(event_id, feat, f"Timestamps obligatorios ausentes: time={time_ms}, updated={updated_ms}")
                descartados += 1
                continue

            try:
                time_utc = datetime.fromtimestamp(float(time_ms) / 1000.0, tz=timezone.utc)
                updated_utc = datetime.fromtimestamp(float(updated_ms) / 1000.0, tz=timezone.utc)
                fecha_id = time_utc.date()
            except Exception as e:
                rechazar(event_id, feat, f"Error parseando timestamps: time={time_ms}, updated={updated_ms} ({e})")
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
    parser.add_argument("--archivo", "-f", type=str, default=None, help="Ruta a un archivo GeoJSON específico a procesar (ej. query.json).")
    parser.add_argument("--limit-months", type=int, default=12, help="Límite de meses a procesar (1 a 12). Default: 12")
    parser.add_argument("--min-mag", type=float, default=4.0, help="Magnitud mínima. Default: 4.0")
    parser.add_argument("--download-only", action="store_true", help="Solo descargar GeoJSON sin insertar en BD.")
    args = parser.parse_args()

    # Directorio de almacenamiento de datos históricos
    data_dir = Path("/app/datos/historico")
    if not data_dir.exists():
        data_dir = current_dir.parent.parent / "datos" / "historico"
    data_dir.mkdir(parents=True, exist_ok=True)

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

    # Si se especificó un archivo concreto (o query.json)
    if args.archivo:
        ruta_archivo = Path(args.archivo)
        if not ruta_archivo.is_absolute():
            # Buscar en directorio actual, raíz del proyecto o /app
            posibles = [
                Path.cwd() / ruta_archivo,
                current_dir.parent.parent / ruta_archivo,
                Path("/app") / ruta_archivo,
                data_dir / ruta_archivo
            ]
            for p in posibles:
                if p.exists():
                    ruta_archivo = p
                    break

        if not ruta_archivo.exists():
            logger.error(f"El archivo especificado no existe: {args.archivo}")
            if conn:
                conn.close()
            sys.exit(1)

        logger.info(f"Cargando dataset histórico directo desde: {ruta_archivo}")
        with open(ruta_archivo, "r", encoding="utf-8") as f:
            data = json.load(f)

        features = data.get("features", [])
        meta = data.get("metadata", {})
        total = len(features)
        logger.info(f"Archivo GeoJSON válido. Metadata: {meta.get('title', 'N/A')} | Registros: {total}")

        ins = 0
        if conn:
            logger.info(f"Insertando {total} eventos de {ruta_archivo.name} con validación y cruce espacial...")
            ins, _, _ = insertar_eventos_historicos(conn, features, catalogo)
            logger.info(f"Insertados/actualizados {ins} registros en evento_actual y evento_version.")

        # Registrar en metadatos de extracción
        meta_path = data_dir / "registro_extraccion_2025.json"
        registro_previo = {}
        if meta_path.exists():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    registro_previo = json.load(f)
            except Exception:
                registro_previo = {}

        meses_list = registro_previo.get("meses", [])
        meses_list.append({
            "periodo": meta.get("title", f"Archivo {ruta_archivo.name}"),
            "archivo": ruta_archivo.name,
            "url_origen": meta.get("url", "Carga local"),
            "registros": total,
            "insertados": ins,
            "cargado_en": datetime.now(timezone.utc).isoformat()
        })
        registro_previo["meses"] = meses_list
        registro_previo["ultima_actualizacion_utc"] = datetime.now(timezone.utc).isoformat()

        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(registro_previo, f, indent=2, ensure_ascii=False)

        if conn:
            conn.close()

        logger.info("=== CARGA DE ARCHIVO COMPLETADA ===")
        return

    intervalos = generar_intervalos_mensuales(2025)[:args.limit_months]

    registro_extraccion = {
        "fecha_extraccion_utc": datetime.now(timezone.utc).isoformat(),
        "fuente": "USGS Earthquake Catalog FDSNWS API",
        "criterio": f"Magnitud >= {args.min_mag}, tipo earthquake, orden time-asc",
        "min_mag": args.min_mag,
        "anio": 2025,
        "dias_periodo_anual": 365,
        "meses": []
    }

    total_descargados = 0
    total_insertados = 0

    headers = {"User-Agent": "ObservatorioSismico/1.0 (IABD Big Data Historical Ingestion)"}
    with httpx.Client(headers=headers) as client:
        for inicio, fin, nombre_archivo in intervalos:
            archivo_path = data_dir / nombre_archivo
            data, url = descargar_mes(client, inicio, fin, archivo_path, min_mag=args.min_mag)
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
                "url": url,
                "registros": count,
                "insertados": ins,
                "fecha_descarga_utc": datetime.now(timezone.utc).isoformat()
            })

    total_deduplicados_bd = total_descargados
    if conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM evento_actual WHERE mode = 'historical';")
            row = cur.fetchone()
            if row:
                total_deduplicados_bd = row[0]
        conn.close()

    media_diaria = round(total_deduplicados_bd / 365.0, 2)
    registro_extraccion["total_anual_descargado"] = total_descargados
    registro_extraccion["total_anual_deduplicado_en_bd"] = total_deduplicados_bd
    registro_extraccion["media_diaria_anual_2025"] = media_diaria

    # Guardar metadatos de extracción para la memoria
    meta_path = data_dir / "registro_extraccion_2025.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(registro_extraccion, f, indent=2, ensure_ascii=False)

    logger.info("=== CARGA HISTÓRICA COMPLETADA ===")
    logger.info(f"Total registros descargados (suma meses): {total_descargados}")
    logger.info(f"Total registros únicos deduplicados en PostgreSQL: {total_deduplicados_bd}")
    logger.info(f"Media diaria anual 2025 (dividida entre 365 días): {media_diaria} sismos/día")
    logger.info(f"Metadatos de procedencia guardados en: {meta_path}")

if __name__ == "__main__":
    main()
