import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

import psycopg2
from psycopg2.extras import Json
from aiokafka import AIOKafkaConsumer

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_REPLAY_TOPIC,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)
from common.geo import get_catalogo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CONSUMER] %(message)s"
)
logger = logging.getLogger("consumer")

# Latido para el healthcheck de Docker: el proceso lo toca al arrancar y tras cada mensaje.
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/consumer_heartbeat"))

MESES_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}
DIAS_ES = {
    1: "Lunes", 2: "Martes", 3: "Miércoles", 4: "Jueves", 5: "Viernes", 6: "Sábado", 7: "Domingo"
}

class TerremotoConsumidor:
    def __init__(self):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topics = [KAFKA_TOPIC]
        if KAFKA_REPLAY_TOPIC and KAFKA_REPLAY_TOPIC != KAFKA_TOPIC:
            self.topics.append(KAFKA_REPLAY_TOPIC)

        self.group_id = "observatorio-consumer-group"
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.pg_conn: Optional[psycopg2.extensions.connection] = None
        self.catalogo = get_catalogo()
        self.running = True

    def get_pg_connection(self):
        if self.pg_conn is None or self.pg_conn.closed != 0:
            logger.info(f"Conectando a PostgreSQL en {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}...")
            self.pg_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=5
            )
            self.pg_conn.autocommit = False
        return self.pg_conn

    async def start(self):
        # 1. Esperar a PostgreSQL
        retries = 20
        delay = 3
        for attempt in range(1, retries + 1):
            try:
                conn = self.get_pg_connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                logger.info(f"¡CONECTADO! Conexión con PostgreSQL establecida.")
                break
            except Exception as e:
                logger.warning(f"PostgreSQL no listo (intento {attempt}/{retries}): {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 20)
        else:
            logger.critical("No se pudo conectar a PostgreSQL. Abortando.")
            sys.exit(1)

        # 2. Iniciar AIOKafkaConsumer con commit manual
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Conectando a Kafka {self.bootstrap_servers} topics={self.topics}...")
                self.consumer = AIOKafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    enable_auto_commit=False,  # CRÍTICO: Commit manual tras commit en PostgreSQL
                    auto_offset_reset="earliest",
                    max_poll_records=50
                )
                await self.consumer.start()
                logger.info(f"¡CONECTADO! Consumidor escuchando en topics: {self.topics}.")
                return
            except Exception as e:
                logger.warning(f"Kafka no listo (intento {attempt}/{retries}): {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 20)

        logger.critical("No se pudo conectar a Kafka. Abortando.")
        sys.exit(1)

    def guardar_en_cuarentena(self, conn, event_id: Optional[str], payload: Any, reason: str):
        """
        Almacena el registro rechazado de forma duradera antes de confirmar el offset.
        """
        logger.warning(f"Desviando a cuarentena evento='{event_id}'. Motivo: {reason}")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cuarentena (received_at, event_id, raw_payload, reason)
                VALUES (NOW(), %s, %s, %s);
            """, (event_id, Json(payload), reason))
        conn.commit()

    def procesar_evento_transaccional(self, conn, message_payload: Dict[str, Any]) -> bool:
        """
        Procesa un evento dentro de una transacción PostgreSQL única y atómica.
        Garantiza idempotencia y control estricto de revisiones.
        """
        event = message_payload.get("event") or {}
        event_id = event.get("id")
        mode = message_payload.get("mode", "live")

        if not event_id:
            self.guardar_en_cuarentena(conn, None, message_payload, "Falta identificador 'id' en evento")
            return True

        props = event.get("properties") or {}
        geom = event.get("geometry") or {}
        coords = geom.get("coordinates") or []

        # 1. Validación de calidad: Tipo de evento
        event_type = props.get("type")
        if event_type != "earthquake":
            self.guardar_en_cuarentena(
                conn, event_id, message_payload,
                f"Tipo no sismico excluido del analisis: '{event_type}'"
            )
            return True

        # 2. Validación de calidad: Coordenadas
        if not isinstance(coords, (list, tuple)) or len(coords) < 2:
            self.guardar_en_cuarentena(
                conn, event_id, message_payload,
                f"Geometria o coordenadas invalidas: {coords}"
            )
            return True

        try:
            lon = float(coords[0])
            lat = float(coords[1])
            depth = float(coords[2]) if len(coords) > 2 and coords[2] is not None else None
        except (ValueError, TypeError) as e:
            self.guardar_en_cuarentena(
                conn, event_id, message_payload,
                f"Error de conversion numerica en coordenadas: {coords} ({e})"
            )
            return True

        if not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
            self.guardar_en_cuarentena(
                conn, event_id, message_payload,
                f"Coordenadas fuera de rango geografico: lon={lon}, lat={lat}"
            )
            return True

        # 3. Validación de calidad: Timestamps
        time_ms = props.get("time")
        updated_ms = props.get("updated")
        if time_ms is None or updated_ms is None:
            self.guardar_en_cuarentena(
                conn, event_id, message_payload,
                f"Timestamps obligatorios ausentes: time={time_ms}, updated={updated_ms}"
            )
            return True

        try:
            time_utc = datetime.fromtimestamp(float(time_ms) / 1000.0, tz=timezone.utc)
            updated_utc = datetime.fromtimestamp(float(updated_ms) / 1000.0, tz=timezone.utc)
            fecha_id = time_utc.date()
        except Exception as e:
            self.guardar_en_cuarentena(
                conn, event_id, message_payload,
                f"Error parseando timestamps: time={time_ms}, updated={updated_ms} ({e})"
            )
            return True

        # 4. Integración geoespacial con catálogo de países (Shapely STRtree O(log N))
        try:
            country_id, country_name = self.catalogo.asignar_pais(lon, lat)
        except Exception as e:
            logger.error(f"Error en asignacion espacial para {event_id}: {e}")
            country_id, country_name = None, "Sin país asignado"

        # 5. Tratamiento de magnitud
        raw_mag = props.get("mag")
        mag = float(raw_mag) if raw_mag is not None else None
        mag_type = props.get("magType")
        place = props.get("place")

        # 6. ESCRITURA ATÓMICA E IDEMPOTENTE EN POSTGRESQL
        try:
            with conn.cursor() as cur:
                # 6.1. Garantizar dimensión fecha
                cur.execute("""
                    INSERT INTO fecha (fecha_id, anio, mes, dia, dia_semana, nombre_mes, nombre_dia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fecha_id) DO NOTHING;
                """, (
                    fecha_id,
                    fecha_id.year,
                    fecha_id.month,
                    fecha_id.day,
                    fecha_id.isoweekday(),
                    MESES_ES.get(fecha_id.month, ""),
                    DIAS_ES.get(fecha_id.isoweekday(), "")
                ))

                # 6.2. Insertar versión observada (inmutable)
                cur.execute("""
                    INSERT INTO evento_version (
                        event_id, updated_utc, received_at, mag, mag_type, depth_km,
                        latitude, longitude, mode, raw_payload
                    )
                    VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, updated_utc) DO NOTHING;
                """, (
                    event_id, updated_utc, mag, mag_type, depth,
                    lat, lon, mode, Json(event)
                ))

                # 6.3. UPSERT en evento_actual:
                # - Si es nuevo: inserta
                # - Si ya existe: actualiza SOLO SI la versión recibida es estrictamente posterior
                cur.execute("""
                    INSERT INTO evento_actual (
                        event_id, time_utc, updated_utc, mag, mag_type, depth_km,
                        latitude, longitude, place, country_id, fecha_id, mode,
                        raw_properties, last_modified_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
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
                        mode = EXCLUDED.mode,
                        raw_properties = EXCLUDED.raw_properties,
                        last_modified_at = NOW()
                    WHERE EXCLUDED.updated_utc > evento_actual.updated_utc;
                """, (
                    event_id, time_utc, updated_utc, mag, mag_type, depth,
                    lat, lon, place, country_id, fecha_id, mode, Json(props)
                ))

                # 6.4. Actualizar timestamp de última ingesta exitosa
                now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
                cur.execute("""
                    INSERT INTO estado_ingesta (clave, valor, updated_at)
                    VALUES ('last_ingestion_success', %s, NOW())
                    ON CONFLICT (clave) DO UPDATE
                    SET valor = EXCLUDED.valor, updated_at = NOW();
                """, (now_ms,))

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Error en transacción PostgreSQL procesando evento {event_id}: {e}", exc_info=True)
            return False

    def touch_heartbeat(self):
        try:
            HEARTBEAT_FILE.touch()
        except OSError:
            pass

    async def run(self):
        await self.start()
        self.touch_heartbeat()
        logger.info("Consumidor esperando mensajes en streaming...")

        msg_count = 0
        try:
            async for record in self.consumer:
                if not self.running:
                    break

                try:
                    payload = json.loads(record.value.decode("utf-8"))
                except Exception as e:
                    logger.error(f"Mensaje no parseable como JSON en offset {record.offset}: {e}")
                    conn = self.get_pg_connection()
                    self.guardar_en_cuarentena(conn, None, {"raw_bytes": str(record.value[:200])}, "JSON corrupto")
                    await self.consumer.commit()
                    continue

                # Procesar transaccionalmente. Si falla, se reintenta ESTE mismo mensaje
                # (con espera progresiva y reconexión) y NO se confirma ningún offset posterior:
                # así un fallo de PostgreSQL nunca provoca pérdida de datos.
                backoff = 2.0
                while self.running:
                    try:
                        conn = self.get_pg_connection()
                        success = self.procesar_evento_transaccional(conn, payload)
                    except Exception as e:
                        logger.error(f"PostgreSQL no disponible: {e}")
                        success = False
                        self.pg_conn = None
                    if success:
                        break
                    logger.warning(f"Reintentando mensaje en offset {record.offset} en {backoff:.0f}s...")
                    if self.pg_conn is not None:
                        try:
                            self.pg_conn.close()
                        except Exception:
                            pass
                        self.pg_conn = None
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)

                # Confirmar el offset SOLO después de commit exitoso en PostgreSQL
                await self.consumer.commit()
                msg_count += 1
                self.touch_heartbeat()
                if msg_count % 50 == 0:
                    logger.info(f"Progreso: {msg_count} mensajes procesados y confirmados exitosamente.")

        except asyncio.CancelledError:
            pass
        finally:
            if self.consumer:
                logger.info("Cerrando consumidor Kafka...")
                await self.consumer.stop()
            if self.pg_conn:
                self.pg_conn.close()
            logger.info("Consumidor detenido.")

def handle_signal(sig, frame):
    logger.info("Señal de parada recibida en consumidor.")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    consumidor = TerremotoConsumidor()
    asyncio.run(consumidor.run())
