import asyncio
import json
import logging
import os
import signal
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

import httpx
from aiokafka import AIOKafkaProducer

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    USGS_FEED_URL,
    INGESTION_INTERVAL_SECONDS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PRODUCER] %(message)s"
)
logger = logging.getLogger("producer")

class PersistentState:
    """
    Gestiona el estado persistente de versiones ya publicadas por el productor.
    Combina un diccionario en RAM para búsquedas O(1) con una base SQLite local
    para sobrevivir a caídas y reinicios del contenedor sin duplicar publicaciones.
    """
    def __init__(self, db_path: Optional[str] = None):
        if not db_path:
            data_dir = Path("/app/data")
            if not data_dir.exists():
                data_dir = Path(__file__).resolve().parent.parent / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(data_dir / "producer_state.db")

        self.db_path = db_path
        self._cache: Dict[str, int] = {}
        self._init_db()

    def _init_db(self):
        logger.info(f"Inicializando almacén de estado persistente en: {self.db_path}")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS published_events (
                    event_id TEXT PRIMARY KEY,
                    updated_ms INTEGER NOT NULL,
                    published_at TIMESTAMP NOT NULL
                );
            """)
            conn.commit()
            cursor = conn.execute("SELECT event_id, updated_ms FROM published_events;")
            rows = cursor.fetchall()
            for eid, upd in rows:
                self._cache[eid] = upd
        logger.info(f"Estado previo restaurado: {len(self._cache)} eventos conocidos en RAM.")

    def should_publish(self, event_id: str, updated_ms: int) -> bool:
        """
        Regla de publicación:
        - Si no conoce el ID: publicar (evento nuevo).
        - Si conoce el ID y updated > conocido: publicar (revisión posterior).
        - Si updated <= conocido: no publicar (duplicado o revisión atrasada).
        """
        known_updated = self._cache.get(event_id)
        if known_updated is None:
            return True
        return updated_ms > known_updated

    def record_published(self, event_id: str, updated_ms: int):
        """
        Persiste en SQLite y actualiza la caché en RAM tras confirmación de Kafka.
        """
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO published_events (event_id, updated_ms, published_at)
                VALUES (?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    updated_ms = excluded.updated_ms,
                    published_at = excluded.published_at;
            """, (event_id, updated_ms, now))
            conn.commit()
        self._cache[event_id] = updated_ms


class TerremotoProductor:
    def __init__(self):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic = KAFKA_TOPIC
        self.feed_url = USGS_FEED_URL
        self.interval = INGESTION_INTERVAL_SECONDS
        self.state = PersistentState()
        self.producer: Optional[AIOKafkaProducer] = None
        self.running = True

    async def start(self):
        retries = 20
        delay = 3
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Conectando con broker Kafka en {self.bootstrap_servers} (intento {attempt}/{retries})...")
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    acks="all",  # Esperar confirmación del broker líder para asegurar durabilidad
                    max_request_size=1048576
                )
                await self.producer.start()
                logger.info(f"¡CONECTADO! Productor listo para publicar en '{self.topic}'.")
                return
            except Exception as e:
                logger.warning(f"Broker no listo todavía ({e}). Reintentando en {delay}s...")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, 30)

        logger.critical("No se pudo establecer conexión con Kafka. Abortando.")
        sys.exit(1)

    async def fetch_feed(self) -> Optional[Dict[str, Any]]:
        """
        Descarga el feed de USGS con backoff y control de errores HTTP (429, 5xx, timeout).
        """
        headers = {"User-Agent": "ObservatorioSismico/1.0 (IABD Big Data Project)"}
        async with httpx.AsyncClient(timeout=20.0) as client:
            try:
                response = await client.get(self.feed_url, headers=headers)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    logger.warning("USGS reporta HTTP 429 (Rate Limit). Esperando próximo ciclo...")
                else:
                    logger.error(f"Error HTTP al consultar USGS: {response.status_code} - {response.text[:200]}")
            except httpx.TimeoutException:
                logger.warning("Timeout al conectar con USGS. Se reintentará en el siguiente intervalo.")
            except Exception as e:
                logger.error(f"Error inesperado consultando feed USGS: {e}")
        return None

    async def process_and_publish(self):
        feed_data = await self.fetch_feed()
        if not feed_data:
            return

        features = feed_data.get("features", [])
        total_features = len(features)
        published_count = 0
        skipped_count = 0
        error_count = 0

        ingested_at = int(datetime.now(timezone.utc).timestamp() * 1000)

        for feat in features:
            event_id = feat.get("id")
            props = feat.get("properties") or {}
            updated = props.get("updated")

            if not event_id or updated is None:
                continue

            # Comparar con estado persistente: solo publicar novedades o revisiones más recientes
            if not self.state.should_publish(event_id, updated):
                skipped_count += 1
                continue

            message = {
                "source": "usgs-feed",
                "ingested_at": ingested_at,
                "mode": "live",
                "event": feat
            }

            key_bytes = event_id.encode("utf-8")
            val_bytes = json.dumps(message).encode("utf-8")

            try:
                # Enviar a Kafka y esperar confirmación (ACK) antes de registrar el estado
                await self.producer.send_and_wait(
                    topic=self.topic,
                    key=key_bytes,
                    value=val_bytes
                )
                self.state.record_published(event_id, updated)
                published_count += 1
            except Exception as e:
                logger.error(f"Fallo al publicar evento {event_id} en Kafka: {e}")
                error_count += 1

        logger.info(
            f"Ciclo completado: {total_features} eventos en feed | "
            f"Publicados: {published_count} | Omitidos (ya conocidos/antiguos): {skipped_count} | "
            f"Errores: {error_count}"
        )

    async def run(self):
        await self.start()
        logger.info(f"Iniciando ciclo periódico de ingesta cada {self.interval} segundos.")

        while self.running:
            start_time = asyncio.get_event_loop().time()
            try:
                await self.process_and_publish()
            except Exception as e:
                logger.error(f"Error no capturado en ciclo de productor: {e}", exc_info=True)

            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0.0, self.interval - elapsed)
            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break

        if self.producer:
            logger.info("Deteniendo productor de Kafka...")
            await self.producer.stop()
            logger.info("Productor detenido limpiamente.")

def handle_signal(sig, frame):
    logger.info("Señal de parada recibida. Cerrando productor...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    productor = TerremotoProductor()
    asyncio.run(productor.run())
