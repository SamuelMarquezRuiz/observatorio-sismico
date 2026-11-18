import asyncio
import gzip
import hashlib
import json
import logging
import os
import signal
import sqlite3
import sys
import time
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

# Retención de snapshots del GeoJSON original (horas). Evita acumular copias indefinidamente.
SNAPSHOT_RETENTION_HOURS = float(os.getenv("SNAPSHOT_RETENTION_HOURS", "6"))
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/producer_heartbeat"))
MAX_FETCH_ATTEMPTS = 4


def data_dir() -> Path:
    d = Path("/app/data")
    if not d.exists():
        d = Path(__file__).resolve().parent.parent / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d


class PersistentState:
    """
    Estado persistente del productor (SQLite en volumen Docker):
    - published_events: última versión (id, updated) publicada y confirmada por Kafka.
    - excluded_events: eventos descartados por no ser terremotos, con el motivo.
    Un diccionario en RAM da búsquedas O(1); SQLite permite sobrevivir a reinicios.
    """
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(data_dir() / "producer_state.db")
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS excluded_events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT,
                    reason TEXT NOT NULL,
                    updated_ms INTEGER,
                    first_seen TIMESTAMP NOT NULL
                );
            """)
            conn.commit()
            for eid, upd in conn.execute("SELECT event_id, updated_ms FROM published_events;").fetchall():
                self._cache[eid] = upd
        logger.info(f"Estado previo restaurado: {len(self._cache)} eventos conocidos en RAM.")

    def should_publish(self, event_id: str, updated_ms: int) -> bool:
        """Nuevo -> publicar; updated posterior -> publicar; igual o anterior -> omitir."""
        known_updated = self._cache.get(event_id)
        if known_updated is None:
            return True
        return updated_ms > known_updated

    def record_published(self, event_id: str, updated_ms: int):
        """Persiste tras la confirmación (ACK) de Kafka."""
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

    def record_exclusion(self, event_id: str, event_type: Optional[str], updated_ms: Optional[int]) -> bool:
        """Guarda el motivo de exclusión. Devuelve True si es la primera vez que se ve."""
        reason = f"properties.type='{event_type}' distinto de 'earthquake'"
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("""
                INSERT OR IGNORE INTO excluded_events (event_id, event_type, reason, updated_ms, first_seen)
                VALUES (?, ?, ?, ?, ?);
            """, (event_id, event_type, reason, updated_ms, now))
            conn.commit()
            return cur.rowcount > 0


class SnapshotStore:
    """
    Conserva el GeoJSON original (comprimido) y los metadatos de cada extracción.
    - Solo guarda un snapshot nuevo si el contenido cambió (hash SHA-256 distinto).
    - Retención limitada (SNAPSHOT_RETENTION_HOURS); los metadatos se registran siempre.
    """
    def __init__(self):
        self.dir = data_dir() / "snapshots"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.meta_file = data_dir() / "extracciones.jsonl"
        self.last_hash: Optional[str] = None

    def registrar(self, raw: Optional[bytes], status: Optional[int], latency_ms: int,
                  n_features: Optional[int], error: Optional[str] = None):
        sha = hashlib.sha256(raw).hexdigest() if raw else None
        saved = None
        if raw and error is None and sha != self.last_hash:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            saved = f"all_day_{ts}.geojson.gz"
            with gzip.open(self.dir / saved, "wb") as fh:
                fh.write(raw)
            self.last_hash = sha
        meta = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "url": USGS_FEED_URL,
            "http_status": status,
            "latency_ms": latency_ms,
            "bytes": len(raw) if raw else 0,
            "sha256": sha,
            "features": n_features,
            "snapshot": saved,
            "error": error,
        }
        with open(self.meta_file, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(meta) + "\n")
        self._apply_retention()

    def _apply_retention(self):
        limit = time.time() - SNAPSHOT_RETENTION_HOURS * 3600
        for f in self.dir.glob("all_day_*.geojson.gz"):
            try:
                if f.stat().st_mtime < limit:
                    f.unlink()
            except OSError:
                pass


class TerremotoProductor:
    def __init__(self):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic = KAFKA_TOPIC
        self.feed_url = USGS_FEED_URL
        self.interval = INGESTION_INTERVAL_SECONDS
        self.state = PersistentState()
        self.snapshots = SnapshotStore()
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
                    acks="all",
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
        GET al feed de USGS con espera progresiva (2s, 4s, 8s) ante timeout, HTTP 429/5xx
        o JSON inválido. Un fallo nunca borra el estado válido anterior: devuelve None.
        """
        headers = {"User-Agent": "ObservatorioSismico/1.0 (IABD Big Data Project)"}
        delay = 2.0
        for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
            t0 = time.monotonic()
            status: Optional[int] = None
            raw: Optional[bytes] = None
            error: Optional[str] = None
            retry_after: Optional[float] = None
            retryable = True
            try:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    response = await client.get(self.feed_url, headers=headers)
                status = response.status_code
                raw = response.content
                if status == 200:
                    try:
                        data = json.loads(raw)
                        if not isinstance(data, dict) or not isinstance(data.get("features"), list):
                            raise ValueError("estructura GeoJSON sin 'features'")
                        latency = int((time.monotonic() - t0) * 1000)
                        self.snapshots.registrar(raw, status, latency, len(data["features"]))
                        return data
                    except ValueError as e:
                        error = f"JSON inválido: {e}"
                elif status == 429 or status >= 500:
                    error = f"HTTP {status}"
                    ra = response.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        retry_after = float(ra)
                else:
                    error = f"HTTP {status} (error de cliente, sin reintento)"
                    retryable = False
            except httpx.TimeoutException:
                error = "timeout"
            except Exception as e:
                error = f"error de red: {e}"

            latency = int((time.monotonic() - t0) * 1000)
            self.snapshots.registrar(None, status, latency, None, error=error)
            logger.warning(f"Fallo al consultar USGS (intento {attempt}/{MAX_FETCH_ATTEMPTS}): {error}")
            if not retryable or attempt == MAX_FETCH_ATTEMPTS:
                break
            await asyncio.sleep(retry_after if retry_after else delay)
            delay *= 2

        logger.error("Ciclo omitido: USGS no disponible. Se conserva el estado previo y se reintenta en el próximo ciclo.")
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
        excluded_types: Dict[str, int] = {}

        ingested_at = int(datetime.now(timezone.utc).timestamp() * 1000)

        for feat in features:
            event_id = feat.get("id")
            props = feat.get("properties") or {}
            updated = props.get("updated")

            if not event_id or updated is None:
                continue

            # Filtro de tipo: solo terremotos. Se conserva el motivo de exclusión.
            event_type = props.get("type")
            if event_type != "earthquake":
                excluded_types[str(event_type)] = excluded_types.get(str(event_type), 0) + 1
                if self.state.record_exclusion(event_id, event_type, updated):
                    logger.info(f"Evento {event_id} excluido: properties.type='{event_type}' (no es earthquake)")
                continue

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
                # ACK de Kafka antes de marcar la versión como publicada
                await self.producer.send_and_wait(topic=self.topic, key=key_bytes, value=val_bytes)
                self.state.record_published(event_id, updated)
                published_count += 1
            except Exception as e:
                logger.error(f"Fallo al publicar evento {event_id} en Kafka: {e}")
                error_count += 1

        logger.info(
            f"Ciclo completado: {total_features} eventos en feed | "
            f"Publicados: {published_count} | Omitidos (ya conocidos/antiguos): {skipped_count} | "
            f"Excluidos por tipo: {excluded_types or 0} | Errores: {error_count}"
        )

    def touch_heartbeat(self):
        try:
            HEARTBEAT_FILE.touch()
        except OSError:
            pass

    async def run(self):
        await self.start()
        logger.info(f"Iniciando ciclo periódico de ingesta cada {self.interval} segundos.")

        while self.running:
            start_time = asyncio.get_event_loop().time()
            try:
                await self.process_and_publish()
            except Exception as e:
                logger.error(f"Error no capturado en ciclo de productor: {e}", exc_info=True)
            self.touch_heartbeat()

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
