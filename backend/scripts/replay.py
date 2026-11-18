"""
Productor de Reproducción Acelerada (Replay)
Transmite una selección de eventos históricos por Kafka hacia el topic 'terremotos.replay'
con el flag mode='replay', velocidad configurable (1-2s por evento) y casos de prueba
controlados (duplicados idénticos, revisiones posteriores y anomalías para cuarentena).
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

from aiokafka import AIOKafkaProducer

current_dir = Path(__file__).resolve().parent
backend_dir = current_dir.parent
sys.path.insert(0, str(backend_dir))

from common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_REPLAY_TOPIC

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [REPLAY] %(message)s"
)
logger = logging.getLogger("replay")

# Casos de prueba controlados para demostrar la secuencia de la FAQ pregunta 12
EVENTOS_CONTROLADOS_PRUEBA = [
    {
        "type": "Feature",
        "id": "demo-q12-E1",
        "geometry": {"type": "Point", "coordinates": [-3.7038, 40.4168, 10.0]},
        "properties": {
            "type": "earthquake",
            "mag": 4.2,
            "magType": "mw",
            "place": "Secuencia FAQ Q12 - Versión Inicial E1",
            "time": 1735689600000,     # 2025-01-01 00:00:00 UTC
            "updated": 1735689900000   # 2025-01-01 00:05:00 UTC
        }
    },
    {
        "type": "Feature",
        "id": "demo-q12-E1",
        "geometry": {"type": "Point", "coordinates": [-3.7038, 40.4168, 10.0]},
        "properties": {
            "type": "earthquake",
            "mag": 4.2,
            "magType": "mw",
            "place": "Secuencia FAQ Q12 - Repetición Idéntica E1 (Duplicado)",
            "time": 1735689600000,
            "updated": 1735689900000   # Mismo updated -> no debe alterar recuentos
        }
    },
    {
        "type": "Feature",
        "id": "demo-q12-E1",
        "geometry": {"type": "Point", "coordinates": [-3.7038, 40.4168, 12.0]},
        "properties": {
            "type": "earthquake",
            "mag": 4.6,
            "magType": "mw",
            "place": "Secuencia FAQ Q12 - Revisión Posterior E1 (Mag 4.6)",
            "time": 1735689600000,
            "updated": 1735690140000   # Updated posterior -> debe actualizar evento_actual a 4.6
        }
    },
    {
        "type": "Feature",
        "id": "demo-q12-E1",
        "geometry": {"type": "Point", "coordinates": [-3.7038, 40.4168, 10.0]},
        "properties": {
            "type": "earthquake",
            "mag": 4.2,
            "magType": "mw",
            "place": "Secuencia FAQ Q12 - Versión Antigua Recibida Tarde E1",
            "time": 1735689600000,
            "updated": 1735689900000   # Updated antiguo -> NO debe degradar el evento_actual
        }
    },
    {
        "type": "Feature",
        "id": "demo-q12-E2",
        "geometry": {"type": "Point", "coordinates": [139.6917, 35.6895, 25.0]},
        "properties": {
            "type": "earthquake",
            "mag": 3.1,
            "magType": "ml",
            "place": "Secuencia FAQ Q12 - Nuevo Terremoto E2 (Tokio)",
            "time": 1735690320000,
            "updated": 1735690320000
        }
    },
    {
        "type": "Feature",
        "id": "demo-q12-E3-ANOMALIA",
        "geometry": {"type": "Point", "coordinates": [999.0, 999.0, 5.0]}, # Coordenadas imposibles
        "properties": {
            "type": "earthquake",
            "mag": 5.0,
            "magType": "mw",
            "place": "Secuencia Controlada - Caso Cuarentena (Coords inválidas)",
            "time": 1735690500000,
            "updated": 1735690500000
        }
    }
]

def cargar_muestra_historica(limite: int = 15) -> List[Dict[str, Any]]:
    """Carga eventos reales de los archivos históricos descargados si existen."""
    candidatos = [
        Path("/app/datos/historico"),
        current_dir.parent.parent / "datos" / "historico"
    ]
    for d in candidatos:
        if d.exists():
            for f in sorted(d.glob("*.geojson")):
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        data = json.load(fp)
                        features = data.get("features", [])
                        if features:
                            logger.info(f"Cargada muestra histórica desde {f.name} ({len(features)} disponibles)")
                            return features[:limite]
                except Exception as e:
                    logger.warning(f"No se pudo leer {f}: {e}")
    return []

async def ejecutar_replay(delay: float, incluir_faq_q12: bool, count: int):
    topic = KAFKA_REPLAY_TOPIC or "terremotos.replay"
    logger.info("=== INICIANDO REPRODUCCIÓN HISTÓRICA ACELERADA ===")
    logger.info(f"Destino Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {topic}")
    logger.info(f"Velocidad: 1 evento cada {delay} segundos | Etiqueta: 'REPRODUCCIÓN'")

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()

    try:
        eventos_a_enviar: List[Dict[str, Any]] = []

        if incluir_faq_q12:
            logger.info("Añadiendo casos de prueba controlados de la FAQ Q12 (duplicados, revisiones y cuarentena).")
            eventos_a_enviar.extend(EVENTOS_CONTROLADOS_PRUEBA)

        muestra_real = cargar_muestra_historica(limite=count)
        if muestra_real:
            eventos_a_enviar.extend(muestra_real)
        else:
            logger.info("Archivos históricos en disco no encontrados; transmitiendo eventos sintéticos de replay.")

        total = len(eventos_a_enviar)
        logger.info(f"Total de eventos en la cola de reproducción: {total}")

        for i, feat in enumerate(eventos_a_enviar, 1):
            event_id = feat.get("id")
            props = feat.get("properties") or {}
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            # Conservar time y updated original, pero registrar momento de reproducción
            mensaje = {
                "source": "usgs-replay",
                "ingested_at": now_ms,
                "mode": "replay",
                "event": feat
            }

            key_bytes = event_id.encode("utf-8")
            val_bytes = json.dumps(mensaje).encode("utf-8")

            await producer.send_and_wait(
                topic=topic,
                key=key_bytes,
                value=val_bytes
            )

            mag = props.get("mag")
            place = props.get("place", "N/A")
            logger.info(
                f"[{i}/{total}] [REPLAY ENVIADO] id={event_id} | mag={mag} | "
                f"updated={props.get('updated')} | lugar='{place[:35]}'"
            )

            if i < total:
                await asyncio.sleep(delay)

        logger.info("=== REPRODUCCIÓN HISTÓRICA FINALIZADA EXITOSAMENTE ===")

    finally:
        await producer.stop()

def main():
    parser = argparse.ArgumentParser(description="Productor de reproducción acelerada (Replay).")
    parser.add_argument("--delay", type=float, default=1.5, help="Segundos entre eventos (default: 1.5s).")
    parser.add_argument("--count", type=int, default=15, help="Número de eventos históricos reales a emitir.")
    parser.add_argument("--sin-faq-q12", action="store_true", help="Omitir casos sintéticos de FAQ Q12.")
    args = parser.parse_args()

    asyncio.run(ejecutar_replay(
        delay=args.delay,
        incluir_faq_q12=not args.sin_faq_q12,
        count=args.count
    ))

if __name__ == "__main__":
    main()
