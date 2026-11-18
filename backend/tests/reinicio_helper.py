"""
Ayudante de la prueba 8 (reiniciar consumidor). Se ejecuta DENTRO del contenedor productor
(que ya tiene aiokafka/psycopg2 y red hacia kafka/postgres) y lo orquesta
scripts/prueba_08_reinicio_consumidor.ps1 desde el host (donde sí hay docker).

  python tests/reinicio_helper.py send  <event_id>   # publica 1 evento (mode=test) en el topic live
  python tests/reinicio_helper.py count [event_id]   # imprime recuentos: totales y del evento
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2
from aiokafka import AIOKafkaProducer

PG = dict(
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "observatorio"),
    user=os.getenv("POSTGRES_USER", "observatorio"),
    password=os.getenv("POSTGRES_PASSWORD", "observatorio_secret_pw"),
)
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "terremotos.live")


async def send(event_id: str):
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    event = {
        "type": "Feature", "id": event_id,
        "geometry": {"type": "Point", "coordinates": [-70.5, -33.4, 15.0]},
        "properties": {"type": "earthquake", "mag": 4.4, "magType": "ml",
                       "place": "Prueba 8 - publicado con el consumidor caido",
                       "time": now - 60000, "updated": now},
    }
    msg = {"source": "prueba-08", "ingested_at": now, "mode": "test", "event": event}
    p = AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all")
    await p.start()
    await p.send_and_wait(TOPIC, key=event_id.encode(), value=json.dumps(msg).encode())
    await p.stop()
    print(f"ENVIADO {event_id}")


def count(event_id=None):
    conn = psycopg2.connect(**PG)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM evento_actual")
        actual = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM evento_version")
        versiones = cur.fetchone()[0]
        evento = versiones_evento = 0
        if event_id:
            cur.execute("SELECT COUNT(*) FROM evento_actual WHERE event_id = %s", (event_id,))
            evento = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM evento_version WHERE event_id = %s", (event_id,))
            versiones_evento = cur.fetchone()[0]
    conn.close()
    print(f"ACTUAL={actual} VERSIONES={versiones} EVENTO={evento} VERSIONES_EVENTO={versiones_evento}")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "send":
        asyncio.run(send(sys.argv[2]))
    elif cmd == "count":
        count(sys.argv[2] if len(sys.argv) > 2 else None)
