"""
==============================================================================
 FASE 8 — Análisis de Calidad (6 dimensiones) y Mediciones de Rendimiento
==============================================================================
"""
import json
import time
import sys
import tracemalloc
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

PG = dict(host="localhost", port=5432, dbname="observatorio",
          user="observatorio", password="observatorio_secret_pw")

def conn():
    return psycopg2.connect(**PG, cursor_factory=RealDictCursor)

def q(sql, params=None):
    c = conn()
    with c.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    c.close()
    return rows

def q1(sql, params=None):
    r = q(sql, params)
    return r[0] if r else {}

def qs(sql, params=None):
    r = q1(sql, params)
    return list(r.values())[0] if r else None

# =====================================================================
print("="*70)
print("   ANÁLISIS DE CALIDAD — 6 DIMENSIONES")
print("="*70)

total = qs("SELECT COUNT(*) FROM evento_actual")
print(f"\nTotal eventos en evento_actual: {total}")

# ── 1. COMPLETITUD ──
print("\n── 1. COMPLETITUD ──")
campos = {
    "event_id":    "SELECT COUNT(*) FROM evento_actual WHERE event_id IS NOT NULL",
    "time_utc":    "SELECT COUNT(*) FROM evento_actual WHERE time_utc IS NOT NULL",
    "updated_utc": "SELECT COUNT(*) FROM evento_actual WHERE updated_utc IS NOT NULL",
    "mag":         "SELECT COUNT(*) FROM evento_actual WHERE mag IS NOT NULL",
    "mag_type":    "SELECT COUNT(*) FROM evento_actual WHERE mag_type IS NOT NULL",
    "depth_km":    "SELECT COUNT(*) FROM evento_actual WHERE depth_km IS NOT NULL",
    "latitude":    "SELECT COUNT(*) FROM evento_actual WHERE latitude IS NOT NULL",
    "longitude":   "SELECT COUNT(*) FROM evento_actual WHERE longitude IS NOT NULL",
    "place":       "SELECT COUNT(*) FROM evento_actual WHERE place IS NOT NULL",
    "country_id":  "SELECT COUNT(*) FROM evento_actual WHERE country_id IS NOT NULL",
    "fecha_id":    "SELECT COUNT(*) FROM evento_actual WHERE fecha_id IS NOT NULL",
}
print(f"  {'Campo':<15} {'Presentes':>10} {'%Completo':>10}")
print(f"  {'-'*15} {'-'*10} {'-'*10}")
for campo, sql in campos.items():
    n = qs(sql)
    pct = (n / total * 100) if total else 0
    print(f"  {campo:<15} {n:>10} {pct:>9.2f}%")

mag_null = qs("SELECT COUNT(*) FROM evento_actual WHERE mag IS NULL")
print(f"\n  Eventos con magnitud NULL: {mag_null}")
print(f"  Decisión: Se conservan como ND (No Disponible). No se rellenan con 0.")

country_null = qs("SELECT COUNT(*) FROM evento_actual WHERE country_id IS NULL")
print(f"  Eventos sin país (marítimos/fronterizos): {country_null}")
print(f"  Decisión: Se mantienen como 'Sin país asignado'. Incluidos en análisis.")

# ── 2. UNICIDAD ──
print("\n── 2. UNICIDAD ──")
total_actual = qs("SELECT COUNT(*) FROM evento_actual")
distinct_ids = qs("SELECT COUNT(DISTINCT event_id) FROM evento_actual")
print(f"  Total filas evento_actual:  {total_actual}")
print(f"  IDs distintos:              {distinct_ids}")
print(f"  ¿Unicidad garantizada?:     {'SÍ (PK event_id)' if total_actual == distinct_ids else 'NO'}")

total_versions = qs("SELECT COUNT(*) FROM evento_version")
distinct_ver = qs("SELECT COUNT(*) FROM (SELECT DISTINCT event_id, updated_utc FROM evento_version) sub")
print(f"  Total filas evento_version: {total_versions}")
print(f"  Pares (id,updated) únicos:  {distinct_ver}")
print(f"  ¿Sin duplicados de versión?: {'SÍ (PK compuesta)' if total_versions == distinct_ver else 'NO'}")

events_with_revisions = qs("""
    SELECT COUNT(*) FROM (
        SELECT event_id FROM evento_version GROUP BY event_id HAVING COUNT(*) > 1
    ) sub
""")
print(f"  Eventos con revisiones (>1 versión): {events_with_revisions}")

# ── 3. VALIDEZ ──
print("\n── 3. VALIDEZ ──")
bad_coords = qs("""
    SELECT COUNT(*) FROM evento_actual
    WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
""")
print(f"  Coordenadas fuera de rango:     {bad_coords}")

bad_dates = qs("""
    SELECT COUNT(*) FROM evento_actual
    WHERE time_utc < '1900-01-01' OR time_utc > NOW() + INTERVAL '1 day'
""")
print(f"  Fechas imposibles (pre-1900 o futuro): {bad_dates}")

negative_depth = qs("SELECT COUNT(*) FROM evento_actual WHERE depth_km < 0")
print(f"  Profundidad negativa:           {negative_depth}")
print(f"    Decisión: Profundidad negativa es válida (sismo superficial/sobre nivel del mar)")

cuarentena_total = qs("SELECT COUNT(*) FROM cuarentena")
cuarentena_razones = q("SELECT reason, COUNT(*) as n FROM cuarentena GROUP BY reason ORDER BY n DESC LIMIT 10")
print(f"  Registros en cuarentena:        {cuarentena_total}")
for r in cuarentena_razones:
    print(f"    - {r['reason'][:60]}: {r['n']}")

non_eq = qs("SELECT COUNT(*) FROM cuarentena WHERE reason LIKE '%Tipo no sismico%'")
print(f"  De los cuales, eventos no-earthquake: {non_eq}")

# ── 4. CONSISTENCIA ──
print("\n── 4. CONSISTENCIA ──")
fk_pais = qs("""
    SELECT COUNT(*) FROM evento_actual ea
    LEFT JOIN pais p ON ea.country_id = p.country_id
    WHERE ea.country_id IS NOT NULL AND p.country_id IS NULL
""")
print(f"  FK country_id rotas (sin match en pais): {fk_pais}")

fk_fecha = qs("""
    SELECT COUNT(*) FROM evento_actual ea
    LEFT JOIN fecha f ON ea.fecha_id = f.fecha_id
    WHERE ea.fecha_id IS NOT NULL AND f.fecha_id IS NULL
""")
print(f"  FK fecha_id rotas (sin match en fecha):  {fk_fecha}")

updated_before_time = qs("""
    SELECT COUNT(*) FROM evento_actual WHERE updated_utc < time_utc
""")
print(f"  Eventos con updated < time (incoherente): {updated_before_time}")
print(f"    (Es normal que updated pueda ser < time en USGS)")

mag_types = q("SELECT mag_type, COUNT(*) as n FROM evento_actual WHERE mag_type IS NOT NULL GROUP BY mag_type ORDER BY n DESC")
print(f"  Tipos de magnitud registrados:")
for r in mag_types:
    print(f"    - {r['mag_type']}: {r['n']}")

# ── 5. ACTUALIDAD ──
print("\n── 5. ACTUALIDAD ──")
last_event = q1("SELECT MAX(time_utc) as ultimo_evento, MAX(updated_utc) as ultima_revision FROM evento_actual WHERE mode = 'live'")
print(f"  Último evento live (time_utc):     {last_event.get('ultimo_evento')}")
print(f"  Última revisión live (updated_utc): {last_event.get('ultima_revision')}")

last_ingesta = q1("SELECT valor FROM estado_ingesta WHERE clave = 'last_ingestion_success'")
if last_ingesta:
    ts_ms = int(last_ingesta['valor'])
    ts_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    lag = (datetime.now(timezone.utc) - ts_dt).total_seconds()
    print(f"  Última ingesta exitosa:            {ts_dt.isoformat()}")
    print(f"  Lag desde última ingesta:           {lag:.0f} segundos")

avg_lag = q1("""
    SELECT
        AVG(EXTRACT(EPOCH FROM (last_modified_at - updated_utc))) as avg_lag_s,
        MAX(EXTRACT(EPOCH FROM (last_modified_at - updated_utc))) as max_lag_s,
        MIN(EXTRACT(EPOCH FROM (last_modified_at - updated_utc))) as min_lag_s
    FROM evento_actual
    WHERE mode = 'live' AND last_modified_at IS NOT NULL AND updated_utc IS NOT NULL
""")
if avg_lag.get('avg_lag_s'):
    print(f"  Latencia media USGS→BD (live):     {float(avg_lag['avg_lag_s']):.1f}s")
    print(f"  Latencia máxima USGS→BD:           {float(avg_lag['max_lag_s']):.1f}s")

# ── 6. EXACTITUD ──
print("\n── 6. EXACTITUD ──")
print("  Fuente: USGS es la autoridad mundial en sismología.")
print("  Limitaciones conocidas:")
print("    - Magnitudes pueden revisarse varias horas después del evento.")
print("    - El catálogo geográfico (Natural Earth 1:110m) simplifica costas,")
print("      lo que puede clasificar como marítimos sismos costeros reales.")
print("    - Profundidades negativas son válidas (volcanes sobre nivel del mar).")
print("    - No contrastamos con otras agencias (EMSC, JMA) por alcance del proyecto.")

depth_stats = q1("""
    SELECT
        MIN(depth_km) as min_depth,
        MAX(depth_km) as max_depth,
        AVG(depth_km) as avg_depth,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY depth_km) as median_depth
    FROM evento_actual WHERE depth_km IS NOT NULL
""")
print(f"\n  Rango profundidad: {depth_stats['min_depth']} - {depth_stats['max_depth']} km")
print(f"  Media: {float(depth_stats['avg_depth']):.1f} km, Mediana: {float(depth_stats['median_depth']):.1f} km")

mag_stats = q1("""
    SELECT
        MIN(mag) as min_mag,
        MAX(mag) as max_mag,
        AVG(mag) as avg_mag,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mag) as median_mag
    FROM evento_actual WHERE mag IS NOT NULL
""")
print(f"  Rango magnitud: {mag_stats['min_mag']} - {mag_stats['max_mag']}")
print(f"  Media: {float(mag_stats['avg_mag']):.2f}, Mediana: {float(mag_stats['median_mag']):.2f}")


# =====================================================================
print("\n\n" + "="*70)
print("   MEDICIONES DE RENDIMIENTO — 3 TAMAÑOS DE LOTE")
print("="*70)

def measure_query(label, sql, params=None, runs=5):
    """Mide tiempo de ejecución y pico de memoria de una consulta."""
    times = []
    c = conn()
    for _ in range(runs):
        tracemalloc.start()
        t0 = time.perf_counter()
        with c.cursor() as cur:
            cur.execute(sql, params)
            cur.fetchall()
        elapsed = time.perf_counter() - t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        times.append(elapsed)
    c.close()
    avg_time = sum(times) / len(times)
    return avg_time, peak

print("\n── A) Búsqueda por event_id (Índice PK → O(log N) / amortizado O(1)) ──")
# Obtener IDs reales para diferentes tamaños
sample_ids = q("SELECT event_id FROM evento_actual ORDER BY RANDOM() LIMIT 1")
if sample_ids:
    sid = sample_ids[0]['event_id']
    # Pequeño: 1 búsqueda
    t1, m1 = measure_query("1 búsqueda PK", "SELECT * FROM evento_actual WHERE event_id = %s", (sid,))
    print(f"  1 búsqueda PK:     {t1*1000:.3f} ms | Memoria: {m1/1024:.1f} KB")

    # Medio: 100 búsquedas en IN
    ids100 = [r['event_id'] for r in q("SELECT event_id FROM evento_actual ORDER BY RANDOM() LIMIT 100")]
    t2, m2 = measure_query("100 búsquedas IN", "SELECT * FROM evento_actual WHERE event_id = ANY(%s)", (ids100,))
    print(f"  100 búsquedas IN:  {t2*1000:.3f} ms | Memoria: {m2/1024:.1f} KB")

    # Grande: 1000 búsquedas en IN
    ids1000 = [r['event_id'] for r in q("SELECT event_id FROM evento_actual ORDER BY RANDOM() LIMIT 1000")]
    t3, m3 = measure_query("1000 búsquedas IN", "SELECT * FROM evento_actual WHERE event_id = ANY(%s)", (ids1000,))
    print(f"  1000 búsquedas IN: {t3*1000:.3f} ms | Memoria: {m3/1024:.1f} KB")
    print(f"  Ratio 1000/100:    {t3/t2:.2f}x (O(N) lineal esperado ≈10x)")

print("\n── B) Búsqueda por event_id SIN índice (seq scan O(N)) vs CON índice ──")
# Medir scan secuencial forzado
t_seq, _ = measure_query("SeqScan",
    "SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT * FROM evento_actual WHERE event_id = %s",
    (sid,))
# Resetear para usar índice
c2 = conn()
with c2.cursor() as cur:
    cur.execute("SET enable_indexscan = on; SET enable_bitmapscan = on;")
c2.close()
t_idx, _ = measure_query("IndexScan", "SELECT * FROM evento_actual WHERE event_id = %s", (sid,))
print(f"  Sin índice (SeqScan):  {t_seq*1000:.3f} ms")
print(f"  Con índice (PK):       {t_idx*1000:.3f} ms")
print(f"  Speedup:               {t_seq/t_idx:.1f}x")

print("\n── C) Agregación por ventanas de 1h (GROUP BY) ──")
# Pequeño: últimas 6h
t_6h, m_6h = measure_query("6h",
    "SELECT date_trunc('hour', time_utc) as ventana, COUNT(*) FROM evento_actual WHERE time_utc > NOW() - INTERVAL '6 hours' GROUP BY ventana ORDER BY ventana")
# Medio: últimas 24h
t_24h, m_24h = measure_query("24h",
    "SELECT date_trunc('hour', time_utc) as ventana, COUNT(*) FROM evento_actual WHERE time_utc > NOW() - INTERVAL '24 hours' GROUP BY ventana ORDER BY ventana")
# Grande: todo el dataset
t_all, m_all = measure_query("Todo",
    "SELECT date_trunc('hour', time_utc) as ventana, COUNT(*) FROM evento_actual GROUP BY ventana ORDER BY ventana")

print(f"  Ventanas 6h:    {t_6h*1000:.3f} ms | Memoria: {m_6h/1024:.1f} KB")
print(f"  Ventanas 24h:   {t_24h*1000:.3f} ms | Memoria: {m_24h/1024:.1f} KB")
print(f"  Ventanas total: {t_all*1000:.3f} ms | Memoria: {m_all/1024:.1f} KB")
print(f"  Complejidad:    O(N) — crece linealmente con el número de filas escaneadas")

print("\n── D) Integración geográfica: consulta por country_id (con índice) ──")
# Pequeño: 1 país
t_1p, _ = measure_query("1 país",
    "SELECT * FROM evento_actual WHERE country_id = 'CHL'")
# Medio: 5 países
t_5p, _ = measure_query("5 países",
    "SELECT * FROM evento_actual WHERE country_id IN ('CHL','JPN','USA','IDN','MEX')")
# Grande: todos los países con count
t_ap, _ = measure_query("Todos países",
    "SELECT country_id, COUNT(*) FROM evento_actual GROUP BY country_id ORDER BY COUNT(*) DESC")

print(f"  1 país (CHL):     {t_1p*1000:.3f} ms")
print(f"  5 países (IN):    {t_5p*1000:.3f} ms")
print(f"  GROUP BY país:    {t_ap*1000:.3f} ms")

print("\n── E) Conteo total de la tabla ──")
sizes = [100, 500, 1000, 3000]
for n in sizes:
    t, _ = measure_query(f"LIMIT {n}",
        f"SELECT COUNT(*) FROM (SELECT 1 FROM evento_actual LIMIT {n}) sub")
    print(f"  COUNT con LIMIT {n:>5}: {t*1000:.3f} ms")

# ── Resumen de complejidad teórica ──
print("\n── RESUMEN DE COMPLEJIDAD TEÓRICA ──")
print(f"  {'Operación':<40} {'Complejidad':>15}")
print(f"  {'-'*40} {'-'*15}")
print(f"  {'Búsqueda por PK (event_id)':<40} {'O(log N) / O(1)':>15}")
print(f"  {'Búsqueda sin índice (seq scan)':<40} {'O(N)':>15}")
print(f"  {'INSERT con ON CONFLICT (upsert)':<40} {'O(log N)':>15}")
print(f"  {'Agregación GROUP BY ventana':<40} {'O(N)':>15}")
print(f"  {'Punto en polígono (STRtree)':<40} {'O(log P)':>15}")
print(f"  {'Lookup en caché dict Python':<40} {'O(1) amortiz.':>15}")

print("\n── EXTRAPOLACIÓN A 100M REGISTROS ──")
ratio = 100_000_000 / total if total else 1
print(f"  Factor de escala actual→100M:    {ratio:.0f}x")
print(f"  Búsqueda PK (B-tree):            {t_idx*1000:.3f}ms → ~{t_idx*1000*1.5:.1f}ms (O(log N), crece poco)")
print(f"  Scan secuencial:                 {t_seq*1000:.3f}ms → ~{t_seq*1000*ratio:.0f}ms (O(N), crece lineal)")
print(f"  Agregación GROUP BY:             {t_all*1000:.3f}ms → ~{t_all*1000*ratio:.0f}ms sin particionado")
print(f"  Propuesta: particionar por fecha (PARTITION BY RANGE) y usar pg_partman")

print("\n✅ Mediciones completadas.")
