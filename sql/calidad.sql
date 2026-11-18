-- =============================================================================
-- Análisis de calidad (ENUNCIADO §6): consultas reproducibles de las 6 dimensiones
-- Ejecutar:  Get-Content sql/calidad.sql | docker exec -i observatorio-postgres psql -U observatorio -d observatorio
-- Las cifras "antes" (entrada) provienen de evento_version + cuarentena; las "después" de evento_actual.
-- =============================================================================

\echo '=== 1. COMPLETITUD: % de campos disponibles en evento_actual ==='
SELECT
    COUNT(*)                                                         AS total,
    ROUND(100.0 * COUNT(mag)        / COUNT(*), 2)                   AS pct_mag,
    ROUND(100.0 * COUNT(mag_type)   / COUNT(*), 2)                   AS pct_mag_type,
    ROUND(100.0 * COUNT(depth_km)   / COUNT(*), 2)                   AS pct_profundidad,
    ROUND(100.0 * COUNT(place)      / COUNT(*), 2)                   AS pct_place,
    ROUND(100.0 * COUNT(country_id) / COUNT(*), 2)                   AS pct_con_pais,
    COUNT(*) - COUNT(mag)                                            AS magnitudes_nulas_ND,
    COUNT(*) FILTER (WHERE country_id IS NULL)                       AS sin_pais_asignado
FROM evento_actual;

\echo '=== 2. UNICIDAD: eventos únicos vs versiones; revisiones por evento ==='
SELECT
    (SELECT COUNT(*) FROM evento_actual)                            AS eventos_actuales,
    (SELECT COUNT(DISTINCT event_id) FROM evento_actual)            AS ids_distintos,
    (SELECT COUNT(*) FROM evento_version)                           AS versiones,
    (SELECT COUNT(*) FROM (SELECT event_id, updated_utc FROM evento_version
                           GROUP BY 1,2 HAVING COUNT(*) > 1) d)     AS pares_id_updated_duplicados,
    (SELECT COUNT(*) FROM (SELECT event_id FROM evento_version
                           GROUP BY 1 HAVING COUNT(*) > 1) r)       AS eventos_con_revisiones;

\echo '=== 3. VALIDEZ: rangos, geometría y registros en cuarentena ==='
SELECT
    COUNT(*) FILTER (WHERE latitude  NOT BETWEEN -90  AND 90)       AS lat_fuera_rango,
    COUNT(*) FILTER (WHERE longitude NOT BETWEEN -180 AND 180)      AS lon_fuera_rango,
    COUNT(*) FILTER (WHERE mag < 0)                                 AS mag_negativas_conservadas,
    COUNT(*) FILTER (WHERE depth_km < 0)                            AS profundidad_negativa_conservada,
    COUNT(*) FILTER (WHERE time_utc > NOW())                        AS fechas_futuras
FROM evento_actual;
SELECT reason, COUNT(*) AS registros, MIN(received_at) AS primero, MAX(received_at) AS ultimo
FROM cuarentena GROUP BY reason ORDER BY registros DESC;

\echo '=== 4. CONSISTENCIA: claves foráneas, modo y secuencia de revisiones ==='
SELECT
    (SELECT COUNT(*) FROM evento_actual e LEFT JOIN pais p ON e.country_id = p.country_id
      WHERE e.country_id IS NOT NULL AND p.country_id IS NULL)       AS fk_pais_rotas,
    (SELECT COUNT(*) FROM evento_actual e LEFT JOIN fecha f ON e.fecha_id = f.fecha_id
      WHERE f.fecha_id IS NULL)                                      AS fk_fecha_rotas,
    (SELECT COUNT(*) FROM evento_actual a JOIN
        (SELECT event_id, MAX(updated_utc) AS ult FROM evento_version GROUP BY 1) v
        ON v.event_id = a.event_id WHERE a.updated_utc <> v.ult)     AS estado_actual_no_es_ultima_version;
SELECT mode, COUNT(*) AS eventos FROM evento_actual GROUP BY mode ORDER BY mode;

\echo '=== 5. ACTUALIDAD: tiempo del evento vs actualización USGS vs ingesta (solo live) ==='
SELECT
    ROUND(AVG(EXTRACT(EPOCH FROM (v.received_at - e.time_utc)))::numeric, 0)   AS retraso_medio_evento_a_ingesta_s,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (v.received_at - e.time_utc)))::numeric, 0) AS mediana_s,
    ROUND(MAX(EXTRACT(EPOCH FROM (v.received_at - e.time_utc)))::numeric, 0)   AS maximo_s
FROM evento_version v JOIN evento_actual e USING (event_id)
WHERE v.mode = 'live';
SELECT clave, valor, updated_at FROM estado_ingesta;

\echo '=== 6. EXACTITUD: distribución de tipos de magnitud (cobertura heterogénea) ==='
SELECT mag_type, COUNT(*) AS eventos, ROUND(AVG(mag)::numeric, 2) AS mag_media
FROM evento_actual WHERE mode = 'historical' GROUP BY mag_type ORDER BY eventos DESC;

\echo '=== Antes/después: entrada total vs. aceptados vs. cuarentena ==='
SELECT
    (SELECT COUNT(*) FROM evento_version)                           AS versiones_aceptadas,
    (SELECT COUNT(*) FROM cuarentena)                               AS rechazadas_cuarentena,
    (SELECT COUNT(*) FROM evento_version) + (SELECT COUNT(*) FROM cuarentena) AS entrada_total;

\echo '=== Muestra de registros en cuarentena (original conservado) ==='
SELECT id, event_id, reason, LEFT(raw_payload::text, 120) AS original_extracto FROM cuarentena ORDER BY id DESC LIMIT 5;
