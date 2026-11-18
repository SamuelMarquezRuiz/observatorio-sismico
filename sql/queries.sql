-- =============================================================================
-- Consultas de verificación del Modelo de Datos (Fase 2)
-- =============================================================================

-- 1. Comprobar que las 177 entidades de países están cargadas en la dimensión pais
SELECT COUNT(*) AS total_paises_cargados FROM pais;

-- 2. Inspeccionar muestra de países por continente
SELECT continent, COUNT(*) AS num_paises, string_agg(country_id, ', ' ORDER BY country_id) AS ejemplos
FROM pais
GROUP BY continent
ORDER BY num_paises DESC;

-- 3. Comprobar las restricciones de clave primaria y claves foráneas
SELECT 
    tc.table_name, 
    kcu.column_name, 
    tc.constraint_type,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.table_schema = 'public'
ORDER BY tc.table_name, tc.constraint_type;

-- 4. Verificar que los índices justificados existen en PostgreSQL
SELECT 
    tablename, 
    indexname, 
    indexdef 
FROM pg_indexes 
WHERE schemaname = 'public' 
ORDER BY tablename, indexname;

-- 5. Consulta de auditoría de calidad: conteo de eventos vs versiones vs cuarentena
SELECT 
    (SELECT COUNT(*) FROM evento_actual) AS eventos_unicos_vigentes,
    (SELECT COUNT(*) FROM evento_version) AS total_versiones_ingestadas,
    (SELECT COUNT(*) FROM cuarentena) AS total_rechazos_cuarentena,
    (SELECT COUNT(*) FROM evento_actual WHERE country_id IS NULL) AS sismos_maritimos_sin_pais;

-- 6. Consulta de prueba de ventana fija de 1 hora UTC (para Fase 3 y 5)
SELECT 
    date_trunc('hour', time_utc) AS ventana_hora_utc,
    COUNT(*) AS total_terremotos,
    AVG(mag) AS mag_media,
    MIN(mag) AS mag_min,
    MAX(mag) AS mag_max
FROM evento_actual
WHERE mode = 'live'
GROUP BY date_trunc('hour', time_utc)
ORDER BY ventana_hora_utc DESC
LIMIT 24;

-- 7. Verificar eventos con múltiples revisiones (para probar control de versiones e idempotencia)
SELECT 
    v.event_id,
    COUNT(v.updated_utc) AS total_versiones_recibidas,
    MIN(v.updated_utc) AS primera_version_utc,
    MAX(v.updated_utc) AS ultima_version_utc,
    a.updated_utc AS version_en_evento_actual
FROM evento_version v
JOIN evento_actual a ON v.event_id = a.event_id
GROUP BY v.event_id, a.updated_utc
HAVING COUNT(v.updated_utc) > 1
ORDER BY total_versiones_recibidas DESC;

-- 8. Auditar la tabla de cuarentena: motivos de rechazo
SELECT 
    reason,
    COUNT(*) AS total_descartados,
    MIN(received_at) AS primer_rechazo,
    MAX(received_at) AS ultimo_rechazo
FROM cuarentena
GROUP BY reason
ORDER BY total_descartados DESC;
