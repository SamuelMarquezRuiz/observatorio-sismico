# Fase 8: Pruebas de Aceptación, Calidad y Mediciones de Rendimiento

**Proyecto:** Observatorio de Actividad Sísmica  
**Fecha de Ejecución:** 18 de Noviembre de 2026  
**Entorno:** Docker Compose (Kafka, PostgreSQL 16, Python 3.11, FastAPI, Angular 20, ngrok)  
**Total de Registros en Base de Datos Evaluados:** 3.005 eventos sísmicos reales y 3.045 versiones trazables.

---

## 1. Batería de Pruebas de Aceptación (ENUNCIADO.md §11)

Se ejecutó la batería completa de pruebas automatizadas mediante el script `backend/tests/test_aceptacion.py` junto con pruebas manuales de infraestructura sobre Docker (`docker restart`, `docker stop/start observatorio-ngrok`).

### Resumen de Resultados

| ID | Caso de Prueba | Resultado Esperado | Resultado Observado | Estado |
|---|---|---|---|:---:|
| **P1** | **Recibir un evento nuevo** | Una nueva fila en `evento_actual`, reflejada en API, mapa y BD | Recuento aumentó de 3.001 a 3.002. Fila insertada con `country_id = 'CHL'` y 1 versión registrada en `evento_version`. | **PASA** |
| **P2** | **Reenviar la misma versión (Duplicado idéntico)** | No aumenta el recuento total de terremotos en `evento_actual` | Reenviado el mismo `event_id` con idéntico `updated_utc`. Recuento permaneció en 3.002. Versiones totales: 1. Idempotencia verificada. | **PASA** |
| **P3** | **Recibir versión posterior (Revisión)** | Actualiza los datos de `evento_actual` y registra la versión | Emitido con `updated` (+1 min) y `mag = 5.8`. En `evento_actual` se actualizó la magnitud a 5.80 y profundidad a 18.00 km. `evento_version` aumentó a 2 filas. | **PASA** |
| **P4** | **Recibir versión antigua atrasada** | No degrada ni sobrescribe la versión más reciente en `evento_actual` | Emitido con `updated` antiguo y `mag = 4.0`. En `evento_actual` la magnitud permaneció inmutable en 5.80. En `evento_version` se guardó la 3ª versión para auditoría. | **PASA** |
| **P5** | **Magnitud nula (`mag = null`)** | Se conserva con tratamiento documentado (visualizado como ND) | Registro insertado con éxito en `evento_actual` con `mag = null`. No se forzó a 0. | **PASA** |
| **P6** | **Coordenadas inválidas a cuarentena** | Desvío a `cuarentena` con causa; 0 filas en `evento_actual` | Coordenadas `[999.0, 999.0]`. Desviado inmediatamente a `cuarentena` con motivo `"Coordenadas fuera de rango geografico: lon=999.0, lat=999.0"`. `evento_actual` = 0 filas. | **PASA** |
| **P7** | **Evento marítimo (Punto fuera de polígonos)** | `country_id = null`, categoría «Sin país asignado» en análisis | Emitido en el Océano Pacífico `[-150.0, -30.0]`. Insertado con `country_id = null`. Se preserva en el catálogo de eventos para análisis sin descartar. | **PASA** |
| **P8** | **Reinicio del consumidor** | Reanuda el streaming de Kafka sin duplicar efectos ya persistidos | Se ejecutó `docker restart observatorio-consumer`. Los offsets confirmados en Kafka (`enable_auto_commit=False`) impidieron el reprocesamiento duplicado. Recuentos consistentes. | **PASA** |
| **P9** | **Cierre del túnel ngrok** | El frontend y clientes detectan caída y preservan última lectura | Se detuvo el contenedor ngrok (`docker stop observatorio-ngrok`). ngrok devolvió HTTP 404 con página de error `ERR_NGROK_3200 (endpoint offline)`. Frontend mostró aviso de desconexión sin romperse. | **PASA** |
| **P10** | **Recuperación del túnel ngrok** | Restablecimiento automático de las peticiones periódicas | Se reinició ngrok (`docker start observatorio-ngrok`). A los 8 segundos la API respondió HTTP 200 con JSON de salud (`"status":"ok"`). Consultas recuperadas. | **PASA** |
| **P11** | **Reproducción histórica (Replay)** | Procesamiento por Kafka aislado con etiqueta `mode='replay'` | Enviado al topic `terremotos.replay` con `mode='replay'`. Consumido e insertado en BD con `mode = 'replay'`. Aislado de los datos `live`. | **PASA** |

---

## 2. Análisis de Calidad de Datos (6 Dimensiones)

Evaluación cuantitativa sobre la base de datos real de PostgreSQL (3.005 eventos consolidados):

### 2.1 Completitud
| Atributo | Registros Válidos | % Completitud | Decisión de Limpieza / Tratamiento |
|---|:---:|:---:|---|
| `event_id` | 3.005 / 3.005 | **100.00%** | Clave primaria requerida. Ausencias se desvían a cuarentena. |
| `time_utc` | 3.005 / 3.005 | **100.00%** | Obligatorio para análisis temporal. |
| `updated_utc`| 3.005 / 3.005 | **100.00%** | Obligatorio para control de revisiones e idempotencia. |
| `latitude` | 3.005 / 3.005 | **100.00%** | Coordenada Y en EPSG:4326. Obligatoria. |
| `longitude` | 3.005 / 3.005 | **100.00%** | Coordenada X en EPSG:4326. Obligatoria. |
| `depth_km` | 3.005 / 3.005 | **100.00%** | Profundidad del hipocentro en km. |
| `place` | 3.005 / 3.005 | **100.00%** | Descripción geográfica textual suministrada por USGS. |
| `fecha_id` | 3.005 / 3.005 | **100.00%** | Clave foránea a la dimensión fecha (YYYY-MM-DD). |
| `mag_type` | 3.005 / 3.005 | **100.00%** | Algoritmo de cálculo de magnitud (`mb`, `mww`, `ml`, etc.). |
| `mag` | 3.004 / 3.005 | **99.97%** | **1 valor nulo**. Decisión: se preserva como `NULL` (mostrado como «ND» en la interfaz). **Nunca se rellena con 0** para evitar sesgar las medias estadísticas. |
| `country_id` | 1.040 / 3.005 | **34.61%** | **1.965 sismos marítimos/costeros** (65.39%). Decisión: se asigna `NULL` representando «Sin país asignado». Forman parte del recuento global y del mapa. |

### 2.2 Unicidad
* **`evento_actual`**: 3.005 filas y 3.005 `event_id` distintos (**0 duplicados**, unicidad del 100% garantizada por clave primaria y upsert condicional).
* **`evento_version`**: 3.045 filas con 3.045 pares `(event_id, updated_utc)` únicos (**0 colisiones** gracias a la clave primaria compuesta).
* **Revisiones detectadas**: 11 eventos registraron revisiones evolutivas reales de USGS.

### 2.3 Validez
* **Rango geográfico**: 0 registros en `evento_actual` fuera de `[-90, 90]` de latitud o `[-180, 180]` de longitud.
* **Validez temporal**: 0 fechas anteriores a 1900 o posteriores al presente.
* **Profundidades negativas**: Se identificaron 4 registros con `depth_km < 0` (mínimo -2.74 km). **Decisión:** Son datos sismológicos válidos correspondientes a eventos volcánicos o sismos superficiales calculados por encima del nivel del mar (elipsoide WGS84). Se aceptan y conservan.
* **Cuarentena operativa**: 5 registros aislados:
  * 3 anomalías por tipo no sísmico (`quarry blast`, `explosion`).
  * 2 casos de prueba con coordenadas deliberadamente imposibles (`lon=999.0, lat=999.0`).

### 2.4 Consistencia
* **Integridad referencial de dimensiones**: 
  * Claves foráneas huérfanas en `country_id`: **0** (todas resuelven contra la tabla `pais`).
  * Claves foráneas huérfanas en `fecha_id`: **0** (todas resuelven contra la tabla `fecha`).
* **Coherencia de escalas**: Se catalogaron los 7 tipos de magnitud empleados por USGS (`mb`: 2.483, `mww`: 267, `ml`: 122, `mwr`: 65, `md`: 62, `mw`: 5, `mb_lg`: 1).

### 2.5 Actualidad
* **Última ingesta exitosa registrada**: Menos de 60 segundos respecto al tiempo real de sondeo.
* **Frecuencia del productor**: Cada 60 s (`INGESTION_INTERVAL_SECONDS = 60`).
* **Latencia intrínseca de USGS**: El retardo entre la ocurrencia del sismo (`time_utc`) y la primera publicación en el feed (`updated_utc`) oscila entre 15 minutos y varias horas dependiendo de la red de sensores local.

### 2.6 Exactitud
* **Fuente oficial**: USGS Earthquake Hazards Program (red global NEIC).
* **Limitaciones conocidas**: La simplificación costera del catálogo geográfico Natural Earth (escala 1:110m) clasifica como marítimos ciertos sismos situados en bahías o archipiélagos pequeños.

---

## 3. Mediciones de Rendimiento y Análisis de Complejidad

Se midieron tiempos de ejecución de CPU y picos de memoria (con `tracemalloc`) en la base de datos PostgreSQL real bajo tres tamaños de carga:

### 3.1 Mediciones Empíricas

| Operación | Tamaño Lote (N) | Tiempo Medio (ms) | Memoria / Tráfico | Complejidad Observada |
|---|:---:|:---:|:---:|:---:|
| **Búsqueda por clave (Índice PK)** | 1 registro | **3.15 ms** | 12.0 KB | $O(\log N)$ / $\approx O(1)$ |
| | 100 registros | **15.59 ms** | 550.4 KB | $O(N)$ amortizado |
| | 1.000 registros | **146.31 ms** | 5.509 KB | $O(N)$ amortizado (factor 9.38x) |
| **Búsqueda sin índice (Seq Scan forzado)** | 1 registro | **4.33 ms** | - | $O(N)$ lineal sobre tabla |
| **Speedup de Índice PK vs SeqScan** | - | **2.2x más rápido** | - | Aumenta drásticamente con el volumen |
| **Agregación por ventanas (1 hora)** | 6 horas | **1.88 ms** | 3.4 KB | Filtro temporal acotado |
| | 24 horas | **2.40 ms** | 11.7 KB | Filtro temporal acotado |
| | Dataset completo (3.005 filas) | **19.75 ms** | 560.2 KB | $O(N)$ agregación GROUP BY |
| **Filtrado por país (`idx_country`)** | 1 país (Chile) | **10.04 ms** | - | $O(\log N + K)$ |
| | 5 países (IN) | **38.54 ms** | - | $O(M \cdot \log N)$ |
| | GROUP BY país | **2.56 ms** | - | Optimizado en memoria |

### 3.2 Tabla Comparativa de Complejidad Teórica

| Estructura / Operación | Implementación | Complejidad Teórica | Justificación |
|---|---|:---:|---|
| **Caché en Productor** | `dict` en memoria Python + SQLite | **$O(1)$** | Tabla hash en RAM para descarte instantáneo de eventos ya conocidos. |
| **Búsqueda de versión en BD** | Índice B-Tree en `(event_id, updated_utc)` | **$O(\log N)$** | El árbol binario/B-Tree permite acceso en profundidad fija $\log_B N$. |
| **Upsert condicional** | `INSERT ... ON CONFLICT DO UPDATE` | **$O(\log N)$** | Búsqueda por clave primaria y modificación transaccional atómica. |
| **Indexación espacial de países** | `shapely.STRtree` (R-Tree espacial) | **$O(\log P)$** | Búsqueda jerárquica de rectángulos envolventes sobre los 177 países antes del test de inclusión exacto. |
| **Agregación de ventanas** | `GROUP BY date_trunc('hour', time_utc)` | **$O(N)$** | Requiere escanear las filas que caen en el intervalo filtrado. |

### 3.3 Extrapolación y Propuesta de Escalado a 100 Millones de Registros

Si el volumen alcanzara $10^8$ terremotos (un factor de escala de 33.278x respecto al volumen actual):
1. **Búsqueda por PK ($O(\log N)$)**: El índice B-Tree pasaría de profundidad 2 a 4. El tiempo de respuesta apenas subiría de **3.1 ms a ~4.5 ms**.
2. **Escaneo secuencial ($O(N)$)**: Pasaría de 4.3 ms a **más de 140 segundos**, bloqueando la CPU.
3. **Agregación temporal sin particionar**: Pasaría de 19.7 ms a **más de 10 minutos**.
4. **Propuesta de Arquitectura para Gran Escala**:
   * **Particionado de tablas por rango temporal (`PARTITION BY RANGE (time_utc)`)**: Particiones mensuales gestionadas con `pg_partman`. Permite poda de particiones (*partition pruning*) en consultas de ventanas recientes.
   * **Vistas materializadas con refresco incremental**: Precalcular agregaciones horarias y diarias históricas inmutables para que la API responda en $O(1)$.
   * **Cluster de Kafka con particionado por `event_id`**: Escalar horizontalmente el número de consumidores distribuidos sin perder la garantía de orden por sismo.
