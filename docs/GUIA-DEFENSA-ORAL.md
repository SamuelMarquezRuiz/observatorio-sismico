# Guía de Ensayo y Preparación de la Defensa Oral

**Proyecto:** Observatorio de Actividad Sísmica  
**Módulo:** Sistemas de Big Data · Consolidación de Sesiones 1–8 · RA1  
**Estudiante:** Samuel Márquez Ruiz  

Esta guía proporciona el guion estructurado, el procedimiento de activación paso a paso, la explicación del flujo de datos de extremo a extremo y la defensa de las decisiones técnicas de diseño para la presentación ante el profesor.

---

## 1. Procedimiento de Activación en 5 Pasos (Puesta en Marcha)

Sigue estos 5 pasos exactos antes o durante el inicio de la defensa para tener el sistema operativo en menos de 2 minutos:

```
[Paso 1: Docker] ──> [Paso 2: Salud API] ──> [Paso 3: ngrok] ──> [Paso 4: Netlify] ──> [Paso 5: Replay]
```

### Paso 1: Levantar los contenedores de Docker
En una terminal en la raíz del proyecto:
```powershell
cd c:\Users\marqu\Desktop\IABD\BD\observatorio-sismico-materiales\observatorio-sismico
docker compose up -d
```
Verifica que los 5 servicios estén arriba (`docker compose ps`):
* `observatorio-kafka` (broker en KRaft)
* `observatorio-postgres` (PostgreSQL 16)
* `observatorio-producer` (productor de ingesta continua)
* `observatorio-consumer` (consumidor con validación y STRtree)
* `observatorio-api` (FastAPI en el puerto 8000)

### Paso 2: Verificar la salud interna del backend
Comprueba que PostgreSQL y la ingesta están activos abriendo en el navegador:  
`http://localhost:8000/api/salud`  
*Deberás observar `{"status":"ok", "postgres_connected":true, "total_eventos_base":3005, "estado_ingesta":"activa"}`.*

### Paso 3: Activar el túnel HTTPS de ngrok
En una segunda terminal (si ngrok no corre como servicio):
```powershell
ngrok http 8000
```
Copia la URL pública HTTPS generada (ej. `https://upturned-blast-tigress.ngrok-free.dev`).  
*Comprueba que responde visitando `https://<tu-subdominio>.ngrok-free.dev/` o `/docs`.*

### Paso 4: Abrir el panel Angular en Netlify o en local
Abre el navegador en tu teléfono móvil o en el ordenador del aula:
* Si usas Netlify: Entra a tu URL pública (ej. `https://<nombre-sitio>.netlify.app`).
* Si usas local: Ejecuta `npm start` en `frontend/` y abre `http://localhost:4200`.  
*Comprueba que el mapa carga los círculos de terremotos, la leyenda de profundidades, los filtros y los gráficos.*

### Paso 5: Tener preparado el comando de Reproducción Acelerada (Replay)
Ten abierta una terminal con el comando listo para ejecutar en el momento en que el profesor te pida una demostración en vivo:
```powershell
docker exec -it observatorio-producer python -m scripts.replay --delay 1.5
```

---

## 2. Explicación Fluida del Recorrido Completo de un Evento (De USGS a la Pantalla)

> **Guion sugerido para explicar de viva voz:**

1. **Extracción en Origen (USGS):**  
   "Todo comienza en el **Productor Python**, que actúa como un proceso asíncrono con `httpx` que sondea cada 60 segundos el feed `all_day.geojson` de USGS. Para no saturar la red ni emitir mensajes redundantes, el productor mantiene una caché de estado persistente en SQLite y memoria RAM (`O(1)`). Solo si detecta un `event_id` nuevo o una revisión con un `updated` posterior, construye un mensaje estructurado y lo envía a Kafka con el `event_id` como clave de partición, esperando la confirmación de recepción (*ACK*) del broker."

2. **Transporte Desacoplado (Apache Kafka):**  
   "El mensaje entra en el broker **Apache Kafka 3.8**, operando en modo KRaft sin ZooKeeper. Kafka actúa como búfer desacoplador: si la base de datos se satura o se reinicia el consumidor, los mensajes quedan retenidos de forma duradera en el topic `terremotos.live` sin perderse."

3. **Consumo, Validación y Calidad:**  
   "El **Consumidor Python**, suscrito al topic mediante `aiokafka`, lee los mensajes por lotes. Antes de tocar la base de datos, aplica tres filtros estrictos de calidad:
   * Verifica que el evento sea de tipo `earthquake`. Si es una explosión o cantera, lo desvía a la tabla `cuarentena`.
   * Comprueba que las coordenadas sean números finitos y estén en rango geográfico válido ($[-180, 180]$ en longitud y $[-90, 90]$ en latitud). Si no lo están, van a `cuarentena`.
   * Parsea los timestamps de milisegundos a objetos `TIMESTAMPTZ` en UTC."

4. **Integración Geoespacial (Punto en Polígono):**  
   "A continuación, el consumidor integra el sismo con el catálogo geográfico vectorial de Natural Earth (177 países). Para evitar una búsqueda secuencial lenta, utiliza un árbol espacial `shapely.STRtree` (R-Tree en $O(\log P)$). Si el sismo ocurre en aguas internacionales o fuera de las fronteras, no se inventa el país más cercano: se clasifica limpiamente con `country_id = null` («Sin país asignado»)."

5. **Persistencia Transaccional e Idempotente en PostgreSQL:**  
   "Dentro de una **única transacción ACID**, el consumidor realiza dos escrituras:
   * Inserta la versión inmutable en `evento_version` para auditoría y trazabilidad histórica.
   * Realiza un `UPSERT` en `evento_actual` mediante `ON CONFLICT (event_id) DO UPDATE ... WHERE EXCLUDED.updated_utc > evento_actual.updated_utc`. Esto garantiza matemáticamente que si se repite un mensaje o llega una versión atrasada, el estado vigente no se corrompe ni se duplica.
   * **Punto clave:** El consumidor confirma el *offset* en Kafka **únicamente después de que PostgreSQL hace `commit`**. Si la transacción falla, se hace `rollback` y el mensaje se vuelve a procesar."

6. **Capa de Exposición (API FastAPI + ngrok):**  
   "La API **FastAPI** lee los datos ya procesados y limpios de PostgreSQL. Responde a los endpoints `/api/salud`, `/api/eventos` y `/api/estadisticas`, calculando en base de datos las agregaciones por ventanas fijas de 1 hora UTC y la comparativa histórica con el año 2025. El túnel seguro de **ngrok** expone esta API local al exterior mediante HTTPS, satisfaciendo los requisitos de seguridad de los navegadores modernos."

7. **Visualización en el Cliente (Angular 20 + Leaflet):**  
   "Por último, el navegador del usuario descarga la aplicación SPA de **Netlify**. Angular consulta periódicamente la API mediante ngrok. Con la respuesta GeoJSON:
   * Dibuja los círculos en el mapa Leaflet (tamaño proporcional a la magnitud, color según la profundidad).
   * Genera los gráficos de distribución y barras por ventanas horarias.
   * Muestra la tabla accesible para personas con dificultades visuales.
   * Y si el túnel de ngrok cae, la interfaz intercepta el fallo sin romperse y muestra un banner de advertencia preservando la última capa válida."

---

## 3. Justificación de Decisiones Técnicas de Diseño

El profesor te pedirá que defiendas una o dos decisiones técnicas tomadas. Elige una de estas dos explicaciones preparadas:

### Opción A: Idempotencia en Streaming y Secuencia de Commit (Recomendada)
> **Pregunta:** *"¿Cómo aseguras que el sistema no duplique sismos si se cae el consumidor o la red falla?"*  
> **Respuesta Técnica:**  
> "En sistemas distribuidos reales, garantizar entrega *exactly-once* extremo a extremo es extremadamente costoso. Por ello, hemos implementado una semántica **at-least-once con persistencia idempotente**:
> 1. Desactivamos el auto-commit de Kafka (`enable_auto_commit = False`). El offset solo se confirma tras el commit exitoso en PostgreSQL.
> 2. Si el consumidor cae justo después de escribir en PostgreSQL pero antes de confirmar en Kafka, al levantarse volverá a recibir el mismo mensaje.
> 3. En PostgreSQL protegemos las tablas mediante restricciones únicas: `evento_version` tiene clave primaria compuesta `(event_id, updated_utc)` y `evento_actual` tiene clave primaria `event_id`.
> 4. En `evento_actual` aplicamos la cláusula `WHERE EXCLUDED.updated_utc > evento_actual.updated_utc`. Si llega el mismo mensaje, la base de datos lo ignora; si llega una versión posterior, se actualiza; y si llega una versión vieja recibida tarde, no degrada el registro moderno. De este modo, la operación es matemáticamente idempotente: procesar el mensaje una o diez veces produce exactamente el mismo resultado."

---

### Opción B: Cálculo de Ventanas Temporales en Base de Datos vs. Acumulación en Memoria
> **Pregunta:** *"¿Por qué calculas las ventanas horarias y estadísticas en PostgreSQL en lugar de acumular un contador en memoria en el consumidor o en Angular?"*  
> **Respuesta Técnica:**  
> "Porque en sismología los datos son **dinámicos y revisables**. Si USGS detecta un sismo a las 14:05 con magnitud 4.2 y dos horas después sus sismólogos corrigen la magnitud a 4.6 o cambian la hora del sismo a las 13:58, un contador en streaming en memoria quedaría desincronizado e introduciría un sesgo irreversible.  
> Al almacenar el estado consolidado en `evento_actual` y calcular las agregaciones horarias mediante SQL (`GROUP BY date_trunc('hour', time_utc)`), cualquier revisión de un evento recalcula automáticamente la ventana afectada en tiempo real. Además, nos apoyamos en el índice temporal `idx_evento_actual_time`, lo que permite calcular las agregaciones horarias de las últimas 24 horas en tan solo **2.4 milisegundos**, liberando completamente de carga a la CPU del cliente."

---

## 4. Preguntas Frecuentes Rápidas para la Evaluación

* **P: ¿Por qué hay tantos eventos sin país asignado (65.39%)?**  
  *R:* Porque la gran mayoría de la actividad sísmica mundial ocurre en las dorsales oceánicas y fosas marinas (ej. dorsal Mesoatlántica o fosa de las Marianas). Asignar esos eventos al país continental más próximo sería un error cartográfico grave; por ello se preservan con `country_id = null` en la categoría «Sin país asignado».
* **P: ¿Una profundidad negativa es un error?**  
  *R:* No. En sismología, la profundidad se mide respecto al geoide/elipsoide de referencia (nivel medio del mar en WGS84). Un sismo originado dentro de un edificio volcánico elevado (como en Hawái o los Andes) tiene hipocentro por encima del nivel del mar y se expresa con signo negativo (ej. -2.74 km).
* **P: ¿Por qué en las últimas 24h hay 28 sismos $\ge 4.0$ mientras que la media de 2025 es 47.58?**  
  *R:* Debido al *reporting lag* de las estaciones sismológicas. Los terremotos de magnitud moderada en zonas remotas tardan entre 12 y 48 horas en consolidarse en el feed oficial de USGS. Además, la sismicidad natural no es uniforme, sino que presenta agrupamiento (*clustering*) por enjambres y réplicas.
