# Observatorio de Actividad Sísmica en Tiempo Real

[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-blue?logo=docker)](compose.yaml)
[![Python](https://img.shields.io/badge/Python-3.11--slim-yellow?logo=python)](backend/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.0%20(KRaft)-black?logo=apachekafka)](compose.yaml)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16--alpine-blue?logo=postgresql)](sql/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green?logo=fastapi)](backend/api/)
[![Angular](https://img.shields.io/badge/Angular-20.3-red?logo=angular)](frontend/)
[![Leaflet](https://img.shields.io/badge/Leaflet-1.9.4-199900?logo=leaflet)](frontend/)
[![Tema UI](https://img.shields.io/badge/UI-Tectonic%20Dark-38BDF8)](frontend/src/app/app.component.css)

Plataforma distribuida de Big Data para la ingesta continua, procesamiento en streaming, control de revisiones con idempotencia estricta, enriquecimiento geoespacial en memoria y análisis comparativo de la sismicidad global en tiempo real frente al registro histórico anual (2025) del Servicio Geológico de los Estados Unidos (USGS).

---

## 1. Visión General y Decisiones de Arquitectura

El observatorio resuelve un reto crítico en flujos de datos sismológicos: **los eventos sísmicos no son inmutables en su origen**. El USGS publica estimaciones preliminares automatizadas que se corrigen, refinan o recalibran minutos u horas más tarde (modificando magnitud, profundidad o coordenadas exactas). Además, las redes distribuidas pueden entregar eventos duplicados o desordenados temporalmente.

### 1.1 Flujo de Datos Distribuido

```
[ USGS Earthquakes Feed ] (Polling HTTP GET cada 60s a all_day.geojson)
         │
         ▼
[ observatorio-producer ] (Python 3.11 / aiokafka)
  ├── Filtro estricto: properties.type == 'earthquake'
  ├── Deduplicación en origen: Caché SQLite (producer_state.db) con par (id, updated)
  └── Clave del mensaje: event_id (particionado coherente)
         │
         ▼ (Topic: terremotos.live | terremotos.replay)
[ observatorio-kafka ] (Apache Kafka 3.8.0 con KRaft, sin dependencias de ZooKeeper)
  └── Retención temporal configurada: 24 horas
         │
         ▼ (Manual Commit Offset tras confirmación transaccional)
[ observatorio-consumer ] (Python 3.11 / aiokafka + Shapely STRtree)
  ├── Validación de calidad: lat [-90, 90], lon [-180, 180], epoch ms a UTC
  ├── Cuarentena: Registros defectuosos desviados a tabla 'cuarentena'
  ├── Cruce espacial: STRtree (177 países de Natural Earth) en O(log P)
  └── Persistencia ACID en PostgreSQL:
        ├── evento_version: Log histórico inmutable (SCD Tipo 2)
        └── evento_actual: Estado vigente condicional (EXCLUDED.updated_utc > evento_actual.updated_utc)
         │
         ▼
[ observatorio-postgres ] (PostgreSQL 16 Alpine + Volumen persistente)
         │
         ▼ (Pool psycopg2 / Consultas agregadas con índices compuestos)
[ observatorio-api ] (FastAPI + Uvicorn)
  ├── Endpoints: /api/salud, /api/eventos, /api/estadisticas, /api/paises
  ├── Comparación analítica: Ventana móvil de 24h vs Media diaria histórica 2025
  └── Middleware CORS estricto con soporte para subdominios Netlify y bypass ngrok
         │
         ├── [ ngrok ] (Túnel seguro HTTPS para exponer el backend local)
         │       │
         ▼       ▼
[ Panel Frontend Angular 20 ] (SPA desplegada en Netlify / Localhost:4200)
  ├── Cartografía interactiva Leaflet con marcadores dinámicos y popups enriquecidos
  ├── Filtros reactivos por magnitud, país, intervalo temporal y modo de ingesta
  ├── Distribuciones estadísticas (profundidad, magnitudes, series temporales)
  └── Tema de alto contraste 'Tectonic Dark' optimizado para salas de monitorización
```

### 1.2 Justificación de la Pila Tecnológica (¿Por qué no Spark o Flink?)
El catálogo global del USGS genera en promedio ~250 a 350 sismos diarios a escala mundial (aproximadamente un evento nuevo o actualización cada 4–5 minutos). Desplegar un clúster distribuido pesado como Apache Spark Streaming o Apache Flink para este caudal introduciría:
- Consumo desproporcionado de memoria (JVMs sobredimensionadas superando 4–8 GB de RAM).
- Latencias de micro-batching innecesarias frente al modelo reactivo por eventos.
- Mayor sobrecarga operativa y costes de infraestructura cloud.

La combinación de **Python asíncrono (`aiokafka`)**, **Apache Kafka con KRaft** y **PostgreSQL 16 con índices B-Tree específicos** ofrece latencias inferiores a 50 milisegundos, aislamiento completo de procesos, bajo consumo (< 800 MB de RAM para todo el stack) y semántica *at-least-once* con procesamiento estrictamente idempotente.

### 1.3 Autoría y Código Propio
* **Material base provisto:** Catálogo simplificado de fronteras internacionales ([`paises.geojson`](datos/paises.geojson)), muestra de consulta histórica ([`query.json`](query.json)) y especificación de contratos.
* **Implementación propia completa:**
  - Productor con persistencia local en SQLite y control de retroceso exponencial ante errores HTTP 429/5xx.
  - Consumidor con indexación espacial R-Tree (`shapely.STRtree`), bifurcación a cuarentena y transacción PostgreSQL con cláusula `ON CONFLICT DO UPDATE WHERE`.
  - Ingesta particionada y deduplicada del histórico anual 2025 (`scripts/carga_historica.py`).
  - Productor de reproducción acelerada (`scripts/replay.py`) con inyección controlada de anomalías de la FAQ Q12.
  - API REST en FastAPI con métricas analíticas e histogramas.
  - Suite de pruebas de aceptación automatizadas ([`tests/test_aceptacion.py`](backend/tests/test_aceptacion.py)) y análisis de rendimiento ([`tests/test_calidad_rendimiento.py`](backend/tests/test_calidad_rendimiento.py)).
  - Rediseño y desarrollo de la interfaz SPA en Angular 20 con paleta *Tectonic Dark*.

---

## 2. Estructura del Repositorio y Entregables

```
observatorio-sismico/
├── .env.example                       # Plantilla de variables de entorno segura (sin secretos)
├── compose.yaml                       # Definición de los 5 servicios Docker obligatorios + ngrok
├── netlify.toml                       # Configuración de compilación y redirecciones SPA para Netlify
├── query.json                         # Muestra histórica oficial USGS (Enero 2025, mag >= 4.0, 1.413 sismos)
├── backend/
│   ├── Dockerfile                     # Imagen Python 3.11-slim optimizada para productor, consumidor y API
│   ├── requirements.txt               # Dependencias cerradas con versión fija (fastapi, aiokafka, shapely...)
│   ├── common/
│   │   ├── config.py                  # Carga centralizada y tipada de variables de entorno
│   │   └── geo.py                     # Carga del GeoJSON y motor Point-in-Polygon con shapely.STRtree
│   ├── producer/
│   │   └── main.py                    # Bucle continuo de ingesta USGS, filtro earthquake y caché SQLite
│   ├── consumer/
│   │   └── main.py                    # Deserializador Kafka, validación, cuarentena, STRtree y upsert PostgreSQL
│   ├── api/
│   │   └── main.py                    # Aplicación FastAPI, endpoints REST, métricas y CORS
│   ├── scripts/
│   │   ├── carga_historica.py         # Descarga particionada de 2025 y carga directa de query.json
│   │   └── replay.py                  # Generador de streaming acelerado con casos controlados de prueba
│   └── tests/
│       ├── test_aceptacion.py         # Suite de las 11 pruebas de aceptación del enunciado
│       ├── test_calidad_rendimiento.py# Medición de 6 dimensiones de calidad y benchmarks O(1) vs O(N)
│       └── test_stream_logic.py       # Pruebas unitarias de parsing y conversión temporal
├── frontend/
│   ├── package.json                   # Dependencias fijadas (Angular 20.3.0, Leaflet 1.9.4)
│   ├── angular.json                   # Configuración del builder de Angular CLI
│   ├── public/
│   │   ├── config.json                # Configuración dinámica en runtime (URL del backend ngrok/local)
│   │   └── data/paises.json           # GeoJSON de países para capas vectoriales
│   └── src/app/
│       ├── app.component.ts           # Lógica reactiva, subscripción a API, Leaflet y transformaciones
│       ├── app.component.html         # Plantilla semántica y accesible con filtros y gráficos
│       └── app.component.css          # Estilos 'Tectonic Dark', tarjetas analíticas y badges de estado
├── sql/
│   ├── init.sql                       # Esquema DDL (tablas, PKs compuestas, FKs e índices B-Tree)
│   ├── seed_paises.sql                # Inserción de los 177 países de Natural Earth
│   ├── queries.sql                    # Consultas de verificación, auditoría y agregación
│   └── calidad.sql                    # Consultas reproducibles de las 6 dimensiones de calidad
├── scripts/
│   └── prueba_08_reinicio_consumidor.ps1 # Automatización desde host de la Prueba 8 (parada/arranque de consumidor)
├── datos/
│   ├── paises.geojson                 # Cartografía de referencia para el cruce espacial
│   └── historico/                     # Datasets mensuales descargados y registro_extraccion_2025.json
└── docs/
    ├── memoria.md                     # Memoria técnica completa (calidad, modelos, complejidad, costes)
    ├── GUIA-DEFENSA-ORAL.md           # Guion estructurado en 5 pasos para la exposición ante el tribunal
    └── FASE-8-EVIDENCIAS-...md        # Evidencias experimentales, capturas y salidas de tests
```

---

## 3. Requisitos Previos y Versiones

Para ejecutar y validar el sistema se requiere:

* **Docker & Docker Compose:**
  * Docker Engine `>= 24.0.0` o Docker Desktop `>= 4.25` (Linux, macOS o Windows con WSL2).
  * Docker Compose `v2.x` (integrado como `docker compose`).
* **Node.js & npm (únicamente si se ejecuta o compila el frontend en la máquina host):**
  * Node.js `>= 22.12.0 LTS` (compatible con la especificación de motores de Angular 20).
  * npm `>= 10.x`.
* **ngrok CLI (opcional para exponer el túnel desde la máquina anfitriona):**
  * ngrok `v3.x` con cuenta gratuita configurada (`ngrok config add-authtoken <TOKEN>`).

---

## 4. Configuración del Entorno (`.env`)

El sistema utiliza variables de entorno para evitar credenciales quemadas en el código fuente. Copia la plantilla base:

```bash
cp .env.example .env
```

### Tabla de Variables de Entorno

| Variable | Valor por defecto | Descripción |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Dirección del broker Kafka en la red interna de Docker (`localhost:29092` desde el host). |
| `KAFKA_TOPIC` | `terremotos.live` | Nombre del topic principal para el flujo en tiempo real. |
| `KAFKA_REPLAY_TOPIC` | `terremotos.replay` | Topic secundario aislado para las pruebas de reproducción acelerada. |
| `POSTGRES_HOST` | `postgres` | Nombre de servicio del contenedor PostgreSQL en la red Docker (`localhost` desde el host). |
| `POSTGRES_PORT` | `5432` | Puerto interno de PostgreSQL (mapeado a `127.0.0.1:5432`). |
| `POSTGRES_DB` | `observatorio` | Nombre de la base de datos relacional. |
| `POSTGRES_USER` | `observatorio` | Usuario administrador de PostgreSQL. |
| `POSTGRES_PASSWORD` | `observatorio_secret_pw` | Contraseña de PostgreSQL (cámbiala en entornos no docentes). |
| `USGS_FEED_URL` | `https://.../all_day.geojson` | URL oficial del feed GeoJSON de terremotos globales de las últimas 24 horas. |
| `INGESTION_INTERVAL_SECONDS`| `60` | Período de sondeo (polling) del productor hacia la API del USGS. |
| `SNAPSHOT_RETENTION_HOURS` | `6` | Retención en horas de los snapshots originales `.geojson.gz` en `/app/data/snapshots` (el registro `extracciones.jsonl` se conserva siempre). |
| `API_HOST` / `API_PORT` | `0.0.0.0` / `8000` | Interfaz y puerto de escucha de Uvicorn/FastAPI. |
| `CORS_ORIGINS` | `http://localhost:4200,...`| Orígenes permitidos (incluye regex nativo para `*.netlify.app`). |
| `NGROK_AUTHTOKEN` | *(Vacío)* | Token de ngrok (necesario únicamente si se arranca el contenedor ngrok de Docker). |

---

## 5. Puesta en Marcha Paso a Paso (Backend y Datos)

### Paso 5.1: Construir y levantar los 5 servicios
Ejecuta la orquestación en segundo plano asegurando la construcción de las imágenes de Python:

```bash
docker compose up --build -d
```

### Paso 5.2: Comprobar el estado y los Healthchecks
Verifica que los servicios estén inicializados y con estado `healthy`:

```bash
docker compose ps
```

Deberás observar los 5 contenedores obligatorios activos:
* `observatorio-kafka` (puerto expuesto `127.0.0.1:29092`)
* `observatorio-postgres` (puerto expuesto `127.0.0.1:5432`)
* `observatorio-producer` (ingesta periódica y filtro de sismos)
* `observatorio-consumer` (consumidor con unión espacial STRtree)
* `observatorio-api` (FastAPI en `127.0.0.1:8000`)

Para inspeccionar los logs de un servicio concreto en tiempo real:
```bash
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f api
```

### Paso 5.3: Inicialización del Esquema SQL
La base de datos se inicializa de manera 100% desatendida al arrancar por primera vez mediante el archivo montado en [`sql/init.sql`](sql/init.sql) hacia `/docker-entrypoint-initdb.d/init.sql`.

Si necesitas forzar una reinicialización manual o ejecutar consultas exploratorias:
```bash
docker exec -it observatorio-postgres psql -U observatorio -d observatorio -c "\dt"
```

### Paso 5.4: Carga del Dataset Histórico Completo de 2025 y `query.json`
El proyecto incluye soporte nativo para procesar el archivo `query.json` (ejemplo de Enero 2025 provisto en el enunciado) y descargar de forma particionada el catálogo histórico completo de los 12 meses de 2025 sin violar el límite de 20.000 registros por llamada de USGS:

* **Opción A: Cargar directamente el dataset de prueba `query.json`:**
  ```bash
  docker exec -it observatorio-producer python scripts/carga_historica.py --archivo /app/query.json
  ```
  *Inserta 1.413 sismos aplicando las mismas reglas de calidad, enriquecimiento espacial y persistencia con `mode='historical'`.*

* **Opción B: Descargar y consolidar los 12 meses completos de 2025 (Recomendado):**
  ```bash
  docker exec -it observatorio-producer python scripts/carga_historica.py --limit-months 12 --min-mag 4.0
  ```
  *Descarga y procesa desde `2025_mes_01.geojson` hasta `2025_mes_12.geojson` (**18.302 sismos únicos**, con una media anual consolidada de **50.14 sismos/día** dividida entre 365 días). Los archivos se guardan localmente en `datos/historico/` y se genera la trazabilidad completa en [`datos/historico/registro_extraccion_2025.json`](datos/historico/registro_extraccion_2025.json).*

### Paso 5.5: Recuperación ante Paradas Superiores a 24 Horas (Reconciliación con el Catálogo USGS)
Dado que el feed `all_day.geojson` es una ventana móvil de 24 horas, que un terremoto desaparezca del siguiente ciclo **no significa que USGS lo haya eliminado**: simplemente ha envejecido más de 24 horas. Si el sistema permanece apagado durante más de 24 horas:
1. Consulta el instante del último evento `live` almacenado antes de la parada:
   ```bash
   docker exec -it observatorio-postgres psql -U observatorio -d observatorio -c "SELECT MAX(time_utc) FROM evento_actual WHERE mode='live';"
   ```
2. Descarga el intervalo faltante (`[starttime, endtime]`) desde el endpoint de catálogo FDSNWS (`/fdsnws/event/1/query?format=geojson&starttime=<ULTIMO_UTC>&endtime=<AHORA_UTC>&eventtype=earthquake&orderby=time-asc`) e ingéstalo con `carga_historica.py --archivo hueco.geojson`. Gracias al `ON CONFLICT (event_id) DO UPDATE WHERE EXCLUDED.updated_utc > evento_actual.updated_utc`, los extremos solapados se deduplican sin duplicar filas.
*(Limitación documentada del alcance mínimo: la reconciliación retrospectiva de eventos que USGS elimine, fusione o revise semanas después de haber salido de la ventana de 24 h requeriría un job periódico de re-sincronización contra FDSNWS).*

---

## 6. Frontend Angular, ngrok y Despliegue en Netlify

El frontend es una Single Page Application (SPA) desarrollada en Angular 20 que consume la API REST mediante la URL definida en [`frontend/public/config.json`](frontend/public/config.json).

### Paso 6.1: Conexión mediante Túnel HTTPS con ngrok
Dado que Netlify sirve la aplicación bajo HTTPS, cualquier petición HTTP directa a `localhost:8000` sería bloqueada por los navegadores debido a políticas de *Mixed Content*. Por ello, se publica la API a través de un túnel ngrok.

* **Si ejecutas ngrok en tu máquina host:**
  ```bash
  ngrok http 8000
  ```
  Copia la URL pública generada (ej. `https://upturned-blast-tigress.ngrok-free.dev`).

* **Si ejecutas ngrok mediante Docker Compose:**
  Asegúrate de haber colocado tu authtoken en `.env` y arranca el perfil:
  ```bash
  docker compose --profile tunnel up -d ngrok
  ```
  Consulta la URL asignada inspeccionando la API local del contenedor en `http://127.0.0.1:4040/api/tunnels`.

### Paso 6.2: Configurar `config.json`
Edita [`frontend/public/config.json`](frontend/public/config.json) indicando la URL HTTPS del túnel **sin barra ni sufijo `/api` al final**:

```json
{
  "mode": "api",
  "apiUrl": "https://tu-subdominio.ngrok-free.dev",
  "refreshMs": 60000,
  "skipNgrokWarning": true
}
```
*(El parámetro `skipNgrokWarning: true` inyecta automáticamente la cabecera HTTP `ngrok-skip-browser-warning: "true"` en cada petición para saltarse la pantalla intermedia de aviso de ngrok).*

### Paso 6.3: Ejecución en Desarrollo Local
Para trabajar en local con recarga en vivo:
```bash
cd frontend
npm install
npm start
```
Accede en tu navegador a: [http://localhost:4200](http://localhost:4200).

### Paso 6.4: Compilación y Publicación en Netlify
1. **Compilar para producción:**
   ```bash
   cd frontend
   npm run build
   ```
   Los artefactos optimizados se generarán en `frontend/dist/observatorio/browser/`.

2. **Despliegue Continuo (Git):**
   El archivo [`netlify.toml`](netlify.toml) en la raíz configura automáticamente el despliegue:
   - **Base directory:** `frontend`
   - **Build command:** `npm run build`
   - **Publish directory:** `dist/observatorio/browser`

3. **Despliegue Manual (Netlify Drop):**
   Si se realiza subida manual directa, arrastra únicamente el contenido de la carpeta `frontend/dist/observatorio/browser` en el panel de Netlify Drop.

> **Resiliencia ante Backend Apagado:** Si el backend local o el túnel ngrok se detienen, la interfaz en Netlify no colapsa: muestra un badge visual de desconexión en la cabecera (*"Backend Desconectado"*), preserva los últimos datos cargados en memoria y continúa reintentando la conexión cada 60 segundos.

---

## 7. Pruebas de Aceptación y Reproducción Acelerada (`replay`)

El sistema incluye una suite de pruebas automatizadas y un emulador de streaming para validar todos los requisitos funcionales sin depender de que ocurran sismos reales durante la evaluación.

### 7.1 Batería de Pruebas de Aceptación Automatizadas
Ejecuta la suite que valida las 11 casuísticas del enunciado (duplicados exactos, revisiones posteriores, versiones atrasadas, cuarentena de coordenadas inválidas, manejo de magnitudes nulas, eventos marítimos y modo replay):

```bash
docker exec -e POSTGRES_HOST=postgres -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e API_URL=http://api:8000 observatorio-producer python tests/test_aceptacion.py
```

Para ejecutar la **Prueba 8 (parada, publicación en cola y reinicio del consumidor)** automatizada desde el host PowerShell:
```powershell
.\scripts\prueba_08_reinicio_consumidor.ps1
```

### 7.2 Análisis de Calidad y Benchmarks de Rendimiento
Para medir las 6 dimensiones de calidad (completitud, unicidad, validez, consistencia, actualidad y exactitud) tanto desde Python como mediante el script SQL reproducible [`sql/calidad.sql`](sql/calidad.sql):

```bash
docker exec -e POSTGRES_HOST=postgres observatorio-producer python tests/test_calidad_rendimiento.py
Get-Content sql/calidad.sql | docker exec -i observatorio-postgres psql -U observatorio -d observatorio
```

### 7.3 Demostración con Reproducción Acelerada (Replay)
Para reproducir una secuencia controlada de terremotos históricos transmitidos evento a evento por Kafka a velocidad acelerada (1 evento cada 1.5s):

```bash
docker exec -it observatorio-producer python scripts/replay.py --delay 1.5 --count 15
```

* **Aislamiento de Datos:** Los eventos se envían al topic `terremotos.replay` con el flag `mode='replay'`.
* **Visualización:** En el frontend, selecciona el filtro de modo **"Reproducción (Replay)"** para observar la llegada de los eventos en streaming sin alterar las estadísticas de datos en vivo (`live`).
* **Casos Sintéticos de la FAQ Q12:** El script inyecta automáticamente los casos de prueba `demo-q12-E1` (duplicado y revisión de magnitud 4.2 a 4.6), `demo-q12-E2` (nuevo sismo) y `demo-q12-E3-ANOMALIA` (coordenadas 999.0 enviadas a cuarentena).

---

## 8. Endpoints Principales de la API FastAPI

La API documenta interactivamente sus contratos OpenAPI en `http://localhost:8000/docs`:

| Método | Endpoint | Parámetros de Consulta | Descripción |
| :--- | :--- | :--- | :--- |
| `GET` | `/` | Ninguno | Información general del servicio, versión y enlaces a endpoints. |
| `GET` | `/api/salud` | Ninguno | Healthcheck del backend: estado de PostgreSQL, latido de última ingesta y latencia. |
| `GET` | `/api/eventos` | `min_mag`, `country_id`, `start`, `end`, `mode`, `limit`, `offset` | GeoJSON FeatureCollection de terremotos con soporte de filtros combinados. |
| `GET` | `/api/estadisticas` | `min_mag`, `country_id` | Agregación horaria UTC (24h), histogramas por magnitud/profundidad y **comparativa histórica frente a la media diaria de 2025**. |
| `GET` | `/api/paises` | Ninguno | Listado ordenado de países para los selectores del frontend con totales agregados. |

---

## 9. Guía Rápida para la Defensa Oral (Activación en 5 Pasos)

Sigue este procedimiento durante la exposición para levantar el sistema en menos de 2 minutos:

```
[ Paso 1: Backend ]      docker compose up --build -d
[ Paso 2: Verificación ] docker compose ps   (comprobar 5 servicios 'healthy')
[ Paso 3: Túnel ngrok ]  ngrok http 8000     (obtener URL HTTPS)
[ Paso 4: Frontend ]     Verificar config.json y abrir URL de Netlify
[ Paso 5: Demostración ] Ejecutar: docker exec -it observatorio-producer python scripts/replay.py --delay 1.0
```

1. **Paso 1:** Arrancar los servicios con `docker compose up --build -d`.
2. **Paso 2:** Verificar en consola que `observatorio-kafka`, `postgres`, `producer`, `consumer` y `api` están en estado saludable con `docker compose ps`.
3. **Paso 3:** Lanzar el túnel ngrok y comprobar que `https://tu-subdominio.ngrok-free.dev/api/salud` responde con `postgres: "conectado"`.
4. **Paso 4:** Abrir el panel en Netlify. Demostrar el mapa 'Tectonic Dark', los sismos de las últimas 24h, los filtros por país (ej. Chile, Japón, EE.UU.) y la comparativa histórica de 2025.
5. **Paso 5:** Lanzar el script de reproducción acelerada (`python scripts/replay.py --delay 1.0`) y cambiar en la web al modo *Replay* para ver cómo los marcadores y las gráficas se actualizan en vivo.

*(El guion exhaustivo con respuestas a preguntas frecuentes del tribunal se encuentra en [`docs/GUIA-DEFENSA-ORAL.md`](docs/GUIA-DEFENSA-ORAL.md)).*

---

## 10. Detención del Sistema y Ciclo de Vida de los Datos

* **Detener los servicios conservando el estado y los datos:**
  ```bash
  docker compose stop
  ```
  *(Para reanudar la ejecución sin reconstruir: `docker compose start`).*

* **Detener y eliminar contenedores manteniendo los volúmenes intactos:**
  ```bash
  docker compose down
  ```
  *Los datos de PostgreSQL (`observatorio_postgres_data`), los offsets de Kafka (`observatorio_kafka_data`) y la caché de ingesta (`observatorio_producer_data`) permanecen almacenados de forma persistente en el disco de Docker.*

* **Eliminación completa para empezar desde cero:**
  ```bash
  docker compose down -v
  ```
  *Elimina todos los contenedores, redes y volúmenes persistentes. El siguiente arranque reconstruirá las tablas y el estado desde cero.*
