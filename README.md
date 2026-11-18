# Observatorio de Actividad Sísmica

[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-blue?logo=docker)](compose.yaml)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-yellow?logo=python)](backend/)
[![FastAPI](https://img.shields.io/badge/FastAPI-1.0.0-green?logo=fastapi)](backend/api/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.0-black?logo=apachekafka)](compose.yaml)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16--alpine-blue?logo=postgresql)](sql/)
[![Angular](https://img.shields.io/badge/Angular-20-red?logo=angular)](frontend/)
[![Netlify](https://img.shields.io/badge/Netlify-Deployed-00C7B7?logo=netlify)](https://app.netlify.com/)

**Sistemas de Big Data · Consolidación de Sesiones 1–8 · RA1**  
Sistema distribuido de ingesta continua, procesamiento en streaming, control de revisiones con idempotencia, persistencia dimensional, API REST analítica y panel cartográfico interactivo para la monitorización de actividad sísmica global y su comparación con el histórico de 2025 de USGS.

---

## 1. Arquitectura del Sistema

El sistema implementa los cinco servicios desacoplados obligatorios en Docker Compose, más túnel seguro ngrok y panel SPA en Netlify:

```
[ USGS Earthquakes Feed ] ──(all_day.geojson cada 60s)──> [ Productor Python ]
                                                                 │
                                                    (Kafka Topic: terremotos.live)
                                                                 ▼
                                                        [ Broker Kafka 3.8 ]
                                                                 │
                                                       (Manual Commit Offset)
                                                                 ▼
[ Catálogo Natural Earth ] ─────────────────────────> [ Consumidor Python ]
  (177 países STRtree)                                           │
                                                     (Idempotent Upsert ACID)
                                                                 ▼
                                                       [ PostgreSQL 16 ]
                                                     (evento_actual / version)
                                                                 ▲
                                                                 │ (Consultas SQL)
                                                       [ API REST FastAPI ]
                                                                 ▲
                                                                 │ (Túnel HTTPS)
                                                           [ ngrok ]
                                                                 ▲
                                                                 │ (REST JSON)
                                                        [ Panel Angular 20 ]
                                                          (Netlify SPA)
```

---

## 2. Requisitos Previos

- **Docker y Docker Compose:** Docker Desktop 4.x o motor Docker en Linux/WSL2 con soporte de Compose v2.
- **Node.js (para desarrollo local del frontend):** Node.js 22.x LTS (o 20.x) y `npm 10.x`.
- **Python (opcional para pruebas fuera de Docker):** Python 3.11 con `pip`.
- **ngrok:** Cuenta gratuita de [ngrok](https://ngrok.com/) para exponer el túnel HTTPS.

---

## 3. Puesta en Marcha Rápida (Reproducible)

### Paso 1: Configurar variables de entorno
Copia la plantilla de variables de entorno y ajusta tus credenciales locales:
```powershell
cp .env.example .env
```
*(El archivo `.env.example` contiene valores por defecto funcionales para ejecución local).*

### Paso 2: Construir y levantar los contenedores
Ejecuta los cinco servicios obligatorios en segundo plano:
```powershell
docker compose up --build -d
```
Verifica que los contenedores estén levantados y saludables (`healthy`):
```powershell
docker compose ps
```
Deberás observar activos:
* `observatorio-kafka` (puerto `127.0.0.1:29092`)
* `observatorio-postgres` (puerto `127.0.0.1:5432`)
* `observatorio-producer` (ingesta periódica cada 60s)
* `observatorio-consumer` (validación, enriquecimiento espacial y persistencia)
* `observatorio-api` (FastAPI escuchando en `127.0.0.1:8000`)

### Paso 3: Carga del catálogo de países y datos históricos
La base de datos se inicializa automáticamente con el esquema de tablas e índices definido en [`sql/init.sql`](sql/init.sql).

Para cargar el catálogo histórico de 2025 (2.807 terremotos mundiales $\ge 4.0$ divididos por meses):
```powershell
docker exec -it observatorio-producer python -m scripts.carga_historica
```

### Paso 4: Iniciar el túnel seguro ngrok
Si usas ngrok en tu máquina anfitriona:
```powershell
ngrok http 8000
```
Obtendrás una URL HTTPS pública (ej. `https://upturned-blast-tigress.ngrok-free.dev`).

### Paso 5: Ejecutar el Frontend Angular

**Opción A: Ejecución local en desarrollo:**
1. Configura la URL pública o local en [`frontend/public/config.json`](frontend/public/config.json):
   ```json
   {
     "mode": "api",
     "apiUrl": "https://tu-subdominio.ngrok-free.dev",
     "refreshMs": 60000,
     "skipNgrokWarning": true
   }
   ```
2. Instala dependencias y arranca el servidor Angular:
   ```powershell
   cd frontend
   npm ci
   npm start
   ```
3. Abre en tu navegador: [http://localhost:4200](http://localhost:4200)

**Opción B: Despliegue en Netlify (Producción):**
* El proyecto incluye [`netlify.toml`](netlify.toml) en la raíz configurado con:
  * **Base directory:** `frontend`
  * **Build command:** `npm run build`
  * **Publish directory:** `dist/observatorio/browser`
* Si usas Netlify Drop (subida manual), arrastra la carpeta: `frontend/dist/observatorio/browser`.

---

## 4. Endpoints de la API FastAPI

La API expone los contratos definidos en [`CONTRATO-API.md`](CONTRATO-API.md):

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/` | Información del servicio y enlaces de navegación |
| `GET` | `/docs` | Documentación interactiva Swagger UI |
| `GET` | `/api/salud` | Estado de conexión con PostgreSQL y tiempo de última ingesta |
| `GET` | `/api/eventos` | GeoJSON FeatureCollection con filtros (`start`, `end`, `min_mag`, `country_id`, `mode`) |
| `GET` | `/api/estadisticas` | Agregaciones por ventana horaria UTC, histogramas y comparativa histórica 2025 |
| `GET` | `/api/paises` | Catálogo de países ordenados para los selectores del frontend |

---

## 5. Pruebas de Aceptación y Demostración en Vivo

### Batería de Pruebas de Aceptación Automatizadas
Ejecuta la suite completa de las 11 pruebas del enunciado (duplicados, revisiones, versiones atrasadas, cuarentena, sismos marítimos y modo replay):
```powershell
python backend/tests/test_aceptacion.py
```

### Demostración con Reproducción Acelerada (Replay)
Para demostrar el flujo completo de streaming durante la defensa sin esperar sismos reales:
```powershell
docker exec -it observatorio-producer python -m scripts.replay --delay 1.5
```
En el frontend, selecciona el selector de modo **"Reproducción (Replay)"** para visualizar los eventos llegando en vivo por Kafka hacia PostgreSQL y el mapa.

---

## 6. Estructura del Repositorio

```
observatorio-sismico/
├── .env.example               # Plantilla de variables de entorno segura
├── compose.yaml               # Orquestación de los 5 servicios Docker
├── netlify.toml               # Configuración de despliegue en Netlify
├── backend/
│   ├── Dockerfile             # Imagen reproducible para los servicios Python
│   ├── requirements.txt       # Dependencias fijadas (aiokafka, fastapi, shapely, psycopg2...)
│   ├── common/                # Configuración y módulo espacial STRtree
│   ├── producer/              # Ingesta periódica USGS con caché SQLite local
│   ├── consumer/              # Validación, cuarentena, STRtree y upsert idempotente
│   ├── api/                   # API REST FastAPI con CORS estricto
│   ├── scripts/               # Carga histórica 2025 y reproductor Replay
│   └── tests/                 # Suites de pruebas de aceptación y rendimiento
├── frontend/
│   ├── package.json           # Dependencias fijadas (Angular 20, Leaflet 1.9.4)
│   ├── src/app/               # Componentes del mapa, filtros, tabla accesible y gráficos
│   └── public/                # Configuración dinámica y catálogo de países
├── sql/
│   ├── init.sql               # Esquema dimensional, tablas de hechos, versiones y cuarentena
│   └── seed_paises.sql        # Inserción inicial de los 177 países de Natural Earth
└── docs/
    ├── memoria.md             # Memoria técnica completa del proyecto
    ├── GUIA-DEFENSA-ORAL.md   # Guion estructurado para la defensa y exposición del proyecto
    └── FASE-8-EVIDENCIAS...   # Evidencias experimentales de calidad y benchmarks
```

---

## 7. Detención del Sistema y Persistencia

Para detener los contenedores conservando todos los datos en los volúmenes persistentes de Docker:
```powershell
docker compose stop
```

Para reiniciar:
```powershell
docker compose start
```

Para eliminar contenedores y redes manteniendo los volúmenes de base de datos y Kafka:
```powershell
docker compose down
```

*(Si deseas borrar también los volúmenes de datos para empezar desde cero: `docker compose down -v`)*.
