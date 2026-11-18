# Contrato mínimo de la API

## GET /api/eventos

Devuelve HTTP 200, `Content-Type: application/json` y un GeoJSON `FeatureCollection`. Sin eventos: `features: []`, no un error. Cada evento aparece una sola vez, con su versión actual. Por defecto devuelve terremotos ocurridos en las últimas 24 horas.

```json
{
  "type": "FeatureCollection",
  "metadata": {
    "generated": 1793113200000,
    "last_ingestion": 1793113140000,
    "mode": "live",
    "count": 1,
    "truncated": false
  },
  "features": [{
    "type": "Feature",
    "id": "ejemplo-ficticio",
    "geometry": {"type": "Point", "coordinates": [-3.7, 40.4, 12]},
    "properties": {
      "mag": 3.2,
      "magType": "ml",
      "place": "Ejemplo ficticio",
      "time": 1793109600000,
      "updated": 1793110000000,
      "country_id": "ESP",
      "country_name": "España"
    }
  }]
}
```

`generated`: generación de esta respuesta; no implica que la ingesta siga activa. `last_ingestion`: última consulta correcta a USGS. Fechas en milisegundos Unix UTC. Coordenadas en orden longitud, latitud, profundidad (km). Usa `null`, nunca NaN/Infinity, para magnitud, profundidad o país desconocidos. Geometría válida obligatoria en este endpoint.

Filtros a implementar: `start` (ISO8601, inclusive), `end` (ISO8601, exclusive), `min_mag` y `country_id`. El mapa inicial aún no envía filtros; corresponde al alumnado añadirlos. La magnitud nula no cumple un filtro `min_mag` y debe contabilizarse por separado.

Para limitar el tamaño, la API puede aplicar un máximo documentado de puntos. Si trunca, debe indicar `truncated: true`, el total completo y un mecanismo de paginación. El panel final deberá mostrarlo y no calcular estadísticas globales sobre una muestra truncada. El componente inicial espera una colección pequeña y completa.

## Otros endpoints implementados

| Ruta | Parámetros | Resultado |
|---|---|---|
| `GET /` | Ninguno | Metadatos del servicio y enlaces de descubrimiento (`/docs`, `/api/salud`, `/api/eventos`, `/api/estadisticas`, `/api/paises`). |
| `GET /api/salud` | Ninguno | Estado de conexión a PostgreSQL (`postgres_connected`), recuento total (`total_eventos_base`), último latido de ingesta (`last_ingestion`, `last_ingestion_utc`), `segundos_desde_ultima_ingesta` y `estado_ingesta` (`activa` $\le 180\text{ s}$ o `desactualizada`). Nunca expone secretos ni credenciales. |
| `GET /api/eventos` | `start`, `end`, `min_mag`, `country_id` (`ADM0_A3` o `SIN_PAIS`), `mode` (`live`, `replay`, `historical`), `limit` (hasta `25000`) | `FeatureCollection` GeoJSON con cada sismo en su versión vigente más reciente (`evento_actual`), `metadata.total_coincidentes` y `metadata.truncated`. |
| `GET /api/estadisticas` | `start`, `end`, `min_mag`, `country_id`, `mode` (`live`, `replay`, `historical`) | `total_eventos`, `resumen_metricas` (incluyendo `total_magnitud_nula`), `por_ventana_hora` (granularidad adaptativa UTC: `hour` en ventanas $\le 3$ días y `day` en rangos mayores o `historical`, informada en `metadata.temporal_granularity`), `por_pais` (incluyendo `"Sin país asignado"`), `distribucion_magnitud`, `distribucion_profundidad` y `comparacion_historica` (últimas 24 h `mag >= 4.0` frente a la media diaria de los 365 días de 2025: `18.302 / 365 = 50,14 sismos/día`). |
| `GET /api/paises` | Ninguno | Lista ordenada de los 177 países del catálogo Natural Earth (`country_id`, `name`, `continent`, `subregion`) para poblar selectores del panel. |

Los filtros de `/api/estadisticas` equivalen exactamente a los de `/api/eventos`. Cada resultado indica periodo, filtros, unidad y registros sin valor (`ND`). Si la base de datos falla, devuelve HTTP 500 con detalle explícito; nunca devuelve cero eventos como si fuera una respuesta válida.

## Acceso desde Netlify

Configura CORS en FastAPI para el origen exacto de la web y `http://localhost:4200` durante desarrollo; métodos GET y OPTIONS, y cabecera `ngrok-skip-browser-warning`. Si utilizas `CORSMiddleware`, este middleware atiende el preflight. No necesitas cookies para una API pública de consulta de estos datos.

El encabezado de ngrok evita su página intermedia cuando es aplicable; no autentica al usuario. No incluir secretos en config.json. Añade timeout y manejo de errores en el frontend.

## Transporte y contenedores

La API es un servicio Docker, igual que productor, consumidor, Kafka y PostgreSQL. Uvicorn escucha en `0.0.0.0:8000`. ngrok termina el HTTPS público y gestiona el certificado; la API recibe HTTP local. El frontend utiliza la URL HTTPS del túnel. Desde otro contenedor de la misma red, el agente se conecta a `http://api:8000`; desde el host, al puerto publicado.
