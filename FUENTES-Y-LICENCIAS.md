# Fuentes y licencias

## Catálogo incluido

`frontend/public/datos/paises.geojson`: copia sin modificar de Natural Earth, Admin 0 Countries, escala 1:110.000.000, 177 entidades en esta copia.

- Descargado: 27 de octubre de 2026.
- Fuente: https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson
- Proyecto: https://www.naturalearthdata.com/
- Licencia de los datos: dominio público. Se conserva el reconocimiento de procedencia.
- SHA-256: `6866c877d39cba9c357620878839b336d569f8c662d3cfab4cb1dbe2d39c977f`
- El repositorio fuente puede cambiar: usa la copia entregada para reproducibilidad.
- `ADM0_A3` identifica la entidad Natural Earth; no asumir que todos los códigos son ISO oficiales. `NAME_ES`, `ADMIN`, `CONTINENT` y `SUBREGION` contienen atributos útiles.
- Fronteras simplificadas y convenciones cartográficas del proveedor; no sirve para delimitar jurisdicción legal ni para asignar con precisión eventos costeros. No incluye todos los pequeños territorios. El mar no es un país.

## Visualización y Capas Cartográficas (Sin API Key)

- **Leaflet** (BSD-2-Clause): https://leafletjs.com/
- **ngx-leaflet** (MIT): https://github.com/bluehalo/ngx-leaflet
- **Angular 20** (MIT): https://angular.dev/
- **Mapas base integrados en el selector de capas (`L.control.layers`):**
  - *Modo Oscuro (Esri — predeterminado)*: `World_Dark_Gray_Base` — `Tiles © Esri — Esri, DeLorme, NAVTEQ`.
  - *Océanos y Batimetría (Esri)*: `World_Ocean_Base` — `Tiles © Esri — Sources: GEBCO, NOAA, CHS, OSU, UNH, CSUMB, National Geographic, DeLorme`.
  - *Relieve Físico (Esri)*: `World_Physical_Map` — `Tiles © Esri — Source: US National Park Service`.
  - *Topográfico (OpenTopoMap)*: `https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png` — Datos `© OpenStreetMap contributors, SRTM`, estilo `© OpenTopoMap (CC-BY-SA 3.0)`.
  - *Vista Satélite (Esri)*: `World_Imagery` — `Tiles © Esri — Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community`.
  - *Mapa Estándar (OSM)*: atribución visible obligatoria y política https://operations.osmfoundation.org/policies/tiles/ . No descargar masivamente ni precargar para uso offline.
- **Límites de Placas Tectónicas (PB2002):** Modelo de Peter Bird (2003), *An updated digital model of plate boundaries*, Geochemistry, Geophysics, Geosystems ($G^3$), conversión GeoJSON pública de Hugo Ahlenius (`fraxen/tectonicplates`, licencia ODC-By / dominio académico).
- El GeoJSON de países está incluido localmente; las bibliotecas se descargan mediante npm. No se distribuyen teselas precargadas.

## Sismicidad y docencia

- Feed: https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
- Formato: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
- Histórico: https://earthquake.usgs.gov/fdsnws/event/1/
- Sesiones: https://juanarrow.github.io/BigData/sesiones/index.html
- Rúbrica: https://juanarrow.github.io/BigData/sesiones/sesion-08.html

Los eventos del archivo `terremotos-demo.geojson` son ficticios y están marcados como demostración. No utilizarlos para extraer conclusiones sobre sismicidad real.

El código de apoyo original de este paquete se ofrece para uso, modificación y distribución educativa sin restricciones adicionales. Las dependencias conservan sus propias licencias.
