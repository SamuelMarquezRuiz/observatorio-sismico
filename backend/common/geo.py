import json
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List
from shapely.geometry import shape, Point
from shapely.strtree import STRtree

logger = logging.getLogger("geo")

class CatalogoPaises:
    """
    Catálogo espacial de países basado en Natural Earth 1:110m (paises.geojson).
    Utiliza shapely.STRtree (R-Tree en memoria) para búsquedas espaciales O(log N).
    """

    def __init__(self, geojson_path: Optional[str] = None):
        self.geojson_path = geojson_path or self._find_geojson_path()
        self.geometries: List[Any] = []
        self.metadata: List[Dict[str, Any]] = []
        self.tree: Optional[STRtree] = None
        self._load_catalog()

    def _find_geojson_path(self) -> str:
        candidates = [
            Path("/app/datos/paises.geojson"),
            Path(__file__).resolve().parent.parent / "datos" / "paises.geojson",
            Path(__file__).resolve().parent.parent.parent / "frontend" / "public" / "datos" / "paises.geojson",
            Path("datos/paises.geojson"),
        ]
        for p in candidates:
            if p.exists():
                return str(p)
        raise FileNotFoundError(
            f"No se encontró paises.geojson en ninguna de las rutas esperadas: {[str(c) for c in candidates]}"
        )

    def _load_catalog(self):
        logger.info(f"Cargando catálogo espacial de países desde: {self.geojson_path}")
        with open(self.geojson_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        features = data.get("features", [])
        for feat in features:
            geom = shape(feat["geometry"])
            props = feat.get("properties", {})
            country_id = props.get("ADM0_A3")
            name = props.get("NAME_ES") or props.get("NAME") or country_id
            continent = props.get("CONTINENT")
            subregion = props.get("SUBREGION")

            self.geometries.append(geom)
            self.metadata.append({
                "country_id": country_id,
                "name": name,
                "continent": continent,
                "subregion": subregion
            })

        self.tree = STRtree(self.geometries)
        logger.info(f"Catálogo espacial cargado con éxito: {len(self.geometries)} entidades indexadas con STRtree.")

    def asignar_pais(self, lon: float, lat: float) -> Tuple[Optional[str], Optional[str]]:
        """
        Determina a qué país pertenece un punto (lon, lat).
        
        Reglas de negocio:
        1. Validez de coordenadas: lon [-180, 180], lat [-90, 90].
        2. Búsqueda espacial optimizada con STRtree: O(log N) para filtrar bounding boxes.
        3. En caso de solape o frontera: resolución determinista seleccionando el primer country_id alfabéticamente.
        4. Si el punto no interseca ningún polígono (ej. evento oceánico/marítimo):
           retorna (None, "Sin país asignado").
        """
        if not (-180.0 <= lon <= 180.0) or not (-90.0 <= lat <= 90.0):
            raise ValueError(f"Coordenadas fuera de rango válido: lon={lon}, lat={lat}")

        punto = Point(lon, lat)

        # Consultar el árbol espacial STRtree para obtener candidatos cuyos bounding boxes contengan el punto
        indices_candidatos = self.tree.query(punto, predicate="intersects")

        matches = []
        for idx in indices_candidatos:
            geom = self.geometries[idx]
            # Validar si efectivamente contiene el punto o lo toca
            if geom.contains(punto) or geom.touches(punto):
                matches.append(self.metadata[idx])

        if not matches:
            # Evento marítimo o fuera de costa
            return None, "Sin país asignado"

        if len(matches) > 1:
            # Regla de desempate determinista: ordenar por country_id
            matches.sort(key=lambda x: x["country_id"])

        ganador = matches[0]
        return ganador["country_id"], ganador["name"]

# Instancia singleton perezosa
_catalogo_instancia: Optional[CatalogoPaises] = None

def get_catalogo() -> CatalogoPaises:
    global _catalogo_instancia
    if _catalogo_instancia is None:
        _catalogo_instancia = CatalogoPaises()
    return _catalogo_instancia
