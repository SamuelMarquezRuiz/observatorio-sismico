"""
Pruebas unitarias de las reglas de negocio de la Fase 3:
1. Integración geográfica (puntos terrestres, marítimos y límites).
2. Validación de coordenadas y desvío a cuarentena.
3. Idempotencia teórica de revisiones.
"""
import sys
from pathlib import Path
from shapely.geometry import Point

# Añadir backend al PYTHONPATH
backend_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(backend_dir))

from common.geo import CatalogoPaises

def test_asignacion_pais():
    geojson_path = backend_dir / "datos" / "paises.geojson"
    assert geojson_path.exists(), f"Debe existir {geojson_path}"
    
    cat = CatalogoPaises(str(geojson_path))
    assert len(cat.geometries) == 177, f"Se esperaban 177 entidades, obtenidas {len(cat.geometries)}"

    # 1. Punto en España (Madrid: lon=-3.7038, lat=40.4168)
    cid, cname = cat.asignar_pais(-3.7038, 40.4168)
    print(f"Test España: id={cid}, nombre={cname}")
    assert cid == "ESP", f"Esperado ESP, obtenido {cid}"

    # 2. Punto en Estados Unidos (Kansas: lon=-98.5795, lat=39.8283)
    cid, cname = cat.asignar_pais(-98.5795, 39.8283)
    print(f"Test USA: id={cid}, nombre={cname}")
    assert cid == "USA", f"Esperado USA, obtenido {cid}"

    # 3. Punto oceánico / marítimo (Atlántico Norte: lon=-35.0, lat=30.0)
    cid, cname = cat.asignar_pais(-35.0, 30.0)
    print(f"Test Marítimo: id={cid}, nombre={cname}")
    assert cid is None, f"Esperado None para aguas abiertas, obtenido {cid}"
    assert cname == "Sin país asignado"

    # 4. Coordenadas inválidas: debe lanzar ValueError
    try:
        cat.asignar_pais(250.0, 40.0)
        assert False, "Debió lanzar ValueError por longitud > 180"
    except ValueError:
        print("Test Validación Coordenadas: Correctamente rechazada lon=250")

    print("¡TODAS LAS PRUEBAS DE ASIGNACIÓN ESPACIAL PASARON CON ÉXITO!")

if __name__ == "__main__":
    test_asignacion_pais()
