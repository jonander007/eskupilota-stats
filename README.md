# EskupilotaStats

Estadísticas de pelota a mano profesional: <https://www.eskupilotastats.com>

Web estática (GitHub Pages) con resultados, cartelera, comparador, perfiles de
pelotaris, ranking y mapa de frontones, en castellano y euskera. Instalable como PWA.

## Estructura

```
index.html            Página (HTML)
css/app.css           Estilos
js/app.js             Lógica de la web
sw.js, manifest.json  PWA (caché sin conexión e instalación)
data/                 Datos que lee la web
  partidos.json         Partidos, con referencias por ID a los catálogos
  pelotaris.json        Catálogo de pelotaris (PEL###)
  frontones.json        Catálogo de frontones (FRO###), con lat/lon para el mapa
  ciudades.json         Catálogo de ciudades (CIU###), con nombre en ES y EU
  competiciones.json    Catálogo de competiciones (COMP###)
  cartelera.json        Próximos partidos
scraper/
  scraper.py            Añade resultados nuevos desde baikopilota.eus/resultados
  scraper_cartelera.py  Regenera la cartelera desde baikopilota.eus/entradas
  competiciones.py      Criterio para asignar competición y tipo a cada partido
  roles.py              Rol (delantero/zaguero) de cada pelotari según sus partidos
tools/
  validar_datos.py      Comprueba la coherencia de data/ (lo usa el workflow)
  recalcular_contadores.py  Recalcula partidos_count y roles
  ...                   Scripts de limpieza puntuales (migración, auditorías)
```

## Actualización de datos

El workflow `.github/workflows/datos.yml` se ejecuta cada día a las 00:01 UTC
(y a mano desde la pestaña *Actions*): lanza el scraper de resultados (que
aún lee la cartelera del día anterior para saber la competición), después el
de cartelera, valida los datos con `tools/validar_datos.py` y, si todo es
correcto, hace un único commit con los cambios en `data/`.

## Desarrollo local

```bash
pip install -r requirements.txt
python scraper/scraper_cartelera.py
python scraper/scraper.py
python -m http.server        # y abrir http://localhost:8000
```

Los scripts de `tools/` se ejecutan desde la raíz del repositorio, p. ej.
`python tools/recalcular_contadores.py --dry-run`.
