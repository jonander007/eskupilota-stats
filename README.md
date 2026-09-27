# EskupilotaStats

Estadísticas de pelota a mano profesional: <https://www.eskupilotastats.com>

Web estática (GitHub Pages) con resultados, cartelera, comparador, perfiles de
pelotaris, ranking y mapa de frontones, en castellano y euskera. Instalable como PWA.

## Estructura

```
index.html            Página (HTML)
css/app.css           Estilos
js/app.js             Lógica de la web
js/analisis.js        Enlaces directos, Elo, campeonatos, gráficos del perfil,
                      previa de la cartelera y ficha de frontón
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
  red.py                Descarga con reintentos (común a los scrapers)
  competiciones.py      Criterio para asignar competición y tipo a cada partido
  roles.py              Rol (delantero/zaguero) de cada pelotari según sus partidos
tests/                  Pruebas de los scrapers con casos reales (sin red)
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

## Varias fuentes

Los dos scrapers tienen una lista `FUENTES` (nombre, URL, función que lee la
página). De la primera fuente se guarda todo; de las siguientes, solo lo que
no esté ya:

- Resultados: es el mismo partido si coincide la fecha y los pelotaris (en
  cualquier orden y aunque cambien tildes o mayúsculas). Si el tanteo no
  coincide se conserva el guardado y queda un aviso en
  `data/avisos_scraper.json`. Cada partido nuevo lleva `"fuente"`.
- Cartelera: es el mismo evento si coincide el día y el frontón, o el día y
  algún partido.

Si una fuente falla, se guarda lo de las demás y la ejecución sale en rojo.

## Pruebas

```bash
python -m unittest discover tests
```

Se ejecutan en cada push (`.github/workflows/tests.yml`) y antes del scraping
diario.

## Desarrollo local

```bash
pip install -r requirements.txt
python scraper/scraper_cartelera.py
python scraper/scraper.py
python -m http.server        # y abrir http://localhost:8000
```

Los scripts de `tools/` se ejecutan desde la raíz del repositorio, p. ej.
`python tools/recalcular_contadores.py --dry-run`.

## Enlaces directos

Cada vista tiene su URL, que se puede compartir:

- `#/pelotari/jaka`, `#/fronton/labrit`, `#/campeonato/COMP026`
- `#/resultados`, `#/cartelera`, `#/comparador`, `#/pelotaris`, `#/frontones`,
  `#/ranking`, `#/campeonatos`, `#/contacto`

## Elo

Puntuación de fuerza de cada pelotari (empieza en 1500) calculada en el
navegador con todos los partidos (`js/analisis.js`). La fuerza de un equipo es
la media de sus pelotaris; K=24 en partidos oficiales y 12 en festivales.
Probado con los partidos desde 2025 acierta el ganador en ~56% de los casos
(los partidos suelen estar muy igualados), así que el pronóstico de la
cartelera se presenta como orientativo.
