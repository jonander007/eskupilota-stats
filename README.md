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
vendor/leaflet/       Leaflet 1.9.4 (mapa); se carga al abrir Frontones
robots.txt, og-image.jpg  Buscadores y vista previa al compartir
pelotari/, eu/pelotari/  Página estática de cada pelotari (ES/EU), generada
sitemap.xml           Generado junto con las páginas de pelotaris
data/                 Datos que lee la web (listas con un elemento por línea)
  partidos.json         Partidos, con referencias por ID a los catálogos
  pelotaris.json        Catálogo de pelotaris (PEL###)
  frontones.json        Catálogo de frontones (FRO###), con lat/lon para el mapa
  ciudades.json         Catálogo de ciudades (CIU###), con nombre en ES y EU
  competiciones.json    Catálogo de competiciones (COMP###)
  cartelera.json        Próximos partidos
scraper/
  scraper.py            Añade resultados nuevos desde baikopilota.eus/resultados
  scraper_cartelera.py  Regenera la cartelera desde baikopilota.eus/entradas
  aspe.py               Lectores de aspepelota.eus (resultados y cartelera), 2ª fuente
  red.py                Descarga con reintentos (común a los scrapers)
  jsonio.py             Escritura de los JSON de data/
  historial_baiko.py    Lector del «Historial de competición» de Baiko (fases reales)
  competiciones.py      Criterio para asignar competición y tipo a cada partido
  roles.py              Rol (delantero/zaguero) de cada pelotari según sus partidos
tests/                  Pruebas de los scrapers con casos reales (sin red)
tools/
  validar_datos.py      Comprueba la coherencia de data/ (lo usa el workflow)
  migrar_clasificacion.py  Añade modalidad/categoria/serie/fase a los partidos
  generar_paginas.py    Páginas estáticas de pelotaris y sitemap.xml
  importar_historial.py Fases reales (y partidos que falten) desde las fichas de
                        campeonato de Baiko (HTML guardado o URL; lista en
                        data/historial_baiko.json, workflow «Historial de Baiko»)
  recalcular_contadores.py  Recalcula partidos_count y roles
  ...                   Scripts de limpieza puntuales (migración, auditorías)
```

## Actualización de datos

El workflow `.github/workflows/datos.yml` se ejecuta cada día a las 00:01 UTC
(y a mano desde la pestaña *Actions*): lanza el scraper de resultados (que
aún lee la cartelera del día anterior para saber la competición), después el
de cartelera, valida los datos con `tools/validar_datos.py` y, si todo es
correcto, hace un único commit con los cambios en `data/`.

## Tipos de partido

Cada partido guarda su clasificación (la decide `scraper/competiciones.py`, la
misma para resultados y cartelera, de Baiko y de Aspe):

| Campo | Valores |
|---|---|
| `modalidad` | `parejas`, `mano`, `cuatro` |
| `categoria` | `campeonato` (Parejas, Manomanista, 4 y Medio de la liga), `torneo` (Masters CaixaBank, San Fermín, San Mateo, Aste Nagusia, La Blanca, Donostia Hiria, Bizkaia…), `desafio` (Desafío Urzante), `festival` |
| `serie` | `A`, `B` (la Promoción es Serie B) o `null` (festivales y desafíos) |
| `fase` | `liga`, `eliminatoria`, `octavos`, `cuartos`, `semifinal`, `final` (+ `grupo`, `jornada`) cuando se conoce |
| `tipo` | derivado de los anteriores, el que usa la web para filtrar por modalidad |

La fase viene de la cartelera (el scraper de resultados lee la del día
anterior). En los campeonatos terminados de los que no se conocía, la final es
su último partido y lleva `"fase_deducida": true`. Oficiales = campeonatos y
torneos.

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

Fuentes actuales: Baiko (primera) y Aspe. Cuando Aspe solo da el pueblo
('Altsasu', 'Bilbo') se usa el frontón principal de esa ciudad, y un nombre con
una errata ('P.Etxberria') se asigna al pelotari existente si solo hay uno
casi igual (con el mismo ordinal), dejando un aviso.

Si una fuente falla, se guarda lo de las demás y la ejecución sale en rojo.

## Pruebas

```bash
python -m unittest discover tests          # scrapers (sin red)
```

Se ejecutan en cada push (`.github/workflows/tests.yml`) y antes del scraping
diario.

Pruebas de la web en un navegador (Chromium con Playwright): enlaces
directos, botón atrás, idiomas, comparadores, cuadro de eliminatorias,
mapa, calendario de la cartelera, móvil, modo oscuro, teclado y páginas de
pelotaris. También se ejecutan en cada push.

```bash
pip install -r requirements-web.txt
python -m playwright install chromium      # la primera vez
python -m unittest discover tests_web -v
```

Si cambian los datos, hay que regenerar las páginas de pelotaris
(`python tools/generar_paginas.py`); el workflow nocturno lo hace solo y
`tests.yml` avisa si se han quedado atrás.

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
- `?lang=eu` / `?lang=es` delante del `#` fija el idioma (ej.
  `https://www.eskupilotastats.com/?lang=eu#/ranking`); es la URL que
  indexan los buscadores para la versión en euskera

## App

La web se puede instalar como app (PWA): el botón «Instalar la app» sale al
final del menú y en el pie cuando el navegador lo permite (y en iPhone, con
las instrucciones de Safari). Dentro de la app instalada no se muestra.

Para ofrecer además una APK de Android (p. ej. generada con PWABuilder),
pon su dirección en `APK_URL` (js/app.js) y publica el `assetlinks.json` en
`.well-known/`: aparecerá «App para Android» solo en móviles Android y
nunca dentro de la app.

## Elo

Puntuación de fuerza de cada pelotari (empieza en 1500) calculada en el
navegador con todos los partidos (`js/analisis.js`). La fuerza de un equipo es
la media de sus pelotaris; K=24 en partidos oficiales y 12 en festivales.
Probado con los partidos desde 2025 acierta el ganador en ~56% de los casos
(los partidos suelen estar muy igualados), así que el pronóstico de la
cartelera se presenta como orientativo.
