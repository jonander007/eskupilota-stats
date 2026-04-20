"""
Eskupilota Stats — Scraper de resultados
Lee https://www.baikopilota.eus/resultados/ y añade al JSON
los partidos nuevos que encuentre.

Requisitos:
    pip install requests beautifulsoup4
"""

from __future__ import annotations
import json, re, os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

DATA_FILE = os.path.join(os.path.dirname(__file__), "../data/partidos.json")
URL       = "https://www.baikopilota.eus/resultados/"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36 baiko-resultados-scraper/1.0"
)

DATE_RE  = re.compile(r"^\d{2}/\d{2}/\d{4}$")
SCORE_RE = re.compile(r"^\d{1,2}$")

COMP_KEYWORDS = (
    "manomanista", "parejas", "campeonato", "final", "semifinal",
    "eliminatoria", "cuartos", "masters", "torneo", "serie a", "serie b",
    "4 1/2", "festival",
)

END_MARKERS = {"Frontón", "LA REVISTA DE LA PELOTA",
               "NOTICIAS, ENTREVISTAS….. TODA LA INFORMACIÓN DE LA PELOTA"}

PELOTARI_MAP = {
    'ALTUNA':            'ALTUNA III',
    'EGIGUREN':          'EGIGUREN V',
    'MARIEZKURRENA':     'MARIEZKURRENA II',
    'PEÑA':              'PEÑA II',
    'SALAVERRI':         'SALAVERRI II',
    'ZUBIZARRETA':       'ZUBIZARRETA III',
    'MORGAETXEBARRIA':   'MORGAETXEBERRIA',
    'MORGA':             'MORGAETXEBERRIA',
    'DARIO':             'DARÍO',
    'P. ETXEBARRIA':     'P.ETXEBERRIA',
}

COMP_NORM = {
    'campeonato parejas serie a':        ('campeonato-a',    'Campeonato Parejas Serie A'),
    'campeonato parejas serie b':        ('campeonato-b',    'Campeonato Parejas Serie B'),
    'campeonato manomanista serie a':    ('manomanista-a',   'Campeonato Manomanista Serie A'),
    'campeonato manomanista serie b':    ('manomanista-b',   'Campeonato Manomanista Serie B'),
    'serie a eliminatoria manomanista':  ('manomanista-a',   'Campeonato Manomanista Serie A'),
    'serie b eliminatoria manomanista':  ('manomanista-b',   'Campeonato Manomanista Serie B'),
    'serie a manomanista':               ('manomanista-a',   'Campeonato Manomanista Serie A'),
    'serie b manomanista':               ('manomanista-b',   'Campeonato Manomanista Serie B'),
    'manomanista serie a':               ('manomanista-a',   'Campeonato Manomanista Serie A'),
    'manomanista serie b':               ('manomanista-b',   'Campeonato Manomanista Serie B'),
    'campeonato 4 1/2 serie a':          ('cuatro-medio-a',  'Campeonato 4 1/2 Serie A'),
    'campeonato 4 1/2 serie b':          ('cuatro-medio-b',  'Campeonato 4 1/2 Serie B'),
    'torneo san fermin serie a':         ('festival',        'Torneo San Fermín Serie A'),
    'torneo san fermín serie a':         ('festival',        'Torneo San Fermín Serie A'),
    'torneo san fermin serie b':         ('festival',        'Torneo San Fermín Serie B'),
    'torneo san fermín serie b':         ('festival',        'Torneo San Fermín Serie B'),
    'masters caixabank serie a':         ('festival',        'Masters CaixaBank Serie A'),
    'masters caixabank serie b':         ('festival',        'Masters CaixaBank Serie B'),
    'torneo la blanca serie a':          ('festival',        'Torneo La Blanca Serie A'),
    'torneo la blanca serie b':          ('festival',        'Torneo La Blanca Serie B'),
    'torneo aste nagusia serie a':       ('festival',        'Torneo Aste Nagusia Serie A'),
    'torneo aste nagusia serie b':       ('festival',        'Torneo Aste Nagusia Serie B'),
    'torneo donostia hiria serie a':     ('festival',        'Torneo Donostia Hiria Serie A'),
    'torneo donostia hiria serie b':     ('festival',        'Torneo Donostia Hiria Serie B'),
    'torneo san mateo serie a':          ('festival',        'Torneo San Mateo Serie A'),
    'torneo san mateo serie b':          ('festival',        'Torneo San Mateo Serie B'),
    'torneo bizkaia parejas':            ('festival',        'Torneo Bizkaia Parejas'),
    'torneo san fermin 4 1/2':           ('festival-cuatro', 'Torneo San Fermín 4 1/2'),
    'torneo san fermín 4 1/2':           ('festival-cuatro', 'Torneo San Fermín 4 1/2'),
    'torneo bizkaia manomanista':        ('festival-mano',   'Torneo Bizkaia Manomanista'),
    'torneo bizkaia 4 1/2':              ('festival-cuatro', 'Torneo Bizkaia 4 1/2'),
}

# -----------------------------------------------------------------------------
# NORMALIZACIONES DE UBICACIÓN
# -----------------------------------------------------------------------------
# Alias de ciudad → forma canónica
CIUDAD_ALIAS = {
    'IRUÑEA':              'PAMPLONA',
    'GASTEIZ':             'VITORIA-GASTEIZ',
    'VITORIA':             'VITORIA-GASTEIZ',
    'AMOREBIETA':          'AMOREBIETA-ETXANO',
    'ESTELLA':             'LIZARRA',
    'ESTELLA-LIZARRA':     'LIZARRA',
    'ALSASUA':             'ALTSASU',
    'ALTSASU/ALSASUA':     'ALTSASU',
    'HENDAYA':             'HENDAIA',
    'BAÑOS DEL RIO TOBIA': 'BAÑOS DE RÍO TOBÍA',
    'OIARTZUN -':          'OIARTZUN',
}

# Correcciones: si el scraper devuelve (fronton, ciudad) errónea, forzar la correcta.
# Clave = fronton, valor = ciudad que debe tener SIEMPRE ese frontón.
FRONTON_CIUDAD_FIJA = {
    'AIZPURUTXO':       'AZKOITIA',
    'ARTUNDUAGA':       'BASAURI',
    'FERNANDO GARAITA': 'LEGUTIO',
    'ARETA':            'LLODIO',
}

# Reasignaciones de frontón cuando viene mal etiquetado.
# Clave = (fronton_origen, ciudad), valor = (fronton_correcto, ciudad_correcta)
FRONTON_REASIGNAR = {
    ('LABRIT', 'ALSASUA'): ('BURUNDA', 'ALTSASU'),
    ('LABRIT', 'ALTSASU'): ('BURUNDA', 'ALTSASU'),
}

def normalizar_ubicacion(fronton, ciudad):
    """Aplica todas las normalizaciones a fronton/ciudad."""
    fronton = (fronton or '').strip().upper()
    ciudad  = (ciudad or '').strip().upper()

    # 1. Normalizar alias de ciudad
    ciudad = CIUDAD_ALIAS.get(ciudad, ciudad)

    # 2. Reasignaciones de frontón (antes del fijo por frontón)
    if (fronton, ciudad) in FRONTON_REASIGNAR:
        fronton, ciudad = FRONTON_REASIGNAR[(fronton, ciudad)]

    # 3. Frontones con ciudad fija
    if fronton in FRONTON_CIUDAD_FIJA:
        ciudad = FRONTON_CIUDAD_FIJA[fronton]

    return fronton, ciudad

# -----------------------------------------------------------------------------

def clean_text(s):
    return " ".join(s.replace("\xa0", " ").split())

def clean_player(s):
    s = clean_text(s)
    s = re.sub(r"\s*\(\d+\)\s*$", "", s)
    return s

def norm(nombre):
    if not nombre: return nombre
    n = re.sub(r'\s*\d+\s*$', '', nombre.strip()).upper()
    return PELOTARI_MAP.get(n, n)

def is_comp_line(s):
    t = s.strip().lstrip('- ').strip().lower()
    return any(k in t for k in COMP_KEYWORDS)

def is_note_line(s):
    t = s.strip()
    if is_comp_line(t): return False
    return t.startswith('-') or t.startswith('^{') or 'sustituye' in t.lower()

def is_location_line(s):
    t = s.strip()
    if DATE_RE.match(t): return False
    if is_comp_line(t) or is_note_line(t): return False
    if SCORE_RE.match(t): return False
    return ' - ' in t

def looks_like_player(tok):
    tok = clean_text(tok)
    if not tok or tok == '-': return False
    if SCORE_RE.match(tok) or DATE_RE.match(tok): return False
    if is_location_line(tok) or is_comp_line(tok) or is_note_line(tok): return False
    return True

def read_side(tokens, i):
    """Lee jugadores hasta encontrar un score. Devuelve (jugadores, score, nuevo_i)."""
    players = []
    while i < len(tokens):
        tok = clean_text(tokens[i])
        if tok == '-':
            i += 1
            continue
        if SCORE_RE.match(tok):
            if not players:
                raise ValueError(f"Score sin jugadores en pos {i}")
            return players, int(tok), i + 1
        if DATE_RE.match(tok) or is_location_line(tok) or is_comp_line(tok) or is_note_line(tok):
            raise ValueError(f"Token de control inesperado: {tok!r}")
        players.append(clean_player(tok))
        i += 1
    raise ValueError("No se encontró score")

def inferir_comp(texto_comp, tiene_zaguero, anio):
    if texto_comp:
        base = texto_comp.lstrip('- ').strip().lower()
        # Quitar fase
        base = re.split(r'\s*-\s*(liga|octavos|cuartos|semifinal|final)', base)[0].strip()
        if base in COMP_NORM:
            tipo, nombre = COMP_NORM[base]
            return tipo, f"{nombre} {anio}"
        for k, (tipo, nombre) in COMP_NORM.items():
            if k in base:
                return tipo, f"{nombre} {anio}"
        # Fallback por palabras clave
        if 'manomanista' in base:
            if 'serie a' in base or ' a ' in base:
                return 'manomanista-a', f'Campeonato Manomanista Serie A {anio}'
            if 'serie b' in base or ' b ' in base:
                return 'manomanista-b', f'Campeonato Manomanista Serie B {anio}'
            return 'festival-mano', f'Festival Manomanista {anio}'
        if '4 1/2' in base:
            if 'serie a' in base: return 'cuatro-medio-a', f'Campeonato 4 1/2 Serie A {anio}'
            if 'serie b' in base: return 'cuatro-medio-b', f'Campeonato 4 1/2 Serie B {anio}'
            return 'festival-cuatro', f'Festival 4 y Medio {anio}'

    if tiene_zaguero:
        return 'festival', f'Festival Parejas {anio}'
    else:
        return 'festival-mano', f'Festival Manomanista {anio}'

def extract_tokens(html):
    soup = BeautifulSoup(html, "html.parser")
    scope = soup.select_one("main") or soup.body or soup
    tokens = [clean_text(s) for s in scope.stripped_strings]
    tokens = [t for t in tokens if t]

    # Recortar a zona útil
    for marker in ["DE LOS PARTIDOS DE PELOTA A MANO", "Resultados"]:
        if marker in tokens:
            tokens = tokens[tokens.index(marker) + 1:]
            break

    for marker in END_MARKERS:
        if marker in tokens:
            tokens = tokens[:tokens.index(marker)]
            break

    return tokens

def parse_tokens(tokens):
    partidos = []
    fecha = fronton = ciudad = comp = None
    expect_venue = False

    i = 0
    while i < len(tokens):
        tok = clean_text(tokens[i])

        if not tok or tok in {"Resultados", "DE LOS PARTIDOS DE PELOTA A MANO"}:
            i += 1
            continue

        if DATE_RE.match(tok):
            fecha = tok
            fronton = ciudad = comp = None
            expect_venue = True
            i += 1
            continue

        if is_location_line(tok):
            partes = [x.strip() for x in tok.split(' - ')]
            fronton_raw = partes[0].upper()
            ciudad_raw  = partes[1].upper() if len(partes) > 1 else ''
            # Aplicar normalizaciones de ubicación
            fronton, ciudad = normalizar_ubicacion(fronton_raw, ciudad_raw)
            expect_venue = False
            i += 1
            continue

        if is_comp_line(tok):
            comp = tok
            i += 1
            continue

        if is_note_line(tok):
            i += 1
            continue

        # Intentar leer partido
        if looks_like_player(tok) and fecha:
            try:
                side1, score1, i = read_side(tokens, i)
                side2, score2, i = read_side(tokens, i)
            except ValueError:
                i += 1
                continue

            if score1 == 0 and score2 == 0:
                continue

            d1 = norm(side1[0]) if side1 else None
            z1 = norm(side1[1]) if len(side1) > 1 else None
            d2 = norm(side2[0]) if side2 else None
            z2 = norm(side2[1]) if len(side2) > 1 else None

            if not d1 or not d2:
                continue

            tiene_zaguero = bool(z1 or z2)
            anio = fecha[-4:]
            tipo, comp_nombre = inferir_comp(comp, tiene_zaguero, anio)
            ganador = 'equipo1' if score1 > score2 else ('equipo2' if score2 > score1 else None)

            partidos.append({
                'fecha':       fecha,
                'fronton':     fronton or '',
                'ciudad':      ciudad or '',
                'provincia':   '',
                'tipo':        tipo,
                'competicion': comp_nombre,
                'equipo1':     {'delantero': d1, 'zaguero': z1},
                'puntos1':     score1,
                'equipo2':     {'delantero': d2, 'zaguero': z2},
                'puntos2':     score2,
                'ganador':     ganador,
            })
            comp = None
            continue

        i += 1

    return partidos

# -----------------------------------------------------------------------------
# DETECCIÓN DE DUPLICADOS
# -----------------------------------------------------------------------------
def _safe(v):
    return v if v is not None else ''

def firma_partido(p):
    """
    Firma robusta que identifica 'el mismo partido':
    misma fecha + mismos 4 jugadores (ordenados, ignorando lado) + mismos puntos ordenados.
    """
    e1 = tuple(sorted([_safe(p['equipo1'].get('delantero')), _safe(p['equipo1'].get('zaguero'))]))
    e2 = tuple(sorted([_safe(p['equipo2'].get('delantero')), _safe(p['equipo2'].get('zaguero'))]))
    jugadores = tuple(sorted([e1, e2]))
    puntos = tuple(sorted([p.get('puntos1', 0), p.get('puntos2', 0)]))
    return (p['fecha'], jugadores, puntos)

def firma_sin_fecha(p):
    """Igual que firma_partido pero sin fecha (para detectar contiguos)."""
    e1 = tuple(sorted([_safe(p['equipo1'].get('delantero')), _safe(p['equipo1'].get('zaguero'))]))
    e2 = tuple(sorted([_safe(p['equipo2'].get('delantero')), _safe(p['equipo2'].get('zaguero'))]))
    jugadores = tuple(sorted([e1, e2]))
    puntos = tuple(sorted([p.get('puntos1', 0), p.get('puntos2', 0)]))
    return (jugadores, puntos)

def es_duplicado(nuevo, existentes_por_firma, existentes_por_firma_sinfecha):
    """
    Devuelve True si 'nuevo' es duplicado de algún partido existente:
    - Mismo día, mismos jugadores, mismos puntos
    - Día contiguo (±1), mismos jugadores, mismos puntos
    """
    # Mismo día
    if firma_partido(nuevo) in existentes_por_firma:
        return True

    # Día contiguo ±1
    try:
        f = datetime.strptime(nuevo['fecha'], '%d/%m/%Y').date()
    except Exception:
        return False

    firma_sf = firma_sin_fecha(nuevo)
    for delta in (-1, 1):
        f_vecina = (f + timedelta(days=delta)).strftime('%d/%m/%Y')
        if (f_vecina, firma_sf) in existentes_por_firma_sinfecha:
            return True
    return False


def main():
    print("Eskupilota Stats — Scraper de resultados")
    print(f"Fuente: {URL}\n")

    resp = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    print(f"Status: {resp.status_code} — {len(resp.text)} chars")

    tokens = extract_tokens(resp.text)
    print(f"Tokens extraídos: {len(tokens)}")

    nuevos = parse_tokens(tokens)
    print(f"Partidos encontrados: {len(nuevos)}")
    for p in nuevos:
        eq1 = f"{p['equipo1']['delantero']}-{p['equipo1']['zaguero']}" if p['equipo1']['zaguero'] else p['equipo1']['delantero']
        eq2 = f"{p['equipo2']['delantero']}-{p['equipo2']['zaguero']}" if p['equipo2']['zaguero'] else p['equipo2']['delantero']
        print(f"  {p['fecha']} | {p['fronton']} ({p['ciudad']}) | {eq1} {p['puntos1']}-{p['puntos2']} {eq2} | {p['competicion']}")

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            existentes = json.load(f)
    else:
        existentes = []

    # Índices para detección de duplicados
    firmas_existentes = {firma_partido(p) for p in existentes}
    firmas_sinfecha_existentes = {(p['fecha'], firma_sin_fecha(p)) for p in existentes}

    sin_dup = []
    descartados = []
    for p in nuevos:
        if es_duplicado(p, firmas_existentes, firmas_sinfecha_existentes):
            descartados.append(p)
        else:
            sin_dup.append(p)
            # Añadir al set para evitar duplicar también entre los propios "nuevos"
            firmas_existentes.add(firma_partido(p))
            firmas_sinfecha_existentes.add((p['fecha'], firma_sin_fecha(p)))

    print(f"\nPartidos nuevos: {len(sin_dup)}")
    if descartados:
        print(f"Descartados por duplicado: {len(descartados)}")
        for p in descartados:
            eq1 = f"{p['equipo1']['delantero']}-{_safe(p['equipo1']['zaguero'])}"
            eq2 = f"{p['equipo2']['delantero']}-{_safe(p['equipo2']['zaguero'])}"
            print(f"  [DUP] {p['fecha']} | {p['fronton']} | {eq1} {p['puntos1']}-{p['puntos2']} {eq2}")

    if not sin_dup:
        print("No hay partidos nuevos. JSON sin cambios.")
        return

    todos = existentes + sin_dup
    todos.sort(key=lambda p: datetime.strptime(p['fecha'], '%d/%m/%Y'), reverse=True)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    print(f"JSON actualizado: {len(todos)} partidos totales")

if __name__ == '__main__':
    main()
