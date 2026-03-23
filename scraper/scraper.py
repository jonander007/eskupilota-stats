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
from datetime import datetime

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
            fronton = partes[0].upper()
            ciudad  = partes[1].upper() if len(partes) > 1 else ''
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
        print(f"  {p['fecha']} | {eq1} {p['puntos1']}-{p['puntos2']} {eq2} | {p['competicion']}")

    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            existentes = json.load(f)
    else:
        existentes = []

    def clave(p):
        return f"{p['fecha']}_{p['fronton']}_{p['equipo1']['delantero']}_{p['equipo2']['delantero']}"

    claves_ex = {clave(p) for p in existentes}
    sin_dup   = [p for p in nuevos if clave(p) not in claves_ex]
    print(f"\nPartidos nuevos: {len(sin_dup)}")

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
