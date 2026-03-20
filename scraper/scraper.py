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
    'campeonato parejas serie a':    ('campeonato-a',    'Campeonato Parejas Serie A'),
    'campeonato parejas serie b':    ('campeonato-b',    'Campeonato Parejas Serie B'),
    'campeonato manomanista serie a':('manomanista-a',   'Campeonato Manomanista Serie A'),
    'campeonato manomanista serie b':('manomanista-b',   'Campeonato Manomanista Serie B'),
    'campeonato 4 1/2 serie a':      ('cuatro-medio-a',  'Campeonato 4 1/2 Serie A'),
    'campeonato 4 1/2 serie b':      ('cuatro-medio-b',  'Campeonato 4 1/2 Serie B'),
    'torneo san fermin serie a':     ('festival',        'Torneo San Fermín Serie A'),
    'torneo san fermín serie a':     ('festival',        'Torneo San Fermín Serie A'),
    'torneo san fermin serie b':     ('festival',        'Torneo San Fermín Serie B'),
    'torneo san fermín serie b':     ('festival',        'Torneo San Fermín Serie B'),
    'masters caixabank serie a':     ('festival',        'Masters CaixaBank Serie A'),
    'masters caixabank serie b':     ('festival',        'Masters CaixaBank Serie B'),
    'torneo la blanca serie a':      ('festival',        'Torneo La Blanca Serie A'),
    'torneo la blanca serie b':      ('festival',        'Torneo La Blanca Serie B'),
    'torneo aste nagusia serie a':   ('festival',        'Torneo Aste Nagusia Serie A'),
    'torneo aste nagusia serie b':   ('festival',        'Torneo Aste Nagusia Serie B'),
    'torneo donostia hiria serie a': ('festival',        'Torneo Donostia Hiria Serie A'),
    'torneo donostia hiria serie b': ('festival',        'Torneo Donostia Hiria Serie B'),
    'torneo san mateo serie a':      ('festival',        'Torneo San Mateo Serie A'),
    'torneo san mateo serie b':      ('festival',        'Torneo San Mateo Serie B'),
    'torneo bizkaia parejas':        ('festival',        'Torneo Bizkaia Parejas'),
    'torneo san fermin 4 1/2':       ('festival-cuatro', 'Torneo San Fermín 4 1/2'),
    'torneo san fermín 4 1/2':       ('festival-cuatro', 'Torneo San Fermín 4 1/2'),
    'torneo bizkaia manomanista':    ('festival-mano',   'Torneo Bizkaia Manomanista'),
    'torneo bizkaia 4 1/2':          ('festival-cuatro', 'Torneo Bizkaia 4 1/2'),
}

def norm(nombre):
    if not nombre: return nombre
    n = re.sub(r'\s*\(\d+\)\s*', '', nombre.strip()).strip()
    n = re.sub(r'\s*\d+\s*$', '', n).strip().upper()
    return PELOTARI_MAP.get(n, n)

def normalize_text(text):
    return " ".join(text.replace("\xa0", " ").split())

def inferir_comp(texto_comp, tiene_zaguero, anio):
    if texto_comp:
        base = re.split(r'\s*-\s*(liga|octavos|cuartos|semifinal|final|eliminatoria)',
                        texto_comp.lower())[0].strip()
        if base in COMP_NORM:
            tipo, nombre = COMP_NORM[base]
            return tipo, f"{nombre} {anio}"
        for k, (tipo, nombre) in COMP_NORM.items():
            if k in base:
                return tipo, f"{nombre} {anio}"
    if tiene_zaguero:
        return 'festival', f'Festival Parejas {anio}'
    else:
        return 'festival-mano', f'Festival Manomanista {anio}'

def extract_tokens(html):
    soup = BeautifulSoup(html, "html.parser")
    raw = [normalize_text(t) for t in soup.stripped_strings]
    raw = [t for t in raw if t]

    try:
        start = raw.index("DE LOS PARTIDOS DE PELOTA A MANO") + 1
    except ValueError:
        raise RuntimeError("No se encontró el bloque de resultados en la página.")

    end_markers = {"Frontón", "LA REVISTA DE LA PELOTA",
                   "NOTICIAS, ENTREVISTAS….. TODA LA INFORMACIÓN DE LA PELOTA"}
    end = len(raw)
    for pos in range(start, len(raw)):
        if raw[pos] in end_markers:
            end = pos
            break

    return raw[start:end]

def parse_tokens(tokens):
    partidos = []
    fecha    = fronton = ciudad = comp = None
    expect_venue = False

    i = 0
    while i < len(tokens):
        t = tokens[i]

        # Fecha
        if DATE_RE.fullmatch(t):
            fecha = t
            fronton = ciudad = comp = None
            expect_venue = True
            i += 1
            continue

        # Frontón
        if expect_venue and ' - ' in t:
            partes  = [x.strip() for x in t.split(' - ')]
            fronton = partes[0].upper()
            ciudad  = partes[1].upper() if len(partes) > 1 else ''
            expect_venue = False
            i += 1
            continue

        # Notas / sustituciones — ignorar
        if t.startswith('- ') or t.startswith('^{') or 'sustituye' in t.lower():
            i += 1
            continue

        # Competición indicada (antes del partido)
        tl = t.lower()
        if any(k in tl for k in ['campeonato','torneo','masters','parejas','manomanista','4 1/2']):
            comp = t
            i += 1
            continue

        # Intento de partido: token, '-', token, score, token, '-', token, score
        if i + 7 < len(tokens):
            t0,t1,t2,t3,t4,t5,t6,t7 = tokens[i:i+8]
            if t1 == '-' and SCORE_RE.fullmatch(t3) and t5 == '-' and SCORE_RE.fullmatch(t7):
                d1, z1 = norm(t0), norm(t2)
                d2, z2 = norm(t4), norm(t6)
                p1, p2 = int(t3), int(t7)
                if p1 == 0 and p2 == 0:
                    i += 8
                    continue
                anio = fecha[-4:] if fecha else ''
                tiene_zaguero = bool(z1 or z2)
                tipo, comp_nombre = inferir_comp(comp, tiene_zaguero, anio)
                ganador = 'equipo1' if p1 > p2 else ('equipo2' if p2 > p1 else None)
                partidos.append({
                    'fecha':    fecha,
                    'fronton':  fronton or '',
                    'ciudad':   ciudad or '',
                    'provincia':'',
                    'tipo':     tipo,
                    'competicion': comp_nombre,
                    'equipo1':  {'delantero': d1, 'zaguero': z1 if z1 else None},
                    'puntos1':  p1,
                    'equipo2':  {'delantero': d2, 'zaguero': z2 if z2 else None},
                    'puntos2':  p2,
                    'ganador':  ganador,
                })
                comp = None  # reset competición tras cada partido
                i += 8
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
