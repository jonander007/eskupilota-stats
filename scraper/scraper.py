"""
Eskupilota Stats — Scraper de resultados
Lee https://www.baikopilota.eus/resultados/ y añade al JSON
los partidos nuevos que encuentre.

Requisitos:
    pip install requests beautifulsoup4
"""

import json, re, os
import requests
from bs4 import BeautifulSoup
from datetime import datetime

DATA_FILE = os.path.join(os.path.dirname(__file__), "../data/partidos.json")
URL       = "https://www.baikopilota.eus/resultados/"

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

def norm(nombre):
    if not nombre: return nombre
    n = re.sub(r'\s*\(\d+\)\s*', '', nombre.strip()).strip()
    n = re.sub(r'\s*\d+\s*$', '', n).strip().upper()
    return PELOTARI_MAP.get(n, n)

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

def parsear_resultados(html):
    soup = BeautifulSoup(html, 'html.parser')
    partidos = []
    fecha_actual = fronton_actual = ciudad_actual = comp_actual = None

    for el in soup.find_all(['h5', 'p', 'ul']):
        tag  = el.name
        text = el.get_text(separator=' ', strip=True)
        if not text: continue

        if tag == 'h5' and re.match(r'^\d{2}/\d{2}/\d{4}$', text):
            fecha_actual = text; comp_actual = None; continue

        if tag == 'h5' and ' - ' in text and fecha_actual:
            partes = [x.strip() for x in text.split(' - ')]
            fronton_actual = partes[0].upper()
            ciudad_actual  = partes[1].upper() if len(partes) > 1 else ''
            comp_actual = None; continue

        if tag == 'p' and fecha_actual and len(text) > 5:
            if 'sustituye' in text.lower(): continue
            tl = text.lower()
            if any(k in tl for k in ['campeonato','torneo','masters','festival','parejas','manomanista','4 1/2']):
                comp_actual = text
            continue

        if tag == 'ul' and fecha_actual and fronton_actual:
            items = el.find_all('li', recursive=False)
            if len(items) != 2: continue

            def parse_li(li):
                raw = re.sub(r'\(\d+\)', '', li.get_text(separator=' ', strip=True)).strip()
                tokens = [t for t in raw.split() if t and t != '-']
                nombres, tantos = [], None
                for t in tokens:
                    if re.match(r'^\d+$', t) and int(t) <= 30:
                        tantos = int(t)
                    else:
                        nombres.append(t)
                d = norm(' '.join(nombres[:1])) if nombres else None
                z = norm(' '.join(nombres[1:])) if len(nombres) > 1 else None
                return d, z, tantos

            try:
                d1, z1, t1 = parse_li(items[0])
                d2, z2, t2 = parse_li(items[1])
            except: continue

            if not d1 or not d2 or t1 is None or t2 is None: continue
            if t1 == 0 and t2 == 0: continue

            anio = fecha_actual[-4:]
            tiene_zaguero = bool(z1 or z2)
            tipo, comp_nombre = inferir_comp(comp_actual, tiene_zaguero, anio)
            ganador = 'equipo1' if t1 > t2 else ('equipo2' if t2 > t1 else None)

            partidos.append({
                'fecha': fecha_actual, 'fronton': fronton_actual, 'ciudad': ciudad_actual,
                'provincia': '', 'tipo': tipo, 'competicion': comp_nombre,
                'equipo1': {'delantero': d1, 'zaguero': z1}, 'puntos1': t1,
                'equipo2': {'delantero': d2, 'zaguero': z2}, 'puntos2': t2,
                'ganador': ganador,
            })

    return partidos

def main():
    print("Eskupilota Stats — Scraper de resultados")
    print(f"Fuente: {URL}\n")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.baikopilota.eus/',
        'Cache-Control': 'no-cache',
    }

    try:
        sess = requests.Session()
        # Primera visita a la home para obtener cookies
        sess.get('https://www.baikopilota.eus/', headers=headers, timeout=15)
        # Luego la página de resultados
        resp = sess.get(URL, headers=headers, timeout=30)
        print(f"Status: {resp.status_code}")
        print(f"Primeros 300 chars: {resp.text[:300]}")
        resp.raise_for_status()
    except Exception as e:
        print(f"ERROR: {e}")
        return

    nuevos = parsear_resultados(resp.text)
    print(f"\nPartidos encontrados: {len(nuevos)}")
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
