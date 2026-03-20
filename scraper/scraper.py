"""
Eskupilota Stats — Scraper de resultados
Usa Selenium para cargar la página con JavaScript y extraer los partidos.

Requisitos:
    pip install selenium beautifulsoup4 requests
"""

import json, re, os, time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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

def clean_text(el):
    return ' '.join(el.get_text(separator=' ').split())

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

def parse_ul_equipo(ul):
    """
    Extrae (delantero, zaguero, tantos) de un <ul> de equipo.
    Estructura: <li>NOMBRE</li><li>-</li><li>ZAGUERO</li> y tantos aparte
    o: <li>NOMBRE</li><li>-</li><li>NOMBRE</li><li>tantos</li>
    """
    items = ul.find_all('li', recursive=False)
    nombres = []
    tantos  = None
    for li in items:
        t = re.sub(r'\(\d+\)', '', clean_text(li)).strip()
        if t == '-' or not t:
            continue
        if re.match(r'^\d+$', t) and int(t) <= 30:
            tantos = int(t)
        else:
            nombres.append(t)
    delantero = norm(nombres[0]) if nombres else None
    zaguero   = norm(nombres[1]) if len(nombres) > 1 else None
    return delantero, zaguero, tantos

def parsear_resultados(html):
    soup = BeautifulSoup(html, 'html.parser')
    partidos = []
    fecha_actual = fronton_actual = ciudad_actual = comp_actual = None

    main_el = soup.find('main') or soup.find('div', id='primary') or soup.body

    # Recorrer todos los elementos en orden
    elementos = main_el.find_all(['h5', 'p', 'ul', 'div'])

    i = 0
    while i < len(elementos):
        el   = elementos[i]
        tag  = el.name
        text = clean_text(el)

        # ── Fecha ──────────────────────────────────────────────────────
        if tag == 'h5' and re.match(r'^\d{2}/\d{2}/\d{4}$', text):
            fecha_actual = text
            comp_actual  = None
            i += 1; continue

        # ── Frontón ────────────────────────────────────────────────────
        if tag == 'h5' and ' - ' in text and fecha_actual and not re.match(r'^\d', text):
            partes         = [x.strip() for x in text.split(' - ')]
            fronton_actual = partes[0].upper()
            ciudad_actual  = partes[1].upper() if len(partes) > 1 else ''
            comp_actual    = None
            i += 1; continue

        # ── Competición ────────────────────────────────────────────────
        if tag == 'p' and fecha_actual and len(text) > 5:
            if 'sustituye' not in text.lower():
                tl = text.lower()
                if any(k in tl for k in ['campeonato','torneo','masters','parejas','manomanista','4 1/2']):
                    comp_actual = text
            i += 1; continue

        # ── Par de ul consecutivos = un partido ────────────────────────
        # Equipo 1 (ul rojo) seguido de equipo 2 (ul azul)
        if tag == 'ul' and fecha_actual and fronton_actual:
            # Buscar el siguiente ul
            j = i + 1
            while j < len(elementos) and elementos[j].name != 'ul':
                # Si hay un p de competición entre los dos ul, capturarlo
                if elementos[j].name == 'p':
                    t2 = clean_text(elementos[j])
                    if 'sustituye' not in t2.lower():
                        tl = t2.lower()
                        if any(k in tl for k in ['campeonato','torneo','masters','parejas','manomanista','4 1/2']):
                            comp_actual = t2
                j += 1

            if j < len(elementos) and elementos[j].name == 'ul':
                ul1 = el
                ul2 = elementos[j]
                try:
                    d1, z1, t1 = parse_ul_equipo(ul1)
                    d2, z2, t2 = parse_ul_equipo(ul2)

                    if d1 and d2 and t1 is not None and t2 is not None:
                        if not (t1 == 0 and t2 == 0):
                            anio          = fecha_actual[-4:]
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
                            i = j + 1
                            continue
                except:
                    pass

        i += 1

    return partidos

def get_html(url):
    opts = Options()
    opts.add_argument('--headless')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--window-size=1920,1080')
    opts.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    driver = webdriver.Chrome(options=opts)
    try:
        driver.get(url)
        time.sleep(5)
        html = driver.page_source
    finally:
        driver.quit()
    return html

def main():
    print("Eskupilota Stats — Scraper de resultados")
    print(f"Fuente: {URL}\n")

    print("Cargando página con Selenium...")
    html = get_html(URL)
    print(f"HTML recibido: {len(html)} chars")

    nuevos = parsear_resultados(html)
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
