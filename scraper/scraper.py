"""
Eskupilota Stats — Scraper de resultados (Fase 4: con catálogos maestros)

Lee https://www.baikopilota.eus/resultados/ y añade al JSON los partidos
nuevos que encuentre, escribiendo en el formato nuevo con IDs.

Si el scraper encuentra una entidad nueva (pelotari, frontón, ciudad,
competición), la añade al catálogo correspondiente con un ID nuevo.

Archivos que lee y escribe:
    data/partidos.json
    data/pelotaris.json
    data/frontones.json
    data/ciudades.json
    data/competiciones.json

Requisitos:
    pip install requests beautifulsoup4
"""

from __future__ import annotations
import json
import os
import re
import sys
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────────────────────────
# RUTAS
# ─────────────────────────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
# El scraper está en /scraper/, los datos en /data/
DATA_DIR = os.path.normpath(os.path.join(HERE, '..', 'data'))

PARTIDOS_FILE      = os.path.join(DATA_DIR, 'partidos.json')
PELOTARIS_FILE     = os.path.join(DATA_DIR, 'pelotaris.json')
FRONTONES_FILE     = os.path.join(DATA_DIR, 'frontones.json')
CIUDADES_FILE      = os.path.join(DATA_DIR, 'ciudades.json')
COMPETICIONES_FILE = os.path.join(DATA_DIR, 'competiciones.json')

URL = "https://www.baikopilota.eus/resultados/"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36 baiko-resultados-scraper/2.0"
)

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
SCORE_RE = re.compile(r"^\d{1,2}$")

COMP_KEYWORDS = (
    "manomanista", "parejas", "campeonato", "final", "semifinal",
    "eliminatoria", "cuartos", "masters", "torneo", "serie a", "serie b",
    "4 1/2", "festival",
)

END_MARKERS = {"Frontón", "LA REVISTA DE LA PELOTA",
               "NOTICIAS, ENTREVISTAS….. TODA LA INFORMACIÓN DE LA PELOTA"}

# ─────────────────────────────────────────────────────────────────
# NORMALIZACIONES (idénticas a la versión plana)
# ─────────────────────────────────────────────────────────────────
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

FRONTON_CIUDAD_FIJA = {
    'AIZPURUTXO':       'AZKOITIA',
    'ARTUNDUAGA':       'BASAURI',
    'FERNANDO GARAITA': 'LEGUTIO',
    'ARETA':            'LLODIO',
}

FRONTON_REASIGNAR = {
    ('LABRIT', 'ALSASUA'): ('BURUNDA', 'ALTSASU'),
    ('LABRIT', 'ALTSASU'): ('BURUNDA', 'ALTSASU'),
}

TRADUCCIONES_CIUDAD = {
    'PAMPLONA':          {'es': 'Pamplona',      'eu': 'Iruñea'},
    'VITORIA-GASTEIZ':   {'es': 'Vitoria',       'eu': 'Gasteiz'},
    'BILBAO':            {'es': 'Bilbao',        'eu': 'Bilbo'},
    'DONOSTIA':          {'es': 'San Sebastián', 'eu': 'Donostia'},
    'SAN SEBASTIAN':     {'es': 'San Sebastián', 'eu': 'Donostia'},
    'ALTSASU':           {'es': 'Alsasua',       'eu': 'Altsasu'},
    'LIZARRA':           {'es': 'Estella',       'eu': 'Lizarra'},
    'AMOREBIETA-ETXANO': {'es': 'Amorebieta',    'eu': 'Amorebieta-Etxano'},
    'HENDAIA':           {'es': 'Hendaya',       'eu': 'Hendaia'},
    'OIARTZUN':          {'es': 'Oyarzun',       'eu': 'Oiartzun'},
    'BERGARA':           {'es': 'Vergara',       'eu': 'Bergara'},
    'ARRASATE':          {'es': 'Mondragón',     'eu': 'Arrasate'},
    'LLODIO':            {'es': 'Llodio',        'eu': 'Laudio'},
}


def normalizar_ubicacion(fronton, ciudad):
    fronton = (fronton or '').strip().upper()
    ciudad  = (ciudad or '').strip().upper()
    ciudad = CIUDAD_ALIAS.get(ciudad, ciudad)
    if (fronton, ciudad) in FRONTON_REASIGNAR:
        fronton, ciudad = FRONTON_REASIGNAR[(fronton, ciudad)]
    if fronton in FRONTON_CIUDAD_FIJA:
        ciudad = FRONTON_CIUDAD_FIJA[fronton]
    return fronton, ciudad


# ─────────────────────────────────────────────────────────────────
# PARSEO DE LA WEB (idéntico a la versión plana)
# ─────────────────────────────────────────────────────────────────
def clean_text(s):
    return " ".join(s.replace("\xa0", " ").split())


def clean_player(s):
    s = clean_text(s)
    s = re.sub(r"\s*\(\d+\)\s*$", "", s)
    return s


def norm(nombre):
    if not nombre:
        return nombre
    n = re.sub(r'\s*\d+\s*$', '', nombre.strip()).upper()
    return PELOTARI_MAP.get(n, n)


def is_comp_line(s):
    t = s.strip().lstrip('- ').strip().lower()
    return any(k in t for k in COMP_KEYWORDS)


def is_note_line(s):
    t = s.strip()
    if is_comp_line(t):
        return False
    return t.startswith('-') or t.startswith('^{') or 'sustituye' in t.lower()


def is_location_line(s):
    t = s.strip()
    if DATE_RE.match(t):
        return False
    if is_comp_line(t) or is_note_line(t):
        return False
    if SCORE_RE.match(t):
        return False
    return ' - ' in t


def looks_like_player(tok):
    tok = clean_text(tok)
    if not tok or tok == '-':
        return False
    if SCORE_RE.match(tok) or DATE_RE.match(tok):
        return False
    if is_location_line(tok) or is_comp_line(tok) or is_note_line(tok):
        return False
    return True


def read_side(tokens, i):
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
        base = re.split(r'\s*-\s*(liga|octavos|cuartos|semifinal|final)', base)[0].strip()
        if base in COMP_NORM:
            tipo, nombre = COMP_NORM[base]
            return tipo, f"{nombre} {anio}"
        for k, (tipo, nombre) in COMP_NORM.items():
            if k in base:
                return tipo, f"{nombre} {anio}"
        if 'manomanista' in base:
            if 'serie a' in base or ' a ' in base:
                return 'manomanista-a', f"Campeonato Manomanista Serie A {anio}"
            if 'serie b' in base or ' b ' in base:
                return 'manomanista-b', f"Campeonato Manomanista Serie B {anio}"
            return 'manomanista-a', f"Campeonato Manomanista {anio}"
        if 'festival' in base or 'torneo' in base or 'masters' in base:
            return 'festival', f"{texto_comp.strip().title()} {anio}"
    if tiene_zaguero:
        return 'campeonato-a', f"Campeonato Parejas Serie A {anio}"
    return 'manomanista-a', f"Campeonato Manomanista Serie A {anio}"


def extract_tokens(html):
    soup = BeautifulSoup(html, 'html.parser')
    tokens = [clean_text(t) for t in soup.stripped_strings if clean_text(t)]
    for marker in ("Resultados", "DE LOS PARTIDOS DE PELOTA A MANO"):
        if marker in tokens:
            tokens = tokens[tokens.index(marker) + 1:]
            break
    for marker in END_MARKERS:
        if marker in tokens:
            tokens = tokens[:tokens.index(marker)]
            break
    return tokens


def parse_tokens(tokens):
    """Devuelve lista de partidos en formato 'plano' (con nombres). Luego
    se convierten a IDs en el siguiente paso."""
    partidos = []
    fecha = fronton = ciudad = comp = None

    i = 0
    while i < len(tokens):
        tok = clean_text(tokens[i])

        if not tok or tok in {"Resultados", "DE LOS PARTIDOS DE PELOTA A MANO"}:
            i += 1
            continue

        if DATE_RE.match(tok):
            fecha = tok
            fronton = ciudad = comp = None
            i += 1
            continue

        if is_location_line(tok):
            partes = [x.strip() for x in tok.split(' - ')]
            fronton_raw = partes[0].upper()
            ciudad_raw = partes[1].upper() if len(partes) > 1 else ''
            fronton, ciudad = normalizar_ubicacion(fronton_raw, ciudad_raw)
            i += 1
            continue

        if is_comp_line(tok):
            comp = tok
            i += 1
            continue

        if is_note_line(tok):
            i += 1
            continue

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


# ─────────────────────────────────────────────────────────────────
# CATÁLOGOS: carga, get_or_create, persistencia
# ─────────────────────────────────────────────────────────────────
def load_catalog(path):
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_catalog(path, items):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def next_id(prefix, items, width=3):
    nums = []
    for it in items:
        m = re.match(rf'^{prefix}(\d+)$', it.get('id', ''))
        if m:
            nums.append(int(m.group(1)))
    n = max(nums) + 1 if nums else 1
    return f"{prefix}{n:0{width}d}"


class Catalogos:
    """Mantiene en memoria los catálogos y permite buscar/crear entradas."""

    def __init__(self):
        self.pelotaris = load_catalog(PELOTARIS_FILE)
        self.frontones = load_catalog(FRONTONES_FILE)
        self.ciudades = load_catalog(CIUDADES_FILE)
        self.competiciones = load_catalog(COMPETICIONES_FILE)
        self._dirty = {'pel': False, 'fro': False, 'ciu': False, 'cmp': False}
        self._idx_pel = {p['nombre']: p for p in self.pelotaris}
        self._idx_fro = {f['nombre']: f for f in self.frontones}
        self._idx_ciu = {c['nombre']: c for c in self.ciudades}
        self._idx_cmp = {c['nombre']: c for c in self.competiciones}

    def get_or_create_pelotari(self, nombre):
        if not nombre:
            return None
        if nombre in self._idx_pel:
            p = self._idx_pel[nombre]
            p['partidos_count'] = p.get('partidos_count', 0) + 1
            self._dirty['pel'] = True
            return p['id']
        pid = next_id('PEL', self.pelotaris)
        nuevo = {
            'id': pid, 'nombre': nombre,
            'nombre_es': nombre, 'nombre_eu': nombre,
            'rol': 'mixto', 'partidos_count': 1,
        }
        self.pelotaris.append(nuevo)
        self._idx_pel[nombre] = nuevo
        self._dirty['pel'] = True
        print(f"  + nuevo pelotari: {pid} {nombre}")
        return pid

    def get_or_create_ciudad(self, nombre):
        if not nombre:
            return None
        if nombre in self._idx_ciu:
            return self._idx_ciu[nombre]['id']
        cid = next_id('CIU', self.ciudades)
        trad = TRADUCCIONES_CIUDAD.get(nombre, {'es': nombre.title(), 'eu': nombre.title()})
        nuevo = {
            'id': cid, 'nombre': nombre,
            'nombre_es': trad['es'], 'nombre_eu': trad['eu'],
        }
        self.ciudades.append(nuevo)
        self._idx_ciu[nombre] = nuevo
        self._dirty['ciu'] = True
        print(f"  + nueva ciudad: {cid} {nombre}")
        return cid

    def get_or_create_fronton(self, nombre, ciudad_nombre):
        if not nombre:
            return None
        if nombre in self._idx_fro:
            f = self._idx_fro[nombre]
            f['partidos_count'] = f.get('partidos_count', 0) + 1
            if not f.get('ciudad_id') and ciudad_nombre:
                f['ciudad_id'] = self.get_or_create_ciudad(ciudad_nombre)
            self._dirty['fro'] = True
            return f['id']
        fid = next_id('FRO', self.frontones)
        ciudad_id = self.get_or_create_ciudad(ciudad_nombre) if ciudad_nombre else None
        nuevo = {
            'id': fid, 'nombre': nombre,
            'ciudad_id': ciudad_id, 'partidos_count': 1,
        }
        self.frontones.append(nuevo)
        self._idx_fro[nombre] = nuevo
        self._dirty['fro'] = True
        print(f"  + nuevo frontón: {fid} {nombre} ({ciudad_nombre})")
        return fid

    def get_or_create_competicion(self, nombre, tipo):
        if not nombre:
            return None
        if nombre in self._idx_cmp:
            c = self._idx_cmp[nombre]
            c['partidos_count'] = c.get('partidos_count', 0) + 1
            self._dirty['cmp'] = True
            return c['id']
        cid = next_id('COMP', self.competiciones)
        nuevo = {
            'id': cid, 'nombre': nombre, 'tipo': tipo, 'partidos_count': 1,
        }
        self.competiciones.append(nuevo)
        self._idx_cmp[nombre] = nuevo
        self._dirty['cmp'] = True
        print(f"  + nueva competición: {cid} {nombre}")
        return cid

    def save_all(self):
        if self._dirty['pel']:
            self.pelotaris.sort(key=lambda p: (-p.get('partidos_count', 0), p['nombre']))
            save_catalog(PELOTARIS_FILE, self.pelotaris)
        if self._dirty['fro']:
            self.frontones.sort(key=lambda f: f['nombre'])
            save_catalog(FRONTONES_FILE, self.frontones)
        if self._dirty['ciu']:
            self.ciudades.sort(key=lambda c: c['nombre'])
            save_catalog(CIUDADES_FILE, self.ciudades)
        if self._dirty['cmp']:
            self.competiciones.sort(key=lambda c: c['nombre'])
            save_catalog(COMPETICIONES_FILE, self.competiciones)


# ─────────────────────────────────────────────────────────────────
# CONVERSIÓN PLANO → CATALOGADO
# ─────────────────────────────────────────────────────────────────
def fecha_to_iso(fecha_ddmmyyyy):
    d, m, y = fecha_ddmmyyyy.split('/')
    return f"{y}-{m}-{d}"


def partido_to_catalogado(p, cats):
    return {
        'fecha':         fecha_to_iso(p['fecha']),
        'fronton_id':    cats.get_or_create_fronton(p['fronton'], p['ciudad']),
        'competicion_id': cats.get_or_create_competicion(p['competicion'], p['tipo']),
        'tipo':          p['tipo'],
        'equipo1': {
            'del_id': cats.get_or_create_pelotari(p['equipo1']['delantero']),
            'zag_id': cats.get_or_create_pelotari(p['equipo1']['zaguero']),
        },
        'puntos1':       p['puntos1'],
        'equipo2': {
            'del_id': cats.get_or_create_pelotari(p['equipo2']['delantero']),
            'zag_id': cats.get_or_create_pelotari(p['equipo2']['zaguero']),
        },
        'puntos2':       p['puntos2'],
        'ganador':       p['ganador'],
    }


# ─────────────────────────────────────────────────────────────────
# DETECCIÓN DE DUPLICADOS (sobre formato catalogado)
# ─────────────────────────────────────────────────────────────────
def _safe(v):
    return v if v is not None else ''


def firma_partido(p):
    e1 = tuple(sorted([_safe(p['equipo1'].get('del_id')), _safe(p['equipo1'].get('zag_id'))]))
    e2 = tuple(sorted([_safe(p['equipo2'].get('del_id')), _safe(p['equipo2'].get('zag_id'))]))
    jugadores = tuple(sorted([e1, e2]))
    puntos = tuple(sorted([p.get('puntos1', 0), p.get('puntos2', 0)]))
    return (p['fecha'], jugadores, puntos)


def firma_sin_fecha(p):
    e1 = tuple(sorted([_safe(p['equipo1'].get('del_id')), _safe(p['equipo1'].get('zag_id'))]))
    e2 = tuple(sorted([_safe(p['equipo2'].get('del_id')), _safe(p['equipo2'].get('zag_id'))]))
    jugadores = tuple(sorted([e1, e2]))
    puntos = tuple(sorted([p.get('puntos1', 0), p.get('puntos2', 0)]))
    return (jugadores, puntos)


def es_duplicado(nuevo, firmas, firmas_sf):
    if firma_partido(nuevo) in firmas:
        return True
    try:
        f = datetime.strptime(nuevo['fecha'], '%Y-%m-%d').date()
    except Exception:
        return False
    sf = firma_sin_fecha(nuevo)
    for delta in (-1, 1):
        f_vecina = (f + timedelta(days=delta)).strftime('%Y-%m-%d')
        if (f_vecina, sf) in firmas_sf:
            return True
    return False


def es_formato_nuevo(partidos):
    if not partidos:
        return True
    p0 = partidos[0]
    return ('fronton_id' in p0) or (
        isinstance(p0.get('equipo1'), dict) and 'del_id' in p0['equipo1']
    )


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("Eskupilota Stats — Scraper de resultados (con catálogos)")
    print(f"Fuente: {URL}\n")

    resp = requests.get(URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    print(f"Status: {resp.status_code} — {len(resp.text)} chars")

    tokens = extract_tokens(resp.text)
    print(f"Tokens extraídos: {len(tokens)}")

    nuevos_planos = parse_tokens(tokens)
    print(f"Partidos encontrados: {len(nuevos_planos)}")
    for p in nuevos_planos:
        eq1 = f"{p['equipo1']['delantero']}-{p['equipo1']['zaguero']}" if p['equipo1']['zaguero'] else p['equipo1']['delantero']
        eq2 = f"{p['equipo2']['delantero']}-{p['equipo2']['zaguero']}" if p['equipo2']['zaguero'] else p['equipo2']['delantero']
        print(f"  {p['fecha']} | {p['fronton']} ({p['ciudad']}) | {eq1} {p['puntos1']}-{p['puntos2']} {eq2} | {p['competicion']}")

    print("\nCargando catálogos...")
    cats = Catalogos()
    print(f"  pelotaris: {len(cats.pelotaris)}, frontones: {len(cats.frontones)}, "
          f"ciudades: {len(cats.ciudades)}, competiciones: {len(cats.competiciones)}")

    if os.path.exists(PARTIDOS_FILE):
        with open(PARTIDOS_FILE, 'r', encoding='utf-8') as f:
            existentes = json.load(f)
    else:
        existentes = []

    if existentes and not es_formato_nuevo(existentes):
        print("\n⚠️  El archivo data/partidos.json está en formato viejo (sin IDs).")
        print("    Ejecuta primero el script de migración:")
        print("        python migrar_a_catalogos.py data/partidos.json data/")
        sys.exit(1)

    print(f"  partidos existentes: {len(existentes)}")

    print("\nConvirtiendo partidos nuevos a formato catalogado...")
    nuevos = [partido_to_catalogado(p, cats) for p in nuevos_planos]

    firmas = {firma_partido(p) for p in existentes}
    firmas_sf = {(p['fecha'], firma_sin_fecha(p)) for p in existentes}

    sin_dup = []
    descartados = []
    for p in nuevos:
        if es_duplicado(p, firmas, firmas_sf):
            descartados.append(p)
        else:
            sin_dup.append(p)
            firmas.add(firma_partido(p))
            firmas_sf.add((p['fecha'], firma_sin_fecha(p)))

    print(f"\nPartidos nuevos: {len(sin_dup)}")
    if descartados:
        print(f"Descartados por duplicado: {len(descartados)}")

    if not sin_dup:
        if any(cats._dirty.values()):
            cats.save_all()
            print("Catálogos actualizados (sin partidos nuevos).")
        else:
            print("No hay partidos nuevos. Sin cambios.")
        return

    todos = existentes + sin_dup
    todos.sort(key=lambda p: p['fecha'], reverse=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PARTIDOS_FILE, 'w', encoding='utf-8') as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    cats.save_all()

    print(f"\n✓ data/partidos.json actualizado: {len(todos)} partidos totales")
    print("✓ Catálogos actualizados")


if __name__ == '__main__':
    main()
