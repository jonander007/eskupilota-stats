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
import difflib
import json
from collections import Counter
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

from competiciones import (Cartelera, HistorialSeries, categoria_de_competicion, clasificar_partido,
                           es_texto_competicion)
from jsonio import guardar_json
from roles import aplicar_roles
from red import descargar
import aspe

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
AVISOS_FILE        = os.path.join(DATA_DIR, 'avisos_scraper.json')
# Se lee ANTES de que scraper_cartelera.py la regenere: aún contiene los
# partidos de ayer, con su competición.
CARTELERA_FILE     = os.path.join(DATA_DIR, 'cartelera.json')

URL_BAIKO = "https://www.baikopilota.eus/resultados/"

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
    'MORGAETXEBERRIA':   'MORGAETXEBARRIA',
    'MORGA':             'MORGAETXEBARRIA',
    'DARIO':             'DARÍO',
    'P. ETXEBARRIA':     'P.ETXEBERRIA',
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
    'EL CIEGO':            'ELCIEGO',
    'ETXARRI-ARANATZ':     'ETXARRI ARANATZ',
    'LARRAINZAR':          'LARRAINTZAR',
    'SANTA MARIA DE LAS HOYAS': 'SANTA MARÍA DE LAS HOYAS',
    'VILLABONA':           'AMASA-VILLABONA',
    'LAUDIO':              'LLODIO',
    'ILUNBERRI':           'LUMBIER',
    'URDUÑA':              'ORDUÑA',
}

# Variantes de nombre de un mismo frontón (ver tools/unificar_frontones_ciudades.py)
FRONTON_ALIAS = {
    'EL CIEGO':                 'ELCIEGO',
    'ETXARRI-ARANATZ':          'ETXARRI ARANATZ',
    'HONDARRIBI':               'HONDARRIBIA',
    'HUERCANOS':                'HUÉRCANOS',
    'LARRAINZAR':               'LARRAINTZAR',
    'MALLABIA I':               'MALLABIA',
    'NAJERA':                   'NÁJERA',
    'SANTA MARIA DE LAS HOYAS': 'SANTA MARÍA DE LAS HOYAS',
    'SANTO DOMINGO':            'SANTO DOMINGO DE LA CALZADA',
    'AMASA-VILLABONA':          'VILLABONA',
    'ORDUÑA':                   'URDUÑA',
    'LAUDIO':                   'LLODIO',
    'ILUNBERRI':                'LUMBIER',
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
    'LUMBIER':           {'es': 'Lumbier',       'eu': 'Ilunberri'},
    'ORDUÑA':            {'es': 'Orduña',        'eu': 'Urduña'},
    'OION':              {'es': 'Oyón',          'eu': 'Oion'},
    'OÑATI':             {'es': 'Oñate',         'eu': 'Oñati'},
    'LEGUTIO':           {'es': 'Legutiano',     'eu': 'Legutio'},
    'VILLAVA':           {'es': 'Villava',       'eu': 'Atarrabia'},
    'BURLADA':           {'es': 'Burlada',       'eu': 'Burlata'},
    'ELCIEGO':           {'es': 'Elciego',       'eu': 'Eltziego'},
    'AMASA-VILLABONA':   {'es': 'Villabona',     'eu': 'Amasa-Villabona'},
}

MINUSCULAS_CIUDAD = {'De', 'La', 'Las', 'Los', 'Del', 'Y'}


def titulo_ciudad(nombre):
    """'SANTO DOMINGO DE LA CALZADA' -> 'Santo Domingo de la Calzada'."""
    palabras = nombre.title().split(' ')
    return ' '.join(p.lower() if i and p in MINUSCULAS_CIUDAD else p
                    for i, p in enumerate(palabras))


def normalizar_ubicacion(fronton, ciudad):
    fronton = (fronton or '').strip().upper()
    ciudad  = (ciudad or '').strip().upper().rstrip(' -')
    ciudad = CIUDAD_ALIAS.get(ciudad, ciudad)
    fronton = FRONTON_ALIAS.get(fronton, fronton)
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


MARCA_RE = re.compile(r"\s*(\([^)]*\)|\^\{[^}]*\}|\*+)\s*$")


def clean_player(s):
    """Limpia el nombre de un pelotari.

    Baiko anota las sustituciones y las bajas con marcas pegadas al nombre:
    '(1)', '(LESION)', '^{1}'. Si no se quitan todas, la marca acaba siendo
    un pelotari fantasma en el catalogo o desplaza al zaguero real.
    """
    s = clean_text(s)
    anterior = None
    while anterior != s:
        anterior = s
        s = MARCA_RE.sub("", s).strip()
    return s


def clave_nombre(s):
    """'Peña II' / 'PENA II' / 'pena-ii' -> 'PENAII'."""
    s = unicodedata.normalize('NFD', (s or '').upper())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^A-Z0-9]', '', s)


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
        nombre = clean_player(tok)
        if not nombre:
            i += 1
            continue
        players.append(nombre)
        i += 1
    raise ValueError("No se encontró score")


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

            ganador = 'equipo1' if score1 > score2 else ('equipo2' if score2 > score1 else None)

            partidos.append({
                'fecha':       fecha,
                'fronton':     fronton or '',
                'ciudad':      ciudad or '',
                # tipo y competicion se deciden en clasificar_partidos()
                'comp_texto':  comp,
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
    guardar_json(path, items)


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
        self.avisos = []
        self._idx_pel = {p['nombre']: p for p in self.pelotaris}
        self._idx_fro = {f['nombre']: f for f in self.frontones}
        self._idx_ciu = {c['nombre']: c for c in self.ciudades}
        self._idx_cmp = {c['nombre']: c for c in self.competiciones}
        # Mismo nombre escrito de otra forma ('Dario', 'PENA II', 'Nájera'):
        # cada fuente escribe distinto y no debe crear duplicados
        self._clave_pel = {clave_nombre(p['nombre']): p for p in self.pelotaris}
        self._clave_fro = {clave_nombre(f['nombre']): f for f in self.frontones}
        self._clave_ciu = {clave_nombre(c['nombre']): c for c in self.ciudades}
        # La ciudad también por su nombre en castellano y euskera ('Bilbo', 'Mondragón')
        for c in self.ciudades:
            for k in ('nombre_es', 'nombre_eu'):
                if c.get(k):
                    self._clave_ciu.setdefault(clave_nombre(c[k]), c)

    def _avisar(self, tipo, nombre, parecidos):
        self.avisos.append({
            'tipo': tipo, 'nombre': nombre, 'parecidos': parecidos,
        })
        print(f"  !! {tipo} nuevo '{nombre}' se parece a {parecidos}. "
              f"Revisa si es un duplicado antes de darlo por bueno.")

    def _parecidos(self, nombre, indice):
        return difflib.get_close_matches(nombre, list(indice.keys()), n=3, cutoff=0.85)

    def _casi_igual(self, nombre):
        """Pelotari existente que solo difiere por una errata ('P.Etxberria').
        Solo si hay uno, y con el mismo ordinal: ZUBIZARRETA III no es IV."""
        ordinal = lambda n: (re.search(r'\s(I{1,3}|IV|V|VI{0,3})$', n.upper()) or [None])[0]
        k = clave_nombre(nombre)
        cands = difflib.get_close_matches(k, list(self._clave_pel), n=3, cutoff=0.9)
        cands = [self._clave_pel[c] for c in cands if ordinal(self._clave_pel[c]['nombre']) == ordinal(nombre)]
        return cands[0] if len(cands) == 1 else None

    def fronton_de_pueblo(self, pueblo):
        """Cuando la fuente solo da el pueblo ('Altsasu', 'Bilbo'): el frontón
        principal (con más partidos) de esa ciudad, si se conoce."""
        for trozo in [pueblo] + re.split(r'\s*[-/]\s*', pueblo):
            ciu = self._clave_ciu.get(clave_nombre(trozo))
            if ciu:
                suyos = [f for f in self.frontones if f.get('ciudad_id') == ciu['id']]
                if suyos:
                    return max(suyos, key=lambda f: f.get('partidos_count', 0)), ciu
                return None, ciu
        return None, None

    def get_or_create_pelotari(self, nombre):
        if not nombre:
            return None
        if nombre in self._idx_pel:
            return self._idx_pel[nombre]['id']
        if clave_nombre(nombre) in self._clave_pel:
            return self._clave_pel[clave_nombre(nombre)]['id']
        casi = self._casi_igual(nombre)
        if casi:
            self.avisos.append({'tipo': 'pelotari_aproximado', 'nombre': nombre, 'se_usa': casi['nombre']})
            print(f"  ~ '{nombre}' se toma como {casi['nombre']} (errata probable)")
            return casi['id']
        parecidos = self._parecidos(nombre, self._idx_pel)
        if parecidos:
            self._avisar('pelotari', nombre, parecidos)
        pid = next_id('PEL', self.pelotaris)
        nuevo = {
            'id': pid, 'nombre': nombre,
            'rol': 'mixto', 'partidos_count': 0,
        }
        self.pelotaris.append(nuevo)
        self._idx_pel[nombre] = nuevo
        self._clave_pel[clave_nombre(nombre)] = nuevo
        self._dirty['pel'] = True
        print(f"  + nuevo pelotari: {pid} {nombre}")
        return pid

    def get_or_create_ciudad(self, nombre):
        if not nombre:
            return None
        if nombre in self._idx_ciu:
            return self._idx_ciu[nombre]['id']
        if clave_nombre(nombre) in self._clave_ciu:
            return self._clave_ciu[clave_nombre(nombre)]['id']
        cid = next_id('CIU', self.ciudades)
        trad = TRADUCCIONES_CIUDAD.get(nombre, {'es': titulo_ciudad(nombre), 'eu': titulo_ciudad(nombre)})
        nuevo = {
            'id': cid, 'nombre': nombre,
            'nombre_es': trad['es'], 'nombre_eu': trad['eu'],
        }
        self.ciudades.append(nuevo)
        self._idx_ciu[nombre] = nuevo
        self._clave_ciu[clave_nombre(nombre)] = nuevo
        self._dirty['ciu'] = True
        print(f"  + nueva ciudad: {cid} {nombre}")
        return cid

    def get_or_create_fronton(self, nombre, ciudad_nombre):
        if not nombre:
            return None
        if nombre not in self._idx_fro and clave_nombre(nombre) in self._clave_fro:
            nombre = self._clave_fro[clave_nombre(nombre)]['nombre']
        if nombre not in self._idx_fro and clave_nombre(nombre) == clave_nombre(ciudad_nombre):
            # Solo sabemos el pueblo: su frontón principal, o uno nuevo con el
            # nombre de la ciudad tal y como está en el catálogo
            fro, ciu = self.fronton_de_pueblo(nombre)
            if fro:
                return fro['id']
            if ciu:
                nombre = ciudad_nombre = ciu['nombre']
        if nombre in self._idx_fro:
            f = self._idx_fro[nombre]
            if not f.get('ciudad_id') and ciudad_nombre:
                f['ciudad_id'] = self.get_or_create_ciudad(ciudad_nombre)
                self._dirty['fro'] = True
            return f['id']
        parecidos = self._parecidos(nombre, self._idx_fro)
        if parecidos:
            self._avisar('fronton', nombre, parecidos)
        fid = next_id('FRO', self.frontones)
        ciudad_id = self.get_or_create_ciudad(ciudad_nombre) if ciudad_nombre else None
        nuevo = {
            'id': fid, 'nombre': nombre,
            'ciudad_id': ciudad_id, 'partidos_count': 0,
        }
        self.frontones.append(nuevo)
        self._idx_fro[nombre] = nuevo
        self._clave_fro[clave_nombre(nombre)] = nuevo
        self._dirty['fro'] = True
        print(f"  + nuevo frontón: {fid} {nombre} ({ciudad_nombre})")
        return fid

    def get_or_create_competicion(self, nombre, tipo):
        if not nombre:
            return None
        if nombre in self._idx_cmp:
            return self._idx_cmp[nombre]['id']
        cid = next_id('COMP', self.competiciones)
        categoria, _ = categoria_de_competicion(nombre)
        nuevo = {
            'id': cid, 'nombre': nombre, 'tipo': tipo, 'categoria': categoria, 'partidos_count': 0,
        }
        self.competiciones.append(nuevo)
        self._idx_cmp[nombre] = nuevo
        self._dirty['cmp'] = True
        print(f"  + nueva competición: {cid} {nombre}")
        return cid

    def recalcular_contadores(self, partidos):
        """Deriva partidos_count desde la lista final de partidos.

        Los contadores no se acumulan: se recalculan enteros en cada
        ejecucion, por lo que es imposible que se descuadren aunque el
        scraper procese los mismos partidos muchas veces.
        """
        c_pel, c_fro, c_cmp = Counter(), Counter(), Counter()
        for p in partidos:
            if p.get('fronton_id'):
                c_fro[p['fronton_id']] += 1
            if p.get('competicion_id'):
                c_cmp[p['competicion_id']] += 1
            for equipo in ('equipo1', 'equipo2'):
                e = p.get(equipo) or {}
                for rol in ('del_id', 'zag_id'):
                    if e.get(rol):
                        c_pel[e[rol]] += 1

        for items, cuenta, flag in (
            (self.pelotaris, c_pel, 'pel'),
            (self.frontones, c_fro, 'fro'),
            (self.competiciones, c_cmp, 'cmp'),
        ):
            for it in items:
                nuevo = cuenta.get(it['id'], 0)
                if it.get('partidos_count') != nuevo:
                    it['partidos_count'] = nuevo
                    self._dirty[flag] = True

        for pid, nombre, antes, despues in aplicar_roles(self.pelotaris, partidos):
            print(f"  rol de {nombre}: {antes} -> {despues}")
            self._dirty['pel'] = True

    def guardar_avisos(self):
        if not self.avisos:
            if os.path.exists(AVISOS_FILE):
                os.remove(AVISOS_FILE)
            return
        with open(AVISOS_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'generado': datetime.now().strftime('%d/%m/%Y %H:%M'),
                'avisos': self.avisos,
            }, f, ensure_ascii=False, indent=2)
        print(f"\n!! {len(self.avisos)} avisos escritos en {AVISOS_FILE}")

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
        'fuente':        p.get('fuente', 'baiko'),
        **campos_clasificacion(p.get('clasificacion')),
    }


def campos_clasificacion(c):
    """modalidad, categoria y serie siempre; fase, grupo y jornada si se conocen."""
    if not c:
        return {}
    campos = {'modalidad': c['modalidad'], 'categoria': c['categoria'], 'serie': c['serie']}
    for k in ('fase', 'grupo', 'jornada'):
        if c.get(k) is not None:
            campos[k] = c[k]
    return campos


# ─────────────────────────────────────────────────────────────────
# DETECCIÓN DE DUPLICADOS (sobre formato catalogado)
# ─────────────────────────────────────────────────────────────────
def _safe(v):
    return v if v is not None else ''


def jugadores_partido(p):
    return frozenset(_safe(p[eq].get(k)) for eq in ('equipo1', 'equipo2')
                     for k in ('del_id', 'zag_id') if p[eq].get(k))


def firma_sin_fecha(p):
    puntos = tuple(sorted([p.get('puntos1', 0), p.get('puntos2', 0)]))
    return (jugadores_partido(p), puntos)


class IndicePartidos:
    """Partidos ya guardados, para saber si uno nuevo es el mismo.

    Es el mismo partido si coincide la fecha y los pelotaris, aunque el
    tanteo no cuadre (una fuente puede equivocarse: se avisa y se conserva el
    que ya estaba). Con un día de diferencia solo se da por el mismo si
    además coincide el tanteo (hay webs que fechan mal los partidos
    nocturnos)."""

    def __init__(self, partidos):
        self._por_fecha = {}
        self._firmas_sf = set()
        for p in partidos:
            self.add(p)

    def add(self, p):
        self._por_fecha[(p['fecha'], jugadores_partido(p))] = p
        self._firmas_sf.add((p['fecha'], firma_sin_fecha(p)))

    def buscar(self, nuevo):
        mismo = self._por_fecha.get((nuevo['fecha'], jugadores_partido(nuevo)))
        if mismo:
            return mismo
        try:
            f = datetime.strptime(nuevo['fecha'], '%Y-%m-%d').date()
        except (TypeError, ValueError):
            return None
        sf = firma_sin_fecha(nuevo)
        for delta in (-1, 1):
            vecina = (f + timedelta(days=delta)).strftime('%Y-%m-%d')
            if (vecina, sf) in self._firmas_sf:
                return True
        return None


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
def cargar_cartelera():
    if not os.path.exists(CARTELERA_FILE):
        return Cartelera()
    try:
        with open(CARTELERA_FILE, 'r', encoding='utf-8') as f:
            return Cartelera(json.load(f).get('partidos', []))
    except (ValueError, AttributeError):
        return Cartelera()


def textos_clasificacion(texto_web, detalle_cartelera):
    """(texto, serie, textos_fase) para clasificar_partido().

    La web de resultados a veces solo pone la fase ('Final', 'Semifinal'):
    eso no dice la competición. Entonces la competición y la serie salen de
    la cartelera ('Torneo San Mateo', 'Serie B') y el texto de la web se usa
    como fase. La cartelera aporta también la fase aunque la web diga la
    competición."""
    texto = texto_web if texto_web and es_texto_competicion(texto_web) else None
    serie = None
    textos_fase = (texto_web,) if texto_web and not texto else ()
    if detalle_cartelera:
        textos_fase += tuple(detalle_cartelera['textos_fase'])
        if not texto:
            texto, serie = detalle_cartelera['texto'], detalle_cartelera['serie']
    return texto, serie, textos_fase


def clasificar_partidos(planos, existentes, cats, cart=None):
    """Decide tipo y competición de cada partido (ver competiciones.py).

    Si la web de resultados no dice la competición, se busca el partido en
    la cartelera; la serie, si no se sabe, se deduce por los pelotaris.
    """
    cart = cart if cart is not None else cargar_cartelera()
    hist = HistorialSeries(existentes)
    for p in planos:
        jugadores = [p['equipo1']['delantero'], p['equipo1']['zaguero'],
                     p['equipo2']['delantero'], p['equipo2']['zaguero']]
        es_pareja = bool(p['equipo1']['zaguero'] or p['equipo2']['zaguero'])
        d = cart.buscar_detalle(p['fecha'], p['fronton'], jugadores)
        texto, serie, textos_fase = textos_clasificacion(p.get('comp_texto'), d)
        ids = [cats._idx_pel[n]['id'] for n in jugadores if n and n in cats._idx_pel]
        p['clasificacion'] = clasificar_partido(
            texto, p['fecha'], es_pareja, serie, lambda: hist.serie(ids, p['fecha']), textos_fase)
        p['tipo'], p['competicion'] = p['clasificacion']['tipo'], p['clasificacion']['competicion']


def planos_baiko(html):
    tokens = extract_tokens(html)
    planos = parse_tokens(tokens)
    fechas = sum(1 for t in tokens if DATE_RE.match(clean_text(t)))
    if fechas and not planos:
        raise RuntimeError(f"La página tiene {fechas} fechas pero no se ha leído ningún partido: "
                           "¿ha cambiado la estructura de la web?")
    return planos


# Fuentes de resultados, por orden de preferencia: de la primera se guarda
# todo; de las siguientes, solo los partidos que no estén ya.
FUENTES = [
    ('baiko', URL_BAIKO, planos_baiko),
    ('aspe', aspe.URL_RESULTADOS, lambda html: aspe.resultados_aspe(html, norm)),
]


def recoger_fuentes(fuentes):
    """Descarga y lee cada fuente. Devuelve (planos, fuentes_fallidas)."""
    planos, fallos = [], []
    for nombre, url, parser in fuentes:
        print(f"Fuente {nombre}: {url}")
        try:
            leidos = parser(descargar(url))
        except Exception as e:  # una fuente caída no debe tumbar a las demás
            print(f"  ✗ {nombre}: {e}")
            fallos.append(nombre)
            continue
        for p in leidos:
            p['fuente'] = nombre
        print(f"  {len(leidos)} partidos leídos")
        planos.extend(leidos)
    return planos, fallos


def fusionar(nuevos, existentes, cats):
    """Devuelve los partidos de `nuevos` que no están ya en `existentes` (ni
    repetidos entre fuentes). Avisa si dos fuentes dan distinto tanteo."""
    indice = IndicePartidos(existentes)
    anadir, repetidos = [], 0
    for p in nuevos:
        igual = indice.buscar(p)
        if igual is None:
            anadir.append(p)
            indice.add(p)
            continue
        repetidos += 1
        if isinstance(igual, dict) and sorted([igual['puntos1'], igual['puntos2']]) != sorted([p['puntos1'], p['puntos2']]):
            cats.avisos.append({'tipo': 'tanteo_distinto', 'fecha': p['fecha'],
                                'guardado': f"{igual['puntos1']}-{igual['puntos2']} ({igual.get('fuente', 'baiko')})",
                                'otra_fuente': f"{p['puntos1']}-{p['puntos2']} ({p.get('fuente')})"})
            print(f"  !! {p['fecha']}: tanteo distinto entre fuentes — se conserva el guardado "
                  f"{igual['puntos1']}-{igual['puntos2']}, {p.get('fuente')} dice {p['puntos1']}-{p['puntos2']}")
    return anadir, repetidos


def main():
    print("Eskupilota Stats — Scraper de resultados (con catálogos)\n")
    nuevos_planos, fallos = recoger_fuentes(FUENTES)
    if len(fallos) == len(FUENTES):
        print("\n✗ No se ha podido leer ninguna fuente. No se toca nada.")
        sys.exit(1)

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
        print("\n⚠️  data/partidos.json está en formato viejo (sin IDs).")
        print("    Ejecuta primero: python tools/migrar_a_catalogos.py data/partidos.json data/")
        sys.exit(1)
    print(f"  partidos existentes: {len(existentes)}")

    clasificar_partidos(nuevos_planos, existentes, cats)
    for p in nuevos_planos:
        eq1 = f"{p['equipo1']['delantero']}-{p['equipo1']['zaguero']}" if p['equipo1']['zaguero'] else p['equipo1']['delantero']
        eq2 = f"{p['equipo2']['delantero']}-{p['equipo2']['zaguero']}" if p['equipo2']['zaguero'] else p['equipo2']['delantero']
        print(f"  [{p['fuente']}] {p['fecha']} | {p['fronton']} ({p['ciudad']}) | {eq1} {p['puntos1']}-{p['puntos2']} {eq2} | {p['competicion']}")

    nuevos = [partido_to_catalogado(p, cats) for p in nuevos_planos]
    sin_dup, repetidos = fusionar(nuevos, existentes, cats)
    por_fuente = Counter(p['fuente'] for p in sin_dup)
    print(f"\nPartidos nuevos: {len(sin_dup)} {dict(por_fuente) if por_fuente else ''}")
    print(f"Ya guardados o repetidos entre fuentes: {repetidos}")

    todos = existentes + sin_dup
    todos.sort(key=lambda p: p['fecha'], reverse=True)
    if sin_dup:
        guardar_json(PARTIDOS_FILE, todos)
    cats.recalcular_contadores(todos)
    if sin_dup or any(cats._dirty.values()):
        cats.save_all()
    cats.guardar_avisos()
    print(f"\n✓ {len(todos)} partidos en total" if sin_dup else "\nSin partidos nuevos.")

    if fallos:
        print(f"✗ Fuentes que han fallado: {', '.join(fallos)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
