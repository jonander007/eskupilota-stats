"""
Eskupilota Stats — Clasificación de partidos

Lo usan los dos scrapers (resultados y cartelera, de Baiko y de Aspe) y las
herramientas de limpieza, para que todos los partidos sigan el mismo criterio.

Cada partido se describe con cuatro datos independientes:

  modalidad  parejas | mano | cuatro          (la marcan los pelotaris y el texto)
  categoria  campeonato | torneo | desafio | festival
               campeonato: los tres campeonatos de la liga (Parejas,
                 Manomanista, 4 y Medio), Serie A y Serie B (la
                 Promoción cuenta como Serie B)
               torneo: Masters CaixaBank, San Fermín, San Mateo, Aste
                 Nagusia, La Blanca, Donostia Hiria, Bizkaia...
               desafio: Desafío Urzante
               festival: todo lo demás
  serie      A | B | None
  fase       liga | eliminatoria | octavos | cuartos | semifinal | final | None
             (+ grupo 'A'/'B' y jornada n cuando se conocen)

y además con el nombre de la competición en el catálogo y el campo `tipo`
de siempre (modalidad + serie), que es el que usa la web para filtrar:
  campeonato-a/b, manomanista-a/b, cuatro-medio-a/b  (campeonatos y torneos)
  festival, festival-mano, festival-cuatro           (desafíos y festivales)

Nombres en el catálogo:
  "Campeonato Parejas Serie A 2026" (el de parejas se juega de noviembre a
  marzo y lleva el año en que termina), "Campeonato 4 y Medio Serie B 2025",
  "Torneo San Mateo Serie A 2026", "Torneo San Fermin 4 y Medio 2026",
  "Masters CaixaBank Serie B 2026", "Desafio Urzante San Mateo 2026", "Festival".

Cuando la web de resultados no dice la competición, se busca el partido en
la cartelera, que sí la anuncia. Si tampoco está ahí, solo se da por
campeonato un partido de parejas jugado en temporada (noviembre a marzo); el
resto se considera festival.
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from datetime import date, timedelta

# Meses en los que se juega cada campeonato. El manomanista termina a
# finales de mayo o primeros de junio: lo de después son festivales.
TEMPORADA = {
    'parejas': {11, 12, 1, 2, 3},
    'mano':    {3, 4, 5, 6},
    'cuatro':  {9, 10, 11},
}
FIN_MANOMANISTA = (6, 15)   # 15 de junio


def en_temporada(mod, fecha):
    f = _fecha(fecha) if isinstance(fecha, str) else fecha
    if f.month not in TEMPORADA[mod]:
        return False
    if mod == 'mano' and (f.month, f.day) > FIN_MANOMANISTA:
        return False
    return True

# Torneos conocidos: palabra clave -> nombre base en el catálogo
TORNEOS = [
    ('masters',        'Masters CaixaBank'),
    ('san fermin',     'Torneo San Fermin'),
    ('san mateo',      'Torneo San Mateo'),
    ('aste nagusia',   'Torneo Aste Nagusia'),
    ('la blanca',      'Torneo La Blanca'),
    ('andre maria zuria', 'Torneo La Blanca'),     # su nombre en euskera
    ('donostia hiria', 'Torneo Donostia Hiria'),
    ('bizkaia',        'Torneo Bizkaia'),
]

PREFIJO_TIPO = {'parejas': 'campeonato', 'mano': 'manomanista', 'cuatro': 'cuatro-medio'}
NOMBRE_MODALIDAD = {'parejas': 'Parejas', 'mano': 'Manomanista', 'cuatro': '4 y Medio'}


def sin_tildes(s):
    s = unicodedata.normalize('NFD', s or '')
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')


def _txt(s):
    return ' '.join(sin_tildes(s).lower().replace('½', ' 1/2').split())


def _fecha(f):
    """Acepta 'dd/mm/yyyy' o 'yyyy-mm-dd'."""
    if '/' in f:
        d, m, y = f.split('/')
    else:
        y, m, d = f.split('-')
    return date(int(y), int(m), int(d))


def modalidad(texto, es_pareja):
    t = _txt(texto)
    if '4 1/2' in t or '4 y medio' in t or 'lau t' in t:
        return 'cuatro'
    if 'parejas' in t or 'binaka' in t:
        return 'parejas'
    if 'manomanista' in t or 'buruz buru' in t:
        return 'mano'
    return 'parejas' if es_pareja else 'mano'


def serie_en_texto(texto):
    t = _txt(texto)
    if re.search(r'\bserie a\b', t):
        return 'a'
    if re.search(r'\bserie b\b', t) or 'promocion' in t:
        return 'b'
    return None


def es_texto_competicion(texto):
    """¿La línea parece el nombre de una competición y no un cartel?"""
    t = _txt(texto)
    if not t or '//' in t:
        return False
    claves = ('campeonato', 'torneo', 'masters', 'festival', 'eusko label',
              'manomanista', 'parejas', '4 1/2', 'desafio', 'serie a', 'serie b')
    return any(k in t for k in claves)


CATEGORIAS = ('campeonato', 'torneo', 'desafio', 'festival')
FASES = ('liga', 'eliminatoria', 'octavos', 'cuartos', 'semifinal', 'tercero', 'final')


def leer_fase(*textos):
    """Fase, grupo y jornada a partir de los textos del partido (en
    castellano o euskera): 'Cuartos de final (1ª Jornada)', '7ª Jornada /
    Semifinal', 'Semifinales (Grupo A)', 'Zortzirenak // Octavos', 'Finala',
    'Campeonato Parejas Serie A - Liga'. Devuelve (fase, grupo, jornada)."""
    t = " · ".join(_txt(x) for x in textos if x)
    fase = None
    if re.search(r"semifinal|finalerdi", t):
        fase = 'semifinal'
    elif re.search(r"cuartos|laurden", t):
        fase = 'cuartos'
    elif re.search(r"octavos|zortziren", t):
        fase = 'octavos'
    elif re.search(r"\bfinal(es|a|ak)?\b", t):
        fase = 'final'
    elif re.search(r"eliminatoria|kanporaketa", t):
        fase = 'eliminatoria'
    elif re.search(r"jornada|jardunaldi|\bliga\b|liguilla|ligaxka", t):
        fase = 'liga'
    g = re.search(r"\bgrupo ([a-d])\b|\b([a-d]) multzoa", t)
    j = re.search(r"(\d+)\s*(?:ª|a\.?|\.)?\s*(?:jornada|jardunaldi)", t)
    return fase, (g.group(1) or g.group(2)).upper() if g else None, int(j.group(1)) if j else None


def _tipo(modalidad, categoria, serie):
    if categoria in ('campeonato', 'torneo'):
        return f"{PREFIJO_TIPO[modalidad]}-{'b' if serie == 'B' else 'a'}"
    return {'parejas': 'festival', 'mano': 'festival-mano', 'cuatro': 'festival-cuatro'}[modalidad]


def clasificar_partido(texto, fecha, es_pareja, serie=None, serie_jugadores=None, textos_fase=()):
    """Clasifica un partido. Devuelve un dict con modalidad, categoria,
    serie, fase, grupo, jornada, competicion (nombre en el catálogo) y tipo.

    texto            línea de competición de la web o de la cartelera (o None)
    fecha            'dd/mm/yyyy' o 'yyyy-mm-dd'
    es_pareja        True si el partido tiene zagueros
    serie            'a'/'b' si ya se conoce (p. ej. por la cartelera)
    serie_jugadores  función sin argumentos que deduce la serie por los
                     pelotaris; solo se llama si hace falta
    textos_fase      otros textos donde puede venir la fase (fase de la
                     cartelera, título del partido...)
    """
    f = _fecha(fecha)
    t = _txt(texto)
    # La modalidad la marca el partido (parejas o individual). Si el texto es
    # de otra modalidad (un partido de parejas en un evento del 4 y medio), el
    # texto no habla de este partido: es un festival dentro de ese evento.
    mod_texto = modalidad(texto, es_pareja) if t else None
    if es_pareja:
        mod = 'parejas'
        otra = mod_texto in ('mano', 'cuatro')
    else:
        mod = mod_texto if mod_texto in ('mano', 'cuatro') else 'mano'
        otra = mod_texto == 'parejas'

    def _serie(deducir=True):
        s = serie_en_texto(texto) or serie
        if not s and deducir and serie_jugadores:
            s = serie_jugadores()
        return s.upper() if s else None

    def resultado(categoria, nombre, serie_=None):
        fase, grupo, jornada = (None, None, None)
        if categoria != 'festival':
            fase, grupo, jornada = leer_fase(texto, *textos_fase)
        return {'modalidad': mod, 'categoria': categoria, 'serie': serie_,
                'fase': fase, 'grupo': grupo, 'jornada': jornada,
                'competicion': nombre, 'tipo': _tipo(mod, categoria, serie_)}

    def festival(nombre='Festival'):
        return resultado('festival', nombre)

    def campeonato():
        s = _serie() or 'A'
        anio = f.year + 1 if (mod == 'parejas' and f.month >= 11) else f.year
        return resultado('campeonato', f"Campeonato {NOMBRE_MODALIDAD[mod]} Serie {s} {anio}", s)

    if not t:
        if mod == 'parejas' and en_temporada('parejas', f):
            return campeonato()
        return festival()
    if otra:
        return festival()

    # Despedidas: festival con su nombre
    if 'despedida' in t:
        return festival(' '.join(texto.split()).title())

    # Desafíos: 'Desafío Urzante' (a veces con la fiesta: San Fermín, San Mateo)
    if 'desafio' in t:
        base = 'Desafio Urzante' if 'urzante' in t else ' '.join(texto.split()).title()
        fiesta = ' San Fermin' if 'san fermin' in t else (' San Mateo' if 'san mateo' in t else '')
        return resultado('desafio', f"{base}{fiesta} {f.year}")

    for clave, base in TORNEOS:
        if clave in t and clave != 'urzante':
            if mod == 'cuatro':
                return resultado('torneo', f"{base} 4 y Medio {f.year}", _serie(deducir=False))
            if mod == 'mano':
                return resultado('torneo', f"{base} Manomanista {f.year}", _serie(deducir=False))
            s = _serie() or 'A'
            return resultado('torneo', f"{base} Serie {s} {f.year}", s)

    if 'festival' in t or 'jaialdi' in t:
        return festival()

    if 'campeonato' in t or 'txapelketa' in t or 'eusko label' in t or 'manomanista' in t or 'serie' in t:
        return campeonato()

    # Otro torneo con nombre propio ('Torneo de Navidad'): torneo, no festival
    if 'torneo' in t:
        nombre = re.sub(r'\s*\b(serie [ab]|20\d\d)\b', '', ' '.join(texto.split()), flags=re.I).strip().title()
        nombre = re.sub(r'(?<=\s)(De|Del|La|Las|Los|Y)(?=\s)', lambda m: m.group(1).lower(), nombre)
        return resultado('torneo', f"{nombre} {f.year}", _serie(deducir=False))

    return festival()


def clasificar(texto, fecha, es_pareja, serie=None, serie_jugadores=None):
    """Compatibilidad: (tipo, competicion)."""
    r = clasificar_partido(texto, fecha, es_pareja, serie, serie_jugadores)
    return r['tipo'], r['competicion']


def categoria_de_competicion(nombre):
    """Categoría y modalidad de una competición del catálogo por su nombre."""
    t = _txt(nombre)
    if t.startswith('festival'):
        cat = 'festival'
    elif 'desafio' in t:
        cat = 'desafio'
    elif t.startswith('campeonato'):
        cat = 'campeonato'
    else:
        cat = 'torneo'
    mod = 'cuatro' if re.search(r'4 y medio|4 1/2', t) else ('mano' if 'manomanista' in t else None)
    return cat, mod


# ─────────────────────────────────────────────────────────────────
# Serie de un partido según quién lo juega
# ─────────────────────────────────────────────────────────────────
class HistorialSeries:
    """Cuenta en qué serie (A/B) ha jugado cada pelotari en los dos años
    anteriores a una fecha, a partir de partidos con tipo *-a / *-b."""

    def __init__(self, partidos):
        self._por_pel = {}
        for p in partidos:
            tipo = p.get('tipo') or ''
            if not (tipo.endswith('-a') or tipo.endswith('-b')):
                continue
            s = tipo[-1]
            f = _fecha(p['fecha'])
            for eq in ('equipo1', 'equipo2'):
                for k in ('del_id', 'zag_id'):
                    pid = (p.get(eq) or {}).get(k)
                    if pid:
                        self._por_pel.setdefault(pid, []).append((f, s))

    def serie(self, ids, fecha):
        f = _fecha(fecha)
        desde = f - timedelta(days=730)
        votos = Counter()
        for pid in ids:
            for fp, s in self._por_pel.get(pid, []):
                if desde <= fp <= f:
                    votos[s] += 1
        if not votos:
            return None
        return 'b' if votos['b'] > votos['a'] else 'a'


# ─────────────────────────────────────────────────────────────────
# Cartelera: buscar la competición de un partido ya jugado
# ─────────────────────────────────────────────────────────────────
def _nombre_pel(s):
    s = re.sub(r'\([^)]*\)', '', s or '')      # "ARTOLA (4 1/2)" -> "ARTOLA"
    return re.sub(r'[^A-Z0-9]', '', sin_tildes(s.upper()))


class Cartelera:
    """Índice de los eventos de una o varias cartelera.json."""

    def __init__(self, eventos=()):
        self._idx = {}
        for ev in eventos:
            self.add(ev)

    def add(self, ev):
        try:
            f = _fecha(ev['fecha'])
        except (KeyError, ValueError):
            return
        self._idx.setdefault(f, []).append(ev)

    @staticmethod
    def texto_evento(ev):
        if ev.get('fase') and 'desaf' in _txt(ev['fase']):
            return ev['fase']
        for cand in (ev.get('competicion'), (ev.get('cartel') or [None])[0], ev.get('fase')):
            if cand and es_texto_competicion(cand):
                return cand
        return None

    @staticmethod
    def es_apertura(ev, p):
        """En las veladas de un torneo con Serie A y Serie B la cartelera marca
        cada partido del torneo con '(Serie A)' o '(Serie B)'; el que va sin
        marca es el partido de apertura, un festival (San Mateo 2026: 'ZUBIZARRETA
        IV – MORGAETXEBARRIA // APEZETXEA II – EROSTARBE' junto a '... (Serie A)'
        y '... (Serie B)'). Solo con marcas bien leídas: si la línea está cortada
        ('... (') o las marcas son raras ('(Serie A | B)') no se decide nada."""
        raw = (p.get('raw') or '').strip()
        if 'serie' in _txt(raw) or raw.endswith('('):
            return False
        marca = re.compile(r'\(serie [ab]\)\s*$')
        return any(marca.search(_txt(q.get('raw'))) for q in ev.get('partidos') or [] if q is not p)

    def buscar(self, fecha, fronton, jugadores):
        """Devuelve (texto_competicion, serie) o (None, None)."""
        d = self.buscar_detalle(fecha, fronton, jugadores)
        return (d['texto'], d['serie']) if d else (None, None)

    def buscar_detalle(self, fecha, fronton, jugadores):
        """Como buscar(), pero con los textos donde viene la fase:
        {'texto', 'serie', 'textos_fase'} o None."""
        fro = _nombre_pel(fronton)
        nombres = {_nombre_pel(j) for j in jugadores if j}
        for ev in self._idx.get(_fecha(fecha), []):
            fro_ev = _nombre_pel(ev.get('fronton'))
            mismo_fronton = not (fro and fro_ev) or fro == fro_ev or fro in fro_ev or fro_ev in fro
            # Si el frontón no coincide (una web da el pueblo y otra el frontón:
            # 'Baños de Rio Tobia' / 'Barberito I') basta con los pelotaris, pero
            # exigiendo más: 3 de 4 en parejas (puede haber un sustituto), todos
            # en individual
            minimo = min(2, len(nombres)) if mismo_fronton else min(3, len(nombres))
            for p in ev.get('partidos') or []:
                del_cartel = {_nombre_pel(j) for j in (p.get('eq1') or []) + (p.get('eq2') or [])}
                if nombres and len(nombres & del_cartel) >= minimo:
                    if self.es_apertura(ev, p):
                        return {'texto': 'Festival 4 1/2' if '4 1/2' in _txt(p.get('raw')) else 'Festival',
                                'serie': None, 'textos_fase': ()}
                    texto = self.texto_evento(ev)
                    # La modalidad a veces solo aparece en la línea del partido: "A // B (4 1/2)"
                    if texto and '4 1/2' in _txt(p.get('raw')) and modalidad(texto, True) != 'cuatro':
                        texto += ' 4 1/2'
                    elif not texto and '4 1/2' in _txt(p.get('raw')):
                        texto = 'Festival 4 1/2'
                    return {'texto': texto, 'serie': p.get('serie'),
                            'textos_fase': (ev.get('fase'), p.get('fase'), p.get('raw'))}
            # Un único partido en el evento sin nombres conocidos (XX // XX)
            if fro and fro_ev == fro and len(ev.get('partidos') or []) <= 1:
                return {'texto': self.texto_evento(ev), 'serie': None, 'textos_fase': (ev.get('fase'),)}
        return None
