"""
Eskupilota Stats — Clasificación de partidos en competiciones

Lo usan el scraper de resultados y tools/limpieza_competiciones.py, para
que los partidos nuevos y los antiguos sigan exactamente el mismo criterio.

Criterio (el de los datos históricos):
  - Campeonatos: "Campeonato Parejas Serie A 2026", "Campeonato Manomanista
    Serie B 2025", "Campeonato 4 y Medio Serie A 2025". Tipo
    campeonato-x / manomanista-x / cuatro-medio-x.
    El de parejas se juega de noviembre a marzo y lleva el año en que
    termina: un partido de diciembre de 2025 es del campeonato 2026.
  - Torneos con serie (Masters, San Fermín, San Mateo...): "Masters
    CaixaBank Serie A 2026", tipo campeonato-x. Los de 4 y medio:
    "Torneo San Fermin 4 y Medio 2026", tipo cuatro-medio-a.
  - Todo lo demás: competición "Festival", tipo festival (parejas),
    festival-mano o festival-cuatro (individuales).

Cuando la web de resultados no dice la competición, se busca el partido en
la cartelera (data/cartelera.json), que sí la anuncia. Si tampoco está ahí,
solo se da por campeonato un partido de parejas jugado en temporada
(noviembre a marzo); el resto se considera festival.
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
    ('donostia hiria', 'Torneo Donostia Hiria'),
    ('bizkaia',        'Torneo Bizkaia'),
    ('urzante',        'Desafio Urzante'),
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


def clasificar(texto, fecha, es_pareja, serie=None, serie_jugadores=None):
    """Devuelve (tipo, nombre_competicion).

    texto            línea de competición de la web o de la cartelera (o None)
    fecha            'dd/mm/yyyy' o 'yyyy-mm-dd'
    es_pareja        True si el partido tiene zagueros
    serie            'a'/'b' si ya se conoce (p. ej. por la cartelera)
    serie_jugadores  función sin argumentos que deduce la serie por los
                     pelotaris; solo se llama si hace falta
    """
    f = _fecha(fecha)
    t = _txt(texto)
    mod = modalidad(texto, es_pareja) if t else ('parejas' if es_pareja else 'mano')

    def _serie():
        s = serie_en_texto(texto) or serie
        if not s and serie_jugadores:
            s = serie_jugadores()
        return s or 'a'

    def _festival():
        if mod == 'parejas':
            return 'festival', 'Festival'
        return ('festival-cuatro' if mod == 'cuatro' else 'festival-mano'), 'Festival'

    def _campeonato():
        s = _serie()
        anio = f.year + 1 if (mod == 'parejas' and f.month >= 11) else f.year
        return (f"{PREFIJO_TIPO[mod]}-{s}",
                f"Campeonato {NOMBRE_MODALIDAD[mod]} Serie {s.upper()} {anio}")

    if not t:
        if mod == 'parejas' and en_temporada('parejas', f):
            return _campeonato()
        return _festival()

    # Despedidas: se conservan con su nombre
    if 'despedida' in t:
        return _festival()[0], ' '.join(texto.split()).title()

    for clave, base in TORNEOS:
        if clave in t:
            if mod == 'cuatro':
                return 'cuatro-medio-a', f"{base} 4 y Medio {f.year}"
            if mod == 'mano':
                return 'manomanista-a', f"{base} Manomanista {f.year}"
            s = _serie()
            return f"campeonato-{s}", f"{base} Serie {s.upper()} {f.year}"

    if 'festival' in t or 'torneo' in t or 'desafio' in t:
        return _festival()

    if 'campeonato' in t or 'eusko label' in t or 'manomanista' in t or 'serie' in t:
        return _campeonato()

    return _festival()


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
        for cand in (ev.get('competicion'), (ev.get('cartel') or [None])[0], ev.get('fase')):
            if cand and es_texto_competicion(cand):
                return cand
        return None

    def buscar(self, fecha, fronton, jugadores):
        """Devuelve (texto_competicion, serie) o (None, None)."""
        fro = _nombre_pel(fronton)
        nombres = {_nombre_pel(j) for j in jugadores if j}
        for ev in self._idx.get(_fecha(fecha), []):
            fro_ev = _nombre_pel(ev.get('fronton'))
            if fro and fro_ev and fro != fro_ev and fro not in fro_ev and fro_ev not in fro:
                continue
            for p in ev.get('partidos') or []:
                del_cartel = {_nombre_pel(j) for j in (p.get('eq1') or []) + (p.get('eq2') or [])}
                if len(nombres & del_cartel) >= min(2, len(nombres)):
                    texto = self.texto_evento(ev)
                    # La modalidad a veces solo aparece en la línea del partido: "A // B (4 1/2)"
                    if texto and '4 1/2' in _txt(p.get('raw')) and modalidad(texto, True) != 'cuatro':
                        texto += ' 4 1/2'
                    elif not texto and '4 1/2' in _txt(p.get('raw')):
                        texto = 'Festival 4 1/2'
                    return texto, p.get('serie')
            # Un único partido en el evento sin nombres conocidos (XX // XX)
            if fro and fro_ev == fro and len(ev.get('partidos') or []) <= 1:
                return self.texto_evento(ev), None
        return None, None
