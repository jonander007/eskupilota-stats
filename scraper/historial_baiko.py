# -*- coding: utf-8 -*-
"""
Lector del «Historial de competición» de las páginas de campeonato de Baiko
(baikopilota.eus/campeonato/<año>-campeonato-...-serie-a/).

La página agrupa los partidos por fase (Octavos, Liga de cuartos,
Semifinal, Tercer y cuarto, Final...) y, dentro de cada fase, a veces por
grupo. Cada partido es una tarjeta con fecha, «Frontón - Ciudad», los dos
equipos (uno o dos pelotaris) y el tanteo.

En las eliminatorias directas Baiko usa «Grupo A/B» para las dos mitades
del cuadro: ahí el grupo no se guarda. Solo en las liguillas («Liga de
cuartos», «Liguilla de semifinales», «Fase de grupos») el grupo es un grupo
de verdad.
"""

import re
import unicodedata

from bs4 import BeautifulSoup


def _txt(s):
    s = unicodedata.normalize('NFD', (s or '').lower())
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')


def _limpio(s):
    return ' '.join((s or '').replace('\xa0', ' ').split())


def fase_de_titulo(titulo):
    """('Liga de cuartos') -> ('cuartos', True): fase y si es una liguilla."""
    t = _txt(titulo)
    liga = bool(re.search(r'\bliga\b|liguilla|grupos', t))
    if re.search(r'tercer|3\W*(y|-)\W*4', t):
        return 'tercero', False
    if re.search(r'semifinal', t):
        return 'semifinal', liga
    if re.search(r'cuartos', t):
        return 'cuartos', liga
    if re.search(r'octavos', t):
        return 'octavos', liga
    if re.search(r'\bfinal\b', t):
        return 'final', False
    if re.search(r'dieciseisavos|previa|eliminatoria|clasificacion|repesca', t):
        return 'eliminatoria', liga
    if liga:
        return 'liga', True
    return None, False


def _equipo(fila):
    nombres = [_limpio(li.get_text()) for li in fila.find_all('li')]
    nombres = [n for n in nombres if n]
    tanteo = None
    for div in fila.find_all('div', recursive=False):
        m = re.fullmatch(r'\s*(\d+)\s*', div.get_text())
        if m:
            tanteo = int(m[1])
    return nombres, tanteo


def leer_historial(html):
    """Devuelve {'titulo', 'partidos': [...]}. Cada partido: fase, grupo,
    fase_texto, fecha (dd/mm/aaaa), fronton, ciudad, equipo1, puntos1,
    equipo2, puntos2. Los suspendidos o sin fecha no se devuelven."""
    soup = BeautifulSoup(html, 'html.parser')
    titulo = _limpio(soup.title.get_text()) if soup.title else ''
    titulo = re.sub(r'\s*-\s*Baiko Pilota\s*$', '', titulo)
    partidos = []
    fase = fase_texto = grupo = None
    liga = False
    for el in soup.find_all(['h3', 'p', 'div']):
        clases = el.get('class') or []
        if el.name == 'h3' and 'linea_abajo' in clases:
            fase_texto = _limpio(el.get_text())
            fase, liga = fase_de_titulo(fase_texto)
            grupo = None
        elif el.name == 'p' and 'linea_abajo_centrada' in clases:
            m = re.search(r'grupo\s+([a-z0-9]+)', _txt(el.get_text()))
            grupo = m[1].upper() if m else None
        elif el.name == 'div' and 'card-body' in clases and fase_texto:
            cab = el.find('span', class_='nombrefrontonsmall')
            fecha = next((_limpio(s.get_text()) for s in el.find_all('span')
                          if re.fullmatch(r'\d{2}/\d{2}/\d{4}', _limpio(s.get_text()))), None)
            filas = [f for f in el.find_all('div', class_='row') if f.find('li')]
            if not fecha or len(filas) != 2:
                continue
            (e1, p1), (e2, p2) = _equipo(filas[0]), _equipo(filas[1])
            if not e1 or not e2 or p1 is None or p2 is None or p1 == p2:
                continue
            lugar = _limpio(cab.get_text()) if cab else ''
            fronton, _, ciudad = lugar.partition(' - ')
            partidos.append({
                'fase': fase, 'grupo': grupo if liga else None, 'fase_texto': fase_texto,
                'fecha': fecha, 'fronton': fronton.strip(), 'ciudad': ciudad.strip(),
                'equipo1': e1, 'puntos1': p1, 'equipo2': e2, 'puntos2': p2,
            })
    return {'titulo': titulo, 'partidos': partidos}


def competicion_de_titulo(titulo, partidos):
    """'[2025] Campeonato 4 1/2 Eusko Label Serie A' -> nombre en nuestro
    catálogo: 'Campeonato 4 y Medio Serie A 2025'. El año es el de la final
    (el del último partido): así el Parejas 2024-2025 es el de 2025."""
    t = _txt(titulo)
    serie = 'B' if re.search(r'serie b|promocion', t) else 'A'
    if 'manomanista' in t or 'mano a mano' in t:
        mod = 'Manomanista'
    elif 'parejas' in t:
        mod = 'Parejas'
    elif re.search(r'4 ?(1/2|y medio|½)', t):
        mod = '4 y Medio'
    else:
        return None
    if not partidos:
        return None
    anio = max(p['fecha'][-4:] + p['fecha'][3:5] + p['fecha'][:2] for p in partidos)[:4]
    return f'Campeonato {mod} Serie {serie} {anio}'
