"""
Eskupilota Stats — Rol de cada pelotari (delantero / zaguero)

Se deduce de los partidos de parejas: el puesto en el que más veces ha
jugado. Así un pelotari nuevo recibe su rol en cuanto juega por parejas, sin
tener que mantener listas a mano. Si nunca ha jugado por parejas conserva el
rol que tuviera.
"""

from collections import Counter, defaultdict


def calcular_roles(partidos):
    """Devuelve {pelotari_id: 'delantero' | 'zaguero'}."""
    puestos = defaultdict(Counter)
    for p in partidos:
        for eq in ('equipo1', 'equipo2'):
            e = p.get(eq) or {}
            if e.get('del_id') and e.get('zag_id'):
                puestos[e['del_id']]['delantero'] += 1
                puestos[e['zag_id']]['zaguero'] += 1
    return {pid: c.most_common(1)[0][0] for pid, c in puestos.items()}


def aplicar_roles(pelotaris, partidos):
    """Actualiza el campo 'rol' del catálogo. Devuelve la lista de cambios."""
    roles = calcular_roles(partidos)
    cambios = []
    for pel in pelotaris:
        nuevo = roles.get(pel['id'])
        if nuevo and pel.get('rol') != nuevo:
            cambios.append((pel['id'], pel['nombre'], pel.get('rol'), nuevo))
            pel['rol'] = nuevo
    return cambios
