#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrar_clasificacion.py — Eskupilota Stats

Añade a cada partido de data/partidos.json la clasificación completa que
usan ahora los scrapers (ver scraper/competiciones.py):

  modalidad  parejas | mano | cuatro
  categoria  campeonato | torneo | desafio | festival
  serie      A | B | null
  fase       liga | eliminatoria | octavos | cuartos | semifinal | final
             (+ grupo y jornada), solo cuando se conoce

y recalcula `tipo` a partir de ellos. Además:

  - La Promoción es la Serie B: "Campeonato Manomanista Promoción 2023" y
    "Campeonato Parejas Promoción 2023" se fusionan en sus Serie B.
  - Los desafíos (Desafío Urzante) dejan de contar como campeonato: su tipo
    pasa a festival / festival-mano / festival-cuatro, con categoria desafio.
  - Los torneos de 4 y medio o manomanista con tipo de festival pasan al tipo
    de su modalidad (Torneo San Fermín 4 y Medio tenía 9 partidos como
    festival-cuatro).
  - Manomanista y 4 y medio: si los partidos de un año forman dos grupos
    de pelotaris que nunca se enfrentan, el grupo que no es el de la
    Serie A es la Serie B (en 2026 el scraper antiguo guardó la Serie B
    del Manomanista como Serie A).
  - La fase se toma del historial de la cartelera en git (desde que se
    guarda). Además, en cada campeonato ya terminado y con al menos 10
    partidos, su último partido es la final (fase 'final' y
    "fase_deducida": true) si se jugó al menos 5 días después del anterior
    (si no, falta la final en los datos y ese es un partido de la fase previa).
  - En Manomanista y 4 y medio, desde la final hacia atrás: el partido
    anterior de cada finalista es su semifinal, y así con cuartos y
    octavos, mientras cuadre como eliminatoria (lo ganó, fue como mucho 5
    semanas antes y el que perdió no volvió a jugar). Se para en cuanto
    no cuadra (liguillas).
  - Cada competición del catálogo recibe su "categoria".

Idempotente: se puede ejecutar varias veces.

Uso:
    python tools/migrar_clasificacion.py --dry-run
    python tools/migrar_clasificacion.py

Después:
    python tools/recalcular_contadores.py
"""

import json
import os
import sys
from collections import Counter
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scraper'))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from competiciones import _tipo, _txt, categoria_de_competicion, clasificar_partido, leer_fase  # noqa: E402
from jsonio import guardar_json  # noqa: E402
from limpieza_competiciones import cartelera_historica  # noqa: E402

DATA_DIR = 'data'
PARTIDOS_FILE = os.path.join(DATA_DIR, 'partidos.json')
PELOTARIS_FILE = os.path.join(DATA_DIR, 'pelotaris.json')
FRONTONES_FILE = os.path.join(DATA_DIR, 'frontones.json')
COMPETICIONES_FILE = os.path.join(DATA_DIR, 'competiciones.json')

PROMOCION = {
    'Campeonato Manomanista Promoción 2023': 'Campeonato Manomanista Serie B 2023',
    'Campeonato Parejas Promoción 2023': 'Campeonato Parejas Serie B 2023',
}
# Un campeonato se da por terminado si lleva este tiempo sin partidos, y
# solo se deduce su final si tenemos bastantes partidos (si faltan, el
# último que tenemos puede no ser la final)
TERMINADO_TRAS = timedelta(days=30)
MIN_PARTIDOS_FINAL = 10
MIN_DIAS_ANTES_FINAL = 5
RONDAS_ATRAS = ('semifinal', 'cuartos', 'octavos')
MAX_ENTRE_RONDAS = timedelta(days=35)


def load(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def save(path, data):
    guardar_json(path, data)


def modalidad(p, cmp_nombre):
    if p['equipo1'].get('zag_id') or p['equipo2'].get('zag_id'):
        return 'parejas'
    t = p.get('tipo') or ''
    if 'cuatro' in t:
        return 'cuatro'
    _, mod = categoria_de_competicion(cmp_nombre)
    return mod or 'mano'


def equipo(p, k):
    return tuple(sorted(x for x in (p[k].get('del_id'), p[k].get('zag_id')) if x))


def equipos(p):
    return equipo(p, 'equipo1'), equipo(p, 'equipo2')


def perdedor(p):
    return equipo(p, 'equipo2' if p['ganador'] == 'equipo1' else 'equipo1')


def ganador(p):
    return equipo(p, p['ganador'])


def separar_series(partidos, comps, cambios):
    """Manomanista / 4 y medio: la Serie A y la B no se enfrentan. Si los
    partidos de un año forman grupos de pelotaris separados, los del grupo
    con más partidos de Serie A son la A y el resto, la B."""
    import re
    por_nombre = {c['nombre']: c for c in comps}
    grupos = {}
    for p in partidos:
        m = re.match(r'Campeonato (Manomanista|4 y Medio) Serie ([AB]) (\d{4})$',
                     next(c['nombre'] for c in comps if c['id'] == p['competicion_id']))
        if m:
            grupos.setdefault((m[1], m[3]), []).append((m[2], p))
    for (mod, anio), lista in grupos.items():
        padre = {}

        def raiz(x):
            while padre.setdefault(x, x) != x:
                x = padre[x]
            return x
        for _, p in lista:
            padre[raiz(p['equipo1']['del_id'])] = raiz(p['equipo2']['del_id'])
        cuenta = {}
        for s, p in lista:
            cuenta.setdefault(raiz(p['equipo1']['del_id']), Counter())[s] += 1
        if len(cuenta) < 2:
            continue
        principal = max(cuenta, key=lambda r: cuenta[r]['A'])
        destino = por_nombre.get(f'Campeonato {mod} Serie B {anio}')
        if not destino:
            continue
        for s, p in lista:
            if s == 'A' and raiz(p['equipo1']['del_id']) != principal:
                p['competicion_id'] = destino['id']
                cambios[f'{mod} {anio}: partido de la Serie B que estaba en la A'] += 1


def deducir_rondas(ps, final, cambios):
    """Desde la final hacia atrás, mientras la competición sea eliminatoria."""
    ronda = [final]
    for nombre in RONDAS_ATRAS:
        nueva = []
        for m in ronda:
            for eq in equipos(m):
                previos = [p for p in ps if p['fecha'] < m['fecha'] and eq in equipos(p)]
                if not previos:
                    return
                q = previos[-1]
                if ganador(q) != eq or date.fromisoformat(m['fecha']) - date.fromisoformat(q['fecha']) > MAX_ENTRE_RONDAS:
                    return
                if any(p['fecha'] > q['fecha'] and perdedor(q) in equipos(p) for p in ps):
                    return
                if q.get('fase') and q['fase'] != nombre:
                    return
                nueva.append(q)
        if len({id(q) for q in nueva}) != len(nueva):
            return
        for q in nueva:
            if q.get('fase') != nombre:
                q['fase'] = nombre
                q['fase_deducida'] = True
                cambios[f'{nombre} deducida'] += 1
        ronda = nueva


def main():
    dry = '--dry-run' in sys.argv
    partidos = load(PARTIDOS_FILE)
    pelotaris = {p['id']: p['nombre'] for p in load(PELOTARIS_FILE)}
    frontones = {f['id']: f['nombre'] for f in load(FRONTONES_FILE)}
    comps = load(COMPETICIONES_FILE)
    por_nombre = {c['nombre']: c for c in comps}
    por_id = {c['id']: c for c in comps}
    cambios = Counter()

    # 1. Promoción -> Serie B
    for origen, destino in PROMOCION.items():
        if origen not in por_nombre:
            continue
        if destino not in por_nombre:
            por_nombre[origen]['nombre'] = destino
            por_nombre[destino] = por_nombre.pop(origen)
            cambios[f'competición renombrada: {origen} -> {destino}'] += 1
            continue
        a, b = por_nombre[origen]['id'], por_nombre[destino]['id']
        for p in partidos:
            if p['competicion_id'] == a:
                p['competicion_id'] = b
                cambios[f'partido de {origen} -> {destino}'] += 1
        comps[:] = [c for c in comps if c['id'] != a]  # noqa
        del por_nombre[origen]
        por_id.pop(a, None)

    separar_series(partidos, comps, cambios)

    # 2. Clasificación de cada partido
    cart = cartelera_historica()

    def comp_id(nombre, tipo):
        if nombre not in por_nombre:
            n = max(int(c['id'][4:]) for c in comps) + 1
            nuevo = {'id': f'COMP{n:03d}', 'nombre': nombre, 'tipo': tipo, 'partidos_count': 0}
            comps.append(nuevo)
            por_nombre[nombre] = por_id[nuevo['id']] = nuevo
        return por_nombre[nombre]['id']

    for p in partidos:
        # Partidos de un evento que la cartelera anunciaba como desafío
        # ('Desafío Urzante' dentro de las fiestas de San Mateo) y que estaban
        # guardados en el torneo de las fiestas
        jug = [pelotaris.get(p[e].get(k)) for e in ('equipo1', 'equipo2') for k in ('del_id', 'zag_id')]
        d = cart.buscar_detalle(p['fecha'], frontones.get(p['fronton_id']), jug)
        if d and d['texto'] and 'desaf' in _txt(d['texto']) and 'desaf' not in _txt(por_id[p['competicion_id']]['nombre']):
            es_pareja = bool(p['equipo1'].get('zag_id') or p['equipo2'].get('zag_id'))
            texto = d['texto'] + (' 4 1/2' if 'cuatro' in (p.get('tipo') or '') else '')
            c = clasificar_partido(texto, p['fecha'], es_pareja)
            if c['categoria'] == 'desafio':
                cambios[f"{por_id[p['competicion_id']]['nombre']} -> {c['competicion']} (la cartelera lo anunciaba como desafío)"] += 1
                p['competicion_id'] = comp_id(c['competicion'], c['tipo'])

        # Guardados como festival que la cartelera anunciaba con competición y
        # con la serie en la línea del partido ('... (Serie B)'): la web de
        # resultados solo decía 'Final' (finales de San Mateo del 27/09/2026)
        if (d and d['texto'] and d['serie'] and
                categoria_de_competicion(por_id[p['competicion_id']]['nombre'])[0] == 'festival'):
            es_pareja = bool(p['equipo1'].get('zag_id') or p['equipo2'].get('zag_id'))
            c = clasificar_partido(d['texto'], p['fecha'], es_pareja, d['serie'], None, d['textos_fase'])
            if c['categoria'] in ('campeonato', 'torneo'):
                cambios[f"festival -> {c['competicion']} (la cartelera lo anunciaba así)"] += 1
                p['competicion_id'] = comp_id(c['competicion'], c['tipo'])
                if c['fase'] and not p.get('fase'):
                    p['fase'] = c['fase']

        comp = por_id[p['competicion_id']]
        cat, _ = categoria_de_competicion(comp['nombre'])
        mod = modalidad(p, comp['nombre'])
        serie = 'A' if ' Serie A' in comp['nombre'] else ('B' if ' Serie B' in comp['nombre'] else None)
        if cat not in ('campeonato', 'torneo'):
            serie = None
        tipo = _tipo(mod, cat, serie)
        if tipo != p.get('tipo'):
            cambios[f"tipo {p.get('tipo')} -> {tipo} ({cat})"] += 1
        nuevo = {'modalidad': mod, 'categoria': cat, 'serie': serie, 'tipo': tipo}

        # Fase desde la cartelera (si el partido pasó por ella)
        # (no pisa una fase ya conocida, p. ej. la del historial de Baiko)
        if cat != 'festival' and not p.get('fase'):
            jug = [pelotaris.get(p[e].get(k)) for e in ('equipo1', 'equipo2') for k in ('del_id', 'zag_id')]
            d = cart.buscar_detalle(p['fecha'], frontones.get(p['fronton_id']), jug)
            if d:
                fase, grupo, jornada = leer_fase(d['texto'], *d['textos_fase'])
                for k, v in (('fase', fase), ('grupo', grupo), ('jornada', jornada)):
                    if v is not None and p.get(k) != v:
                        nuevo[k] = v
                        cambios[f'fase desde la cartelera: {k}'] += 1
        p.update(nuevo)

    # 3. Finales deducidas: último partido de cada campeonato terminado
    hoy = max(date.fromisoformat(p['fecha']) for p in partidos)
    ultimos, n_por_comp = {}, Counter(p['competicion_id'] for p in partidos)
    for p in partidos:
        if p['categoria'] == 'campeonato' and n_por_comp[p['competicion_id']] >= MIN_PARTIDOS_FINAL:
            u = ultimos.get(p['competicion_id'])
            if u is None or p['fecha'] > u['fecha']:
                ultimos[p['competicion_id']] = p
    def es_final_probable(p):
        ps = sorted((q for q in partidos if q['competicion_id'] == p['competicion_id']), key=lambda q: q['fecha'])
        antes = [q for q in ps if q['fecha'] < p['fecha']]
        return bool(antes) and date.fromisoformat(p['fecha']) - date.fromisoformat(antes[-1]['fecha']) >= timedelta(days=MIN_DIAS_ANTES_FINAL)

    # Deducciones que ya no cumplen el criterio (se recalculan abajo)
    for p in partidos:
        if p.get('fase_deducida'):
            del p['fase'], p['fase_deducida']
            cambios['fase deducida recalculada'] += 1

    print('\nFINALES DEDUCIDAS (último partido de cada campeonato terminado)')
    for cid, p in sorted(ultimos.items(), key=lambda x: por_id[x[0]]['nombre']):
        if hoy - date.fromisoformat(p['fecha']) < TERMINADO_TRAS or p.get('fase'):
            continue
        if not es_final_probable(p):
            print(f"  {por_id[cid]['nombre']:<38} {p['fecha']}  (no parece la final: falta el partido)")
            continue
        p['fase'] = 'final'
        p['fase_deducida'] = True
        cambios['final deducida'] += 1
        e = lambda k: ' - '.join(pelotaris.get(p[k].get(x), '') for x in ('del_id', 'zag_id') if p[k].get(x))
        print(f"  {por_id[cid]['nombre']:<38} {p['fecha']} {frontones.get(p['fronton_id'], ''):<18} "
              f"{e('equipo1')} {p['puntos1']}-{p['puntos2']} {e('equipo2')}")

    # 3b. Rondas eliminatorias hacia atrás desde la final (mano y 4 y medio)
    for cid in {p['competicion_id'] for p in partidos}:
        ps = sorted((p for p in partidos if p['competicion_id'] == cid), key=lambda p: p['fecha'])
        final = next((p for p in ps if p.get('fase') == 'final'), None)
        if final and ps[0]['modalidad'] in ('mano', 'cuatro') and ps[0]['categoria'] == 'campeonato':
            deducir_rondas(ps, final, cambios)

    # 4. Categoría y tipo de cada competición del catálogo
    tipos = {}
    for p in partidos:
        tipos.setdefault(p['competicion_id'], Counter())[p['tipo']] += 1
    for c in comps:
        cat, _ = categoria_de_competicion(c['nombre'])
        if c.get('categoria') != cat:
            c['categoria'] = cat
            cambios['categoría en el catálogo'] += 1
        if c['id'] in tipos:
            t = tipos[c['id']].most_common(1)[0][0]
            if c.get('tipo') != t:
                cambios[f"tipo de competición {c['nombre']}: {c.get('tipo')} -> {t}"] += 1
                c['tipo'] = t

    print('\nCAMBIOS')
    for k, n in sorted(cambios.items()):
        print(f'  {n:5}  {k}')
    print('\nPartidos por categoría:', dict(Counter(p['categoria'] for p in partidos)))
    print('Partidos con fase:', dict(Counter(p.get('fase') for p in partidos if p.get('fase'))))
    if dry:
        print('\n[--dry-run] No se ha escrito nada.')
        return
    save(PARTIDOS_FILE, partidos)
    save(COMPETICIONES_FILE, comps)
    print('\n  Siguiente paso: python tools/recalcular_contadores.py')


if __name__ == '__main__':
    main()
