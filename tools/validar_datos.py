#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validar_datos.py — Eskupilota Stats

Comprueba que los datos de data/ son coherentes. Lo ejecuta el workflow
diario antes de publicar: si hay ERRORES no se hace commit y la ejecución
sale en rojo; los AVISOS solo se muestran.

Errores:
  - IDs repetidos o referencias a pelotaris, frontones, ciudades o
    competiciones que no existen
  - fecha inválida o futura, tanteo imposible, ganador que no cuadra
  - el mismo pelotari en los dos equipos o repetido en un equipo
  - partido duplicado (misma fecha y mismos pelotaris)
  - tipo desconocido o que no cuadra con la modalidad (parejas/individual)
  - campeonato de parejas fuera de temporada (noviembre a marzo)
  - clasificación incoherente: modalidad, categoria, serie, fase y tipo
    (ver scraper/competiciones.py); categoria distinta de la de su
    competición; campeonato sin serie; festival o desafío con serie
  - nombres duplicados en un catálogo (iguales sin tildes, guiones ni espacios)
  - contadores partidos_count desactualizados

Avisos:
  - tanteos poco habituales, nombres muy parecidos, frontones sin
    coordenadas, pelotaris sin rol

Uso:
    python tools/validar_datos.py
"""

import difflib
import json
import os
import re
import sys
import unicodedata
from collections import Counter
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scraper'))
from competiciones import CATEGORIAS, FASES, _tipo, categoria_de_competicion  # noqa: E402

DATA_DIR = 'data'

TIPOS_PAREJAS = {'campeonato-a', 'campeonato-b', 'festival'}
TIPOS_INDIVIDUAL = {'manomanista-a', 'manomanista-b', 'cuatro-medio-a', 'cuatro-medio-b',
                    'festival-mano', 'festival-cuatro'}
TEMPORADA_PAREJAS = {11, 12, 1, 2, 3}
TANTEOS_HABITUALES = {16, 18, 22, 25}
# Nombres parecidos que son sitios distintos de verdad
PARECIDOS_OK = {frozenset({'ETXEBARRI', 'ETXEBARRIA'}), frozenset({'CIZUR', 'CIZURMENOR'})}

errores, avisos = [], []


def error(msg):
    errores.append(msg)


def aviso(msg):
    avisos.append(msg)


def load(nombre):
    with open(os.path.join(DATA_DIR, nombre), encoding='utf-8') as f:
        return json.load(f)


def clave(s):
    s = unicodedata.normalize('NFD', (s or '').upper())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^A-Z0-9]', '', s)


def ids_unicos(items, etiqueta):
    for i, n in Counter(it['id'] for it in items).items():
        if n > 1:
            error(f"{etiqueta}: ID {i} repetido {n} veces")
    return {it['id']: it for it in items}


def nombres_unicos(items, etiqueta, similares=True):
    por_clave = {}
    for it in items:
        por_clave.setdefault(clave(it['nombre']), []).append(it)
    for grupo in por_clave.values():
        if len(grupo) > 1:
            error(f"{etiqueta}: nombres duplicados " + ', '.join(f"{g['id']} '{g['nombre']}'" for g in grupo))
    if not similares:
        return
    claves = sorted(por_clave)
    for i, a in enumerate(claves):
        for b in claves[i + 1:]:
            if frozenset({a, b}) in PARECIDOS_OK:
                continue
            if abs(len(a) - len(b)) <= 2 and difflib.SequenceMatcher(None, a, b).ratio() >= 0.9:
                na, nb = por_clave[a][0]['nombre'], por_clave[b][0]['nombre']
                aviso(f"{etiqueta}: nombres muy parecidos '{na}' / '{nb}'")


def main():
    partidos = load('partidos.json')
    pelotaris = ids_unicos(load('pelotaris.json'), 'pelotaris')
    frontones = ids_unicos(load('frontones.json'), 'frontones')
    ciudades = ids_unicos(load('ciudades.json'), 'ciudades')
    comps = ids_unicos(load('competiciones.json'), 'competiciones')
    load('cartelera.json')  # al menos que sea JSON válido

    nombres_unicos(pelotaris.values(), 'pelotaris')
    nombres_unicos(frontones.values(), 'frontones')
    nombres_unicos(ciudades.values(), 'ciudades')
    # Las competiciones solo cambian en el año o la serie: no se avisa de parecidos
    nombres_unicos(comps.values(), 'competiciones', similares=False)

    for f in frontones.values():
        if f.get('ciudad_id') not in ciudades:
            error(f"frontón {f['id']} {f['nombre']}: ciudad {f.get('ciudad_id')} no existe")
        if 'lat' not in f:
            aviso(f"frontón {f['id']} {f['nombre']}: sin coordenadas")
    for p in pelotaris.values():
        if p.get('rol') not in ('delantero', 'zaguero'):
            aviso(f"pelotari {p['id']} {p['nombre']}: rol '{p.get('rol')}'")

    manana = date.today() + timedelta(days=1)
    vistos = {}
    c_pel, c_fro, c_cmp = Counter(), Counter(), Counter()

    for n, p in enumerate(partidos):
        ref = f"partido {p.get('fecha')} #{n}"
        try:
            f = date.fromisoformat(p['fecha'])
            if f > manana:
                error(f"{ref}: fecha futura")
        except (KeyError, TypeError, ValueError):
            error(f"{ref}: fecha inválida")
            continue

        if p.get('fronton_id') not in frontones:
            error(f"{ref}: frontón {p.get('fronton_id')} no existe")
        comp = comps.get(p.get('competicion_id'))
        if not comp:
            error(f"{ref}: competición {p.get('competicion_id')} no existe")
        c_fro[p.get('fronton_id')] += 1
        c_cmp[p.get('competicion_id')] += 1

        jugadores = []
        for eq in ('equipo1', 'equipo2'):
            e = p.get(eq) or {}
            if not e.get('del_id'):
                error(f"{ref}: {eq} sin delantero")
            for k in ('del_id', 'zag_id'):
                pid = e.get(k)
                if pid:
                    if pid not in pelotaris:
                        error(f"{ref}: pelotari {pid} no existe")
                    jugadores.append(pid)
                    c_pel[pid] += 1
        if len(jugadores) != len(set(jugadores)):
            nombres = [pelotaris.get(j, {}).get('nombre', j) for j in jugadores]
            error(f"{ref}: pelotari repetido en el partido {nombres}")

        pareja = [bool((p.get(eq) or {}).get('zag_id')) for eq in ('equipo1', 'equipo2')]
        tipo = p.get('tipo')
        if pareja[0] != pareja[1]:
            # "Uno contra dos": solo tiene sentido en un festival
            (aviso if tipo == 'festival' else error)(f"{ref}: un equipo tiene zaguero y el otro no")
        if tipo not in TIPOS_PAREJAS | TIPOS_INDIVIDUAL:
            error(f"{ref}: tipo desconocido '{tipo}'")
        elif all(pareja) and tipo in TIPOS_INDIVIDUAL:
            error(f"{ref}: partido de parejas con tipo individual '{tipo}'")
        elif not any(pareja) and tipo in TIPOS_PAREJAS:
            error(f"{ref}: partido individual con tipo de parejas '{tipo}'")

        if comp and comp['nombre'].startswith('Campeonato Parejas') and f.month not in TEMPORADA_PAREJAS:
            error(f"{ref}: '{comp['nombre']}' fuera de temporada")

        # Clasificación
        mod, cat, serie = p.get('modalidad'), p.get('categoria'), p.get('serie')
        if mod not in ('parejas', 'mano', 'cuatro'):
            error(f"{ref}: modalidad '{mod}'")
        elif (mod == 'parejas') != any(pareja):
            error(f"{ref}: modalidad '{mod}' no cuadra con los pelotaris")
        if cat not in CATEGORIAS:
            error(f"{ref}: categoria '{cat}'")
        elif comp and categoria_de_competicion(comp['nombre'])[0] != cat:
            error(f"{ref}: categoria '{cat}' distinta de la de '{comp['nombre']}'")
        if serie not in ('A', 'B', None):
            error(f"{ref}: serie '{serie}'")
        elif cat == 'campeonato' and not serie:
            error(f"{ref}: campeonato sin serie")
        elif cat in ('festival', 'desafio') and serie:
            error(f"{ref}: {cat} con serie {serie}")
        if mod in ('parejas', 'mano', 'cuatro') and cat in CATEGORIAS and tipo != _tipo(mod, cat, serie):
            error(f"{ref}: tipo '{tipo}' no cuadra con {mod}/{cat}/{serie} (sería '{_tipo(mod, cat, serie)}')")
        if p.get('fase') is not None and p['fase'] not in FASES:
            error(f"{ref}: fase '{p['fase']}'")
        if p.get('fase') and cat == 'festival':
            error(f"{ref}: festival con fase '{p['fase']}'")
        if p.get('grupo') is not None and p['grupo'] not in ('A', 'B', 'C', 'D'):
            error(f"{ref}: grupo '{p['grupo']}'")

        p1, p2 = p.get('puntos1'), p.get('puntos2')
        if not (isinstance(p1, int) and isinstance(p2, int) and 0 <= p1 <= 40 and 0 <= p2 <= 40):
            error(f"{ref}: tanteo imposible {p1}-{p2}")
        else:
            esperado = 'equipo1' if p1 > p2 else ('equipo2' if p2 > p1 else None)
            if p.get('ganador') != esperado:
                error(f"{ref}: ganador '{p.get('ganador')}' no cuadra con {p1}-{p2}")
            if max(p1, p2) not in TANTEOS_HABITUALES:
                aviso(f"{ref}: tanteo poco habitual {p1}-{p2}")

        firma = (p['fecha'], frozenset(jugadores))
        if firma in vistos:
            error(f"{ref}: duplicado de #{vistos[firma]} (misma fecha y pelotaris)")
        vistos[firma] = n

    for items, cuenta, etiqueta in ((pelotaris, c_pel, 'pelotari'), (frontones, c_fro, 'frontón'),
                                     (comps, c_cmp, 'competición')):
        for it in items.values():
            if it.get('partidos_count') != cuenta.get(it['id'], 0):
                error(f"{etiqueta} {it['id']} {it['nombre']}: partidos_count {it.get('partidos_count')} "
                      f"pero hay {cuenta.get(it['id'], 0)} (python tools/recalcular_contadores.py)")

    print(f"{len(partidos)} partidos, {len(pelotaris)} pelotaris, {len(frontones)} frontones, "
          f"{len(ciudades)} ciudades, {len(comps)} competiciones")
    for a in avisos:
        print(f"  aviso: {a}")
    for e in errores:
        print(f"  ERROR: {e}")
    print(f"\n{len(errores)} errores, {len(avisos)} avisos")
    sys.exit(1 if errores else 0)


if __name__ == '__main__':
    main()
