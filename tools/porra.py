#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
porra.py — Eskupilota Stats

Mantiene al día los partidos de la porra en Supabase (tabla porra_partidos,
ver supabase/porra.sql). Lo ejecuta el workflow de datos después de los
scrapers:

  1. Sube los partidos de la cartelera (data/cartelera.json). La hora de
     la velada es la hora de cierre de los pronósticos.
  2. Los partidos ya empezados que aparecen en data/partidos.json pasan a
     'jugado' con su tanteo (los puntos los calcula la base de datos).
  3. Se anulan los partidos que desaparecen de una velada que sigue en la
     cartelera (cambio de cartel) y los que llevan 3 días sin resultado.

Necesita la variable de entorno SUPABASE_SECRET_KEY (secreto del repo).
Sin ella solo muestra lo que haría.

Uso:
    python tools/porra.py
"""

import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
DATA = os.path.join(RAIZ, 'data')
SUPABASE_URL = 'https://nckeadvyeymewibrqpuj.supabase.co'
MADRID = ZoneInfo('Europe/Madrid')
DIAS_SIN_RESULTADO = 3


def cargar(nombre):
    with open(os.path.join(DATA, nombre + '.json'), encoding='utf-8') as f:
        return json.load(f)


def clave(nombre):
    """'P.Etxeberria' y 'P. ETXEBERRIA' → 'petxeberria'."""
    s = unicodedata.normalize('NFD', nombre or '')
    return re.sub(r'[^a-z0-9]', '', ''.join(c for c in s if not unicodedata.combining(c)).lower())


def slug(nombre):
    s = unicodedata.normalize('NFD', nombre or '')
    return re.sub(r'[^a-z0-9]+', '-', ''.join(c for c in s if not unicodedata.combining(c)).lower()).strip('-')


def valido(eq):
    return bool(eq) and all(n and not re.fullmatch(r'\?+|X+', n.strip(), re.I) for n in eq)


def id_partido(fecha_iso, eq1, eq2):
    return f"{fecha_iso}_{'-'.join(slug(n) for n in eq1)}_vs_{'-'.join(slug(n) for n in eq2)}"


def partidos_cartelera(cartelera):
    """Partidos de la cartelera con cartel completo, en el formato de porra_partidos."""
    salida = []
    for v in cartelera.get('partidos', []):
        try:
            d, m, y = v['fecha'].split('/')
            hh, mm = (v.get('hora') or '').split(':')
            inicio = datetime(int(y), int(m), int(d), int(hh), int(mm), tzinfo=MADRID)
        except (KeyError, ValueError):
            continue
        fecha_iso = f'{y}-{m}-{d}'
        for p in v.get('partidos', []):
            eq1, eq2 = p.get('eq1') or [], p.get('eq2') or []
            if not (valido(eq1) and valido(eq2)):
                continue
            salida.append({
                'id': id_partido(fecha_iso, eq1, eq2),
                'inicio': inicio.astimezone(timezone.utc).isoformat(),
                'competicion': p.get('competicion') or v.get('competicion'),
                'fase': p.get('fase') or v.get('fase'),
                'fronton': v.get('fronton'),
                'modalidad': p.get('modalidad'),
                'categoria': p.get('categoria'),       # el modo «oficiales» deja fuera los festivales
                'eq1': eq1,
                'eq2': eq2,
                'estado': 'abierto',      # si vuelve a anunciarse tras un cambio de cartel, se reabre
            })
    # Un mismo partido no puede ir dos veces en la misma petición
    return list({p['id']: p for p in salida}.values())


def indice_resultados(partidos, pelotaris):
    """{(fecha, frozenset(eq1), frozenset(eq2)): (puntos1, puntos2)} con nombres en clave()."""
    nombre = {p['id']: p['nombre'] for p in pelotaris}
    idx = {}
    for p in partidos:
        if p.get('puntos1') is None or p.get('puntos2') is None:
            continue
        e1 = frozenset(clave(nombre.get(i)) for i in (p['equipo1'].get('del_id'), p['equipo1'].get('zag_id')) if i)
        e2 = frozenset(clave(nombre.get(i)) for i in (p['equipo2'].get('del_id'), p['equipo2'].get('zag_id')) if i)
        idx[(p['fecha'], e1, e2)] = (p['puntos1'], p['puntos2'])
        idx[(p['fecha'], e2, e1)] = (p['puntos2'], p['puntos1'])
    return idx


def resultado(partido, idx):
    """(puntos1, puntos2) orientado como en la porra, o None si aún no está."""
    fecha = partido['id'][:10]
    e1 = frozenset(clave(n) for n in partido['eq1'])
    e2 = frozenset(clave(n) for n in partido['eq2'])
    return idx.get((fecha, e1, e2))


def decidir(abiertos, cartelera_ids, veladas, idx, ahora):
    """Qué hacer con los partidos abiertos de la base de datos: lista de (id, cambios)."""
    cambios = []
    for p in abiertos:
        inicio = datetime.fromisoformat(p['inicio'])
        if inicio <= ahora:
            r = resultado(p, idx)
            if r:
                cambios.append((p['id'], {'estado': 'jugado', 'puntos1': r[0], 'puntos2': r[1]}))
            elif ahora - inicio > timedelta(days=DIAS_SIN_RESULTADO):
                cambios.append((p['id'], {'estado': 'anulado'}))
        elif p['id'] not in cartelera_ids and (p['id'][:10], p.get('fronton')) in veladas:
            # La velada sigue anunciada pero este cartel ya no: ha cambiado
            cambios.append((p['id'], {'estado': 'anulado'}))
    return cambios


def resultados_podio(partidos, pelotaris, competiciones):
    """Podio de los torneos individuales con la final jugada:
    [{competicion, campeon, subcampeon, semis}] con los nombres del catálogo."""
    nombre = {p['id']: p['nombre'] for p in pelotaris}
    comp = {c['id']: c for c in competiciones}
    por_comp = {}
    for p in partidos:
        c = comp.get(p['competicion_id']) or {}
        if c.get('categoria') not in ('campeonato', 'torneo') or p.get('modalidad') not in ('mano', 'cuatro'):
            continue
        por_comp.setdefault(c['nombre'], []).append(p)
    salida = []
    for nom, ps in sorted(por_comp.items()):
        final = next((p for p in ps if p.get('fase') == 'final' and p.get('ganador')), None)
        if not final:
            continue
        uno = lambda p, k: nombre.get(p[k].get('del_id'))
        perdedor = lambda p: uno(p, 'equipo2' if p['ganador'] == 'equipo1' else 'equipo1')
        semis = sorted({perdedor(p) for p in ps if p.get('fase') == 'semifinal' and p.get('ganador')} - {None})
        salida.append({'competicion': nom, 'campeon': uno(final, final['ganador']), 'subcampeon': perdedor(final),
                       'semis': semis})
    return salida


class Supabase:
    def __init__(self, clave_secreta):
        self.s = requests.Session()
        self.s.headers.update({'apikey': clave_secreta, 'Content-Type': 'application/json'})

    def _ok(self, r):
        if r.status_code >= 300:
            raise RuntimeError(f'{r.status_code} {r.text[:300]}')
        return r

    def subir(self, partidos):
        self._ok(self.s.post(f'{SUPABASE_URL}/rest/v1/porra_partidos?on_conflict=id', data=json.dumps(partidos),
                             headers={'Prefer': 'resolution=merge-duplicates,return=minimal'}, timeout=30))

    def subir_podios(self, podios):
        self._ok(self.s.post(f'{SUPABASE_URL}/rest/v1/porra_podio_resultados?on_conflict=competicion', data=json.dumps(podios),
                             headers={'Prefer': 'resolution=merge-duplicates,return=minimal'}, timeout=30))

    def abiertos(self):
        r = self._ok(self.s.get(f'{SUPABASE_URL}/rest/v1/porra_partidos',
                                params={'select': 'id,inicio,fronton,eq1,eq2', 'estado': 'eq.abierto'}, timeout=30))
        return r.json()

    def cambiar(self, pid, cambios):
        cambios = dict(cambios, actualizado=datetime.now(timezone.utc).isoformat())
        self._ok(self.s.patch(f'{SUPABASE_URL}/rest/v1/porra_partidos', params={'id': f'eq.{pid}'},
                              data=json.dumps(cambios), headers={'Prefer': 'return=minimal'}, timeout=30))


def main():
    cartelera = cargar('cartelera')
    nuevos = partidos_cartelera(cartelera)
    idx = indice_resultados(cargar('partidos'), cargar('pelotaris'))
    clave_secreta = os.environ.get('SUPABASE_SECRET_KEY', '').strip()
    if not clave_secreta:
        print(f'Sin SUPABASE_SECRET_KEY: no se toca la base de datos. {len(nuevos)} partidos en la cartelera:')
        for p in nuevos:
            print(f"  {p['inicio']}  {p['id']}")
        return 0

    db = Supabase(clave_secreta)
    # Solo se suben partidos futuros: uno ya empezado no se reabre ni se mueve
    ahora = datetime.now(timezone.utc)
    futuros = [p for p in nuevos if datetime.fromisoformat(p['inicio']) > ahora]
    if futuros:
        db.subir(futuros)
    veladas = {(p['id'][:10], p['fronton']) for p in nuevos}
    cambios = decidir(db.abiertos(), {p['id'] for p in nuevos}, veladas, idx, ahora)
    for pid, c in cambios:
        db.cambiar(pid, c)
        print(f"  {c['estado']:8} {pid} {c.get('puntos1', '')}{'-' if 'puntos1' in c else ''}{c.get('puntos2', '')}")
    podios = resultados_podio(cargar('partidos'), cargar('pelotaris'), cargar('competiciones'))
    if podios:
        db.subir_podios(podios)
    print(f'✓ Porra: {len(futuros)} partidos de la cartelera subidos, {len(cambios)} actualizados, {len(podios)} podios')
    return 0


if __name__ == '__main__':
    sys.exit(main())
