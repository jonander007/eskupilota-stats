#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
limpieza_competiciones.py — Eskupilota Stats

Corrige partidos mal clasificados por el scraper antiguo, que cuando la web
no decía la competición metía el partido en "Campeonato Parejas Serie A" o
"Campeonato Manomanista Serie A" del año, aunque fuera un festival de
verano o un partido del 4 y medio.

Revisa los partidos de estas competiciones:
  - Campeonato Parejas (con y sin serie)
  - Campeonato Manomanista (con y sin serie; no toca la Promoción 2023)
  - Masters Caixabank 2026 (sin serie)

Además pasa a "festival-mano" los partidos individuales guardados con el
tipo "festival", que es el de parejas.

y los vuelve a clasificar con scraper/competiciones.py:
  - Fuera de temporada (parejas: nov-mar; manomanista: mar-jun) se busca el
    partido en el historial de la cartelera (git log de data/cartelera.json)
    y, si no aparece, pasa a "Festival".
  - Los campeonatos sin serie reciben la serie según sus pelotaris.
  - El campeonato de parejas lleva el año en que termina la temporada.

Al final corrige el tipo de cada competición para que coincida con el de
sus partidos y borra las competiciones que se quedan vacías.

Uso:
    python tools/limpieza_competiciones.py --dry-run
    python tools/limpieza_competiciones.py

Después:
    python tools/recalcular_contadores.py
"""

import json
import os
import subprocess
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scraper'))
from competiciones import Cartelera, HistorialSeries, clasificar  # noqa: E402

DATA_DIR = 'data'
PARTIDOS_FILE = os.path.join(DATA_DIR, 'partidos.json')
PELOTARIS_FILE = os.path.join(DATA_DIR, 'pelotaris.json')
FRONTONES_FILE = os.path.join(DATA_DIR, 'frontones.json')
COMPETICIONES_FILE = os.path.join(DATA_DIR, 'competiciones.json')
CARTELERA_FILE = os.path.join(DATA_DIR, 'cartelera.json')

TEMPORADA_PAREJAS = {11, 12, 1, 2, 3}
TEMPORADA_MANO = {3, 4, 5, 6}


def load(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def save(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def cartelera_historica():
    """Todos los eventos que han pasado por data/cartelera.json en git."""
    cart = Cartelera()
    vistos = set()
    try:
        commits = subprocess.run(
            ['git', 'log', '--format=%H', '--', CARTELERA_FILE],
            capture_output=True, text=True, check=True).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        commits = []
    fuentes = []
    for h in commits:
        try:
            fuentes.append(subprocess.run(['git', 'show', f'{h}:{CARTELERA_FILE}'],
                                          capture_output=True, text=True, check=True).stdout)
        except subprocess.CalledProcessError:
            pass
    if os.path.exists(CARTELERA_FILE):
        with open(CARTELERA_FILE, encoding='utf-8') as f:
            fuentes.append(f.read())
    for src in fuentes:
        try:
            eventos = json.loads(src).get('partidos', [])
        except (ValueError, AttributeError):
            continue
        for ev in eventos:
            clave = (ev.get('fecha'), ev.get('fronton'), ev.get('hora'), tuple(ev.get('cartel') or []))
            if clave not in vistos:
                vistos.add(clave)
                cart.add(ev)
    print(f"Cartelera histórica: {len(commits)} versiones, {len(vistos)} eventos")
    return cart


def a_revisar(comp):
    n = comp['nombre']
    if n.startswith('Campeonato Parejas') and 'Promoción' not in n:
        return True
    if n.startswith('Campeonato Manomanista') and 'Promoción' not in n:
        return True
    return n == 'Masters Caixabank 2026'


def main():
    dry = '--dry-run' in sys.argv
    partidos = load(PARTIDOS_FILE)
    pelotaris = {p['id']: p['nombre'] for p in load(PELOTARIS_FILE)}
    frontones = {f['id']: f['nombre'] for f in load(FRONTONES_FILE)}
    comps = load(COMPETICIONES_FILE)
    por_id = {c['id']: c for c in comps}
    por_nombre = {c['nombre']: c for c in comps}
    revisar = {c['id'] for c in comps if a_revisar(c)}

    cart = cartelera_historica()
    hist = HistorialSeries(partidos)

    def comp_id(nombre, tipo):
        if nombre in por_nombre:
            return por_nombre[nombre]['id']
        n = max(int(c['id'][4:]) for c in comps) + 1
        nuevo = {'id': f'COMP{n:03d}', 'nombre': nombre, 'tipo': tipo, 'partidos_count': 0}
        comps.append(nuevo)
        por_id[nuevo['id']] = nuevo
        por_nombre[nombre] = nuevo
        print(f"  + nueva competición {nuevo['id']} {nombre}")
        return nuevo['id']

    cambios = Counter()
    tocadas = set(revisar)
    for p in partidos:
        if p['competicion_id'] not in revisar:
            continue
        actual = por_id[p['competicion_id']]
        mes = int(p['fecha'][5:7])
        es_pareja = bool(p['equipo1'].get('zag_id') or p['equipo2'].get('zag_id'))
        ids = [p[e].get(k) for e in ('equipo1', 'equipo2') for k in ('del_id', 'zag_id') if p[e].get(k)]
        nombres = [pelotaris.get(i) for i in ids]

        en_temporada = mes in (TEMPORADA_PAREJAS if es_pareja else TEMPORADA_MANO)
        texto, serie = (None, None)
        if not en_temporada or 'Masters' in actual['nombre']:
            texto, serie = cart.buscar(p['fecha'], frontones.get(p['fronton_id']), nombres)
        if texto is None and (en_temporada or 'Masters' in actual['nombre']):
            # Campeonato en temporada: se conserva, solo se normaliza serie y año
            texto = actual['nombre'].rsplit(' ', 1)[0]
        tipo, nombre = clasificar(texto, p['fecha'], es_pareja, serie,
                                  lambda: hist.serie(ids, p['fecha']))
        nuevo_id = comp_id(nombre, tipo)
        if nuevo_id != p['competicion_id'] or tipo != p['tipo']:
            cambios[(actual['nombre'], nombre, tipo)] += 1
            tocadas.add(nuevo_id)
            p['competicion_id'] = nuevo_id
            p['tipo'] = tipo

    # Partidos individuales guardados con el tipo de festival de parejas
    for p in partidos:
        individual = not (p['equipo1'].get('zag_id') or p['equipo2'].get('zag_id'))
        if individual and p['tipo'] == 'festival':
            cambios[(por_id[p['competicion_id']]['nombre'], '(mismo) individual', 'festival-mano')] += 1
            p['tipo'] = 'festival-mano'

    print('\nCAMBIOS (antes -> después [tipo]: partidos)')
    for (antes, despues, tipo), n in sorted(cambios.items()):
        print(f"  {antes:<40} -> {despues:<40} [{tipo}]: {n}")

    # Tipo de cada competición = el más frecuente entre sus partidos
    tipos = {}
    for p in partidos:
        tipos.setdefault(p['competicion_id'], Counter())[p['tipo']] += 1
    for c in comps:
        if c['id'] in tipos and c['id'] in tocadas:
            t = tipos[c['id']].most_common(1)[0][0]
            if c['nombre'] == 'Festival':
                t = 'festival'
            if c['tipo'] != t:
                print(f"  tipo {c['id']} {c['nombre']}: {c['tipo']} -> {t}")
                c['tipo'] = t
    vacias = [c for c in comps if c['id'] not in tipos and c['id'] in tocadas]
    for c in vacias:
        print(f"  - se borra {c['id']} {c['nombre']} (sin partidos)")
    comps[:] = [c for c in comps if c not in vacias]

    print(f"\n{sum(cambios.values())} partidos reclasificados, {len(vacias)} competiciones borradas")
    if dry:
        print('[--dry-run] No se ha escrito nada.')
        return
    save(PARTIDOS_FILE, partidos)
    save(COMPETICIONES_FILE, comps)
    print('  Siguiente paso: python tools/recalcular_contadores.py')


if __name__ == '__main__':
    main()
