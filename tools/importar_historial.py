#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
importar_historial.py — Eskupilota Stats

Importa el «Historial de competición» de páginas de campeonato de Baiko
(guardadas como HTML, ver scraper/historial_baiko.py):

  - A cada partido nuestro que aparece en el historial (misma fecha, o un
    día de diferencia, y mismos pelotaris) le pone la fase y el grupo
    reales y le quita la marca de «deducida». Si estaba en otra competición,
    lo pasa a esta.
  - Los partidos del historial que no tenemos se añaden (fuente
    'baiko-historial').
  - En esa competición se quitan las fases deducidas de los partidos que el
    historial no confirma.
  - Avisa de los tanteos que no coinciden (se deja el nuestro).

Las páginas pueden ser ficheros HTML guardados o direcciones de Baiko, y
--lista data/historial_baiko.json lee las direcciones de ese fichero (las
fichas de los campeonatos). Lo usa el workflow «Historial de Baiko».

Uso:
    python tools/importar_historial.py --dry-run pagina1.html pagina2.html ...
    python tools/importar_historial.py pagina1.html https://baikopilota.eus/campeonato/...
    python tools/importar_historial.py --lista data/historial_baiko.json
    python tools/recalcular_contadores.py
    python tools/generar_paginas.py
"""

import json
import os
import sys
from datetime import date, timedelta

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, os.path.join(RAIZ, 'scraper'))
from competiciones import _tipo  # noqa: E402
from historial_baiko import competicion_de_titulo, leer_historial  # noqa: E402
from jsonio import guardar_json  # noqa: E402
from red import descargar  # noqa: E402
from scraper import PARTIDOS_FILE, Catalogos, normalizar_ubicacion  # noqa: E402

MODALIDAD = {'Manomanista': 'mano', 'Parejas': 'parejas', '4 y Medio': 'cuatro'}


def importar(rutas, dry):
    with open(PARTIDOS_FILE, encoding='utf-8') as f:
        partidos = json.load(f)
    cats = Catalogos()
    nombre_pel = {p['id']: p['nombre'] for p in cats.pelotaris}
    resumen = []

    for ruta in rutas:
        try:
            if ruta.startswith('http'):
                html = descargar(ruta)
            else:
                with open(ruta, encoding='utf-8', errors='replace') as f:
                    html = f.read()
        except Exception as e:      # una ficha que falla no para las demás
            print(f"\n== {ruta}\n   !! no se ha podido leer: {e}")
            continue
        hist = leer_historial(html)
        comp = competicion_de_titulo(hist['titulo'], hist['partidos'])
        print(f"\n== {hist['titulo'] or ruta} -> {comp} ({len(hist['partidos'])} partidos)")
        if not hist['partidos']:
            print('   !! la página no tiene historial (o ha cambiado su formato); se salta')
            continue
        if not comp:
            print('   !! no sé a qué competición corresponde; se salta')
            continue
        mod = next(v for k, v in MODALIDAD.items() if f'Campeonato {k} ' in comp)
        serie = comp.split(' Serie ')[1][0]
        tipo = _tipo(mod, 'campeonato', serie)
        comp_id = cats.get_or_create_competicion(comp, tipo)
        confirmados = set()
        n = {'fase': 0, 'movido': 0, 'nuevo': 0, 'tanteo': 0}

        for h in hist['partidos']:
            iso = f"{h['fecha'][6:]}-{h['fecha'][3:5]}-{h['fecha'][:2]}"
            ids = [[cats.get_or_create_pelotari(x.upper()) for x in h[k]] for k in ('equipo1', 'equipo2')]
            todos = {i for eq in ids for i in eq}
            mio = None
            for delta in (0, -1, 1):
                f = (date.fromisoformat(iso) + timedelta(days=delta)).isoformat()
                mio = next((p for p in partidos if p['fecha'] == f and
                            {x for k in ('equipo1', 'equipo2') for x in (p[k].get('del_id'), p[k].get('zag_id')) if x} == todos), None)
                if mio:
                    break
            if mio:
                # Tanteo en el orden del historial
                en1 = set(ids[0]) == {x for x in (mio['equipo1'].get('del_id'), mio['equipo1'].get('zag_id')) if x}
                t_mio = (mio['puntos1'], mio['puntos2']) if en1 else (mio['puntos2'], mio['puntos1'])
                if t_mio != (h['puntos1'], h['puntos2']):
                    n['tanteo'] += 1
                    print(f"   ~ tanteo distinto {h['fecha']} {h['equipo1']} {h['puntos1']}-{h['puntos2']} "
                          f"{h['equipo2']}: tenemos {t_mio[0]}-{t_mio[1]}")
                if mio['competicion_id'] != comp_id:
                    n['movido'] += 1
                    mio['competicion_id'] = comp_id
                    mio.update({'modalidad': mod, 'categoria': 'campeonato', 'serie': serie, 'tipo': tipo})
                antes = (mio.get('fase'), mio.get('grupo'), mio.get('fase_deducida'))
                mio.pop('fase_deducida', None)
                if h['fase'] != 'liga':
                    mio.pop('jornada', None)
                mio['fase'] = h['fase']
                if h['grupo']:
                    mio['grupo'] = h['grupo']
                else:
                    mio.pop('grupo', None)
                if antes != (mio['fase'], mio.get('grupo'), None):
                    n['fase'] += 1
                confirmados.add(id(mio))
            else:
                fronton, ciudad = normalizar_ubicacion(h['fronton'], h['ciudad'])
                del_zag = lambda eq: {'del_id': eq[0], 'zag_id': eq[1] if len(eq) > 1 else None}
                nuevo = {
                    'fecha': iso, 'fronton_id': cats.get_or_create_fronton(fronton, ciudad),
                    'competicion_id': comp_id, 'tipo': tipo,
                    'equipo1': del_zag(ids[0]), 'puntos1': h['puntos1'],
                    'equipo2': del_zag(ids[1]), 'puntos2': h['puntos2'],
                    'ganador': 'equipo1' if h['puntos1'] > h['puntos2'] else 'equipo2',
                    'fuente': 'baiko-historial', 'modalidad': mod, 'categoria': 'campeonato',
                    'serie': serie, 'fase': h['fase'],
                }
                if h['grupo']:
                    nuevo['grupo'] = h['grupo']
                partidos.append(nuevo)
                confirmados.add(id(nuevo))
                n['nuevo'] += 1
                print(f"   + nuevo: {h['fecha']} {h['fronton']} {' / '.join(h['equipo1'])} "
                      f"{h['puntos1']}-{h['puntos2']} {' / '.join(h['equipo2'])} ({h['fase_texto']})")

        # Fases deducidas que el historial no confirma
        quitadas = 0
        for p in partidos:
            if p['competicion_id'] == comp_id and id(p) not in confirmados and p.get('fase_deducida'):
                p.pop('fase', None)
                p.pop('fase_deducida', None)
                quitadas += 1
        sobran = [p for p in partidos if p['competicion_id'] == comp_id and id(p) not in confirmados]
        for p in sobran:
            e = lambda k: ' / '.join(nombre_pel.get(x, '?') for x in (p[k].get('del_id'), p[k].get('zag_id')) if x)
            print(f"   ? no está en el historial: {p['fecha']} {e('equipo1')} {p['puntos1']}-{p['puntos2']} {e('equipo2')}")
        resumen.append((comp, n, quitadas, len(sobran)))

    partidos.sort(key=lambda p: p['fecha'], reverse=True)
    print('\nRESUMEN')
    for comp, n, quitadas, sobran in resumen:
        print(f"  {comp}: {n['fase']} fases puestas, {n['nuevo']} partidos nuevos, {n['movido']} cambiados "
              f"de competición, {quitadas} fases deducidas quitadas, {n['tanteo']} tanteos distintos, "
              f"{sobran} partidos nuestros que no están en el historial")
    if dry:
        print('\n[--dry-run] No se ha escrito nada.')
        return
    guardar_json(PARTIDOS_FILE, partidos)
    cats.save_all()
    print('\nSiguiente: python tools/recalcular_contadores.py && python tools/generar_paginas.py')


if __name__ == '__main__':
    args = [a for a in sys.argv[1:] if a != '--dry-run']
    if '--lista' in args:
        i = args.index('--lista')
        with open(args[i + 1], encoding='utf-8') as f:
            args = args[:i] + json.load(f) + args[i + 2:]
    if not args:
        print(__doc__)
        sys.exit(1)
    rutas = [a if a.startswith('http') else os.path.abspath(a) for a in args]
    os.chdir(RAIZ)          # los catálogos se leen con rutas relativas (data/...)
    importar(rutas, '--dry-run' in sys.argv)
