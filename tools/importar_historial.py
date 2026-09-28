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
from competiciones import TORNEOS, _tipo, clasificar_partido, sin_tildes  # noqa: E402
from historial_baiko import competicion_de_titulo, equipo_limpio, leer_historial  # noqa: E402
from jsonio import guardar_json  # noqa: E402
from red import descargar  # noqa: E402
from scraper import PARTIDOS_FILE, Catalogos, clave_nombre, normalizar_ubicacion  # noqa: E402

MODALIDAD = {'Manomanista': 'mano', 'Parejas': 'parejas', '4 y Medio': 'cuatro'}


def destino_torneo(hist, competiciones, partidos):
    """Competición de nuestro catálogo para la ficha de un torneo (San Fermín,
    San Mateo, Masters...): (nombre, 'torneo', modalidad de sus partidos
    individuales, serie). None si la ficha no es de un torneo."""
    t = sin_tildes(hist['titulo']).lower()
    base = next((b for clave, b in TORNEOS if clave in t), None)
    if not base:
        return None
    ult = max(hist['partidos'], key=lambda p: p['fecha'][-4:] + p['fecha'][3:5] + p['fecha'][:2])
    r = clasificar_partido(hist['titulo'], ult['fecha'], any(len(h['equipo1']) > 1 for h in hist['partidos']))
    anio = ult['fecha'][-4:]
    cuatro = r['modalidad'] == 'cuatro'
    # Los nombres de los torneos han cambiado con los años ('Torneo San
    # Fermín 2023', 'Torneo San Fermin Serie A 2025'): se busca el que ya
    # tenemos antes de crear otro
    cands = {}
    for c in competiciones:
        n = sin_tildes(c['nombre']).lower()
        if (n.startswith(sin_tildes(base).lower()) and n.endswith(anio) and
                cuatro == ('4 y medio' in n) and ('manomanista' in n) == ('manomanista' in t) and
                (r['serie'] == 'B') == ('serie b' in n)):
            cands[c['nombre']] = c['id']
    if r['competicion'] in cands or not cands:
        nombre = r['competicion']
    elif len(cands) == 1:
        nombre = next(iter(cands))
    else:
        return None
    serie = 'B' if 'Serie B' in nombre else ('A' if 'Serie A' in nombre else None)
    # Modalidad de los partidos individuales: la del título, o la que ya
    # tienen los nuestros de ese torneo ('Torneo Bizkaia 2023' es de 4 y medio)
    ya = [p['modalidad'] for p in partidos
          if p['competicion_id'] == cands.get(nombre) and not p['equipo1'].get('zag_id')]
    mod_ind = 'cuatro' if cuatro else max(set(ya), key=ya.count) if ya else 'mano'
    return nombre, 'torneo', mod_ind, serie


def importar(rutas, dry):
    with open(PARTIDOS_FILE, encoding='utf-8') as f:
        partidos = json.load(f)
    cats = Catalogos()
    nombre_pel = {p['id']: p['nombre'] for p in cats.pelotaris}
    nombre_comp = {c['id']: c['nombre'] for c in cats.competiciones}

    def buscar(nombre):
        """Id del pelotari si ya existe (sin crear ninguno)."""
        p = cats._idx_pel.get(nombre.upper()) or cats._clave_pel.get(clave_nombre(nombre)) or cats._casi_igual(nombre)
        return p['id'] if p else None

    def equipo(p, k):
        return {x for x in (p[k].get('del_id'), p[k].get('zag_id')) if x}

    def texto(p):
        e = lambda k: ' / '.join(nombre_pel.get(x, '?') for x in (p[k].get('del_id'), p[k].get('zag_id')) if x)
        return f"{e('equipo1')} {p['puntos1']}-{p['puntos2']} {e('equipo2')}"
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
        torneo = hist['partidos'] and destino_torneo(hist, cats.competiciones, partidos)
        comp = torneo[0] if torneo else competicion_de_titulo(hist['titulo'], hist['partidos'])
        print(f"\n== {hist['titulo'] or ruta} -> {comp} ({len(hist['partidos'])} partidos)")
        if not hist['partidos']:
            print('   !! la página no tiene historial (o ha cambiado su formato); se salta')
            continue
        if not comp:
            print('   !! no sé a qué competición corresponde; se salta')
            continue
        if max(max(h['puntos1'], h['puntos2']) for h in hist['partidos']) < 10:
            print('   !! se juega a sets (2-1, 0-2...), no a tantos; se salta')
            continue
        # Se trabaja sobre una copia: si la ficha no cuadra con lo que tenemos,
        # se descarta entera
        copia = json.loads(json.dumps(partidos))
        partidos_orig, partidos = partidos, copia
        if torneo:
            # En un torneo cada partido lleva su modalidad (parejas o individual)
            _, categoria, mod_ind, serie = torneo
        else:
            categoria = 'campeonato'
            mod_ind = next(v for k, v in MODALIDAD.items() if f'Campeonato {k} ' in comp)
            serie = comp.split(' Serie ')[1][0]

        def clase(pareja):
            mod = 'parejas' if (pareja and categoria == 'torneo') else mod_ind
            return mod, _tipo(mod, categoria, serie)
        comp_id = cats.get_or_create_competicion(comp, clase(any(len(h['equipo1']) > 1 for h in hist['partidos']))[1])
        confirmados = set()
        n = {'fase': 0, 'movido': 0, 'nuevo': 0, 'tanteo': 0, 'sustitucion': 0, 'sin_fronton': 0, 'parecido': 0}

        def parecido(iso, conocidos, tanteo):
            todos = (conocidos[0] | conocidos[1]) - {None}
            for delta in (0, -1, 1):
                f = (date.fromisoformat(iso) + timedelta(days=delta)).isoformat()
                for p in (q for q in partidos if q['fecha'] == f):
                    suyos = equipo(p, 'equipo1') | equipo(p, 'equipo2')
                    if (sorted(tanteo) == sorted((p['puntos1'], p['puntos2'])) and
                            len(todos & suyos) >= max(1, len(suyos) - 1)):
                        return True
            return False

        for h in hist['partidos']:
            iso = f"{h['fecha'][6:]}-{h['fecha'][3:5]}-{h['fecha'][:2]}"
            # Pelotaris que jugaron seguro (sin nota de sustitución)
            eqs = [equipo_limpio(h[k]) for k in ('equipo1', 'equipo2')]
            sustitucion = any(s for _, s in eqs)
            conocidos = [{buscar(n) for n in nombres} for nombres, _ in eqs]
            mio, en1 = None, True
            for delta in (0, -1, 1):
                f = (date.fromisoformat(iso) + timedelta(days=delta)).isoformat()
                for p in (q for q in partidos if q['fecha'] == f):
                    s1, s2 = equipo(p, 'equipo1'), equipo(p, 'equipo2')
                    for o1, o2, orden in ((s1, s2, True), (s2, s1, False)):
                        pts = (p['puntos1'], p['puntos2']) if orden else (p['puntos2'], p['puntos1'])
                        if None in conocidos[0] | conocidos[1]:
                            continue
                        if not sustitucion and conocidos[0] == o1 and conocidos[1] == o2:
                            mio, en1 = p, orden
                        # Con sustitución: los que seguro jugaron están, y el
                        # tanteo coincide
                        elif (sustitucion and conocidos[0] <= o1 and conocidos[1] <= o2 and
                              len(conocidos[0] | conocidos[1]) >= 1 and pts == (h['puntos1'], h['puntos2'])):
                            mio, en1 = p, orden
                        if mio:
                            break
                    if mio:
                        break
                if mio:
                    break
            if mio:
                t_mio = (mio['puntos1'], mio['puntos2']) if en1 else (mio['puntos2'], mio['puntos1'])
                if t_mio != (h['puntos1'], h['puntos2']):
                    n['tanteo'] += 1
                    print(f"   ~ tanteo distinto {h['fecha']} {h['equipo1']} {h['puntos1']}-{h['puntos2']} "
                          f"{h['equipo2']}: tenemos {t_mio[0]}-{t_mio[1]} (se deja el nuestro)")
                if mio['competicion_id'] != comp_id:
                    n['movido'] += 1
                    print(f"   > a {comp}: {mio['fecha']} {texto(mio)} (estaba en {nombre_comp.get(mio['competicion_id'])})")
                    mio['competicion_id'] = comp_id
                    mod, tipo = clase(bool(mio['equipo1'].get('zag_id')))
                    mio.update({'modalidad': mod, 'categoria': categoria, 'serie': serie, 'tipo': tipo})
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
            elif not h['fronton'].strip(' -'):
                n['sin_fronton'] += 1
                print(f"   · sin frontón en el historial, no se añade: {h['fecha']} "
                      f"{' / '.join(h['equipo1'])} {h['puntos1']}-{h['puntos2']} {' / '.join(h['equipo2'])}")
            elif parecido(iso, conocidos, (h['puntos1'], h['puntos2'])):
                # Mismo día y tanteo con casi los mismos pelotaris: es el nuestro
                # con otro nombre en la ficha; no se duplica
                n['parecido'] += 1
                print(f"   · parece uno nuestro con otro pelotari, no se añade: {h['fecha']} "
                      f"{' / '.join(h['equipo1'])} {h['puntos1']}-{h['puntos2']} {' / '.join(h['equipo2'])}")
            elif sustitucion:
                # No sabemos quién jugó en lugar del anunciado: no se añade
                n['sustitucion'] += 1
                print(f"   · sin emparejar (hubo sustitución, no se añade): {h['fecha']} "
                      f"{' / '.join(h['equipo1'])} {h['puntos1']}-{h['puntos2']} {' / '.join(h['equipo2'])}")
            else:
                ids = [[cats.get_or_create_pelotari(x.upper()) for x in nombres] for nombres, _ in eqs]
                fronton, ciudad = normalizar_ubicacion(h['fronton'], h['ciudad'])
                mod, tipo = clase(len(ids[0]) > 1)
                del_zag = lambda eq: {'del_id': eq[0], 'zag_id': eq[1] if len(eq) > 1 else None}
                nuevo = {
                    'fecha': iso, 'fronton_id': cats.get_or_create_fronton(fronton, ciudad),
                    'competicion_id': comp_id, 'tipo': tipo,
                    'equipo1': del_zag(ids[0]), 'puntos1': h['puntos1'],
                    'equipo2': del_zag(ids[1]), 'puntos2': h['puntos2'],
                    'ganador': 'equipo1' if h['puntos1'] > h['puntos2'] else 'equipo2',
                    'fuente': 'baiko-historial', 'modalidad': mod, 'categoria': categoria,
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
            print(f"   ? no está en el historial: {p['fecha']} {texto(p)}")
        nuestros = sum(1 for p in partidos_orig if p['competicion_id'] == comp_id)
        if nuestros >= 10 and len(sobran) > 0.25 * nuestros:
            print(f"   !! {len(sobran)} de nuestros {nuestros} partidos no están en esta ficha: no cuadra, "
                  f"no se aplica (revisar a mano)")
            partidos = partidos_orig
            resumen.append((comp + ' [NO APLICADO]', n, quitadas, len(sobran)))
            continue
        resumen.append((comp, n, quitadas, len(sobran)))

    partidos.sort(key=lambda p: p['fecha'], reverse=True)
    print('\nRESUMEN')
    for comp, n, quitadas, sobran in resumen:
        print(f"  {comp}: {n['fase']} fases puestas, {n['nuevo']} partidos nuevos, {n['movido']} cambiados "
              f"de competición, {quitadas} fases deducidas quitadas, {n['tanteo']} tanteos distintos, "
              f"{n['sustitucion']} con sustitución sin emparejar, {n['sin_fronton']} sin frontón, {n['parecido']} parecidos a uno nuestro, "
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
