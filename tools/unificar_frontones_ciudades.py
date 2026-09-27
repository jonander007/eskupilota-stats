#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
unificar_frontones_ciudades.py — Eskupilota Stats

Fusiona frontones y ciudades duplicados en los catálogos. Son el mismo sitio
escrito de varias formas (con o sin tilde, con guion, en euskera y en
castellano, truncado...):

  Frontones
    EL CIEGO + ELCIEGO                       -> ELCIEGO
    ETXARRI ARANATZ + ETXARRI-ARANATZ        -> ETXARRI ARANATZ
    HONDARRIBI + HONDARRIBIA                 -> HONDARRIBIA
    HUERCANOS + HUÉRCANOS                    -> HUÉRCANOS
    LARRAINTZAR + LARRAINZAR                 -> LARRAINTZAR
    MALLABIA + MALLABIA I                    -> MALLABIA
    NAJERA + NÁJERA                          -> NÁJERA
    SANTA MARIA / MARÍA DE LAS HOYAS         -> SANTA MARÍA DE LAS HOYAS
    SANTO DOMINGO + SANTO DOMINGO DE LA CALZADA -> SANTO DOMINGO DE LA CALZADA
    AMASA-VILLABONA + VILLABONA              -> VILLABONA
    ORDUÑA + URDUÑA                          -> URDUÑA
    LAUDIO + LLODIO                          -> LLODIO
    ILUNBERRI + LUMBIER                      -> LUMBIER

  Ciudades
    EL CIEGO + ELCIEGO, ETXARRI ARANATZ + ETXARRI-ARANATZ,
    LARRAINTZAR + LARRAINZAR, SANTA MARIA/MARÍA DE LAS HOYAS,
    AMASA-VILLABONA + VILLABONA, LLODIO + LAUDIO, LUMBIER + ILUNBERRI,
    BERUETE + "BERUETE -", OTXANDIO + "OTXANDIO -"

Se conserva el ID más bajo de cada grupo y los partidos se reasignan a él.
Además corrige los nombres de ciudad en castellano y euskera (traducciones
conocidas y "De La" -> "de la").

Los registros se localizan por nombre, así que el script es idempotente.

Uso:
    python tools/unificar_frontones_ciudades.py --dry-run
    python tools/unificar_frontones_ciudades.py

Después:
    python tools/recalcular_contadores.py
"""

import json
import os
import re
import sys
from urllib.parse import quote

DATA_DIR = 'data'
PARTIDOS_FILE = os.path.join(DATA_DIR, 'partidos.json')
FRONTONES_FILE = os.path.join(DATA_DIR, 'frontones.json')
CIUDADES_FILE = os.path.join(DATA_DIR, 'ciudades.json')

# nombre final: nombres que se fusionan
FRONTONES = {
    'ELCIEGO':                     ['EL CIEGO', 'ELCIEGO'],
    'ETXARRI ARANATZ':             ['ETXARRI ARANATZ', 'ETXARRI-ARANATZ'],
    'HONDARRIBIA':                 ['HONDARRIBI', 'HONDARRIBIA'],
    'HUÉRCANOS':                   ['HUERCANOS', 'HUÉRCANOS'],
    'LARRAINTZAR':                 ['LARRAINTZAR', 'LARRAINZAR'],
    'MALLABIA':                    ['MALLABIA', 'MALLABIA I'],
    'NÁJERA':                      ['NAJERA', 'NÁJERA'],
    'SANTA MARÍA DE LAS HOYAS':    ['SANTA MARIA DE LAS HOYAS', 'SANTA MARÍA DE LAS HOYAS'],
    'SANTO DOMINGO DE LA CALZADA': ['SANTO DOMINGO', 'SANTO DOMINGO DE LA CALZADA'],
    'VILLABONA':                   ['AMASA-VILLABONA', 'VILLABONA'],
    'URDUÑA':                      ['ORDUÑA', 'URDUÑA'],
    'LLODIO':                      ['LAUDIO', 'LLODIO'],
    'LUMBIER':                     ['ILUNBERRI', 'LUMBIER'],
}

CIUDADES = {
    'ELCIEGO':                  ['EL CIEGO', 'ELCIEGO'],
    'ETXARRI ARANATZ':          ['ETXARRI ARANATZ', 'ETXARRI-ARANATZ'],
    'LARRAINTZAR':              ['LARRAINTZAR', 'LARRAINZAR'],
    'SANTA MARÍA DE LAS HOYAS': ['SANTA MARIA DE LAS HOYAS', 'SANTA MARÍA DE LAS HOYAS'],
    'AMASA-VILLABONA':          ['AMASA-VILLABONA', 'VILLABONA'],
    'LLODIO':                   ['LLODIO', 'LAUDIO'],
    'LUMBIER':                  ['LUMBIER', 'ILUNBERRI'],
    'BERUETE':                  ['BERUETE', 'BERUETE -'],
    'OTXANDIO':                 ['OTXANDIO', 'OTXANDIO -'],
}

# Nombre en castellano y en euskera cuando difieren
TRADUCCIONES = {
    'AMASA-VILLABONA':   ('Villabona',        'Amasa-Villabona'),
    'AOIZ':              ('Aoiz',             'Agoitz'),
    'BURLADA':           ('Burlada',          'Burlata'),
    'CIZUR MAYOR':       ('Cizur Mayor',      'Zizur Nagusia'),
    'CIZUR MENOR':       ('Cizur Menor',      'Zizur Txikia'),
    'ELBURGO':           ('Elburgo',          'Burgelu'),
    'ELCIEGO':           ('Elciego',          'Eltziego'),
    'LABASTIDA':         ('Labastida',        'Bastida'),
    'LANCIEGO':          ('Lanciego',         'Lantziego'),
    'LEGUTIO':           ('Legutiano',        'Legutio'),
    'LLODIO':            ('Llodio',           'Laudio'),
    'LOS ARCOS':         ('Los Arcos',        'Arkoak'),
    'LUMBIER':           ('Lumbier',          'Ilunberri'),
    'OION':              ('Oyón',             'Oion'),
    'OÑATI':             ('Oñate',            'Oñati'),
    'SANTA MARÍA DE LAS HOYAS': ('Santa María de las Hoyas', 'Santa María de las Hoyas'),
    'ORDUÑA':            ('Orduña',           'Urduña'),
    'VALCARLOS':         ('Valcarlos',        'Luzaide'),
    'VILLAVA':           ('Villava',          'Atarrabia'),
}

MINUSCULAS = {'De', 'La', 'Las', 'Los', 'Del', 'Y'}


def cap_nombre(s):
    """'Santo Domingo De La Calzada' -> 'Santo Domingo de la Calzada'."""
    palabras = s.split(' ')
    return ' '.join(p.lower() if i and p in MINUSCULAS else p
                    for i, p in enumerate(palabras))


def maps_link(nombre):
    return 'https://www.google.com/maps/search/?api=1&query=' + quote('Fronton ' + nombre)


def load(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def save(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def fusionar(items, grupos, etiqueta):
    """Devuelve {id_borrado: id_conservado} y deja `items` fusionado."""
    remap = {}
    por_nombre = {}
    for it in items:
        por_nombre.setdefault(it['nombre'], []).append(it)
    for final, nombres in grupos.items():
        grupo = [it for n in nombres for it in por_nombre.get(n, [])]
        if not grupo:
            continue
        grupo.sort(key=lambda it: it['id'])
        keep, resto = grupo[0], grupo[1:]
        if keep['nombre'] != final:
            print(f"  {etiqueta}: {keep['id']} '{keep['nombre']}' -> '{final}'")
            keep['nombre'] = final
        for it in resto:
            print(f"  {etiqueta}: {it['id']} '{it['nombre']}' se fusiona en {keep['id']} '{final}'")
            remap[it['id']] = keep['id']
            # Aprovecha las coordenadas del duplicado si el conservado no tiene
            if 'lat' in it and 'lat' not in keep:
                keep['lat'], keep['lon'] = it['lat'], it['lon']
    items[:] = [it for it in items if it['id'] not in remap]
    return remap


def main():
    dry = '--dry-run' in sys.argv
    partidos = load(PARTIDOS_FILE)
    frontones = load(FRONTONES_FILE)
    ciudades = load(CIUDADES_FILE)

    print('FRONTONES')
    remap_fro = fusionar(frontones, FRONTONES, 'frontón')
    print('CIUDADES')
    remap_ciu = fusionar(ciudades, CIUDADES, 'ciudad')

    n_par = 0
    for p in partidos:
        if p.get('fronton_id') in remap_fro:
            p['fronton_id'] = remap_fro[p['fronton_id']]
            n_par += 1
    for f in frontones:
        f['ciudad_id'] = remap_ciu.get(f.get('ciudad_id'), f.get('ciudad_id'))
        # Coordenadas justo después de ciudad_id, como en el resto del catálogo
        if 'lat' in f:
            orden = {}
            for k, v in f.items():
                if k in ('lat', 'lon'):
                    continue
                orden[k] = v
                if k == 'ciudad_id':
                    orden['lat'], orden['lon'] = f['lat'], f['lon']
            f.clear()
            f.update(orden)
        if 'google_maps_link' in f:
            f['google_maps_link'] = maps_link(f['nombre'])

    n_nom = 0
    for c in ciudades:
        es, eu = TRADUCCIONES.get(c['nombre'], (c.get('nombre_es') or c['nombre'].title(),
                                                c.get('nombre_eu') or c['nombre'].title()))
        es, eu = cap_nombre(es), cap_nombre(eu)
        if (es, eu) != (c.get('nombre_es'), c.get('nombre_eu')):
            print(f"  nombres {c['id']} {c['nombre']}: {c.get('nombre_es')}/{c.get('nombre_eu')} -> {es}/{eu}")
            c['nombre_es'], c['nombre_eu'] = es, eu
            n_nom += 1

    print(f"\n{len(remap_fro)} frontones y {len(remap_ciu)} ciudades fusionados, "
          f"{n_par} partidos reasignados, {n_nom} ciudades con nombres corregidos")
    if dry:
        print('[--dry-run] No se ha escrito nada.')
        return
    save(PARTIDOS_FILE, partidos)
    save(FRONTONES_FILE, frontones)
    save(CIUDADES_FILE, ciudades)
    print('  Siguiente paso: python tools/recalcular_contadores.py')


if __name__ == '__main__':
    main()
