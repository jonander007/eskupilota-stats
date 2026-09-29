#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
juntar_frontones.py — Eskupilota Stats

Junta el frontón «genérico» (el que lleva el nombre del pueblo porque la
fuente no daba el frontón) con el frontón con nombre propio de ese pueblo,
revisado a mano (septiembre de 2026). Los partidos pasan al frontón con
nombre; si este no tenía coordenadas, se quedan las del genérico.

Para que no vuelvan a aparecer, los nombres genéricos están también en
FRONTON_ALIAS (scraper/scraper.py).

Es idempotente: los frontones se buscan por nombre.

Uso:
    python tools/juntar_frontones.py --dry-run
    python tools/juntar_frontones.py
"""

import json
import os
import sys

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, os.path.join(RAIZ, 'scraper'))
from jsonio import guardar_json  # noqa: E402

DATA = os.path.join(RAIZ, 'data')

# genérico -> frontón con nombre (del mismo pueblo)
JUNTAR = {
    'VILLABONA': 'BEAR ZANA',
    'AOIZ': 'TOKI EDER',
    'ARETXABALETA': 'ITURRIGORRI',
    'ARRASATE': 'UARKAPE',
    'ASTIGARRAGA': 'TXOMIÑENEA',
    'ATAUN': 'AUZOETA',
    'AZPEITIA': 'IZARRAITZ',
    'BARAKALDO': 'BARAKALDES',
    'BARAÑAIN': 'RETEGUI',
    'BASAURI': 'ARTUNDUAGA',
    'BAÑOS DE RIO TOBIA': 'BARBERITO I',
    'BEASAIN': 'ANTZIZAR',
    'BERA': 'EZTEGARA',
    'BERMEO': 'ARTZA',
    'BERUETE': 'ERGIZAROTZ',
    'BURLADA': 'ASKATASUNA',
    'ELGOIBAR': 'IKASTOLA',
    'ELIZONDO': 'BAZTAN',
    'ERRATZU': 'ETXELEBERT',
    'ERRIGOITI': 'KIRRU',
    'GERNIKA': 'SANTANAPE',
    'HONDARRIBIA': 'JOSTALDI',
    'IBARRANGELU': 'IBAETA',
    'IDIAZABAL': 'IGARONDO',
    'IRUN': 'URANZU',
    'IURRETA': 'KEPA ARROITAJAUREGI',
    'LEGAZPI': 'URBELTZ',
    'LEGUTIO': 'FERNANDO GARAITA',
    'LEKEITIO': 'SANTI BROUARD',
    'LEKUNBERRI': 'JAIAN JAI',
    'LIZARRA': 'REMONTIVAL',
    'LLODIO': 'ARETA',
    'MARKINA-XEMEIN': 'ARIZPE',
    'MUSKIZ': 'DONIBANE',
    'OIARTZUN': 'MADALENSORO',
    'ORDIZIA': 'BETI ALAI',
    'OTXANDIO': 'MAINONDO',
    'OÑATI': 'ZUBIKOA',
    'SEGURA': 'BARATZE',
    'SESTAO': 'LAS LLANAS',
    'SORIA': 'LA JUVENTUD',
    'TAFALLA': 'ERETA',
    'URRETXU': 'EDERRENA',
    'ZALDIBAR': 'OLAZAR',
    'ZALLA': 'MIMETIZ',
    'ZARAUTZ': 'ARITZBATALDE',
    'ZESTOA': 'GURUTZEAGA',
    'ZUMAIA': 'AITZURI',
    'ZUMARRAGA': 'BELOKI',
    # Durango: todo es Ezkurdi
    'DURANGO': 'EZKURDI',
    'KURUTZIAGA': 'EZKURDI',
    # Pueblos con varios frontones: el genérico es este
    'BILBAO': 'BIZKAIA FRONTOIA',
    'GETARIA': 'SAHATSAGA',
    'MUNGIA': 'TROBIKA',
    'EZCARAY': 'DARÍO GÓMEZ GIL',
    'AZKOITIA': 'GUREA',
}


def cargar(nombre):
    with open(os.path.join(DATA, nombre), encoding='utf-8') as f:
        return json.load(f)


def main(dry):
    frontones, partidos = cargar('frontones.json'), cargar('partidos.json')
    por_nombre = {f['nombre']: f for f in frontones}
    quitar, n = set(), 0
    for gen, dest in JUNTAR.items():
        g, d = por_nombre.get(gen), por_nombre.get(dest)
        if not g:
            continue                      # ya junto
        if not d:
            print(f"  !! {dest} no existe; {gen} se deja")
            continue
        if g['ciudad_id'] != d['ciudad_id']:
            print(f"  !! {gen} y {dest} están en ciudades distintas; se dejan")
            continue
        movidos = [p for p in partidos if p.get('fronton_id') == g['id']]
        for p in movidos:
            p['fronton_id'] = d['id']
        if not d.get('lat') and g.get('lat'):
            d['lat'], d['lon'] = g['lat'], g['lon']
        quitar.add(g['id'])
        n += len(movidos)
        print(f"  {gen} ({len(movidos)}) -> {dest}")
    frontones = [f for f in frontones if f['id'] not in quitar]
    print(f"\n{len(quitar)} frontones juntados, {n} partidos movidos")
    if dry:
        print('[--dry-run] No se ha escrito nada.')
        return
    guardar_json(os.path.join(DATA, 'frontones.json'), frontones)
    guardar_json(os.path.join(DATA, 'partidos.json'), partidos)


if __name__ == '__main__':
    main('--dry-run' in sys.argv)
