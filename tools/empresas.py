#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
empresas.py — Eskupilota Stats

Pone a cada pelotari del catálogo su empresa ('baiko' o 'aspe') según la
plantilla que publica cada una en su web. Se buscan en la página los nombres
del catálogo ('P. Etxeberria' = 'P.ETXEBERRIA', 'Peña II' = 'PEÑA II').

  - Si una web no se puede leer, o salen muy pocos pelotaris (ha cambiado),
    esa empresa no se toca.
  - Un pelotari que sale en las dos (fichaje, noticia...) no se cambia y se avisa.
  - A los que no salen en ninguna se les deja la que tenían.

Uso:
    python tools/empresas.py --dry-run
    python tools/empresas.py
"""

import json
import os
import re
import sys
import unicodedata

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, os.path.join(RAIZ, 'scraper'))
from jsonio import guardar_json  # noqa: E402
from red import descargar  # noqa: E402

PELOTARIS = os.path.join(RAIZ, 'data', 'pelotaris.json')

# Posibles direcciones de la plantilla de cada empresa (vale la primera que se lea)
PLANTILLAS = {
    'baiko': ['https://www.baikopilota.eus/pelotaris/', 'https://baikopilota.eus/pelotaris/',
              'https://www.baikopilota.eus/es/pelotaris/'],
    'aspe': ['https://aspepelota.eus/pelotaris/', 'https://aspepelota.eus/es/pelotaris/',
             'https://aspepelota.eus/pilotariak/'],
}
MIN_ENCONTRADOS = 5


def clave(s):
    s = unicodedata.normalize('NFD', s or '')
    return ''.join(c for c in s if c.isalnum() and unicodedata.category(c) != 'Mn').lower()


def claves_de_pagina(html):
    """Nombres de 1 a 3 palabras seguidas del texto, sin separadores:
    'P. Etxeberria' -> {'p', 'etxeberria', 'petxeberria'}."""
    try:
        from bs4 import BeautifulSoup
        texto = BeautifulSoup(html, 'html.parser').get_text(' ')
    except Exception:
        texto = re.sub(r'<[^>]+>', ' ', html)
    palabras = [clave(w) for w in re.split(r'[\s/,;:|()·–-]+', texto)]
    palabras = [w for w in palabras if w]
    romano = re.compile(r'^(i{1,3}|iv|v|vi{1,3})$')
    claves = set()
    for n in (1, 2, 3):
        for i in range(len(palabras) - n + 1):
            # 'Alberdi II' no es 'ALBERDI': no se corta antes de un número romano
            if i + n < len(palabras) and romano.match(palabras[i + n]):
                continue
            claves.add(''.join(palabras[i:i + n]))
    return claves


def encontrados(nombres, html):
    ks = claves_de_pagina(html)
    return {n for n in nombres if clave(n) in ks}


def main(dry):
    with open(PELOTARIS, encoding='utf-8') as f:
        pelotaris = json.load(f)
    nombres = [p['nombre'] for p in pelotaris]
    en = {}
    for empresa, urls in PLANTILLAS.items():
        for url in urls:
            try:
                html = descargar(url)
            except Exception as e:
                print(f"  {empresa}: {url} no se ha podido leer ({e})")
                continue
            hallados = encontrados(nombres, html)
            print(f"  {empresa}: {url} -> {len(hallados)} pelotaris")
            if len(hallados) >= MIN_ENCONTRADOS:
                en[empresa] = hallados
                break
        else:
            print(f"  !! {empresa}: ninguna página válida; no se toca")
    cambios = 0
    for p in pelotaris:
        suyas = [e for e, hs in en.items() if p['nombre'] in hs]
        antes = p.get('empresa')
        if len(suyas) == 1:
            if antes != suyas[0]:
                print(f"  {p['nombre']}: {antes or '—'} -> {suyas[0]}")
                p['empresa'] = suyas[0]
                cambios += 1
        elif len(suyas) > 1:
            print(f"  ? {p['nombre']}: sale en {' y '.join(suyas)}; se deja {antes or 'sin empresa'}")
    sin = [p['nombre'] for p in pelotaris if not p.get('empresa')]
    print(f"\n{cambios} cambios. Sin empresa: {len(sin)}{': ' + ', '.join(sin) if sin else ''}")
    if dry:
        print('[--dry-run] No se ha escrito nada.')
    elif cambios:
        guardar_json(PELOTARIS, pelotaris)


if __name__ == '__main__':
    main('--dry-run' in sys.argv)
