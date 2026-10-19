#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fotos_wikidata.py — Eskupilota Stats

Pone a cada pelotari del catálogo la foto de su elemento de Wikidata
(propiedad P18, una imagen de Wikimedia Commons), con su autor y licencia,
para la ficha de la web y su página estática:

    "wikidata": "Q12263639",
    "foto": {"url": miniatura, "autor": ..., "autor_url": ..., "licencia": ...,
             "licencia_url": ..., "commons": página del fichero en Commons}

  - El elemento de Wikidata de cada pelotari se guarda en «wikidata». Si no
    lo tiene, se busca entre los pelotaris de Wikidata (deporte: pelota vasca)
    uno cuyo nombre o alias contenga el del catálogo como palabras enteras
    («JAKA» = «Erik Jaka», «PEÑA II» = «Peña II»); si no sale exactamente
    uno, se deja sin foto (se puede poner el Q a mano).
  - Solo se aceptan fotos con licencia libre: CC0, CC BY, CC BY-SA, GFDL o
    dominio público.
  - Sin foto (o con una licencia que no es libre) se quita «foto» y la web
    pone la inicial.

Uso:
    python tools/fotos_wikidata.py --dry-run
    python tools/fotos_wikidata.py
"""

import html
import json
import os
import re
import sys
import time
import unicodedata

import requests

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, os.path.join(RAIZ, 'scraper'))
from jsonio import guardar_json  # noqa: E402

PELOTARIS = os.path.join(RAIZ, 'data', 'pelotaris.json')
WIKIDATA = 'https://www.wikidata.org/w/api.php'
COMMONS = 'https://commons.wikimedia.org/w/api.php'
# Wikimedia pide un User-Agent que diga quién hace las peticiones
HEADERS = {'User-Agent': 'EskupilotaStatsBot/1.0 (https://www.eskupilotastats.com; '
                         'https://github.com/jonander007/eskupilota-stats)'}
PELOTA_VASCA = 'Q212845'     # P641 (deporte) de los pelotaris en Wikidata
ANCHO = 330                  # ancho de la miniatura (px), uno de los tamaños estándar de Commons
PAUSA = 3                    # segundos entre peticiones (la API limita las ráfagas)

ROMANO = re.compile(r'^(i{1,3}|iv|v|vi{1,3})$')
LIBRE = re.compile(r'^(cc0( 1\.0)?|cc[ -]by(-sa)?([ -]\d(\.\d)?( [a-z]{2,3})?)?|gfdl( [\d.]+)?|attribution'
                   r'|public domain|pd([ -].*)?)$', re.I)


def licencia_libre(nombre):
    """'CC BY-SA 4.0', 'CC BY 2.0', 'CC0', 'Public domain'… sí;
    'CC BY-NC 4.0', 'CC BY-ND 3.0', 'Fair use'… no."""
    return bool(LIBRE.match((nombre or '').strip()))


def palabras(s):
    """'P.ETXEBERRIA' -> ('p', 'etxeberria'); 'Peña II' -> ('pena', 'ii')."""
    s = unicodedata.normalize('NFD', s or '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn').lower()
    return tuple(re.findall(r'[a-z0-9]+', s))


def texto(html_):
    """Texto de un trozo de HTML de Commons (el autor viene con enlaces)."""
    t = re.sub(r'</li>\s*<li[^>]*>', '; ', html_ or '')       # obra derivada: «original; derivada»
    t = html.unescape(re.sub(r'<[^>]+>', '', re.sub(r'<br\s*/?>', ' ', t)))
    return re.sub(r'\s+', ' ', t).strip()


def primer_enlace(html_):
    m = re.search(r'href="([^"]+)"', html_ or '')
    if not m:
        return None
    url = html.unescape(m[1])
    if url.startswith('//'):
        return 'https:' + url
    if url.startswith('/wiki/'):
        return 'https://commons.wikimedia.org' + url
    return url if url.startswith('http') else None


def pedir(url, **params):
    """GET a la API de Wikimedia (o a otra dirección que dé JSON), con espera
    entre peticiones y reintentos si limita (429 o un texto en vez de JSON)."""
    if url.endswith('api.php'):
        params.update(format='json', formatversion=2)
    for n in range(6):
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        try:
            datos = r.json()
        except ValueError:
            datos = None
        if r.status_code == 200 and datos is not None and 'error' not in datos:
            time.sleep(PAUSA)
            return datos
        espera = 60 * (n + 1)
        print(f"  ! {r.status_code} {r.text[:80]!r} — reintento en {espera} s", flush=True)
        time.sleep(espera)
    raise RuntimeError(f'{url}: no responde')


def nombres_de(entidad):
    """Nombre y alias (en castellano, euskera e inglés) de un elemento, en palabras."""
    ns = [v['value'] for v in entidad.get('labels', {}).values()]
    ns += [a['value'] for l in entidad.get('aliases', {}).values() for a in l]
    # «Urrutikoetxea (pilotaria)» -> «Urrutikoetxea»
    return {palabras(re.sub(r'\s*\(.*\)$', '', n)) for n in ns}


def coincide(nombre, entidad):
    """El nombre del catálogo sale como palabras enteras y seguidas en el
    nombre o un alias: 'JAKA' en 'Erik Jaka', pero no 'ALBERDI' en 'Alberdi II'."""
    buscado = palabras(nombre)
    for ns in nombres_de(entidad):
        for i in range(len(ns) - len(buscado) + 1):
            if ns[i:i + len(buscado)] == buscado and not ROMANO.match(''.join(ns[i + len(buscado):i + len(buscado) + 1])):
                return True
    return False


def buscar_elemento(nombre):
    """Q del pelotari de Wikidata cuyo nombre o alias es `nombre`, o None si
    no hay exactamente uno."""
    q = re.sub(r'\.(?=\S)', '. ', nombre)       # 'P.ETXEBERRIA' -> 'P. ETXEBERRIA'
    res = pedir(WIKIDATA, action='query', list='search', srlimit=10,
                srsearch=f'{q} haswbstatement:P641={PELOTA_VASCA}')['query']['search']
    if not res:
        return None
    ents = pedir(WIKIDATA, action='wbgetentities', ids='|'.join(r['title'] for r in res),
                 props='labels|aliases', languages='es|eu|en')['entities']
    buenos = [q for q, e in ents.items() if coincide(nombre, e)]
    return buenos[0] if len(buenos) == 1 else None


def imagen_de(qid):
    """Nombre del fichero de Commons de la propiedad P18 (la primera, la preferida si la hay)."""
    # Special:EntityData está en caché: no cuenta para el límite de la API
    e = next(iter(pedir(f'https://www.wikidata.org/wiki/Special:EntityData/{qid}.json')['entities'].values()))
    claims = [c for c in e.get('claims', {}).get('P18', []) if c.get('rank') != 'deprecated']
    claims.sort(key=lambda c: c.get('rank') != 'preferred')
    for c in claims:
        valor = c['mainsnak'].get('datavalue', {}).get('value')
        if valor:
            return valor
    return None


def foto_de_commons(info):
    """De la respuesta de imageinfo (url + extmetadata) de Commons a la «foto»
    del catálogo, o None si la licencia no es libre."""
    meta = {k: v.get('value') for k, v in (info.get('extmetadata') or {}).items()}
    licencia = texto(meta.get('LicenseShortName'))
    if str(meta.get('NonFree', '')).lower() == 'true' or not licencia_libre(licencia):
        return None
    artista = meta.get('Artist') or ''
    foto = {
        'url': info['thumburl'] if info.get('thumburl') else info['url'],
        'autor': texto(artista) or None,
        'autor_url': primer_enlace(artista),
        'licencia': licencia,
        'licencia_url': meta.get('LicenseUrl') or None,
        'commons': info['descriptionurl'],
    }
    return {k: v for k, v in foto.items() if v}


def foto_de_fichero(fichero):
    pags = pedir(COMMONS, action='query', titles='File:' + fichero, prop='imageinfo',
                 iiprop='url|extmetadata', iiurlwidth=ANCHO,
                 iiextmetadatafilter='Artist|LicenseShortName|LicenseUrl|NonFree')['query']['pages']
    info = (pags[0].get('imageinfo') or [None])[0] if pags else None
    return foto_de_commons(info) if info else None


def main(dry):
    with open(PELOTARIS, encoding='utf-8') as f:
        pelotaris = json.load(f)
    cambios = 0
    for p in pelotaris:
        antes = (p.get('wikidata'), p.get('foto'))
        qid = p.get('wikidata') or buscar_elemento(p['nombre'])
        fichero = imagen_de(qid) if qid else None
        foto = foto_de_fichero(fichero) if fichero else None
        if qid:
            p['wikidata'] = qid
        if foto:
            p['foto'] = foto
        else:
            p.pop('foto', None)
        if fichero and not foto:
            estado = f'{fichero}: licencia no libre, sin foto'
        else:
            estado = f"{foto['licencia']} · {foto.get('autor', '¿autor?')}" if foto else 'sin foto'
        print(f"  {p['nombre']}: {qid or 'no está en Wikidata'} · {estado}", flush=True)
        cambios += (p.get('wikidata'), p.get('foto')) != antes
    con = sum('foto' in p for p in pelotaris)
    print(f"\n{cambios} cambios. Con foto: {con} de {len(pelotaris)}")
    if dry:
        print('[--dry-run] No se ha escrito nada.')
    elif cambios:
        guardar_json(PELOTARIS, pelotaris)


if __name__ == '__main__':
    main('--dry-run' in sys.argv)
