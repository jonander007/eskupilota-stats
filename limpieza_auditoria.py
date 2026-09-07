#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
limpieza_auditoria.py — Eskupilota Stats

Aplica las correcciones decididas en la auditoria de partidos:

  1. Borra 13 registros de ALBERDI (PEL044) que duplican un partido ya
     registrado a nombre de ALBERDI II (PEL032).
  2. Reasigna 8 partidos de PEL044 a PEL032: ALBERDI estaba retirado
     desde el 04/11/2023, asi que no pudo jugarlos.
  3. Rellena el zaguero IZTUETA (PEL016) en 3 partidos del Masters
     CaixaBank donde el parser lo perdio por la marca de sustitucion.
  4. Unifica dos pelotaris duplicados del catalogo:
         PEL059 MORGAETXEBERRIA        -> PEL022 MORGAETXEBARRIA
         PEL060 MARIEZKURRENA II (LESION) -> PEL011 MARIEZKURRENA II

Los partidos se localizan por su contenido (fecha, jugadores y tantos),
no por posicion, asi que el script sigue siendo valido aunque el scraper
haya anadido partidos nuevos desde que se preparo.

Uso:
    python limpieza_auditoria.py --dry-run
    python limpieza_auditoria.py

Despues hay que ejecutar:
    python recalcular_contadores.py
"""

import json
import os
import sys

DATA_DIR = 'data'
PARTIDOS_FILE = os.path.join(DATA_DIR, 'partidos.json')
PELOTARIS_FILE = os.path.join(DATA_DIR, 'pelotaris.json')

RETIRADA = '2023-11-04'
FUSIONES = {'PEL059': 'PEL022', 'PEL060': 'PEL011'}

BORRAR = [["2024-11-09", "PEL044", "PEL041", 9, "PEL026", "PEL020", 18], ["2024-10-20", "PEL005", "PEL020", 18, "PEL044", "PEL027", 11], ["2024-10-19", "PEL028", "PEL020", 22, "PEL044", "PEL041", 14], ["2024-09-23", "PEL044", "PEL041", 17, "PEL042", "PEL030", 18], ["2024-09-01", "PEL019", "PEL027", 22, "PEL044", "PEL043", 11], ["2024-08-22", "PEL033", "PEL030", 18, "PEL044", "PEL027", 14], ["2024-08-16", "PEL001", "PEL023", 22, "PEL044", "PEL008", 19], ["2024-08-10", "PEL044", "PEL043", 19, "PEL026", "PEL027", 22], ["2024-07-25", "PEL005", "PEL020", 9, "PEL044", "PEL006", 22], ["2024-07-12", "PEL044", "PEL041", 9, "PEL039", "PEL036", 18], ["2024-07-06", "PEL044", "PEL020", 18, "PEL042", "PEL034", 12], ["2024-03-10", "PEL031", "PEL036", 18, "PEL044", "PEL043", 7], ["2024-02-13", "PEL024", "PEL030", 22, "PEL044", "PEL041", 11]]
REASIGNAR = [["2024-11-16", "PEL044", "PEL020", 14, "PEL042", "PEL036", 18], ["2024-08-26", "PEL044", "PEL041", 15, "PEL039", "PEL030", 18], ["2024-07-31", "PEL005", "PEL020", 22, "PEL044", "PEL006", 13], ["2024-05-04", "PEL019", "PEL041", 22, "PEL044", "PEL016", 18], ["2024-04-27", "PEL019", "PEL020", 18, "PEL044", "PEL043", 8], ["2024-04-20", "PEL002", "PEL041", 22, "PEL044", "PEL043", 21], ["2024-04-13", "PEL019", "PEL043", 22, "PEL044", "PEL020", 9], ["2024-01-20", "PEL002", "PEL006", 22, "PEL044", "PEL043", 5]]
ZAGUEROS = [[["2026-08-23", "PEL025", "PEL017", 18, "PEL007", None, 22], "e2", "PEL016"], [["2026-08-02", "PEL007", None, 20, "PEL002", "PEL018", 22], "e1", "PEL016"], [["2026-07-17", "PEL009", "PEL013", 6, "PEL007", None, 22], "e2", "PEL016"]]


def firma(m):
    return [m['fecha'],
            m['equipo1']['del_id'], m['equipo1'].get('zag_id'), m['puntos1'],
            m['equipo2']['del_id'], m['equipo2'].get('zag_id'), m['puntos2']]


def localizar(partidos, objetivo, etiqueta):
    hits = [i for i, m in enumerate(partidos) if firma(m) == objetivo]
    assert hits, f"{etiqueta}: no encuentro el partido {objetivo}"
    assert len(hits) == 1, f"{etiqueta}: {len(hits)} coincidencias para {objetivo}"
    return hits[0]


def main():
    dry = '--dry-run' in sys.argv

    if not os.path.exists(PARTIDOS_FILE):
        print(f"ERROR: no encuentro {PARTIDOS_FILE}. Ejecuta desde la raiz del repo.")
        sys.exit(1)

    with open(PARTIDOS_FILE, encoding='utf-8') as f:
        partidos = json.load(f)
    with open(PELOTARIS_FILE, encoding='utf-8') as f:
        pelotaris = json.load(f)

    n0 = len(partidos)
    print(f"Partidos de partida: {n0}")

    idx_borrar = set()
    for objetivo in BORRAR:
        i = localizar(partidos, objetivo, 'borrar')
        assert partidos[i]['fecha'] > RETIRADA, f"#{i} es anterior a la retirada"
        idx_borrar.add(i)
    assert len(idx_borrar) == len(BORRAR), "Alguna firma de borrado apunta al mismo partido"

    for objetivo in REASIGNAR:
        i = localizar(partidos, objetivo, 'reasignar')
        m = partidos[i]
        assert m['fecha'] > RETIRADA, f"#{i} es anterior a la retirada"
        assert i not in idx_borrar, f"#{i} esta a la vez en borrar y reasignar"
        for eq in ('equipo1', 'equipo2'):
            for k, v in m[eq].items():
                if v == 'PEL044':
                    m[eq][k] = 'PEL032'

    for objetivo, slot, zaguero in ZAGUEROS:
        i = localizar(partidos, objetivo, 'zaguero')
        eq = 'equipo1' if slot == 'e1' else 'equipo2'
        assert partidos[i][eq]['zag_id'] is None, f"#{i} ya tiene zaguero en {eq}"
        partidos[i][eq]['zag_id'] = zaguero

    partidos = [m for i, m in enumerate(partidos) if i not in idx_borrar]

    fusionados = 0
    for m in partidos:
        for eq in ('equipo1', 'equipo2'):
            for k, v in m[eq].items():
                if v in FUSIONES:
                    m[eq][k] = FUSIONES[v]
                    fusionados += 1

    for m in partidos:
        if m['fecha'] > RETIRADA:
            for eq in ('equipo1', 'equipo2'):
                assert 'PEL044' not in m[eq].values(), \
                    f"{m['fecha']} sigue con ALBERDI despues de su retirada"
    texto = json.dumps(partidos)
    for viejo in FUSIONES:
        assert viejo not in texto, f"{viejo} sigue apareciendo en partidos"

    pelotaris = [p for p in pelotaris if p['id'] not in FUSIONES]

    print(f"  {len(idx_borrar)} duplicados borrados")
    print(f"  {len(REASIGNAR)} partidos reasignados a ALBERDI II")
    print(f"  {len(ZAGUEROS)} zagueros rellenados con PEL016 IZTUETA")
    print(f"  {fusionados} referencias fusionadas, 2 pelotaris fuera del catalogo")
    print(f"Partidos resultantes: {len(partidos)} (antes {n0})")

    if dry:
        print("\n[--dry-run] No se ha escrito nada.")
        return

    with open(PARTIDOS_FILE, 'w', encoding='utf-8') as f:
        json.dump(partidos, f, ensure_ascii=False, indent=2)
    with open(PELOTARIS_FILE, 'w', encoding='utf-8') as f:
        json.dump(pelotaris, f, ensure_ascii=False, indent=2)

    print("\n\u2713 partidos.json y pelotaris.json actualizados.")
    print("  Siguiente paso: python recalcular_contadores.py")


if __name__ == '__main__':
    main()
