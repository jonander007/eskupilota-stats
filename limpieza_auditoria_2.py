#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
limpieza_auditoria_2.py — Eskupilota Stats

Segunda ronda de la auditoria de partidos. Aplica ocho decisiones:

  1. Borra el partido del 15/09/2024 en Burunda donde JAKA aparecia en los
     dos equipos. El bueno es el gemelo de Alsasua (Masters CaixaBank).
     Ademas fusiona el fronton ALSASUA en BURUNDA: son el mismo, en Alsasua.
  2. Borra el duplicado del 26/08/2024 en Elgoibar con ALBISU repetido.
  3. Borra el duplicado del 29/03/2024 en Labastida con ESKIROZ repetido.
  4. El 24-25 de Santa Maria de las Hoyas se queda: festival a 25.
  5. Los tres festivales a 16 tantos se quedan: son correctos.
  6. Unifica el 4 y Medio bajo un solo criterio con Serie A / Serie B y anio,
     fusionando los cajones Eusko Label 2024 en los de Serie A y Serie B.
     Los torneos de San Fermin de 4 y medio se renombran igual.
  7. Renombra el Masters Caixabank 2026 a Serie A 2026 y corrige el tipo de
     los 39 partidos del Masters 2026: campeonato-a y campeonato-b.
  8. Colapsa las competiciones de festival en una sola llamada "Festival".
     Se salvan las dos despedidas de pelotari, que si aportan informacion.

Los partidos se localizan por contenido, no por posicion.

Uso:
    python limpieza_auditoria_2.py --dry-run
    python limpieza_auditoria_2.py

Despues:
    python recalcular_contadores.py
"""

import json
import os
import sys

DATA_DIR = 'data'
PARTIDOS_FILE = os.path.join(DATA_DIR, 'partidos.json')
FRONTONES_FILE = os.path.join(DATA_DIR, 'frontones.json')
COMPETICIONES_FILE = os.path.join(DATA_DIR, 'competiciones.json')

# --- 1. Partidos a borrar (pelotari repetido en los dos equipos) -------
BORRAR = [
    ["2024-09-15", "PEL001", "PEL017", 17, "PEL001", "PEL018", 22],
    ["2024-08-26", "PEL001", "PEL008", 22, "PEL003", "PEL008", 20],
    ["2024-03-29", "PEL025", "PEL006", 16, "PEL015", "PEL006", 22],
]

# --- 1b. Frontones a fusionar: {viejo: nuevo} -------------------------
FUSION_FRONTONES = {'FRO008': 'FRO058'}          # ALSASUA -> BURUNDA

# --- 6 y 7. Competiciones a fusionar: {viejo: nuevo} ------------------
FUSION_COMPETICIONES = {
    'COMP008': 'COMP001',   # 4 1/2 Eusko Label 2024        -> Serie A 2024
    'COMP009': 'COMP003',   # 4 1/2 Serie B Eusko Label 2024 -> Serie B 2024
}

# --- 6 y 7. Renombrados ------------------------------------------------
RENOMBRAR = {
    'COMP005': 'Campeonato 4 y Medio Serie A 2022',
    'COMP006': 'Campeonato 4 y Medio Serie A 2023',
    'COMP007': 'Campeonato 4 y Medio Serie B 2023',
    'COMP001': 'Campeonato 4 y Medio Serie A 2024',
    'COMP003': 'Campeonato 4 y Medio Serie B 2024',
    'COMP002': 'Campeonato 4 y Medio Serie A 2025',
    'COMP004': 'Campeonato 4 y Medio Serie B 2025',
    'COMP229': 'Torneo San Fermin 4 y Medio 2023',
    'COMP213': 'Torneo San Fermin 4 y Medio 2024',
    'COMP214': 'Torneo San Fermin 4 y Medio 2025',
    'COMP235': 'Torneo San Fermin 4 y Medio 2026',
    'COMP237': 'Masters CaixaBank Serie A 2026',
}

# --- 7. Tipos a corregir: {competicion: tipo} -------------------------
TIPO_POR_COMPETICION = {
    'COMP237': 'campeonato-a',   # Masters CaixaBank Serie A 2026
    'COMP236': 'campeonato-b',   # Masters CaixaBank Serie B 2026
}

# --- 8. Colapso de festivales -----------------------------------------
FESTIVAL_ID = 'COMP036'          # se reutiliza el id mas bajo
FESTIVAL_NOMBRE = 'Festival'
FESTIVAL_EXCEPCIONES = {'COMP086', 'COMP087'}   # las dos despedidas


def firma(m):
    return [m['fecha'],
            m['equipo1']['del_id'], m['equipo1'].get('zag_id'), m['puntos1'],
            m['equipo2']['del_id'], m['equipo2'].get('zag_id'), m['puntos2']]


def localizar(partidos, objetivo):
    hits = [i for i, m in enumerate(partidos) if firma(m) == objetivo]
    assert hits, f"no encuentro el partido {objetivo}"
    assert len(hits) == 1, f"{len(hits)} coincidencias para {objetivo}"
    return hits[0]


def main():
    dry = '--dry-run' in sys.argv

    if not os.path.exists(PARTIDOS_FILE):
        print(f"ERROR: no encuentro {PARTIDOS_FILE}. Ejecuta desde la raiz del repo.")
        sys.exit(1)

    with open(PARTIDOS_FILE, encoding='utf-8') as f:
        partidos = json.load(f)
    with open(FRONTONES_FILE, encoding='utf-8') as f:
        frontones = json.load(f)
    with open(COMPETICIONES_FILE, encoding='utf-8') as f:
        competiciones = json.load(f)

    n0, nf0, nc0 = len(partidos), len(frontones), len(competiciones)
    print(f"Partida: {n0} partidos, {nf0} frontones, {nc0} competiciones")

    ids_comp = {c['id'] for c in competiciones}
    ids_fro = {f['id'] for f in frontones}
    for viejo, nuevo in list(FUSION_COMPETICIONES.items()) + list(RENOMBRAR.items())[:0]:
        assert viejo in ids_comp and nuevo in ids_comp, f"{viejo}/{nuevo} no existen"
    for cid in list(RENOMBRAR) + list(TIPO_POR_COMPETICION) + [FESTIVAL_ID]:
        assert cid in ids_comp, f"{cid} no existe"
    for viejo, nuevo in FUSION_FRONTONES.items():
        assert viejo in ids_fro and nuevo in ids_fro, f"{viejo}/{nuevo} no existen"

    # 1. Borrados
    fuera = set()
    for objetivo in BORRAR:
        i = localizar(partidos, objetivo)
        eq1 = {partidos[i]['equipo1']['del_id'], partidos[i]['equipo1'].get('zag_id')}
        eq2 = {partidos[i]['equipo2']['del_id'], partidos[i]['equipo2'].get('zag_id')}
        assert eq1 & eq2, f"#{i} no tiene ningun pelotari repetido, revisa la firma"
        fuera.add(i)
    partidos = [m for i, m in enumerate(partidos) if i not in fuera]
    print(f"  {len(fuera)} partidos borrados (pelotari en los dos equipos)")

    # 1b. Fusion de frontones
    nfro = 0
    for m in partidos:
        if m['fronton_id'] in FUSION_FRONTONES:
            m['fronton_id'] = FUSION_FRONTONES[m['fronton_id']]
            nfro += 1
    frontones = [f for f in frontones if f['id'] not in FUSION_FRONTONES]
    print(f"  {nfro} partidos movidos de fronton, {len(FUSION_FRONTONES)} fronton(es) fuera")

    # 6/7. Fusion de competiciones
    ncmp = 0
    for m in partidos:
        if m['competicion_id'] in FUSION_COMPETICIONES:
            m['competicion_id'] = FUSION_COMPETICIONES[m['competicion_id']]
            ncmp += 1
    competiciones = [c for c in competiciones if c['id'] not in FUSION_COMPETICIONES]
    print(f"  {ncmp} partidos movidos al 4 y Medio unificado")

    # 8. Colapso de festivales
    a_colapsar = {c['id'] for c in competiciones
                  if c['nombre'].startswith('Festival')
                  and c['id'] not in FESTIVAL_EXCEPCIONES
                  and c['id'] != FESTIVAL_ID}
    nfest = 0
    for m in partidos:
        if m['competicion_id'] in a_colapsar:
            m['competicion_id'] = FESTIVAL_ID
            nfest += 1
    competiciones = [c for c in competiciones if c['id'] not in a_colapsar]
    for c in competiciones:
        if c['id'] == FESTIVAL_ID:
            c['nombre'] = FESTIVAL_NOMBRE
            c['tipo'] = 'festival'
    print(f"  {nfest} partidos colapsados en '{FESTIVAL_NOMBRE}', "
          f"{len(a_colapsar)} competiciones fuera")

    # 6/7. Renombrados
    for c in competiciones:
        if c['id'] in RENOMBRAR:
            c['nombre'] = RENOMBRAR[c['id']]
    print(f"  {len(RENOMBRAR)} competiciones renombradas")

    # 7. Tipos
    ntipo = 0
    for m in partidos:
        nuevo = TIPO_POR_COMPETICION.get(m['competicion_id'])
        if nuevo and m['tipo'] != nuevo:
            m['tipo'] = nuevo
            ntipo += 1
    print(f"  {ntipo} tipos corregidos en el Masters 2026")

    # --- Comprobaciones finales --------------------------------------
    ids_comp = {c['id'] for c in competiciones}
    ids_fro = {f['id'] for f in frontones}
    for m in partidos:
        assert m['competicion_id'] in ids_comp, f"competicion huerfana {m['competicion_id']}"
        assert m['fronton_id'] in ids_fro, f"fronton huerfano {m['fronton_id']}"
    for m in partidos:
        eq1 = {m['equipo1']['del_id'], m['equipo1'].get('zag_id')} - {None}
        eq2 = {m['equipo2']['del_id'], m['equipo2'].get('zag_id')} - {None}
        assert not (eq1 & eq2), f"{m['fecha']} sigue con un pelotari en los dos equipos"
    nombres = [c['nombre'] for c in competiciones]
    assert len(nombres) == len(set(nombres)), "hay nombres de competicion repetidos"
    assert sum(1 for c in competiciones if c['nombre'].startswith('Festival')) == 3, \
        "deberian quedar solo Festival y las dos despedidas"

    print(f"\nResultado: {len(partidos)} partidos (antes {n0}), "
          f"{len(frontones)} frontones (antes {nf0}), "
          f"{len(competiciones)} competiciones (antes {nc0})")

    if dry:
        print("\n[--dry-run] No se ha escrito nada.")
        return

    with open(PARTIDOS_FILE, 'w', encoding='utf-8') as f:
        json.dump(partidos, f, ensure_ascii=False, indent=2)
    with open(FRONTONES_FILE, 'w', encoding='utf-8') as f:
        json.dump(frontones, f, ensure_ascii=False, indent=2)
    with open(COMPETICIONES_FILE, 'w', encoding='utf-8') as f:
        json.dump(competiciones, f, ensure_ascii=False, indent=2)

    print("\n\u2713 partidos.json, frontones.json y competiciones.json actualizados.")
    print("  Siguiente paso: python recalcular_contadores.py")


if __name__ == '__main__':
    main()
