#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
recalcular_contadores.py — Eskupilota Stats

Recalcula `partidos_count` en los catálogos a partir de la verdad:
data/partidos.json. Los contadores dejan de ser acumulados y pasan a ser
derivados, por lo que es idempotente: ejecutarlo N veces da el mismo resultado.

Criterio de conteo (el mismo de migrar_a_catalogos.py):
  - pelotaris:     1 aparición en un partido = 1 (como delantero o zaguero)
  - frontones:     1 partido disputado ahí   = 1
  - competiciones: 1 partido de esa comp.    = 1

Uso:
    python recalcular_contadores.py            # aplica los cambios
    python recalcular_contadores.py --dry-run  # solo muestra el informe
"""

import json
import os
import sys
from collections import Counter

DATA_DIR = 'data'
PARTIDOS_FILE = os.path.join(DATA_DIR, 'partidos.json')
PELOTARIS_FILE = os.path.join(DATA_DIR, 'pelotaris.json')
FRONTONES_FILE = os.path.join(DATA_DIR, 'frontones.json')
COMPETICIONES_FILE = os.path.join(DATA_DIR, 'competiciones.json')


def cargar(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def guardar(path, items):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def contar(partidos):
    """Devuelve (contador_pelotaris, contador_frontones, contador_competiciones)."""
    pel, fro, cmp_ = Counter(), Counter(), Counter()
    for p in partidos:
        if p.get('fronton_id'):
            fro[p['fronton_id']] += 1
        if p.get('competicion_id'):
            cmp_[p['competicion_id']] += 1
        for equipo in ('equipo1', 'equipo2'):
            e = p.get(equipo) or {}
            for rol in ('del_id', 'zag_id'):
                pid = e.get(rol)
                if pid:
                    pel[pid] += 1
    return pel, fro, cmp_


def aplicar(items, reales, etiqueta):
    """Ajusta partidos_count y devuelve la lista de cambios."""
    cambios = []
    for it in items:
        antes = it.get('partidos_count', 0)
        ahora = reales.get(it['id'], 0)
        if antes != ahora:
            cambios.append((it['id'], it['nombre'], antes, ahora))
            it['partidos_count'] = ahora
        else:
            it['partidos_count'] = ahora
    if cambios:
        print(f"\n{etiqueta}: {len(cambios)}/{len(items)} descuadrados")
        for cid, nombre, antes, ahora in sorted(cambios, key=lambda c: -(c[2] - c[3]))[:15]:
            delta = antes - ahora
            print(f"  {cid}  {nombre[:38]:40} {antes:6} → {ahora:5}   (-{delta})")
        if len(cambios) > 15:
            print(f"  … y {len(cambios) - 15} más")
    else:
        print(f"\n{etiqueta}: todo correcto, sin cambios")
    return cambios


def main():
    dry = '--dry-run' in sys.argv

    if not os.path.exists(PARTIDOS_FILE):
        print(f"ERROR: no encuentro {PARTIDOS_FILE}. Ejecuta desde la raíz del repo.")
        sys.exit(1)

    partidos = cargar(PARTIDOS_FILE)
    pelotaris = cargar(PELOTARIS_FILE)
    frontones = cargar(FRONTONES_FILE)
    competiciones = cargar(COMPETICIONES_FILE)

    print(f"Partidos: {len(partidos)}")
    print(f"Catálogos: {len(pelotaris)} pelotaris, {len(frontones)} frontones, "
          f"{len(competiciones)} competiciones")

    # ── Guardas de seguridad ─────────────────────────────────────────
    ids_pel = {p['id'] for p in pelotaris}
    ids_fro = {f['id'] for f in frontones}
    ids_cmp = {c['id'] for c in competiciones}

    c_pel, c_fro, c_cmp = contar(partidos)

    huerfanos = ([f"pelotari {i}" for i in c_pel if i not in ids_pel] +
                 [f"frontón {i}" for i in c_fro if i not in ids_fro] +
                 [f"competición {i}" for i in c_cmp if i not in ids_cmp])
    assert not huerfanos, f"IDs en partidos.json que no existen en catálogo: {huerfanos[:10]}"

    total_slots = sum(c_pel.values())
    assert sum(c_fro.values()) == len(partidos), "Hay partidos sin fronton_id"
    assert sum(c_cmp.values()) == len(partidos), "Hay partidos sin competicion_id"
    assert total_slots <= len(partidos) * 4, "Más apariciones de pelotari de las posibles"

    print(f"\nApariciones reales de pelotari: {total_slots} "
          f"(suma actual en catálogo: {sum(p.get('partidos_count', 0) for p in pelotaris)})")

    cambios = 0
    cambios += len(aplicar(pelotaris, c_pel, 'PELOTARIS'))
    cambios += len(aplicar(frontones, c_fro, 'FRONTONES'))
    cambios += len(aplicar(competiciones, c_cmp, 'COMPETICIONES'))

    # ── Reordenado (mismo criterio que scraper.save_all) ─────────────
    pelotaris.sort(key=lambda p: (-p['partidos_count'], p['nombre']))
    frontones.sort(key=lambda f: f['nombre'])
    competiciones.sort(key=lambda c: c['nombre'])

    if dry:
        print(f"\n[--dry-run] {cambios} entradas cambiarían. No se ha escrito nada.")
        return

    guardar(PELOTARIS_FILE, pelotaris)
    guardar(FRONTONES_FILE, frontones)
    guardar(COMPETICIONES_FILE, competiciones)
    print(f"\n✓ {cambios} contadores corregidos. Catálogos reescritos y reordenados.")
    print("  partidos.json NO se ha tocado.")


if __name__ == '__main__':
    main()
