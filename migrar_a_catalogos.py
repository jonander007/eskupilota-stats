"""
Eskupilota Stats — Migración a catálogos maestros
Lee un partidos.json "plano" (con nombres literales) y genera 5 archivos
normalizados con IDs: pelotaris, ciudades, frontones, competiciones, partidos.

Uso:
    python migrar_a_catalogos.py  <partidos_origen.json>  <carpeta_destino>

Ejemplo:
    python migrar_a_catalogos.py partidos.json data/
"""
import json
import sys
import os
from collections import defaultdict
from datetime import datetime


# ─── Traducciones ES ↔ EU conocidas ──────────────────────────────
TRADUCCIONES_CIUDAD = {
    'PAMPLONA':          {'es': 'Pamplona',     'eu': 'Iruñea'},
    'VITORIA-GASTEIZ':   {'es': 'Vitoria',      'eu': 'Gasteiz'},
    'BILBAO':            {'es': 'Bilbao',       'eu': 'Bilbo'},
    'DONOSTIA':          {'es': 'San Sebastián','eu': 'Donostia'},
    'SAN SEBASTIAN':     {'es': 'San Sebastián','eu': 'Donostia'},
    'ALTSASU':           {'es': 'Alsasua',      'eu': 'Altsasu'},
    'LIZARRA':           {'es': 'Estella',      'eu': 'Lizarra'},
    'AMOREBIETA-ETXANO': {'es': 'Amorebieta',   'eu': 'Amorebieta-Etxano'},
    'HENDAIA':           {'es': 'Hendaya',      'eu': 'Hendaia'},
    'OIARTZUN':          {'es': 'Oyarzun',      'eu': 'Oiartzun'},
    'BERGARA':           {'es': 'Vergara',      'eu': 'Bergara'},
    'ARRASATE':          {'es': 'Mondragón',    'eu': 'Arrasate'},
    'LLODIO':            {'es': 'Llodio',       'eu': 'Laudio'},
}


def migrar(origen_path, destino_dir):
    with open(origen_path, encoding='utf-8') as f:
        data = json.load(f)
    print(f"Partidos leídos: {len(data)}")

    # ─── Pelotaris ──────────────────────────────────────────────
    pelotari_partidos = defaultdict(int)
    pelotari_roles = defaultdict(lambda: {'del': 0, 'zag': 0})
    for p in data:
        for eq in [p['equipo1'], p['equipo2']]:
            d = eq.get('delantero'); z = eq.get('zaguero')
            if d:
                pelotari_partidos[d] += 1
                pelotari_roles[d]['del'] += 1
            if z:
                pelotari_partidos[z] += 1
                pelotari_roles[z]['zag'] += 1

    sorted_pels = sorted(pelotari_partidos.keys(), key=lambda x: (-pelotari_partidos[x], x))
    pelotaris = {}
    nombre_to_pid = {}
    for i, nombre in enumerate(sorted_pels, 1):
        pid = f"PEL{i:03d}"
        roles = pelotari_roles[nombre]
        if roles['del'] > roles['zag'] * 1.5:
            rol = 'delantero'
        elif roles['zag'] > roles['del'] * 1.5:
            rol = 'zaguero'
        else:
            rol = 'mixto'
        pelotaris[pid] = {
            'id': pid, 'nombre': nombre,
            'nombre_es': nombre, 'nombre_eu': nombre,
            'rol': rol, 'partidos_count': pelotari_partidos[nombre]
        }
        nombre_to_pid[nombre] = pid

    # ─── Ciudades ───────────────────────────────────────────────
    ciudades_raw = set()
    for p in data:
        if p.get('ciudad'): ciudades_raw.add(p['ciudad'])

    ciudades = {}
    ciudad_to_cid = {}
    for i, c in enumerate(sorted(ciudades_raw), 1):
        cid = f"CIU{i:03d}"
        trad = TRADUCCIONES_CIUDAD.get(c, {'es': c.title(), 'eu': c.title()})
        ciudades[cid] = {
            'id': cid, 'nombre': c,
            'nombre_es': trad['es'], 'nombre_eu': trad['eu']
        }
        ciudad_to_cid[c] = cid

    # ─── Frontones ──────────────────────────────────────────────
    fronton_ciudad = {}
    fronton_count = defaultdict(int)
    for p in data:
        f = p.get('fronton'); c = p.get('ciudad')
        if f:
            fronton_count[f] += 1
            if c: fronton_ciudad[f] = c

    frontones = {}
    fronton_to_fid = {}
    for i, (nombre, ciudad) in enumerate(sorted(fronton_ciudad.items()), 1):
        fid = f"FRO{i:03d}"
        frontones[fid] = {
            'id': fid, 'nombre': nombre,
            'ciudad_id': ciudad_to_cid.get(ciudad),
            'partidos_count': fronton_count[nombre]
        }
        fronton_to_fid[nombre] = fid

    # ─── Competiciones ──────────────────────────────────────────
    comp_count = defaultdict(int); comp_tipo = {}
    for p in data:
        c = p.get('competicion')
        if c:
            comp_count[c] += 1
            comp_tipo[c] = p.get('tipo')

    competiciones = {}
    comp_to_cid = {}
    for i, nombre in enumerate(sorted(comp_count.keys()), 1):
        cid = f"COMP{i:03d}"
        competiciones[cid] = {
            'id': cid, 'nombre': nombre,
            'tipo': comp_tipo[nombre],
            'partidos_count': comp_count[nombre]
        }
        comp_to_cid[nombre] = cid

    # ─── Partidos migrados ──────────────────────────────────────
    partidos_mig = []
    for p in data:
        fecha_iso = datetime.strptime(p['fecha'], '%d/%m/%Y').strftime('%Y-%m-%d')
        partidos_mig.append({
            'fecha': fecha_iso,
            'fronton_id': fronton_to_fid.get(p.get('fronton')),
            'competicion_id': comp_to_cid.get(p.get('competicion')),
            'tipo': p.get('tipo'),
            'equipo1': {
                'del_id': nombre_to_pid.get(p['equipo1'].get('delantero')),
                'zag_id': nombre_to_pid.get(p['equipo1'].get('zaguero'))
            },
            'puntos1': p.get('puntos1'),
            'equipo2': {
                'del_id': nombre_to_pid.get(p['equipo2'].get('delantero')),
                'zag_id': nombre_to_pid.get(p['equipo2'].get('zaguero'))
            },
            'puntos2': p.get('puntos2'),
            'ganador': p.get('ganador')
        })

    # ─── Guardar ────────────────────────────────────────────────
    os.makedirs(destino_dir, exist_ok=True)
    def save(path, obj):
        full = os.path.join(destino_dir, path)
        with open(full, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        print(f"  {path}: {len(obj)} registros")

    save('pelotaris.json',    list(pelotaris.values()))
    save('ciudades.json',     list(ciudades.values()))
    save('frontones.json',    list(frontones.values()))
    save('competiciones.json',list(competiciones.values()))
    save('partidos.json',     partidos_mig)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    migrar(sys.argv[1], sys.argv[2])
