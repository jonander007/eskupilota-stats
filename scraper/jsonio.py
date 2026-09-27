# -*- coding: utf-8 -*-
"""Escritura de los JSON de data/.

Las listas (partidos y catálogos) se guardan con un elemento por línea y
sin espacios: ocupan un 30 % menos que con sangría y los cambios en git
se siguen leyendo partido a partido.
"""

import json
import os


def dumps(data):
    if isinstance(data, list):
        filas = (json.dumps(x, ensure_ascii=False, separators=(',', ':')) for x in data)
        return '[\n' + ',\n'.join(filas) + '\n]\n' if data else '[]\n'
    return json.dumps(data, ensure_ascii=False, indent=2) + '\n'


def guardar_json(path, data):
    carpeta = os.path.dirname(path)
    if carpeta:
        os.makedirs(carpeta, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(dumps(data))
