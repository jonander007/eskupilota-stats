"""
La versión de la web tiene que ir a la par en index.html (app.js?v=NN, que
usa comprobarVersion() para detectar que hay una nueva) y en sw.js (caché
'eskupilota-vNN'): si se cambia una y no la otra, los móviles pueden seguir
con los ficheros viejos.

    python -m unittest discover tests
"""

import os
import re
import unittest

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')


def leer(nombre):
    with open(os.path.join(RAIZ, nombre), encoding='utf-8') as f:
        return f.read()


class VersionWeb(unittest.TestCase):
    def test_misma_version_en_index_y_service_worker(self):
        index, sw = leer('index.html'), leer('sw.js')
        versiones = set(re.findall(r'\.(?:css|js)\?v=(\d+)', index))
        self.assertEqual(len(versiones), 1, f'index.html mezcla versiones: {versiones}')
        cache = re.search(r"const CACHE = 'eskupilota-v(\d+)'", sw)
        self.assertIsNotNone(cache)
        self.assertEqual(versiones.pop(), cache[1])


if __name__ == '__main__':
    unittest.main()
