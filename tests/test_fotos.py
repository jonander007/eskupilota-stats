# -*- coding: utf-8 -*-
"""Fotos de Wikimedia Commons (tools/fotos_wikidata.py), sin red: solo
licencias libres, autor y enlaces del crédito, y la página estática."""

import os
import sys
import unittest

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, os.path.join(RAIZ, 'tools'))
from fotos_wikidata import coincide, foto_de_commons, licencia_libre  # noqa: E402
from generar_paginas import html_foto  # noqa: E402


def imageinfo(licencia, artista='<a href="//commons.wikimedia.org/wiki/User:Ane" title="User:Ane">Ane</a>', **extra):
    meta = {'LicenseShortName': {'value': licencia}, 'Artist': {'value': artista},
            'LicenseUrl': {'value': 'https://creativecommons.org/licenses/by-sa/4.0'}}
    meta.update({k: {'value': v} for k, v in extra.items()})
    return {'url': 'https://upload.wikimedia.org/wikipedia/commons/a/ab/Jaka.jpg',
            'thumburl': 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Jaka.jpg/400px-Jaka.jpg',
            'descriptionurl': 'https://commons.wikimedia.org/wiki/File:Jaka.jpg', 'extmetadata': meta}


class Licencias(unittest.TestCase):
    def test_libres(self):
        for lic in ('CC BY-SA 4.0', 'CC BY 2.0', 'CC BY-SA 3.0 es', 'CC0', 'Public domain', 'PD-self', 'GFDL'):
            self.assertTrue(licencia_libre(lic), lic)

    def test_no_libres(self):
        for lic in ('CC BY-NC 4.0', 'CC BY-NC-SA 2.0', 'CC BY-ND 3.0', 'Fair use', '', None):
            self.assertFalse(licencia_libre(lic), lic)


class FotoDeCommons(unittest.TestCase):
    def test_miniatura_autor_y_licencia(self):
        f = foto_de_commons(imageinfo('CC BY-SA 4.0'))
        self.assertEqual(f, {
            'url': 'https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Jaka.jpg/400px-Jaka.jpg',
            'autor': 'Ane', 'autor_url': 'https://commons.wikimedia.org/wiki/User:Ane',
            'licencia': 'CC BY-SA 4.0', 'licencia_url': 'https://creativecommons.org/licenses/by-sa/4.0',
            'commons': 'https://commons.wikimedia.org/wiki/File:Jaka.jpg'})

    def test_autor_sin_enlace(self):
        f = foto_de_commons(imageinfo('CC0', artista='Ane &amp; Jon'))
        self.assertEqual(f['autor'], 'Ane & Jon')
        self.assertNotIn('autor_url', f)

    def test_obra_derivada(self):
        f = foto_de_commons(imageinfo('CC BY 3.0', artista='<ul><li>original work: Guaixe</li>\n<li>derivative work: '
                                      '<a href="/wiki/User:Xabier_Armendaritz">Xabier Armendaritz</a></li></ul>'))
        self.assertEqual(f['autor'], 'original work: Guaixe; derivative work: Xabier Armendaritz')
        self.assertEqual(f['autor_url'], 'https://commons.wikimedia.org/wiki/User:Xabier_Armendaritz')

    def test_licencia_no_libre_sin_foto(self):
        self.assertIsNone(foto_de_commons(imageinfo('CC BY-NC 4.0')))
        self.assertIsNone(foto_de_commons(imageinfo('CC BY-SA 4.0', NonFree='true')))


class Nombres(unittest.TestCase):
    def test_nombre_dentro_del_de_wikidata(self):
        e = {'labels': {'eu': {'value': 'Mikel Urrutikoetxea'}},
             'aliases': {'eu': [{'value': 'Urrutikoetxea (pilotaria)'}, {'value': 'Peña II'}]}}
        self.assertTrue(coincide('URRUTIKOETXEA', e))
        self.assertTrue(coincide('PEÑA II', e))
        self.assertFalse(coincide('PEÑA', e))           # es Peña II
        self.assertFalse(coincide('URRUTI', e))         # palabras enteras
        self.assertTrue(coincide('P.ETXEBERRIA', {'labels': {'es': {'value': 'P. Etxeberria'}}}))


class PaginaEstatica(unittest.TestCase):
    def test_credito_con_enlaces(self):
        f = foto_de_commons(imageinfo('CC BY-SA 4.0'))
        h = html_foto('JAKA', f, 'es')
        self.assertIn('<img src="https://upload.wikimedia.org/', h)
        self.assertIn('Foto: <a href="https://commons.wikimedia.org/wiki/User:Ane" rel="noopener">Ane</a> · '
                      '<a href="https://creativecommons.org/licenses/by-sa/4.0" rel="noopener">CC BY-SA 4.0</a> · '
                      '<a href="https://commons.wikimedia.org/wiki/File:Jaka.jpg" rel="noopener">Wikimedia Commons</a>', h)
        self.assertIn('Argazkia: ', html_foto('JAKA', f, 'eu'))

    def test_sin_foto_la_inicial(self):
        h = html_foto('JAKA', None, 'es')
        self.assertIn('>J</span>', h)
        self.assertNotIn('<img', h)


    def test_paginas_generadas(self):
        """Cada página de pelotari lleva su foto con el crédito, o la inicial."""
        import json
        from generar_paginas import slugify
        with open(os.path.join(RAIZ, 'data', 'pelotaris.json'), encoding='utf-8') as f:
            pelotaris = json.load(f)
        for p in pelotaris:
            ruta = os.path.join(RAIZ, 'pelotari', slugify(p['nombre']), 'index.html')
            if not os.path.isfile(ruta):
                continue
            with open(ruta, encoding='utf-8') as f:
                h = f.read()
            if p.get('foto'):
                self.assertIn(p['foto']['url'].replace('&', '&amp;'), h, p['nombre'])
                self.assertIn('Wikimedia Commons</a></figcaption>', h, p['nombre'])
            else:
                self.assertIn('<figure class="foto sin">', h, p['nombre'])


if __name__ == '__main__':
    unittest.main()
