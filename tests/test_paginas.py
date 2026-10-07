# -*- coding: utf-8 -*-
"""Las páginas estáticas (pelotaris, competiciones, frontones) no tienen enlaces rotos."""

import glob
import os
import re
import unittest

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
CARPETAS = ('pelotari', 'competicion', 'fronton', 'privacidad')


class PaginasEstaticas(unittest.TestCase):

    def paginas(self):
        for c in CARPETAS:
            for pre in ('', 'eu/'):
                yield from glob.glob(os.path.join(RAIZ, pre + c, '**', 'index.html'), recursive=True)

    def test_enlaces_internos_existen(self):
        rotos = set()
        n = 0
        for ruta in self.paginas():
            n += 1
            with open(ruta, encoding='utf-8') as f:
                for href in re.findall(r'href="(/(?:eu/)?(?:%s)/[^"#?]*)"' % '|'.join(CARPETAS), f.read()):
                    if not os.path.isfile(os.path.join(RAIZ, href.lstrip('/'), 'index.html')):
                        rotos.add(href)
        self.assertGreater(n, 100)
        self.assertEqual(sorted(rotos), [])

    def test_sitemap_lista_las_paginas(self):
        with open(os.path.join(RAIZ, 'sitemap.xml'), encoding='utf-8') as f:
            locs = set(re.findall(r'<loc>https://www\.eskupilotastats\.com/([^<]*)</loc>', f.read()))
        for ruta in self.paginas():
            rel = os.path.relpath(os.path.dirname(ruta), RAIZ).replace(os.sep, '/') + '/'
            self.assertIn(rel, locs)


if __name__ == '__main__':
    unittest.main()
