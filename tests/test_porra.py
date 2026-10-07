# -*- coding: utf-8 -*-
"""tools/porra.py: partidos de la cartelera, resultados y anulaciones."""

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tools'))
import porra  # noqa: E402

CARTELERA = {'partidos': [
    {'fecha': '09/10/2026', 'hora': '18:00', 'fronton': 'Labrit', 'partidos': [
        {'eq1': ['AGIRRE'], 'eq2': ['ZUBIZARRETA III'], 'modalidad': 'cuatro', 'competicion': 'Campeonato 4 y Medio Serie B 2026'},
        {'eq1': ['P.ETXEBERRIA', 'MARIEZKURRENA II'], 'eq2': ['?', 'ALBISU'], 'modalidad': 'parejas'},
    ]},
    {'fecha': '10/10/2026', 'fronton': 'Ogueta', 'partidos': [{'eq1': ['A'], 'eq2': ['B']}]},   # sin hora
]}
PELOTARIS = [{'id': 'P1', 'nombre': 'P. Etxeberria'}, {'id': 'P2', 'nombre': 'MARIEZKURRENA II'},
             {'id': 'P3', 'nombre': 'LASO'}, {'id': 'P4', 'nombre': 'ALBISU'}]


class Porra(unittest.TestCase):

    def test_cartelera(self):
        ps = porra.partidos_cartelera(CARTELERA)
        self.assertEqual([p['id'] for p in ps], ['2026-10-09_agirre_vs_zubizarreta-iii'])
        # 18:00 en Madrid (horario de verano) son las 16:00 UTC
        self.assertEqual(ps[0]['inicio'], '2026-10-09T16:00:00+00:00')
        self.assertEqual(ps[0]['estado'], 'abierto')

    def test_resultado_en_cualquier_orden(self):
        partidos = [{'fecha': '2026-10-09', 'puntos1': 22, 'puntos2': 15,
                     'equipo1': {'del_id': 'P3'}, 'equipo2': {'del_id': 'P4'}},
                    {'fecha': '2026-10-09', 'puntos1': 18, 'puntos2': 22,
                     'equipo1': {'del_id': 'P1', 'zag_id': 'P2'}, 'equipo2': {'del_id': 'P3', 'zag_id': 'P4'}}]
        idx = porra.indice_resultados(partidos, PELOTARIS)
        self.assertEqual(porra.resultado({'id': '2026-10-09_albisu_vs_laso', 'eq1': ['ALBISU'], 'eq2': ['LASO']}, idx), (15, 22))
        self.assertEqual(porra.resultado({'id': '2026-10-09_x', 'eq1': ['LASO', 'ALBISU'],
                                          'eq2': ['P.ETXEBERRIA', 'MARIEZKURRENA II']}, idx), (22, 18))
        self.assertIsNone(porra.resultado({'id': '2026-10-10_x', 'eq1': ['LASO'], 'eq2': ['ALBISU']}, idx))

    def test_decidir(self):
        ahora = datetime(2026, 10, 9, 20, 0, tzinfo=timezone.utc)
        h = lambda horas: (ahora + timedelta(hours=horas)).isoformat()
        abiertos = [
            {'id': '2026-10-09_laso_vs_albisu', 'inicio': h(-3), 'fronton': 'Labrit', 'eq1': ['LASO'], 'eq2': ['ALBISU']},
            {'id': '2026-10-09_sin_resultado', 'inicio': h(-3), 'fronton': 'Labrit', 'eq1': ['A'], 'eq2': ['B']},
            {'id': '2026-10-05_viejo', 'inicio': h(-24 * 4), 'fronton': 'Labrit', 'eq1': ['A'], 'eq2': ['B']},
            {'id': '2026-10-11_cambiado', 'inicio': h(40), 'fronton': 'Ogueta', 'eq1': ['A'], 'eq2': ['B']},
            {'id': '2026-10-11_sigue', 'inicio': h(40), 'fronton': 'Ogueta', 'eq1': ['A'], 'eq2': ['C']},
            {'id': '2026-10-12_otra_velada', 'inicio': h(60), 'fronton': 'Astelena', 'eq1': ['A'], 'eq2': ['B']},
        ]
        idx = {('2026-10-09', frozenset({'laso'}), frozenset({'albisu'})): (22, 9)}
        cambios = dict(porra.decidir(abiertos, {'2026-10-11_sigue'}, {('2026-10-11', 'Ogueta')}, idx, ahora))
        self.assertEqual(cambios, {
            '2026-10-09_laso_vs_albisu': {'estado': 'jugado', 'puntos1': 22, 'puntos2': 9},
            '2026-10-05_viejo': {'estado': 'anulado'},
            '2026-10-11_cambiado': {'estado': 'anulado'},
        })


if __name__ == '__main__':
    unittest.main()
