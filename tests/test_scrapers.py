"""
Pruebas de los scrapers con casos reales sacados del historial de
data/cartelera.json y de data/partidos.json. No necesitan red.

    python -m unittest discover tests
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scraper'))

import scraper_cartelera as SC  # noqa: E402
from competiciones import clasificar  # noqa: E402
from scraper import IndicePartidos, clave_nombre, fusionar, planos_baiko  # noqa: E402


def partidos(lineas, fecha='24/09/2026', fase=None, competicion=None):
    return [(p['eq1'], p['eq2'], p['tipo'], bool(p.get('pendiente')))
            for p in SC.parse_partidos(lineas, fecha, fase, competicion)]


class CarteleraPartidos(unittest.TestCase):

    def test_pareja_normal(self):
        self.assertEqual(partidos(['ELORDI – ESKUZA // PEÑA II – SALAVERRI II (Serie B)'],
                                  competicion='Torneo San Mateo'),
                         [(['ELORDI', 'ESKUZA'], ['PEÑA II', 'SALAVERRI II'], 'campeonato-b', False)])

    def test_pareja_partida_en_dos_lineas(self):
        # Antes se leía como JAKA contra MARIEZKURRENA II y LARRAZABAL contra IZTUETA
        r = partidos(['Torneo San Mateo',
                      'JAKA – MARIEZKURRENA II //',
                      'LARRAZABAL – IZTUETA (Serie A)'], fase='Semifinales (Grupo A)')
        self.assertEqual(r, [(['JAKA', 'MARIEZKURRENA II'], ['LARRAZABAL', 'IZTUETA'], 'campeonato-a', False)])

    def test_zaguero_en_la_linea_siguiente(self):
        r = partidos(['Masters CaixaBank', 'LASO – MARTIJA // ZABALA', '– IZTUETA (Serie A)'],
                     fecha='13/09/2026', fase='7ª Jornada')
        self.assertEqual(r[0][:2], (['LASO', 'MARTIJA'], ['ZABALA', 'IZTUETA']))

    def test_pareja_suelta_no_es_mano_a_mano(self):
        (eq1, eq2, tipo, pendiente), = partidos(['LARRAZABAL – IZTUETA (Serie A)'])
        self.assertEqual((eq1, eq2, pendiente), (['LARRAZABAL', 'IZTUETA'], ['?', '?'], True))
        self.assertFalse(tipo.startswith('manomanista'))

    def test_individual_con_modalidad(self):
        r = partidos(['Torneo San Mateo', 'DARÍO // ARTOLA (4 1/2)', 'MURUA // AMIANO (MANO A MANO)'],
                     fecha='26/09/2026', fase='Desafío Urzante')
        self.assertEqual(r[0][:3], (['DARÍO'], ['ARTOLA'], 'cuatro-medio-a'))
        self.assertEqual(r[1][:3], (['MURUA'], ['AMIANO'], 'manomanista-a'))

    def test_modalidad_delante(self):
        r = partidos(['4 1/2 AGIRRE // ALBERDI II'], fecha='15/09/2026', fase='Festival')
        self.assertEqual(r, [(['AGIRRE'], ['ALBERDI II'], 'festival-cuatro', False)])

    def test_por_anunciar(self):
        r = partidos(['XX – XX // XX – XX', 'AMIANO – GABIRONDO // XX -XX SEMIFINAL'], fase='Final')
        self.assertEqual([x[3] for x in r], [True, True])
        self.assertEqual(r[1][:2], (['AMIANO', 'GABIRONDO'], ['?', '?']))

    def test_plaza_por_decidir(self):
        r = partidos(['LASO – LANDA // GANADORES DÍA 21', 'PERDEDOR DÍA 18 // REKALDE – ALDABE (Serie B)'])
        self.assertEqual(r[0][:2], (['LASO', 'LANDA'], ['GANADORES DÍA 21']))
        self.assertEqual(r[1][:2], (['PERDEDOR DÍA 18'], ['REKALDE', 'ALDABE']))

    def test_lineas_de_competicion_no_son_partidos(self):
        r = partidos(['EZKURDIA – ALBISU // ALTUNA III – IMAZ',
                      'Campeonato 4 1/2 Eusko Label – Octavos de Final (Serie A)'],
                     fecha='11/10/2026', fase='Octavos de final', competicion='Campeonato 4 1/2 Eusko Label')
        self.assertEqual(len(r), 1)
        # Partido de parejas en un evento del 4 y medio: festival, no campeonato de 4 y medio
        self.assertEqual(r[0][2], 'festival')


class CarteleraEventos(unittest.TestCase):
    TOKENS = [
        '24/09/2026 - 19:15h', 'Adarraga, Logroño', 'Semifinales (Grupo A)',
        'Torneo San Mateo',
        'ELORDI – ESKUZA // PEÑA II – SALAVERRI II (Serie B)',
        'JAKA – MARIEZKURRENA II //', 'LARRAZABAL – IZTUETA (Serie A)',
        'DESDE', '23.00€', 'Comprar entradas',
        '25/09/2026 - 17:00h', 'Beotibar, Tolosa', 'Festival',
        'BAKAIKOA // ZUBIZARRETA III (4 1/2)', 'Gratuito', 'TV',
    ]

    def test_eventos(self):
        evs = SC.parse_eventos(self.TOKENS, ['https://ejemplo/entradas'])
        self.assertEqual(len(evs), 2)
        a, b = evs
        self.assertEqual((a['fronton'], a['ciudad'], a['fase'], a['precio'], a['url']),
                         ('Adarraga', 'Logroño', 'Semifinales (Grupo A)', '23.00', 'https://ejemplo/entradas'))
        self.assertEqual(len(a['partidos']), 2)
        self.assertNotIn('DESDE', a['cartel'])
        self.assertEqual((b['precio'], b['tv'], b['partidos'][0]['tipo']), ('0', True, 'festival-cuatro'))

    def test_fusion_de_fuentes(self):
        baiko = SC.parse_eventos(self.TOKENS)
        otra = SC.parse_eventos([
            '24/09/2026 - 19:15h', 'Frontón Adarraga, Logroño', 'JAKA – MARIEZKURRENA II // LARRAZABAL – IZTUETA',
            '28/09/2026 - 18:00h', 'Gurea, Azkoitia', 'Festival', 'ALTUNA III – IMAZ // ARTOLA – REZUSTA',
        ])
        todos = SC.fusionar_eventos([('baiko', baiko), ('aspe', otra)])
        self.assertEqual([(e['fecha'], e['fuente']) for e in todos],
                         [('24/09/2026', 'baiko'), ('25/09/2026', 'baiko'), ('28/09/2026', 'aspe')])


def catalogado(fecha, e1, e2, p1, p2, fuente='baiko'):
    return {'fecha': fecha, 'equipo1': {'del_id': e1[0], 'zag_id': e1[1] if len(e1) > 1 else None},
            'equipo2': {'del_id': e2[0], 'zag_id': e2[1] if len(e2) > 1 else None},
            'puntos1': p1, 'puntos2': p2, 'fuente': fuente}


class _Cats:
    def __init__(self):
        self.avisos = []


class ResultadosFusion(unittest.TestCase):

    def setUp(self):
        self.existentes = [catalogado('2026-09-24', ['PEL001', 'PEL011'], ['PEL002', 'PEL016'], 22, 18)]

    def test_mismo_partido_otra_fuente_no_se_duplica(self):
        # Otra fuente con los equipos al revés
        nuevo = catalogado('2026-09-24', ['PEL002', 'PEL016'], ['PEL001', 'PEL011'], 18, 22, 'aspe')
        anadir, rep = fusionar([nuevo], self.existentes, _Cats())
        self.assertEqual((anadir, rep), ([], 1))

    def test_tanteo_distinto_avisa_y_conserva(self):
        cats = _Cats()
        nuevo = catalogado('2026-09-24', ['PEL001', 'PEL011'], ['PEL002', 'PEL016'], 22, 19, 'aspe')
        anadir, _ = fusionar([nuevo], self.existentes, cats)
        self.assertEqual(anadir, [])
        self.assertEqual(cats.avisos[0]['tipo'], 'tanteo_distinto')

    def test_partido_distinto_se_guarda(self):
        nuevo = catalogado('2026-09-24', ['PEL003', 'PEL008'], ['PEL004', 'PEL012'], 22, 10, 'aspe')
        anadir, _ = fusionar([nuevo], self.existentes, _Cats())
        self.assertEqual(len(anadir), 1)

    def test_repetido_entre_dos_fuentes_nuevas(self):
        a = catalogado('2026-09-25', ['PEL003'], ['PEL004'], 22, 10, 'baiko')
        b = catalogado('2026-09-25', ['PEL004'], ['PEL003'], 10, 22, 'aspe')
        anadir, rep = fusionar([a, b], self.existentes, _Cats())
        self.assertEqual((len(anadir), rep), (1, 1))

    def test_fecha_vecina_con_mismo_tanteo(self):
        nuevo = catalogado('2026-09-25', ['PEL001', 'PEL011'], ['PEL002', 'PEL016'], 22, 18, 'aspe')
        self.assertTrue(IndicePartidos(self.existentes).buscar(nuevo))

    def test_clave_nombre(self):
        self.assertEqual(clave_nombre('Peña II'), clave_nombre('PENA II'))
        self.assertEqual(clave_nombre('Darío'), clave_nombre('DARIO'))
        self.assertNotEqual(clave_nombre('ZUBIZARRETA III'), clave_nombre('ZUBIZARRETA IV'))

    def test_pagina_sin_partidos_es_error(self):
        html = '<html><body>Resultados<p>24/09/2026</p><p>texto raro</p></body></html>'
        with self.assertRaises(RuntimeError):
            planos_baiko(html)


class Competiciones(unittest.TestCase):

    def test_pareja_en_evento_de_4_y_medio(self):
        self.assertEqual(clasificar('Campeonato 4 1/2 Eusko Label', '11/10/2026', True)[0], 'festival')

    def test_individual_4_y_medio(self):
        self.assertEqual(clasificar('Campeonato 4 1/2 Eusko Label', '11/10/2026', False, 'a'),
                         ('cuatro-medio-a', 'Campeonato 4 y Medio Serie A 2026'))

    def test_parejas_en_diciembre_es_del_ano_siguiente(self):
        self.assertEqual(clasificar(None, '15/12/2026', True, 'a')[1], 'Campeonato Parejas Serie A 2027')


if __name__ == '__main__':
    unittest.main()
