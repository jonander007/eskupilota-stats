"""
Pruebas de los scrapers con casos reales sacados del historial de
data/cartelera.json y de data/partidos.json. No necesitan red.

    python -m unittest discover tests
"""

import json
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
        # Dentro de las fiestas de San Mateo, pero el evento es el Desafío Urzante:
        # categoría desafío, con el tipo de festival de su modalidad
        r = partidos(['Torneo San Mateo', 'DARÍO // ARTOLA (4 1/2)', 'MURUA // AMIANO (MANO A MANO)'],
                     fecha='26/09/2026', fase='Desafío Urzante')
        self.assertEqual(r[0][:3], (['DARÍO'], ['ARTOLA'], 'festival-cuatro'))
        self.assertEqual(r[1][:3], (['MURUA'], ['AMIANO'], 'festival-mano'))
        r = partidos(['Torneo San Mateo', 'DARÍO // ARTOLA (4 1/2)'], fecha='26/09/2026', fase='Día 26')
        self.assertEqual(r[0][:3], (['DARÍO'], ['ARTOLA'], 'cuatro-medio-a'))

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


FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')


def fixture(nombre):
    with open(os.path.join(FIXTURES, nombre), encoding='utf-8') as f:
        return f.read()


class Aspe(unittest.TestCase):
    """Con extractos reales de aspepelota.eus (27/09/2026)."""

    @classmethod
    def setUpClass(cls):
        import aspe
        from scraper import norm
        cls.res = aspe.resultados_aspe(fixture('aspe_resultados.html'), norm)
        cls.cart = aspe.cartelera_aspe(fixture('aspe_cartelera.html'))

    def test_resultados(self):
        self.assertEqual(len(self.res), 35)
        p = self.res[1]
        self.assertEqual((p['fecha'], p['fronton'], p['ciudad']), ('26/09/2026', 'ADARRAGA', 'LOGROÑO'))
        self.assertEqual((p['equipo1'], p['puntos1'], p['equipo2'], p['puntos2'], p['ganador']),
                         ({'delantero': 'ZABALA', 'zaguero': 'ZABALETA'}, 22,
                          {'delantero': 'LASO', 'zaguero': 'IZTUETA'}, 20, 'equipo1'))

    def test_resultados_individual_y_pueblo(self):
        p = self.res[0]
        self.assertEqual((p['equipo1']['delantero'], p['equipo1']['zaguero'], p['equipo2']['delantero']),
                         ('DARÍO', None, 'ARTOLA'))
        altsasu = [p for p in self.res if p['fronton'] == 'ALTSASU']
        self.assertEqual(len(altsasu), 2)

    def test_cartelera(self):
        self.assertEqual(len(self.cart), 15)
        final = self.cart[1]
        self.assertTrue(final['tv'])
        self.assertEqual([(p['eq1'], p['eq2'], p['tipo'], p['competicion']) for p in final['partidos']], [
            (['ELORDI', 'ESKUZA'], ['P.ETXEBERRIA', 'LOZA'], 'campeonato-b', 'Torneo San Mateo Serie B 2026'),
            (['EZKURDIA', 'MARTIJA'], ['JAKA', 'MARIEZKURRENA II'], 'campeonato-a', 'Torneo San Mateo Serie A 2026')])

    def test_cartelera_sin_pelotaris(self):
        villava = next(e for e in self.cart if e['fecha'] == '09/10/2026' and e['hora'] == '18:00')
        cuatro = villava['partidos'][0]
        self.assertEqual((cuatro['eq1'], cuatro['tipo'], cuatro.get('pendiente')), (['?'], 'cuatro-medio-b', True))
        final = next(e for e in self.cart if e['fecha'] == '04/10/2026')['partidos'][0]
        self.assertEqual((final['eq1'], final.get('pendiente')), (['?', '?'], True))
        tolosa = next(e for e in self.cart if e['fecha'] == '12/10/2026')['partidos'][-1]
        self.assertEqual((tolosa['eq1'], tolosa['eq2']), (['?', 'ARANGUREN'], ['?', 'BIKUÑA']))

    def test_cartelera_alternativa(self):
        soria = next(e for e in self.cart if e['fecha'] == '03/10/2026' and e['hora'] == '18:00')
        self.assertEqual(soria['partidos'][1]['eq1'], ['DARÍO', 'ALBISU o IZTUETA'])

    def test_mismo_evento_con_otra_fuente(self):
        bilbo = next(e for e in self.cart if e['fecha'] == '17/10/2026')
        baiko = {'fecha': '17/10/2026', 'hora': '17:15', 'fronton': 'Bizkaia Frontoia', 'ciudad': 'Bilbao',
                 'partidos': [{'eq1': ['?'], 'eq2': ['?']}]}
        otro = dict(baiko, fronton='Gurea', ciudad='Azkoitia')
        self.assertTrue(SC.mismo_evento(bilbo, baiko))
        self.assertFalse(SC.mismo_evento(bilbo, otro))

    def test_misma_fuente_no_se_fusiona(self):
        a = {'fecha': '02/10/2026', 'hora': '21:00', 'fronton': 'Navarra Arena', 'ciudad': 'Pamplona', 'partidos': []}
        todos = SC.fusionar_eventos([('baiko', [dict(a), dict(a)])])
        self.assertEqual(len(todos), 2)


class CatalogosFuentes(unittest.TestCase):
    """Con los catálogos reales de data/ (solo lectura)."""

    @classmethod
    def setUpClass(cls):
        from scraper import Catalogos
        cls.cats = Catalogos()

    def test_errata_en_nombre(self):
        pid = self.cats.get_or_create_pelotari('P.ETXBERRIA')
        self.assertEqual(self.cats._idx_pel['P.ETXEBERRIA']['id'], pid)

    def test_ordinal_distinto_no_es_errata(self):
        self.assertIsNone(self.cats._casi_igual('ZUBIZARRETA V'))

    def test_fronton_por_pueblo(self):
        self.assertEqual(self.cats.fronton_de_pueblo('ALTSASU')[0]['nombre'], 'BURUNDA')
        self.assertEqual(self.cats.fronton_de_pueblo('Bilbo')[1]['nombre'], 'BILBAO')


class Clasificacion(unittest.TestCase):
    """Tipos de partido con textos reales de Baiko (resultados y cartelera) y Aspe."""

    CASOS = [
        # texto, fecha, parejas, serie, textos de fase -> categoria, modalidad, serie, fase, grupo, jornada, competicion
        ('Campeonato Parejas Serie A - Liga', '10/01/2026', True, None, (),
         ('campeonato', 'parejas', 'A', 'liga', None, None, 'Campeonato Parejas Serie A 2026')),
        (None, '14/12/2025', True, 'b', (),
         ('campeonato', 'parejas', 'B', None, None, None, 'Campeonato Parejas Serie B 2026')),
        ('Campeonato Parejas Promoción', '20/03/2023', True, None, (),
         ('campeonato', 'parejas', 'B', None, None, None, 'Campeonato Parejas Serie B 2023')),
        ('Campeonato 4 1/2 Eusko Label', '09/10/2026', False, 'b', ('Octavos de final',),
         ('campeonato', 'cuatro', 'B', 'octavos', None, None, 'Campeonato 4 y Medio Serie B 2026')),
        ('Campeonato 4 1/2 Eusko Label', '16/10/2026', False, 'a', ('Cuartos de final (1ª Jornada)',),
         ('campeonato', 'cuatro', 'A', 'cuartos', None, 1, 'Campeonato 4 y Medio Serie A 2026')),
        ('Eusko Label 4 1/2ko Txapelketa - Seria A · Campeonato Eusko Label 4 1/2 - Serie A', '10/10/2026', False, None,
         ('Zortzirenak // Octavos',),
         ('campeonato', 'cuatro', 'A', 'octavos', None, None, 'Campeonato 4 y Medio Serie A 2026')),
        ('Campeonato Manomanista Serie A', '31/05/2026', False, None, ('Final',),
         ('campeonato', 'mano', 'A', 'final', None, None, 'Campeonato Manomanista Serie A 2026')),
        ('Masters CaixaBank', '13/09/2026', True, 'a', ('7ª Jornada / Semifinal',),
         ('torneo', 'parejas', 'A', 'semifinal', None, 7, 'Masters CaixaBank Serie A 2026')),
        ('Masters CaixaBank', '06/09/2026', True, 'b', ('6ª Jornada',),
         ('torneo', 'parejas', 'B', 'liga', None, 6, 'Masters CaixaBank Serie B 2026')),
        ('Masters CaixaBank semifinal', '02/10/2026', True, None, (),
         ('torneo', 'parejas', 'A', 'semifinal', None, None, 'Masters CaixaBank Serie A 2026')),
        ('Torneo San Mateo', '24/09/2026', True, 'b', ('Semifinales (Grupo A)',),
         ('torneo', 'parejas', 'B', 'semifinal', 'A', None, 'Torneo San Mateo Serie B 2026')),
        ('Final San Mateo Serie A', '27/09/2026', True, None, (),
         ('torneo', 'parejas', 'A', 'final', None, None, 'Torneo San Mateo Serie A 2026')),
        ('Torneo San Fermin 4 1/2', '07/07/2026', False, None, (),
         ('torneo', 'cuatro', None, None, None, None, 'Torneo San Fermin 4 y Medio 2026')),
        ('Desafío Urzante 4 1/2', '26/09/2026', False, None, (),
         ('desafio', 'cuatro', None, None, None, None, 'Desafio Urzante 2026')),
        ('Desafío Urzante', '26/09/2026', True, None, (),
         ('desafio', 'parejas', None, None, None, None, 'Desafio Urzante 2026')),
        ('Festival', '10/08/2026', True, None, (),
         ('festival', 'parejas', None, None, None, None, 'Festival')),
        ('Campeonato 4 1/2 Eusko Label', '11/10/2026', True, 'a', ('Octavos de final',),
         ('festival', 'parejas', None, None, None, None, 'Festival')),
        (None, '10/08/2026', True, None, (),
         ('festival', 'parejas', None, None, None, None, 'Festival')),
        ('Festival Despedida Elizegi', '20/12/2025', True, None, (),
         ('festival', 'parejas', None, None, None, None, 'Festival Despedida Elizegi')),
    ]

    def test_casos_reales(self):
        from competiciones import clasificar_partido
        for texto, fecha, pareja, serie, fases, esperado in self.CASOS:
            with self.subTest(texto=texto, fecha=fecha):
                r = clasificar_partido(texto, fecha, pareja, serie, textos_fase=fases)
                self.assertEqual((r['categoria'], r['modalidad'], r['serie'], r['fase'], r['grupo'],
                                  r['jornada'], r['competicion']), esperado)

    def test_tipo_derivado(self):
        from competiciones import _tipo
        self.assertEqual(_tipo('parejas', 'torneo', 'B'), 'campeonato-b')
        self.assertEqual(_tipo('cuatro', 'campeonato', 'A'), 'cuatro-medio-a')
        self.assertEqual(_tipo('mano', 'desafio', None), 'festival-mano')

    def test_fases_en_euskera_y_castellano(self):
        from competiciones import leer_fase
        for texto, esperado in [('Laurdenak / Cuartos', 'cuartos'), ('Finalerdiak', 'semifinal'), ('Finala', 'final'),
                                ('Finales', 'final'), ('Eliminatoria (Grupo A)', 'eliminatoria'), ('Día 24', None),
                                ('Abono', None), ('Kanporaketa', 'eliminatoria')]:
            self.assertEqual(leer_fase(texto)[0], esperado, texto)

    def test_cartelera_desafio_entero(self):
        ps = SC.parse_partidos(['Torneo San Mateo', 'DARÍO // ARTOLA (4 1/2)', 'ZABALA – ZABALETA // LASO – IZTUETA'],
                               '26/09/2026', 'Desafío Urzante', None)
        self.assertEqual([(p['categoria'], p['modalidad']) for p in ps], [('desafio', 'cuatro'), ('desafio', 'parejas')])

    def test_cartelera_titulo_da_la_fase(self):
        ps = SC.parse_partidos(['Final Torneo San Mateo (Serie B)', 'ELORDI – ESKUZA // P.ETXEBERRIA – LOZA (Serie B)'],
                               '27/09/2026', 'Finales', None)
        self.assertEqual((ps[0]['categoria'], ps[0]['serie'], ps[0]['fase']), ('torneo', 'b', 'final'))


class WebSoloConFase(unittest.TestCase):
    """La web de resultados pone solo 'Final'; la competición está en la
    cartelera (finales del Torneo San Mateo del 27/09/2026 en Adarraga)."""

    EVENTO = {
        'fecha': '27/09/2026', 'hora': '17:15', 'fronton': 'Adarraga', 'ciudad': 'Logroño',
        'fase': 'Finales', 'competicion': None,
        'cartel': ['Torneo San Mateo',
                   'ELORDI – ESKUZA // p.etxeberria – loza (Serie B)',
                   'EZKURDIA – MARTIJA // JAKA – MARIEZKURRENA II (Serie A)'],
        'partidos': [
            {'eq1': ['ELORDI', 'ESKUZA'], 'eq2': ['p.etxeberria', 'loza'],
             'raw': 'ELORDI – ESKUZA // p.etxeberria – loza (Serie B)', 'tipo': 'campeonato-b', 'serie': 'b'},
            {'eq1': ['EZKURDIA', 'MARTIJA'], 'eq2': ['JAKA', 'MARIEZKURRENA II'],
             'raw': 'EZKURDIA – MARTIJA // JAKA – MARIEZKURRENA II (Serie A)', 'tipo': 'campeonato-a', 'serie': 'a'},
        ],
    }

    def clasificar(self, texto_web, jugadores):
        from competiciones import Cartelera, clasificar_partido
        from scraper import textos_clasificacion
        d = Cartelera([self.EVENTO]).buscar_detalle('27/09/2026', 'ADARRAGA', jugadores)
        texto, serie, fases = textos_clasificacion(texto_web, d)
        return clasificar_partido(texto, '27/09/2026', True, serie, None, fases)

    def test_final_serie_b(self):
        c = self.clasificar('Final', ['ELORDI', 'ESKUZA', 'P.ETXEBERRIA', 'LOZA'])
        self.assertEqual((c['competicion'], c['categoria'], c['fase']), ('Torneo San Mateo Serie B 2026', 'torneo', 'final'))

    def test_final_serie_a(self):
        c = self.clasificar('Final', ['EZKURDIA', 'MARTIJA', 'JAKA', 'MARIEZKURRENA II'])
        self.assertEqual((c['competicion'], c['fase']), ('Torneo San Mateo Serie A 2026', 'final'))

    def test_texto_de_competicion_de_la_web_manda(self):
        c = self.clasificar('Campeonato Parejas Serie A', ['EZKURDIA', 'MARTIJA', 'JAKA', 'MARIEZKURRENA II'])
        self.assertEqual(c['categoria'], 'campeonato')


class HistorialBaiko(unittest.TestCase):
    """Página de campeonato de Baiko con el «Historial de competición»."""

    @classmethod
    def setUpClass(cls):
        from historial_baiko import competicion_de_titulo, leer_historial
        with open(os.path.join(FIXTURES, 'baiko_historial_cuatro_2025.html'), encoding='utf-8') as f:
            cls.h = leer_historial(f.read())
        cls.comp = competicion_de_titulo(cls.h['titulo'], cls.h['partidos'])

    def test_competicion(self):
        self.assertEqual(self.comp, 'Campeonato 4 y Medio Serie A 2025')

    def test_fases(self):
        from collections import Counter
        fases = Counter(p['fase'] for p in self.h['partidos'])
        # El partido suspendido (sin fecha) no cuenta
        self.assertEqual(fases, {'octavos': 4, 'cuartos': 11, 'semifinal': 2, 'tercero': 1, 'final': 1})

    def test_grupo_solo_en_liguillas(self):
        octavos = [p for p in self.h['partidos'] if p['fase'] == 'octavos']
        self.assertTrue(all(p['grupo'] is None for p in octavos))     # mitades del cuadro
        cuartos = {p['grupo'] for p in self.h['partidos'] if p['fase'] == 'cuartos'}
        self.assertEqual(cuartos, {'A', 'B'})

    def test_final(self):
        final = next(p for p in self.h['partidos'] if p['fase'] == 'final')
        self.assertEqual((final['fecha'], final['fronton'], final['ciudad']), ('16/11/2025', 'Bizkaia Frontoia', 'Bilbao'))
        self.assertEqual((final['equipo1'], final['puntos1'], final['equipo2'], final['puntos2']),
                         (['P.ETXEBERRIA'], 22, ['ZABALA'], 11))

    def test_nombres_de_fase(self):
        from historial_baiko import fase_de_titulo
        casos = {'Liga de cuartos': ('cuartos', True), 'Liguilla de semifinales': ('semifinal', True),
                 'Octavos': ('octavos', False), 'Semifinales': ('semifinal', False),
                 'Tercer y cuarto': ('tercero', False), 'Final': ('final', False),
                 'Dieciseisavos': ('eliminatoria', False), 'Liga': ('liga', True)}
        for titulo, esperado in casos.items():
            self.assertEqual(fase_de_titulo(titulo), esperado, titulo)


class FormatoJson(unittest.TestCase):
    def test_lista_un_elemento_por_linea(self):
        from jsonio import dumps
        datos = [{'fecha': '2026-09-26', 'equipo1': {'del_id': 'PEL001', 'zag_id': None}}, {'n': 'ÑANDÚ'}]
        texto = dumps(datos)
        self.assertEqual(texto.splitlines(), [
            '[',
            '{"fecha":"2026-09-26","equipo1":{"del_id":"PEL001","zag_id":null}},',
            '{"n":"ÑANDÚ"}',
            ']',
        ])
        self.assertEqual(json.loads(texto), datos)
        self.assertEqual(dumps([]), '[]\n')


if __name__ == '__main__':
    unittest.main()
