import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'tools'))
from generar_paginas import eventos_cartelera, migas  # noqa: E402


class EventosCartelera(unittest.TestCase):
    """Los partidos de la cartelera salen como SportsEvent para los buscadores."""

    CART = {'partidos': [{
        'fecha': '24/10/2026', 'hora': '17:15', 'fronton': 'Labrit', 'ciudad': 'Iruñea/Pamplona', 'fuente': 'baiko',
        'fase': 'Cuartos de final', 'url': 'https://entradas.example/1', 'precio': '38.00', 'agotado': False,
        'partidos': [
            {'eq1': ['EZKURDIA'], 'eq2': ['ELORDI'], 'competicion': 'Campeonato 4 y Medio Serie A 2026'},
            {'eq1': ['AGIRRE', 'IMAZ'], 'eq2': ['LARRAZABAL', 'LIZEAGA'], 'competicion': 'Festival'},
            {'eq1': ['?'], 'eq2': ['MURUA'], 'competicion': 'Festival'},     # cartel sin completar: fuera
        ]}]}

    def test_eventos(self):
        (v, ps), = eventos_cartelera(self.CART, 'es')
        self.assertEqual(len(ps), 2)
        ld = ps[0][1]
        self.assertEqual(ld['@type'], 'SportsEvent')
        self.assertEqual(ld['startDate'], '2026-10-24T17:15:00+02:00')     # hora de Madrid (verano)
        self.assertEqual(ld['location']['address']['addressCountry'], 'ES')
        self.assertEqual(ld['offers']['price'], '38.00')
        self.assertEqual(ld['organizer']['name'], 'Baiko Pilota')
        self.assertEqual(ps[1][1]['competitor'][0]['@type'], 'SportsTeam')

    def test_migas(self):
        html = migas('eu', [('Pilotariak', 'https://www.eskupilotastats.com/eu/pelotari/')])
        self.assertIn('BreadcrumbList', html)
        self.assertIn('?lang=eu', html)


if __name__ == '__main__':
    unittest.main()
