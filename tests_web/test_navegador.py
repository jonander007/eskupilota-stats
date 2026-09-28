# -*- coding: utf-8 -*-
"""
Pruebas de la web en un navegador de verdad (Chromium con Playwright).

Levantan un servidor local con el repositorio y recorren la web como un
usuario: enlaces directos, botón atrás, cambio de idioma, comparadores,
campeonatos, mapa, cartelera, modo oscuro, móvil, teclado y las páginas
estáticas de pelotaris. Las peticiones a otros dominios (fuentes, teselas
del mapa) se bloquean: la web tiene que funcionar sin ellas.

    pip install -r requirements-web.txt
    python -m playwright install chromium      # solo la primera vez
    python -m unittest discover tests_web -v

Están separadas de tests/ (scrapers) para que el workflow nocturno no
necesite el navegador.
"""

import functools
import http.server
import os
import re
import threading
import unittest

try:
    from playwright.sync_api import sync_playwright
except ImportError:          # sin Playwright instalado, se saltan
    sync_playwright = None

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
EXTERNO = re.compile(r'^https?://(?!127\.0\.0\.1)')


class _Silencioso(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


@unittest.skipIf(sync_playwright is None, 'Playwright no está instalado')
class Web(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        handler = functools.partial(_Silencioso, directory=RAIZ)
        cls.servidor = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
        threading.Thread(target=cls.servidor.serve_forever, daemon=True).start()
        cls.url = f'http://127.0.0.1:{cls.servidor.server_port}/'
        cls.pw = sync_playwright().start()
        cls.navegador = cls.pw.chromium.launch()

    @classmethod
    def tearDownClass(cls):
        cls.navegador.close()
        cls.pw.stop()
        cls.servidor.shutdown()

    def pagina(self, ruta='', ancho=1300, esquema='light', descargas=False):
        ctx = self.navegador.new_context(viewport={'width': ancho, 'height': 900},
                                         color_scheme=esquema, accept_downloads=descargas)
        self.addCleanup(ctx.close)
        pg = ctx.new_page()
        pg.set_default_timeout(15000)
        self.errores = []
        pg.on('pageerror', lambda e: self.errores.append(str(e)))
        pg.route(EXTERNO, lambda r: r.abort())
        pg.goto(self.url + ruta, wait_until='networkidle')
        pg.wait_for_function('typeof PARTIDOS !== "undefined" && PARTIDOS.length > 0')
        return pg

    def seccion(self, pg):
        return pg.evaluate("document.querySelector('.section.active').id")

    def tearDown(self):
        self.assertEqual(getattr(self, 'errores', []), [], 'errores de JavaScript en la página')

    # ── Carga y rutas ──
    def test_carga_sin_errores_y_sin_leaflet(self):
        peticiones = []
        ctx = self.navegador.new_context()
        self.addCleanup(ctx.close)
        pg = ctx.new_page()
        pg.on('request', lambda r: peticiones.append(r.url))
        pg.route(EXTERNO, lambda r: r.abort())
        pg.goto(self.url, wait_until='networkidle')
        self.assertGreater(pg.evaluate('PARTIDOS.length'), 1000)
        self.assertFalse(any('leaflet' in u for u in peticiones), 'el mapa solo se carga al abrir Frontones')

    def test_enlace_directo_y_atras(self):
        pg = self.pagina('#/pelotari/laso')
        self.assertEqual(self.seccion(pg), 'sec-pelotaris')
        self.assertEqual(pg.text_content('#perfilTitle').strip(), 'LASO')
        pg.click('header nav button[onclick*="campeonatos"]')
        self.assertTrue(pg.evaluate('location.hash').startswith('#/campeonato'))
        pg.go_back()
        pg.wait_for_timeout(300)
        self.assertEqual(pg.evaluate('location.hash'), '#/pelotari/laso')

    def test_ruta_desconocida_va_a_resultados(self):
        pg = self.pagina('#/no-existe')
        self.assertEqual(self.seccion(pg), 'sec-partidos')

    # ── Idioma ──
    def test_cambio_de_idioma_mantiene_la_vista(self):
        pg = self.pagina('#/pelotari/laso')
        pg.click('#langEu')
        self.assertEqual(pg.text_content('header nav button[onclick*="campeonatos"]').strip(), 'Txapelketak')
        self.assertEqual(self.seccion(pg), 'sec-pelotaris')
        self.assertEqual(pg.evaluate('document.documentElement.lang'), 'eu')

    def test_idioma_en_la_url(self):
        pg = self.pagina('?lang=eu#/ranking')
        self.assertEqual(pg.evaluate('LANG'), 'eu')
        self.assertIn('lang=eu', pg.get_attribute('link[rel=canonical]', 'href'))
        pg.click('#langEs')
        self.assertIn('lang=es', pg.evaluate('location.search'))

    # ── Comparador de parejas ──
    def test_comparador_tres_de_cuatro(self):
        pg = self.pagina('#/comparador')
        pg.click('[onclick*="setCompType(\'parejas\'"]')
        for sel, valor in (('#c4d1', 'P.ETXEBERRIA'), ('#c4d2', 'ALTUNA III')):
            pg.select_option(sel, valor)
        pg.select_option('#c4z1', 'ZABALETA')
        pg.select_option('#c4z2', 'MARTIJA')
        pg.evaluate('renderC4()')
        ultimo = pg.locator('.c4-bloque').last
        self.assertIn('3', ultimo.locator('.c4-bloque-title').inner_text())
        self.assertGreater(ultimo.locator('.twrap tbody tr').count(), 0)
        self.assertTrue(ultimo.locator('.c4-sin-chip').count() >= 2)

    # ── Campeonatos ──
    def test_campeonato_con_cuadro_de_eliminatorias(self):
        # 4 y Medio Serie B 2025 (fases del historial de Baiko): octavos,
        # liguilla de cuartos, semifinales y final
        pg = self.pagina('#/campeonato/COMP004')
        rondas = pg.locator('.an-ko-col')
        self.assertEqual(rondas.count(), 4)
        self.assertEqual(rondas.nth(0).locator('.an-ko-m').count(), 4)
        self.assertEqual(rondas.nth(2).locator('.an-ko-m').count(), 2)
        self.assertEqual(rondas.nth(3).locator('.an-ko-m').count(), 1)
        self.assertTrue(pg.locator('.an-campeon').is_visible())

    def test_fases_con_liguilla_del_historial(self):
        # 4 y Medio Serie A 2025, importado del historial de Baiko: octavos,
        # liguilla de cuartos (grupos A y B), semifinales, final y tercer puesto
        pg = self.pagina('#/campeonato/COMP002')
        cols = pg.locator('.an-ko-col')
        self.assertEqual(cols.count(), 4)
        self.assertEqual(pg.locator('.an-ko-col.liga .an-ko-grupo').count(), 2)
        # En cada grupo se resaltan los dos que pasan a semifinales
        self.assertEqual(pg.locator('.an-ko-col.liga .an-ko-grupo').first.locator('.an-ko-eq.gana').count(), 2)
        self.assertIn('ETXEBERRIA', pg.locator('.an-campeon').inner_text())

    def test_filtros_competicion_y_anio(self):
        pg = self.pagina('#/campeonatos')
        pg.select_option('#campSel', label='Campeonato 4 y Medio Serie A')
        # Al elegir la competición sale su año más reciente
        anios = pg.locator('#campAnio option').all_text_contents()
        self.assertEqual(anios, sorted(anios, reverse=True))
        self.assertEqual(pg.input_value('#campAnio'), pg.locator('#campAnio option').first.get_attribute('value'))
        pg.select_option('#campAnio', label='2023')
        self.assertIn('2023', pg.text_content('.an-camp-nombre'))
        self.assertIn('#/campeonato/', pg.evaluate('location.hash'))
        # Sin competiciones repetidas en el desplegable
        nombres = pg.locator('#campSel option').all_text_contents()
        self.assertEqual(len(nombres), len(set(nombres)))

    def test_clasificacion_de_campeonato(self):
        pg = self.pagina('#/campeonatos')
        self.assertGreater(pg.locator('#campContent table tbody tr').count(), 3)

    # ── Perfil y ranking ──
    def test_perfil_por_fronton_y_mejor_pareja(self):
        pg = self.pagina('#/pelotari/laso')
        self.assertTrue(pg.locator('.pf-mejor').is_visible())
        filas = pg.locator('.an-fr-tabla tbody tr')
        visibles = lambda: sum(filas.nth(i).is_visible() for i in range(filas.count()))
        self.assertEqual(visibles(), 8)
        pg.click('.an-fr-mas')
        self.assertEqual(visibles(), filas.count())

    def test_ficha_jpg_con_el_filtro(self):
        pg = self.pagina('#/pelotari/laso', descargas=True)
        with pg.expect_download() as d:
            pg.click('button[onclick^="descargarFicha"]')
        self.assertEqual(d.value.suggested_filename, 'ficha-laso.jpg')
        with open(d.value.path(), 'rb') as f:
            self.assertEqual(f.read(3), b'\xff\xd8\xff')          # JPEG
        # Con filtro de año y modalidad, el nombre del fichero lo indica
        pg.evaluate("activeYearPel='2025'; activeTipo='campeonato'; renderPCards(); openPerfil('LASO')")
        with pg.expect_download() as d:
            pg.click('button[onclick^="descargarFicha"]')
        self.assertEqual(d.value.suggested_filename, 'ficha-laso-campeonato-2025.jpg')

    def test_ranking_de_parejas(self):
        pg = self.pagina('#/ranking')
        pg.click('#rktab-parejas')
        self.assertEqual(pg.locator('.rk-card').count(), 2)
        # Cada pelotari de la pareja enlaza a su perfil
        pg.locator('.rk-card .rk-name .clk').first.click()
        pg.wait_for_timeout(300)
        self.assertTrue(pg.evaluate('location.hash').startswith('#/pelotari/'))

    def test_ranking_elo(self):
        pg = self.pagina('#/ranking')
        pg.click('#rktab-elo')
        self.assertGreaterEqual(pg.locator('#rkContent tbody tr').count(), 10)

    # ── Frontones y mapa ──
    def test_mapa_de_frontones(self):
        pg = self.pagina('#/frontones')
        pg.wait_for_selector('.fmap-cluster')
        # Todos los frontones con coordenadas están en el mapa, agrupados
        self.assertGreater(pg.evaluate('_frontonCapa.getLayers().length'), 100)
        # Encuadrado en la zona de los frontones, no en toda España
        self.assertGreaterEqual(pg.evaluate('_frontonMap.getZoom()'), 7)
        # La atribución va plegada en un botón ⓘ y se abre al pulsarlo
        self.assertFalse(pg.is_visible('.fmap-atrib-txt'))
        pg.click('.fmap-atrib-btn')
        self.assertIn('OpenStreetMap', pg.text_content('.fmap-atrib-txt'))
        self.assertTrue(pg.is_visible('.fmap-atrib-txt'))

    # ── Cartelera ──
    def test_cartelera_y_calendario(self):
        pg = self.pagina('#/cartelera', descargas=True)
        pg.wait_for_timeout(500)
        botones = pg.locator('.cart-evento-btns button')
        if not botones.count():
            self.skipTest('La cartelera no tiene partidos ahora mismo')
        with pg.expect_download() as d:
            botones.first.click()
        with open(d.value.path(), encoding='utf-8', newline='') as f:
            ics = f.read()
        self.assertTrue(ics.startswith('BEGIN:VCALENDAR'))
        self.assertIn('DTSTART;TZID=Europe/Madrid:', ics)
        self.assertTrue(all(len(linea.encode()) <= 75 for linea in ics.split('\r\n')))

    def test_cartelera_lleva_a_estadisticas(self):
        pg = self.pagina('#/cartelera')
        pg.wait_for_timeout(500)
        tarjetas = pg.locator('.cart-partido-card')
        if not tarjetas.count():
            self.skipTest('La cartelera no tiene partidos ahora mismo')
        # Sin paso intermedio de "Opciones": se pulsa y va al comparador
        tarjetas.first.locator('.an-previa, .cart-partido-equipos').first.click()
        pg.wait_for_timeout(400)
        self.assertEqual(pg.evaluate('location.hash'), '#/comparador')

    def test_instalar_app(self):
        pg = self.pagina()
        visibles = lambda: pg.evaluate("[...document.querySelectorAll('.app-instalar')].filter(b => !b.hidden).length")
        self.assertEqual(visibles(), 0)            # el navegador aún no ofrece instalarla
        # El navegador avisa de que se puede instalar: aparece en el menú y el pie
        pg.evaluate("""(() => { const e = new Event('beforeinstallprompt');
          e.prompt = () => { window.__instalada = 1; }; e.userChoice = Promise.resolve({outcome: 'accepted'});
          window.dispatchEvent(e); })()""")
        self.assertEqual(visibles(), 2)
        pg.locator('.site-footer .app-instalar').click()
        pg.wait_for_timeout(200)
        self.assertEqual(pg.evaluate('window.__instalada'), 1)
        self.assertEqual(visibles(), 0)
        # Dentro de la app (abierta desde la APK) no se ofrece
        pg.evaluate("sessionStorage.setItem('eskupilota-app','1')")
        pg.evaluate("""(() => { const e = new Event('beforeinstallprompt'); e.prompt = () => {};
          e.userChoice = Promise.resolve({}); window.dispatchEvent(e); })()""")
        self.assertEqual(visibles(), 0)
        # Sin dirección de APK, el enlace de Android no sale
        self.assertEqual(pg.evaluate("[...document.querySelectorAll('.app-apk')].filter(a => !a.hidden).length"), 0)

    # ── Móvil, modo oscuro y accesibilidad ──
    def test_movil_sin_scroll_horizontal(self):
        pg = self.pagina(ancho=390)
        for ruta in ('#/resultados', '#/cartelera', '#/pelotari/laso', '#/comparador',
                     '#/ranking', '#/campeonato/COMP004', '#/frontones', '#/contacto'):
            pg.goto(self.url + ruta)
            pg.wait_for_timeout(400)
            ancho = pg.evaluate('document.documentElement.scrollWidth')
            self.assertLessEqual(ancho, 390, f'{ruta} desborda en móvil')

    def test_modo_oscuro(self):
        pg = self.pagina(esquema='dark')
        fondo = pg.evaluate('getComputedStyle(document.body).backgroundColor')
        r, g, b = map(int, re.findall(r'\d+', fondo)[:3])
        self.assertLess(r + g + b, 150, 'el fondo debería ser oscuro')

    def test_teclado(self):
        pg = self.pagina('#/pelotaris')
        tarjeta = pg.locator('.pcard').first
        self.assertEqual(tarjeta.get_attribute('role'), 'button')
        tarjeta.focus()
        pg.keyboard.press('Enter')
        pg.wait_for_timeout(300)
        self.assertTrue(pg.evaluate('location.hash').startswith('#/pelotari/'))
        # El primer Tab lleva al enlace "Saltar al contenido"
        pg.goto(self.url)
        pg.keyboard.press('Tab')
        self.assertIn('skip-link', pg.evaluate('document.activeElement.className'))

    def test_formularios_con_etiqueta(self):
        pg = self.pagina('#/pelotari/laso')     # incluye los filtros del perfil
        sin_etiqueta = pg.evaluate("""[...document.querySelectorAll('input,select,textarea')]
          .filter(el => el.type !== 'hidden' && !el.labels?.length && !el.getAttribute('aria-label'))
          .map(el => el.id)""")
        self.assertEqual(sin_etiqueta, [])

    # ── Páginas estáticas ──
    def test_paginas_de_pelotaris_coinciden_con_la_web(self):
        pg = self.pagina()
        web = pg.evaluate("""(() => {
          const act = getActivePlayers();
          const rk = Object.keys(ELO).filter(n => act.has(n.toUpperCase())).sort((a,b) => ELO[b]-ELO[a]);
          return rk.slice(0, 5).map(n => [slugify(n), Math.round(ELO[n]), rk.indexOf(n)+1]);
        })()""")
        for slug, elo, pos in web:
            pg.goto(f'{self.url}pelotari/{slug}/')
            kpis = dict(zip(pg.locator('.kpi span').all_text_contents(), pg.locator('.kpi b').all_text_contents()))
            self.assertEqual(int(kpis['Elo']), elo, slug)
            self.assertTrue(any(v == f'{pos}º' for v in kpis.values()), slug)
        pg.goto(f'{self.url}eu/pelotari/')
        self.assertEqual(pg.evaluate('document.documentElement.lang'), 'eu')


if __name__ == '__main__':
    unittest.main()
