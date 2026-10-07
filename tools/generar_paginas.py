#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generar_paginas.py — Eskupilota Stats

Genera una página HTML estática por pelotari, en castellano y en euskera,
para que los buscadores encuentren "Laso estadísticas" (la web es una sola
página con #/pelotari/laso, y eso no lo indexan):

    pelotari/<slug>/index.html        pelotari/index.html
    eu/pelotari/<slug>/index.html     eu/pelotari/index.html
    sitemap.xml

Cada página tiene el resumen (partidos, victorias, Elo, palmarés, últimos
partidos, compañeros y frontones) y enlaza a la ficha interactiva.
Se regenera cada noche en el workflow, después de los scrapers.

Uso:
    python tools/generar_paginas.py
"""

import html
import json
import os
import re
import shutil
import unicodedata
from collections import Counter, defaultdict
from datetime import date

RAIZ = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
DATA = os.path.join(RAIZ, 'data')
WEB = 'https://www.eskupilotastats.com'

# Mismo cálculo que calcElo() en js/analisis.js
ELO_BASE, ELO_K = 1500, 24

TXT = {
    'es': {
        'titulo': '{n}: estadísticas de pelota a mano | EskupilotaStats',
        'desc': '{n}, pelotari profesional ({rol}): {pj} partidos, {pg} victorias ({pct}%). '
                'Elo, palmarés, últimos resultados, compañeros y frontones.',
        'rol_del': 'delantero', 'rol_zag': 'zaguero',
        'partidos': 'Partidos', 'victorias': 'Victorias', 'derrotas': 'Derrotas', 'pct': '% victorias',
        'dif': 'Diferencia de tantos', 'elo': 'Elo', 'elo_max': 'Elo máximo', 'ranking': 'Ranking Elo',
        'de_activos': 'de {n} activos', 'modalidad': 'Por modalidad',
        'parejas': 'Parejas', 'mano': 'Mano a mano', 'cuatro': '4 y medio',
        'palmares': 'Palmarés', 'campeon': 'Campeón', 'finalista': 'Finalista',
        'ultimos': 'Últimos partidos', 'fecha': 'Fecha', 'competicion': 'Competición', 'fronton': 'Frontón',
        'partido': 'Partido', 'res': 'Resultado', 'v': 'V', 'd': 'D',
        'companeros': 'Compañeros más habituales', 'frontones': 'Frontones donde más ha jugado',
        'pj': 'PJ', 'ficha': 'Ver la ficha completa: gráficos, comparador y cara a cara →',
        'actualizado': 'Datos actualizados el {f}. Fuente: resultados de Baiko Pilota y Aspe.',
        'derechos': '© 2022-2026 EskupilotaStats · Todos los derechos reservados',
        'todos': 'Todos los pelotaris', 'indice_titulo': 'Pelotaris: estadísticas de pelota a mano | EskupilotaStats',
        'indice_desc': 'Estadísticas de los {n} pelotaris profesionales de pelota a mano: partidos, victorias, Elo y palmarés.',
        'indice_h1': 'Pelotaris', 'festival': 'Festival', 'otro_idioma': 'Euskaraz', 'inicio': 'Inicio',
        'deducida': 'final deducida del calendario',
        'menu_comp': 'Competiciones', 'menu_fro': 'Frontones',
        'comp_titulo': '{c}: resultados, cuadro y clasificación | EskupilotaStats',
        'comp_desc': '{c} de pelota a mano: {pj} partidos{camp}. Resultados de todas las fases y clasificación de los pelotaris.',
        'comp_desc_camp': ', campeón {n}',
        'comp_ficha': 'Ver el cuadro interactivo en la web →',
        'campeones': 'Campeón', 'subcampeones': 'Subcampeón', 'en_juego': 'En juego',
        'fechas': 'Fechas', 'clasif': 'Pelotaris', 'pelotari': 'Pelotari', 'dif_c': 'Dif.',
        'resultados': 'Resultados', 'ediciones': 'Otras ediciones',
        'fase_final': 'Final', 'fase_tercero': 'Tercer puesto', 'fase_semifinal': 'Semifinales',
        'fase_cuartos': 'Cuartos de final', 'fase_octavos': 'Octavos de final',
        'fase_eliminatoria': 'Eliminatorias', 'fase_liga': 'Liguilla',
        'comp_idx_titulo': 'Competiciones de pelota a mano: campeones y resultados | EskupilotaStats',
        'comp_idx_desc': 'Campeonatos, torneos y desafíos de pelota a mano profesional desde 2022: campeones, finales, cuadros y resultados.',
        'campeonatos': 'Campeonatos', 'torneos': 'Torneos', 'desafios': 'Desafíos',
        'fro_titulo': 'Frontón {f}{c}: partidos y estadísticas | EskupilotaStats',
        'fro_desc': 'Frontón {f}{c}: {pj} partidos de pelota a mano profesional desde {desde}. '
                    'Pelotaris con más victorias, competiciones y últimos resultados.',
        'fro_ficha': 'Ver el frontón en la web →', 'mapa': 'Ver en el mapa',
        'primero': 'Primer partido', 'ultimo': 'Último partido', 'n_comp': 'Competiciones',
        'mas_victorias': 'Pelotaris con más victorias aquí', 'comps_aqui': 'Competiciones jugadas aquí',
        'fro_idx_titulo': 'Frontones de pelota a mano: partidos y estadísticas | EskupilotaStats',
        'fro_idx_desc': 'Los {n} frontones donde se ha jugado pelota a mano profesional desde 2022, con sus partidos y estadísticas.',
        'localidad': 'Localidad',
    },
    'eu': {
        'titulo': '{n}: esku pilotako estatistikak | EskupilotaStats',
        'desc': '{n}, pilotari profesionala ({rol}): {pj} partida, {pg} garaipen (%{pct}). '
                'Elo, palmaresa, azken emaitzak, bikotekideak eta frontoiak.',
        'rol_del': 'aurrelaria', 'rol_zag': 'atzelaria',
        'partidos': 'Partidak', 'victorias': 'Garaipenak', 'derrotas': 'Porrotak', 'pct': 'Garaipen %',
        'dif': 'Tanto aldea', 'elo': 'Elo', 'elo_max': 'Elo gorena', 'ranking': 'Elo sailkapena',
        'de_activos': '{n} aktiboren artean', 'modalidad': 'Modalitateka',
        'parejas': 'Binaka', 'mano': 'Buruz buru', 'cuatro': "Lau t'erdia",
        'palmares': 'Palmaresa', 'campeon': 'Txapelduna', 'finalista': 'Finalista',
        'ultimos': 'Azken partidak', 'fecha': 'Data', 'competicion': 'Txapelketa', 'fronton': 'Frontoia',
        'partido': 'Partida', 'res': 'Emaitza', 'v': 'G', 'd': 'P',
        'companeros': 'Bikotekide ohikoenak', 'frontones': 'Gehien jokatu duen frontoiak',
        'pj': 'PJ', 'ficha': 'Fitxa osoa ikusi: grafikoak, konparatzailea eta aurrez aurrekoak →',
        'actualizado': 'Datuak {f}an eguneratuak. Iturria: Baiko Pilota eta Aspe-ren emaitzak.',
        'derechos': '© 2022-2026 EskupilotaStats · Eskubide guztiak erreserbatuta',
        'todos': 'Pilotari guztiak', 'indice_titulo': 'Pilotariak: esku pilotako estatistikak | EskupilotaStats',
        'indice_desc': 'Esku pilotako {n} pilotari profesionalen estatistikak: partidak, garaipenak, Elo eta palmaresa.',
        'indice_h1': 'Pilotariak', 'festival': 'Jaialdia', 'otro_idioma': 'En castellano', 'inicio': 'Hasiera',
        'deducida': 'egutegitik ondorioztatutako finala',
        'menu_comp': 'Txapelketak', 'menu_fro': 'Frontoiak',
        'comp_titulo': '{c}: emaitzak, koadroa eta sailkapena | EskupilotaStats',
        'comp_desc': 'Esku pilotako {c}: {pj} partida{camp}. Fase guztietako emaitzak eta pilotarien sailkapena.',
        'comp_desc_camp': ', txapelduna {n}',
        'comp_ficha': 'Koadro interaktiboa webgunean ikusi →',
        'campeones': 'Txapelduna', 'subcampeones': 'Txapeldunordea', 'en_juego': 'Jokoan',
        'fechas': 'Datak', 'clasif': 'Pilotariak', 'pelotari': 'Pilotaria', 'dif_c': 'Aldea',
        'resultados': 'Emaitzak', 'ediciones': 'Beste edizioak',
        'fase_final': 'Finala', 'fase_tercero': 'Hirugarren postua', 'fase_semifinal': 'Finalerdiak',
        'fase_cuartos': 'Final-laurdenak', 'fase_octavos': 'Final-zortzirenak',
        'fase_eliminatoria': 'Kanporaketak', 'fase_liga': 'Liga fasea',
        'comp_idx_titulo': 'Esku pilotako txapelketak: txapeldunak eta emaitzak | EskupilotaStats',
        'comp_idx_desc': '2022tik esku pilota profesionaleko txapelketak, torneoak eta desafioak: txapeldunak, finalak, koadroak eta emaitzak.',
        'campeonatos': 'Txapelketak', 'torneos': 'Torneoak', 'desafios': 'Desafioak',
        'fro_titulo': '{f} frontoia{c}: partidak eta estatistikak | EskupilotaStats',
        'fro_desc': '{f} frontoia{c}: esku pilota profesionaleko {pj} partida {desde}tik. '
                    'Garaipen gehien dituzten pilotariak, txapelketak eta azken emaitzak.',
        'fro_ficha': 'Frontoia webgunean ikusi →', 'mapa': 'Mapan ikusi',
        'primero': 'Lehen partida', 'ultimo': 'Azken partida', 'n_comp': 'Txapelketak',
        'mas_victorias': 'Hemen garaipen gehien dituzten pilotariak', 'comps_aqui': 'Hemen jokatutako txapelketak',
        'fro_idx_titulo': 'Esku pilotako frontoiak: partidak eta estatistikak | EskupilotaStats',
        'fro_idx_desc': '2022tik esku pilota profesionala jokatu den {n} frontoiak, beren partida eta estatistikekin.',
        'localidad': 'Herria',
    },
}


def cargar(nombre):
    with open(os.path.join(DATA, nombre + '.json'), encoding='utf-8') as f:
        return json.load(f)


def slugify(s):
    """Igual que slugify() en js/analisis.js."""
    s = unicodedata.normalize('NFD', s or '')
    s = ''.join(c for c in s if not unicodedata.combining(c)).lower()
    return re.sub(r'[^a-z0-9]+', '-', s).strip('-')


def e(s):
    return html.escape(str(s if s is not None else ''), quote=True)


def fecha_es(iso):
    y, m, d = iso.split('-')
    return f'{d}/{m}/{y}'


def t_comp(nombre, lang):
    """Nombre de competición en el idioma (como tComp() en js/app.js, con el año)."""
    anio = (re.search(r'\b(20\d\d)\b', nombre) or [None, ''])[1]
    s = re.sub(r'\s*\b20\d\d\b', '', nombre).strip()
    if s.startswith('Festival') and not s.startswith('Festival Despedida'):
        s = 'Festival'
    if lang != 'eu':
        s = s.replace('San Fermin', 'San Fermín').replace('Desafio', 'Desafío')
        return f'{s} {anio}'.strip()
    serie = (re.search(r'\bSerie ([AB])\b', s) or [None, None])[1]
    s = re.sub(r'\s*\bSerie [AB]\b', '', s).strip()
    cuatro = r'(4 y Medio|4 1/2|4½)'
    if s == 'Festival':
        base = 'Jaialdia'
    elif re.match(r'^Campeonato Parejas', s):
        base = 'Binakako Txapelketa'
    elif re.match(r'^Campeonato Manomanista', s):
        base = 'Buruz Buruko Txapelketa'
    elif re.match(r'^Campeonato', s) and re.search(cuatro, s):
        base = "Lau t'erdiko Txapelketa"
    elif (m := re.match(r'^Torneo (.+?) ' + cuatro + '$', s)):
        base = f"{m[1]} Lau t'erdiko Txapelketa"
    elif (m := re.match(r'^Torneo (.+?) Manomanista$', s)):
        base = f'{m[1]} Buruz Buruko Txapelketa'
    elif (m := re.match(r'^Torneo (.+)$', s)):
        base = f'{m[1]} Txapelketa'
    elif (m := re.match(r'^Desaf[ií]o Urzante ?(.*)$', s)):
        base = f"{m[1] + ' ' if m[1] else ''}Urzante Desafioa"
    else:
        base = s
    return f"{base}{f' {serie} Seriea' if serie else ''} {anio}".strip()


def menos_seis_meses(d):
    m = d.month - 6
    y = d.year + (m <= 0) * -1
    m = m + 12 if m <= 0 else m
    return date(y, m, min(d.day, 28 if m == 2 else 30 if m in (4, 6, 9, 11) else 31))


def main():
    pel = {p['id']: p for p in cargar('pelotaris')}
    fro = {f['id']: f for f in cargar('frontones')}
    comp = {c['id']: c for c in cargar('competiciones')}
    partidos = cargar('partidos')      # del más reciente al más antiguo
    nombre = lambda pid: pel[pid]['nombre'] if pid else None
    eq = lambda p, k: [n for n in (nombre(p[k].get('del_id')), nombre(p[k].get('zag_id'))) if n]

    # Elo (mismo orden que el navegador: ordenación estable por fecha)
    elo, elo_max = {}, {}
    for p in sorted(partidos, key=lambda p: p['fecha']):
        t1, t2 = eq(p, 'equipo1'), eq(p, 'equipo2')
        if not t1 or not t2 or not p.get('ganador'):
            continue
        r1 = sum(elo.get(n, ELO_BASE) for n in t1) / len(t1)
        r2 = sum(elo.get(n, ELO_BASE) for n in t2) / len(t2)
        esp = 1 / (1 + 10 ** ((r2 - r1) / 400))
        k = ELO_K / 2 if (p.get('tipo') or '').startswith('festival') else ELO_K
        d = k * ((1 if p['ganador'] == 'equipo1' else 0) - esp)
        for n in t1:
            elo[n] = elo.get(n, ELO_BASE) + d
            elo_max[n] = max(elo_max.get(n, 0), elo[n])
        for n in t2:
            elo[n] = elo.get(n, ELO_BASE) - d
            elo_max[n] = max(elo_max.get(n, 0), elo[n])

    ultima = max(date.fromisoformat(p['fecha']) for p in partidos)
    corte = menos_seis_meses(ultima).isoformat()
    activos = {n for p in partidos if p['fecha'] >= corte for k in ('equipo1', 'equipo2') for n in eq(p, k)}
    ranking = sorted((n for n in elo if n in activos), key=lambda n: -elo[n])

    # Estadísticas por pelotari
    st = defaultdict(lambda: {'pj': 0, 'pg': 0, 'tf': 0, 'tc': 0, 'mod': defaultdict(lambda: [0, 0]),
                              'comp': Counter(), 'compg': Counter(), 'fro': Counter(), 'frog': Counter(),
                              'ultimos': [], 'palmares': []})
    for p in partidos:
        for k, otro, pts, ptc in (('equipo1', 'equipo2', 'puntos1', 'puntos2'), ('equipo2', 'equipo1', 'puntos2', 'puntos1')):
            gana = p['ganador'] == k
            for n in eq(p, k):
                s = st[n]
                s['pj'] += 1
                s['pg'] += gana
                s['tf'] += p[pts]
                s['tc'] += p[ptc]
                s['mod'][p.get('modalidad') or 'parejas'][0] += 1
                s['mod'][p.get('modalidad') or 'parejas'][1] += gana
                s['fro'][p['fronton_id']] += 1
                s['frog'][p['fronton_id']] += gana
                for c in eq(p, k):
                    if c != n:
                        s['comp'][c] += 1
                        s['compg'][c] += gana
                if len(s['ultimos']) < 10:
                    s['ultimos'].append((p, k))
                if p.get('fase') == 'final' and p.get('categoria') in ('campeonato', 'torneo'):
                    s['palmares'].append((p, gana))

    slugs = {n: slugify(n) for n in st}

    for lang in ('es', 'eu'):
        hoy = fecha_es(ultima.isoformat()) if lang == 'es' else ultima.isoformat().replace('-', '/')
        base = 'pelotari' if lang == 'es' else 'eu/pelotari'
        carpeta = os.path.join(RAIZ, base)
        if os.path.isdir(carpeta):
            shutil.rmtree(carpeta)
        for n, s in st.items():
            os.makedirs(os.path.join(carpeta, slugs[n]))
            with open(os.path.join(carpeta, slugs[n], 'index.html'), 'w', encoding='utf-8') as f:
                f.write(pagina_pelotari(n, s, lang, pel, fro, comp, elo, elo_max, ranking, slugs, hoy, eq))
        with open(os.path.join(carpeta, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(pagina_indice(st, lang, elo, slugs, hoy))

    sl_comp = generar_competiciones(partidos, pel, fro, comp, slugs, eq, ultima)
    sl_fro = generar_frontones(partidos, pel, fro, comp, slugs, eq, ultima)
    generar_privacidad(ultima)
    generar_porra_info(ultima)
    escribir_sitemap(sorted(slugs.values()), sl_comp, sl_fro)
    print(f'✓ {len(st)} pelotaris, {len(sl_comp)} competiciones y {len(sl_fro)} frontones × 2 idiomas; sitemap.xml actualizado')


# ─────────────────────────────────────────────────────────────────
# HTML
# ─────────────────────────────────────────────────────────────────
CSS = """
:root{--bg:#f5f7f4;--surface:#fff;--border:#e3e8e1;--green:#007A3D;--green-dark:#005a2c;--red:#C8102E;--text:#1A1A1A;--muted:#6b7a70;color-scheme:light dark}
@media (prefers-color-scheme:dark){:root{--bg:#0e1411;--surface:#151d18;--border:#29362f;--green:#1f9a57;--green-dark:#12663a;--red:#ef5361;--text:#e4ebe6;--muted:#8d9d93}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.55 Inter,system-ui,sans-serif}
header{background:linear-gradient(135deg,var(--green),var(--green-dark));padding:.7rem 1rem}
header .in{max-width:960px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;gap:1rem}
header img{height:40px;display:block}header nav{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:.2rem 1rem}header nav a{color:#fff;font-size:.8rem;text-decoration:none;font-weight:600}
main{max-width:960px;margin:0 auto;padding:1.2rem 1rem 3rem}
h1{font-size:2rem;margin:.3rem 0 0;letter-spacing:.01em}.sub{color:var(--muted);margin:0 0 1.2rem}
h2{font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin:0 0 .7rem}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem 1.1rem;margin-bottom:1rem}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:.8rem}
.kpi b{display:block;font-size:1.6rem;line-height:1.1}.kpi span{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
.g{color:var(--green)}.r{color:var(--red)}
.tw{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:.85rem}
th{text-align:left;font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);border-bottom:2px solid var(--green);padding:.35rem .4rem}
td{padding:.4rem;border-bottom:1px solid var(--border);vertical-align:top}td.n{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
a{color:var(--green)}.cta{display:inline-block;background:var(--green);color:#fff;text-decoration:none;font-weight:600;padding:.65rem 1rem;border-radius:999px;margin:.4rem 0 1rem}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}.grid2 .card{margin:0}
ul.pal{margin:0;padding-left:1.1rem}.lista{columns:3 180px;padding-left:1.1rem}
footer{color:var(--muted);font-size:.75rem;margin-top:1.5rem}
@media (max-width:600px){table.ult th:nth-child(3),table.ult td:nth-child(3),table.ult2 th:nth-child(2),table.ult2 td:nth-child(2){display:none}h1{font-size:1.6rem}}
.kpi b.s{font-size:1.05rem;padding-top:.45rem}
"""


def cabecera(lang, titulo, desc, url, alt_es, alt_eu, extra=''):
    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{e(titulo)}</title>
<meta name="description" content="{e(desc)}">
<link rel="canonical" href="{url}">
<link rel="alternate" hreflang="es" href="{alt_es}">
<link rel="alternate" hreflang="eu" href="{alt_eu}">
<link rel="alternate" hreflang="x-default" href="{alt_es}">
<meta property="og:type" content="profile">
<meta property="og:site_name" content="EskupilotaStats">
<meta property="og:title" content="{e(titulo)}">
<meta property="og:description" content="{e(desc)}">
<meta property="og:url" content="{url}">
<meta property="og:image" content="{WEB}/og-image.jpg">
<meta name="twitter:card" content="summary_large_image">
<meta name="theme-color" content="#007A3D">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon-32.png">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<style>{CSS}</style>
{extra}
</head>
<body>
"""


def barra(lang, alt_url):
    T = TXT[lang]
    inicio = '/' if lang == 'es' else '/?lang=eu'
    pre = '/' if lang == 'es' else '/eu/'
    return f"""<header><div class="in">
  <a href="{inicio}"><img src="/logo-header.png" alt="EskupilotaStats" width="126" height="40"></a>
  <nav><a href="{pre}pelotari/">{T['indice_h1']}</a><a href="{pre}competicion/">{T['menu_comp']}</a><a href="{pre}fronton/">{T['menu_fro']}</a><a href="{alt_url}" lang="{'eu' if lang == 'es' else 'es'}">{T['otro_idioma']}</a></nav>
</div></header>
"""


def pagina_pelotari(n, s, lang, pel, fro, comp, elo, elo_max, ranking, slugs, hoy, eq):
    T = TXT[lang]
    slug = slugs[n]
    url_es, url_eu = f'{WEB}/pelotari/{slug}/', f'{WEB}/eu/pelotari/{slug}/'
    url = url_es if lang == 'es' else url_eu
    rol_id = next((p.get('rol') for p in pel.values() if p['nombre'] == n), 'delantero')
    rol = T['rol_zag'] if rol_id == 'zaguero' else T['rol_del']
    pct = round(s['pg'] / s['pj'] * 100) if s['pj'] else 0
    dif = s['tf'] - s['tc']
    titulo = T['titulo'].format(n=n)
    desc = T['desc'].format(n=n, rol=rol, pj=s['pj'], pg=s['pg'], pct=pct)
    pos = ranking.index(n) + 1 if n in ranking else None
    ficha = f"/#/pelotari/{slug}" if lang == 'es' else f"/?lang=eu#/pelotari/{slug}"

    ld = {'@context': 'https://schema.org', '@type': 'Person', 'name': n, 'jobTitle': 'Pelotari',
          'url': url, 'description': desc}
    extra = f'<script type="application/ld+json">{json.dumps(ld, ensure_ascii=False)}</script>'

    kpis = [(s['pj'], T['partidos'], ''), (s['pg'], T['victorias'], 'g'), (s['pj'] - s['pg'], T['derrotas'], 'r'),
            (f'{pct}%', T['pct'], 'g' if pct >= 50 else 'r'), (f"{'+' if dif > 0 else ''}{dif}", T['dif'], 'g' if dif >= 0 else 'r')]
    if n in elo:
        kpis.append((round(elo[n]), T['elo'], ''))
        kpis.append((round(elo_max[n]), T['elo_max'], ''))
    if pos:
        kpis.append((f'{pos}º', f"{T['ranking']} ({T['de_activos'].format(n=len(ranking))})", ''))
    kpis_html = ''.join(f'<div class="kpi"><b class="{c}">{e(v)}</b><span>{e(l)}</span></div>' for v, l, c in kpis)

    mods = ''.join(f"<tr><td>{T[m]}</td><td class=n>{v[0]}</td><td class=n>{v[1]}</td>"
                   f"<td class=n>{round(v[1] / v[0] * 100)}%</td></tr>"
                   for m, v in sorted(s['mod'].items(), key=lambda x: -x[1][0]) if m in ('parejas', 'mano', 'cuatro'))

    pal = sorted(s['palmares'], key=lambda x: x[0]['fecha'], reverse=True)
    marca_deducida = f' <span title="{e(T["deducida"])}">ⓘ</span>'
    pal_html = ''.join(
        f"<li><b class=\"{'g' if g else ''}\">{T['campeon'] if g else T['finalista']}</b> · "
        f"{enlace_comp(comp[p['competicion_id']], lang)}"
        f"{marca_deducida if p.get('fase_deducida') else ''}</li>"
        for p, g in pal)

    def fila(p, k):
        otro = 'equipo2' if k == 'equipo1' else 'equipo1'
        gana = p['ganador'] == k
        suyo, rival = ' / '.join(eq(p, k)), ' / '.join(eq(p, otro))
        pts = f"{p['puntos1' if k == 'equipo1' else 'puntos2']}–{p['puntos2' if k == 'equipo1' else 'puntos1']}"
        cn = comp[p['competicion_id']]['nombre']
        nom = T['festival'] if p.get('categoria') == 'festival' else t_comp(cn, lang)
        nom = e(nom) if p.get('categoria') == 'festival' else enlace_comp(comp[p['competicion_id']], lang)
        return (f"<tr><td class=n>{fecha_es(p['fecha'])}</td><td>{nom}</td><td>{enlace_fro(fro[p['fronton_id']], lang)}</td>"
                f"<td>{e(suyo)} <span style=\"color:var(--muted)\">vs</span> {e(rival)}</td>"
                f"<td class=n><b class=\"{'g' if gana else 'r'}\">{T['v'] if gana else T['d']}</b> {pts}</td></tr>")
    ult = ''.join(fila(p, k) for p, k in s['ultimos'])

    base = '/pelotari/' if lang == 'es' else '/eu/pelotari/'
    comps = ''.join(f"<tr><td><a href=\"{base}{slugs[c]}/\">{e(c)}</a></td><td class=n>{pj}</td>"
                    f"<td class=n>{s['compg'][c]}</td><td class=n>{round(s['compg'][c] / pj * 100)}%</td></tr>"
                    for c, pj in s['comp'].most_common(6))
    frons = ''.join(f"<tr><td>{enlace_fro(fro[f], lang)}</td><td class=n>{pj}</td><td class=n>{s['frog'][f]}</td>"
                    f"<td class=n>{round(s['frog'][f] / pj * 100)}%</td></tr>"
                    for f, pj in s['fro'].most_common(6))
    th = f"<th></th><th style=text-align:right>{T['pj']}</th><th style=text-align:right>{T['v']}</th><th style=text-align:right>%</th>"

    return cabecera(lang, titulo, desc, url, url_es, url_eu, extra) + barra(lang, url_eu if lang == 'es' else url_es) + f"""<main>
<h1>{e(n)}</h1>
<p class="sub">{e(rol.capitalize())} · {T['partidos'].lower()}: {s['pj']}</p>
<a class="cta" href="{ficha}">{T['ficha']}</a>
<section class="card"><div class="kpis">{kpis_html}</div></section>
<div class="grid2">
<section class="card"><h2>{T['modalidad']}</h2><div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{mods}</tbody></table></div></section>
{f'<section class="card"><h2>{T["palmares"]}</h2><ul class="pal">{pal_html}</ul></section>' if pal_html else ''}
</div>
<section class="card" style="margin-top:1rem"><h2>{T['ultimos']}</h2><div class="tw"><table class="ult">
<thead><tr><th>{T['fecha']}</th><th>{T['competicion']}</th><th>{T['fronton']}</th><th>{T['partido']}</th><th style="text-align:right">{T['res']}</th></tr></thead>
<tbody>{ult}</tbody></table></div></section>
<div class="grid2">
{f'<section class="card"><h2>{T["companeros"]}</h2><div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{comps}</tbody></table></div></section>' if comps else ''}
<section class="card"><h2>{T['frontones']}</h2><div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{frons}</tbody></table></div></section>
</div>
<footer>{T['actualizado'].format(f=hoy)}<br>{T['derechos']}</footer>
</main>
</body>
</html>
"""


def pagina_indice(st, lang, elo, slugs, hoy):
    T = TXT[lang]
    url_es, url_eu = f'{WEB}/pelotari/', f'{WEB}/eu/pelotari/'
    url = url_es if lang == 'es' else url_eu
    base = '/pelotari/' if lang == 'es' else '/eu/pelotari/'
    items = ''.join(f"<li><a href=\"{base}{slugs[n]}/\">{e(n)}</a> <span style=\"color:var(--muted)\">· {s['pj']}</span></li>"
                    for n, s in sorted(st.items()))
    return cabecera(lang, T['indice_titulo'], T['indice_desc'].format(n=len(st)), url, url_es, url_eu) + \
        barra(lang, url_eu if lang == 'es' else url_es) + f"""<main>
<h1>{T['indice_h1']}</h1>
<p class="sub">{T['indice_desc'].format(n=len(st))}</p>
<section class="card"><ul class="lista">{items}</ul></section>
<footer>{T['actualizado'].format(f=hoy)}<br>{T['derechos']}</footer>
</main>
</body>
</html>
"""


def escribir_sitemap(slugs, sl_comp=(), sl_fro=()):
    def url(es, eu, freq='daily'):
        alt = (f'    <xhtml:link rel="alternate" hreflang="es" href="{es}"/>\n'
               f'    <xhtml:link rel="alternate" hreflang="eu" href="{eu}"/>\n'
               f'    <xhtml:link rel="alternate" hreflang="x-default" href="{es}"/>\n')
        return ''.join(f'  <url>\n    <loc>{loc}</loc>\n    <changefreq>{freq}</changefreq>\n{alt}  </url>\n'
                       for loc in (es, eu))
    cuerpo = url(f'{WEB}/', f'{WEB}/?lang=eu') + url(f'{WEB}/pelotari/', f'{WEB}/eu/pelotari/')
    cuerpo += ''.join(url(f'{WEB}/pelotari/{s}/', f'{WEB}/eu/pelotari/{s}/') for s in slugs)
    cuerpo += url(f'{WEB}/privacidad/', f'{WEB}/eu/privacidad/', 'monthly')
    cuerpo += url(f'{WEB}/porra/', f'{WEB}/eu/porra/', 'monthly')
    for tipo, lista in (('competicion', sl_comp), ('fronton', sl_fro)):
        cuerpo += url(f'{WEB}/{tipo}/', f'{WEB}/eu/{tipo}/')
        cuerpo += ''.join(url(f'{WEB}/{tipo}/{s}/', f'{WEB}/eu/{tipo}/{s}/', 'weekly') for s in lista)
    with open(os.path.join(RAIZ, 'sitemap.xml'), 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n'
                '        xmlns:xhtml="http://www.w3.org/1999/xhtml">\n' + cuerpo.replace('&', '&amp;') + '</urlset>\n')


# ─────────────────────────────────────────────────────────────────
# COMPETICIONES Y FRONTONES
# ─────────────────────────────────────────────────────────────────
FASES = ('final', 'tercero', 'semifinal', 'cuartos', 'octavos', 'eliminatoria', None)
ROMANOS = re.compile(r'^(I{1,3}|IV|V|VI{0,3}|IX|X)$')


def bonito(nombre):
    """ATANO III → Atano III, BIZKAIA FRONTOIA → Bizkaia Frontoia."""
    return ' '.join(w if ROMANOS.match(w) else w[:1].upper() + w[1:].lower() for w in (nombre or '').split())


def pre(lang):
    return '/' if lang == 'es' else '/eu/'


def con_comp(c):
    return c.get('categoria') in ('campeonato', 'torneo', 'desafio')


def enlace_comp(c, lang):
    nom = e(t_comp(c['nombre'], lang))
    return f'<a href="{pre(lang)}competicion/{slugify(c["nombre"])}/">{nom}</a>' if con_comp(c) else nom


def enlace_fro(f, lang):
    return f'<a href="{pre(lang)}fronton/{slugify(f["nombre"])}/">{e(bonito(f["nombre"]))}</a>'


def enlace_pel(n, slugs, lang):
    return f'<a href="{pre(lang)}pelotari/{slugs[n]}/">{e(n)}</a>' if n in slugs else e(n)


def base_comp(nombre):
    return re.sub(r'\s*\b20\d\d\b', '', nombre).strip()


def anio_comp(nombre):
    m = re.search(r'\b(20\d\d)\b', nombre)
    return m[1] if m else ''


def fila_partido(p, lang, fro, comp, slugs, eq, con_comp_col=False, con_fro_col=True):
    T = TXT[lang]
    def lado(k):
        txt = ' / '.join(enlace_pel(n, slugs, lang) for n in eq(p, k))
        return f'<b>{txt}</b>' if p['ganador'] == k else txt
    c = comp[p['competicion_id']]
    nom = T['festival'] if p.get('categoria') == 'festival' else enlace_comp(c, lang)
    return (f"<tr><td class=n>{fecha_es(p['fecha'])}</td>"
            + (f"<td>{nom}</td>" if con_comp_col else '')
            + (f"<td>{enlace_fro(fro[p['fronton_id']], lang)}</td>" if con_fro_col else '')
            + f"<td>{lado('equipo1')} <span style=\"color:var(--muted)\">vs</span> {lado('equipo2')}</td>"
            f"<td class=n><b>{p['puntos1']}–{p['puntos2']}</b></td></tr>")


def tabla_pelotaris(parts, lang, slugs, eq, limite=None, minimo=1):
    T = TXT[lang]
    st = defaultdict(lambda: [0, 0, 0])
    for p in parts:
        for k, a, b in (('equipo1', 'puntos1', 'puntos2'), ('equipo2', 'puntos2', 'puntos1')):
            for n in eq(p, k):
                st[n][0] += 1
                st[n][1] += p['ganador'] == k
                st[n][2] += p[a] - p[b]
    filas = sorted((x for x in st.items() if x[1][0] >= minimo), key=lambda x: (-x[1][1], -x[1][2], x[0]))[:limite]
    cuerpo = ''.join(f"<tr><td>{enlace_pel(n, slugs, lang)}</td><td class=n>{pj}</td><td class=n>{v}</td>"
                     f"<td class=n>{pj - v}</td><td class=n>{'+' if d > 0 else ''}{d}</td></tr>" for n, (pj, v, d) in filas)
    th = (f"<th>{T['pelotari']}</th><th style=text-align:right>{T['pj']}</th><th style=text-align:right>{T['v']}</th>"
          f"<th style=text-align:right>{T['d']}</th><th style=text-align:right>{T['dif_c']}</th>")
    return f'<div class="tw"><table><thead><tr>{th}</tr></thead><tbody>{cuerpo}</tbody></table></div>'


def pie(lang, hoy):
    T = TXT[lang]
    return f"<footer>{T['actualizado'].format(f=hoy)}<br>{T['derechos']}</footer>\n</main>\n</body>\n</html>\n"


def fecha_hoy(ultima, lang):
    return fecha_es(ultima.isoformat()) if lang == 'es' else ultima.isoformat().replace('-', '/')


def limpiar(tipo, lang):
    carpeta = os.path.join(RAIZ, tipo if lang == 'es' else os.path.join('eu', tipo))
    if os.path.isdir(carpeta):
        shutil.rmtree(carpeta)
    os.makedirs(carpeta)
    return carpeta


def escribir(carpeta, slug, contenido):
    destino = os.path.join(carpeta, slug) if slug else carpeta
    os.makedirs(destino, exist_ok=True)
    with open(os.path.join(destino, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(contenido)


def generar_competiciones(partidos, pel, fro, comp, slugs, eq, ultima):
    por_comp = defaultdict(list)
    for p in partidos:
        if con_comp(comp[p['competicion_id']]):
            por_comp[p['competicion_id']].append(p)
    cs = sorted((comp[c] for c in por_comp), key=lambda c: (base_comp(c['nombre']), anio_comp(c['nombre'])))
    sl = {c['id']: slugify(c['nombre']) for c in cs}
    assert len(set(sl.values())) == len(sl), 'slugs de competición repetidos'
    ediciones = defaultdict(list)
    for c in cs:
        ediciones[base_comp(c['nombre'])].append(c)

    for lang in ('es', 'eu'):
        T = TXT[lang]
        hoy = fecha_hoy(ultima, lang)
        carpeta = limpiar('competicion', lang)
        for c in cs:
            parts = por_comp[c['id']]
            final = next((p for p in parts if p.get('fase') == 'final'), None)
            camp = sub = None
            if final:
                ganador = final['ganador']
                camp = ' / '.join(eq(final, ganador))
                sub = ' / '.join(eq(final, 'equipo2' if ganador == 'equipo1' else 'equipo1'))
            nom = t_comp(c['nombre'], lang)
            slug = sl[c['id']]
            url_es, url_eu = f'{WEB}/competicion/{slug}/', f'{WEB}/eu/competicion/{slug}/'
            url = url_es if lang == 'es' else url_eu
            desc = T['comp_desc'].format(c=nom, pj=len(parts), camp=T['comp_desc_camp'].format(n=camp) if camp else '')
            ld = {'@context': 'https://schema.org', '@type': 'SportsEvent', 'name': nom, 'sport': 'Pelota vasca',
                  'url': url, 'description': desc,
                  'startDate': min(p['fecha'] for p in parts), 'endDate': max(p['fecha'] for p in parts)}
            extra = f'<script type="application/ld+json">{json.dumps(ld, ensure_ascii=False)}</script>'
            fechas = f"{fecha_es(ld['startDate'])} – {fecha_es(ld['endDate'])}"
            kpis = [(len(parts), T['partidos'], ''), (fechas, T['fechas'], 's')]
            kpis_html = ''.join(f'<div class="kpi"><b class="{cl}">{e(v)}</b><span>{e(l)}</span></div>' for v, l, cl in kpis)
            podio = ''
            if camp:
                ganadores = eq(final, final['ganador'])
                perdedores = eq(final, 'equipo2' if final['ganador'] == 'equipo1' else 'equipo1')
                podio = (f"<p>🏆 <b class=g>{T['campeones']}:</b> {' / '.join(enlace_pel(n, slugs, lang) for n in ganadores)}"
                         f" <span style=\"color:var(--muted)\">({final['puntos1']}–{final['puntos2']})</span><br>"
                         f"<b>{T['subcampeones']}:</b> {' / '.join(enlace_pel(n, slugs, lang) for n in perdedores)}</p>")
            elif c.get('categoria') != 'desafio':
                podio = f"<p><b>{T['campeones']}:</b> {T['en_juego']}</p>"

            bloques = ''
            for fase in FASES:
                ps = sorted((p for p in parts if p.get('fase') == fase), key=lambda p: p['fecha'], reverse=True)
                if not ps:
                    continue
                titulo = T['fase_' + fase] if fase else (T['resultados'] if c.get('categoria') == 'desafio' else T['fase_liga'])
                filas = ''.join(fila_partido(p, lang, fro, comp, slugs, eq) for p in ps)
                bloques += (f'<section class="card"><h2>{e(titulo)}</h2><div class="tw"><table class="ult2">'
                            f"<thead><tr><th>{T['fecha']}</th><th>{T['fronton']}</th><th>{T['partido']}</th>"
                            f"<th style=\"text-align:right\">{T['res']}</th></tr></thead><tbody>{filas}</tbody></table></div></section>")

            otras = [x for x in ediciones[base_comp(c['nombre'])] if x['id'] != c['id']]
            otras_html = ''.join(f"<li>{enlace_comp(x, lang)}</li>" for x in reversed(otras))
            ficha = f"/#/campeonato/{c['id']}" if lang == 'es' else f"/?lang=eu#/campeonato/{c['id']}"
            escribir(carpeta, slug, cabecera(lang, T['comp_titulo'].format(c=nom), desc, url, url_es, url_eu, extra)
                     + barra(lang, url_eu if lang == 'es' else url_es) + f"""<main>
<h1>{e(nom)}</h1>
{podio}
<a class="cta" href="{ficha}">{T['comp_ficha']}</a>
<section class="card"><div class="kpis">{kpis_html}</div></section>
{bloques}
<div class="grid2">
<section class="card"><h2>{T['clasif']}</h2>{tabla_pelotaris(parts, lang, slugs, eq)}</section>
{f'<section class="card"><h2>{T["ediciones"]}</h2><ul class="pal">{otras_html}</ul></section>' if otras_html else ''}
</div>
""" + pie(lang, hoy))

        # Índice: por categoría y competición, con el campeón de cada edición
        url_es, url_eu = f'{WEB}/competicion/', f'{WEB}/eu/competicion/'
        secciones = ''
        for cat, tit in (('campeonato', T['campeonatos']), ('torneo', T['torneos']), ('desafio', T['desafios'])):
            filas = ''
            for base, eds in ediciones.items():
                if eds[0].get('categoria') != cat:
                    continue
                for c in reversed(eds):
                    final = next((p for p in por_comp[c['id']] if p.get('fase') == 'final'), None)
                    camp = ' / '.join(eq(final, final['ganador'])) if final else ''
                    filas += f"<tr><td>{enlace_comp(c, lang)}</td><td>{e(camp)}</td><td class=n>{len(por_comp[c['id']])}</td></tr>"
            if filas:
                secciones += (f'<section class="card"><h2>{tit}</h2><div class="tw"><table><thead><tr><th></th>'
                              f"<th>{T['campeones']}</th><th style=text-align:right>{T['pj']}</th></tr></thead>"
                              f'<tbody>{filas}</tbody></table></div></section>')
        escribir(carpeta, None, cabecera(lang, T['comp_idx_titulo'], T['comp_idx_desc'], url_es if lang == 'es' else url_eu, url_es, url_eu)
                 + barra(lang, url_eu if lang == 'es' else url_es)
                 + f"<main>\n<h1>{T['menu_comp']}</h1>\n<p class=\"sub\">{T['comp_idx_desc']}</p>\n{secciones}\n" + pie(lang, hoy))
    return sorted(sl.values())


def generar_frontones(partidos, pel, fro, comp, slugs, eq, ultima):
    ciu = {c['id']: c for c in cargar('ciudades')}
    por_fro = defaultdict(list)
    for p in partidos:
        por_fro[p['fronton_id']].append(p)
    fs = sorted((fro[f] for f in por_fro), key=lambda f: f['nombre'])
    sl = {f['id']: slugify(f['nombre']) for f in fs}
    assert len(set(sl.values())) == len(sl), 'slugs de frontón repetidos'

    def ciudad(f, lang):
        c = ciu.get(f.get('ciudad_id')) or {}
        n = c.get('nombre_eu' if lang == 'eu' else 'nombre_es') or bonito(c.get('nombre'))
        return n if n and n.upper() != f['nombre'].upper() else ''

    for lang in ('es', 'eu'):
        T = TXT[lang]
        hoy = fecha_hoy(ultima, lang)
        carpeta = limpiar('fronton', lang)
        for f in fs:
            parts = por_fro[f['id']]          # del más reciente al más antiguo
            nom, loc = bonito(f['nombre']), ciudad(f, lang)
            slug = sl[f['id']]
            url_es, url_eu = f'{WEB}/fronton/{slug}/', f'{WEB}/eu/fronton/{slug}/'
            url = url_es if lang == 'es' else url_eu
            desde = fecha_es(parts[-1]['fecha'])
            sufijo = f' ({loc})' if loc else ''
            desc = T['fro_desc'].format(f=nom, c=sufijo, pj=len(parts), desde=desde)
            ld = {'@context': 'https://schema.org', '@type': 'SportsActivityLocation', 'name': f'Frontón {nom}', 'url': url,
                  'description': desc}
            if loc:
                ld['address'] = {'@type': 'PostalAddress', 'addressLocality': loc}
            if f.get('lat') and f.get('lon'):
                ld['geo'] = {'@type': 'GeoCoordinates', 'latitude': f['lat'], 'longitude': f['lon']}
            extra = f'<script type="application/ld+json">{json.dumps(ld, ensure_ascii=False)}</script>'
            ncomp = Counter(p['competicion_id'] for p in parts if con_comp(comp[p['competicion_id']]))
            kpis = [(len(parts), T['partidos'], ''), (desde, T['primero'], 's'), (fecha_es(parts[0]['fecha']), T['ultimo'], 's'),
                    (len(ncomp), T['n_comp'], '')]
            kpis_html = ''.join(f'<div class="kpi"><b class="{cl}">{e(v)}</b><span>{e(l)}</span></div>' for v, l, cl in kpis)
            comps_html = ''.join(f"<tr><td>{enlace_comp(comp[c], lang)}</td><td class=n>{n}</td></tr>"
                                 for c, n in sorted(ncomp.items(), key=lambda x: (anio_comp(comp[x[0]]['nombre']), x[1]), reverse=True))
            filas = ''.join(fila_partido(p, lang, fro, comp, slugs, eq, con_comp_col=True, con_fro_col=False) for p in parts[:15])
            mapa = f.get('google_maps_link') or ''
            ficha = f"/#/fronton/{slug}" if lang == 'es' else f"/?lang=eu#/fronton/{slug}"
            titulo_h1 = f'Frontón {nom}' if lang == 'es' else f'{nom} frontoia'
            escribir(carpeta, slug, cabecera(lang, T['fro_titulo'].format(f=nom, c=f', {loc}' if loc else ''), desc, url, url_es, url_eu, extra)
                     + barra(lang, url_eu if lang == 'es' else url_es) + f"""<main>
<h1>{e(titulo_h1)}</h1>
<p class="sub">{e(loc)}{' · ' if loc and mapa else ''}{f'<a href="{e(mapa)}" rel="noopener">{T["mapa"]}</a>' if mapa else ''}</p>
<a class="cta" href="{ficha}">{T['fro_ficha']}</a>
<section class="card"><div class="kpis">{kpis_html}</div></section>
<div class="grid2">
<section class="card"><h2>{T['mas_victorias']}</h2>{tabla_pelotaris(parts, lang, slugs, eq, limite=10)}</section>
{f'<section class="card"><h2>{T["comps_aqui"]}</h2><div class="tw"><table><thead><tr><th></th><th style=text-align:right>{T["pj"]}</th></tr></thead><tbody>{comps_html}</tbody></table></div></section>' if comps_html else ''}
</div>
<section class="card" style="margin-top:1rem"><h2>{T['ultimos']}</h2><div class="tw"><table class="ult2">
<thead><tr><th>{T['fecha']}</th><th>{T['competicion']}</th><th>{T['partido']}</th><th style="text-align:right">{T['res']}</th></tr></thead>
<tbody>{filas}</tbody></table></div></section>
""" + pie(lang, hoy))

        url_es, url_eu = f'{WEB}/fronton/', f'{WEB}/eu/fronton/'
        filas = ''.join(f"<tr><td>{enlace_fro(f, lang)}</td><td>{e(ciudad(f, lang))}</td><td class=n>{len(por_fro[f['id']])}</td></tr>"
                        for f in sorted(fs, key=lambda f: -len(por_fro[f['id']])))
        desc = T['fro_idx_desc'].format(n=len(fs))
        escribir(carpeta, None, cabecera(lang, T['fro_idx_titulo'], desc, url_es if lang == 'es' else url_eu, url_es, url_eu)
                 + barra(lang, url_eu if lang == 'es' else url_es)
                 + f"<main>\n<h1>{T['menu_fro']}</h1>\n<p class=\"sub\">{desc}</p>\n<section class=\"card\"><div class=\"tw\"><table>"
                 f"<thead><tr><th>{T['fronton']}</th><th>{T['localidad']}</th><th style=text-align:right>{T['pj']}</th></tr></thead>"
                 f"<tbody>{filas}</tbody></table></div></section>\n" + pie(lang, hoy))
    return sorted(sl.values())


# ─────────────────────────────────────────────────────────────────
# POLÍTICA DE PRIVACIDAD
# ─────────────────────────────────────────────────────────────────
PRIVACIDAD = {
    'es': ('Política de privacidad | EskupilotaStats',
           'Qué datos recoge EskupilotaStats (también al entrar con Google), para qué se usan, cómo se protegen y cómo borrarlos.',
           'Política de privacidad', """
<p class="sub">Última actualización: octubre de 2026</p>
<section class="card"><h2>1. Quién es el responsable</h2>
<p>EskupilotaStats (<a href="https://www.eskupilotastats.com">www.eskupilotastats.com</a>) es una web de estadísticas de
pelota a mano profesional hecha por aficionados, con una porra gratuita de pronósticos
(<a href="/porra/">qué es la porra</a>). Contacto para cualquier cuestión de privacidad:
<a href="mailto:euskopilotastats@gmail.com">euskopilotastats@gmail.com</a>.</p></section>
<section class="card"><h2>2. Si solo consultas la web</h2>
<p>No hace falta cuenta y no te pedimos ningún dato. Contamos las visitas con Cloudflare Web Analytics, que no usa
cookies ni identifica a las personas. Tus preferencias (idioma, modo oscuro, pelotaris que sigues) se guardan solo en tu
propio navegador y no nos llegan.</p></section>
<section class="card"><h2>3. Qué datos recogemos si usas la porra</h2>
<p>Para jugar a la porra hay que crear una cuenta, entrando con Google («Iniciar sesión con Google») o con un enlace
enviado a tu correo. Recogemos:</p>
<ul class="pal">
<li><b>Datos de tu cuenta de Google</b>, solo si entras con Google: tu <b>dirección de correo</b>, tu <b>nombre</b>, tu
<b>foto de perfil</b> y el <b>identificador de tu cuenta de Google</b>. Son los permisos básicos de inicio de sesión
(<i>openid</i>, <i>email</i> y <i>profile</i>). No pedimos acceso a tus contactos, Gmail, Drive ni a ningún otro servicio de Google.</li>
<li><b>El alias</b> (nombre público) que eliges para la clasificación.</li>
<li><b>Tus pronósticos</b>, los puntos que consigues y las <b>ligas privadas</b> que creas o a las que te unes.</li>
<li>Datos técnicos de la sesión (fecha de alta y del último acceso), necesarios para mantenerte conectado.</li>
</ul></section>
<section class="card"><h2>4. Para qué los usamos</h2>
<ul class="pal">
<li>Para <b>identificarte y que puedas entrar</b> en tu cuenta.</li>
<li>Para <b>guardar tus pronósticos</b>, calcular tus puntos y mostrarte en las clasificaciones y en tus ligas.</li>
<li>Para responderte si nos escribes.</li>
</ul>
<p>No usamos tus datos para publicidad ni para crear perfiles, no los vendemos y no los cedemos a terceros. Los demás
participantes solo ven tu alias, tus puntos y, cuando un partido ya ha empezado, lo que pronosticaste. Tu correo, tu
nombre real y tu foto no se muestran nunca a otros usuarios.</p>
<p><b>Uso limitado de los datos de Google:</b> el uso y la transferencia de la información recibida de las API de Google
se ajustan a la <a href="https://developers.google.com/terms/api-services-user-data-policy" rel="noopener">Política de
datos de usuario de los servicios de las API de Google</a>, incluidos los requisitos de uso limitado. Los datos de Google
solo se usan para el inicio de sesión descrito aquí y no se transfieren a nadie salvo a los proveedores técnicos del
apartado 6, ni se usan para publicidad ni para entrenar modelos de inteligencia artificial.</p></section>
<section class="card"><h2>5. Base legal</h2>
<p>Tu <b>consentimiento</b>, que das al crear la cuenta, y la necesidad de tratar los datos para prestarte el servicio
de la porra que solicitas. Puedes retirarlo en cualquier momento borrando tu cuenta.</p></section>
<section class="card"><h2>6. Dónde se guardan y cómo se protegen</h2>
<ul class="pal">
<li>En <b>Supabase</b>, con servidores en la <b>Unión Europea</b> (Fráncfort, Alemania), que actúa como encargado del tratamiento.</li>
<li>Las conexiones van <b>cifradas</b> (HTTPS/TLS) y los datos se guardan cifrados en reposo.</li>
<li>Las reglas de la base de datos impiden que un usuario lea o cambie los datos de otro; solo el responsable de la web
tiene acceso de administración.</li>
<li>El inicio de sesión con Google lo gestiona Google según su propia
<a href="https://policies.google.com/privacy" rel="noopener">política de privacidad</a>. La web se sirve a través de
GitHub Pages y Cloudflare.</li>
<li>Para mantener la sesión abierta, tu navegador guarda un identificador de sesión en su almacenamiento local. No usamos
cookies publicitarias ni de seguimiento.</li>
</ul></section>
<section class="card"><h2>7. Cuánto tiempo los guardamos</h2>
<p>Mientras tengas la cuenta. Si la borras, se eliminan <b>al momento y de forma definitiva</b> tu cuenta, tus datos de
Google, tu alias, tus pronósticos, tus ligas y tus membresías. Las copias de seguridad técnicas del proveedor se
sobrescriben en un plazo máximo de 30 días.</p></section>
<section class="card"><h2>8. Tus derechos y cómo borrar tus datos</h2>
<ul class="pal">
<li><b>Borrar tu cuenta y todos tus datos</b> tú mismo, cuando quieras: en la porra, abre «Normas, puntuación y tus
datos» y pulsa <b>«Borrar mi cuenta»</b>.</li>
<li>Pedirnos <b>acceso, rectificación, supresión, limitación, portabilidad u oposición</b> escribiendo a
<a href="mailto:euskopilotastats@gmail.com">euskopilotastats@gmail.com</a>. Respondemos en un mes como máximo.</li>
<li>Quitar el acceso de EskupilotaStats a tu cuenta de Google en
<a href="https://myaccount.google.com/permissions" rel="noopener">myaccount.google.com/permissions</a>.</li>
<li>Reclamar ante la Agencia Española de Protección de Datos (<a href="https://www.aepd.es" rel="noopener">aepd.es</a>).</li>
</ul></section>
<section class="card"><h2>9. Menores</h2>
<p>La porra no está dirigida a menores de 14 años, que no deben crear una cuenta.</p></section>
<section class="card"><h2>10. Cambios</h2>
<p>Si cambiamos esta política lo indicaremos en esta página con su fecha de actualización.</p></section>
"""),
    'eu': ('Pribatutasun politika | EskupilotaStats',
           'EskupilotaStatsek zer datu biltzen dituen (Google-rekin sartzean ere), zertarako, nola babesten diren eta nola ezabatu.',
           'Pribatutasun politika', """
<p class="sub">Azken eguneraketa: 2026ko urria</p>
<section class="card"><h2>1. Nor da arduraduna</h2>
<p>EskupilotaStats (<a href="https://www.eskupilotastats.com">www.eskupilotastats.com</a>) zaleek egindako esku pilota
profesionaleko estatistika webgunea da, iragarpenen porra doako batekin (<a href="/eu/porra/">zer da porra</a>).
Pribatutasunari buruzko harremana: <a href="mailto:euskopilotastats@gmail.com">euskopilotastats@gmail.com</a>.</p></section>
<section class="card"><h2>2. Webgunea kontsultatzen baduzu bakarrik</h2>
<p>Ez da konturik behar eta ez dizugu daturik eskatzen. Bisitak Cloudflare Web Analytics-ekin zenbatzen ditugu: ez du
cookierik erabiltzen eta ez ditu pertsonak identifikatzen. Zure hobespenak (hizkuntza, modu iluna, jarraitzen dituzun
pilotariak) zure nabigatzailean bakarrik gordetzen dira eta ez zaizkigu iristen.</p></section>
<section class="card"><h2>3. Zer datu biltzen ditugu porra erabiltzen baduzu</h2>
<p>Porran jokatzeko kontu bat sortu behar da, Google-rekin («Hasi saioa Google-rekin») edo zure postara bidalitako esteka
batekin sartuta. Hau biltzen dugu:</p>
<ul class="pal">
<li><b>Zure Google kontuko datuak</b>, Google-rekin sartzen bazara bakarrik: <b>helbide elektronikoa</b>, <b>izena</b>,
<b>profileko argazkia</b> eta <b>Google kontuaren identifikatzailea</b>. Saioa hasteko oinarrizko baimenak dira
(<i>openid</i>, <i>email</i> eta <i>profile</i>). Ez dugu zure kontaktuetarako, Gmailerako, Driverako edo Google-ren beste
zerbitzuetarako sarbiderik eskatzen.</li>
<li>Sailkapenerako aukeratzen duzun <b>aliasa</b> (izen publikoa).</li>
<li><b>Zure iragarpenak</b>, lortzen dituzun puntuak eta sortzen edo batzen zaren <b>liga pribatuak</b>.</li>
<li>Saioaren datu teknikoak (alta eta azken sarbidearen data), konektatuta mantentzeko beharrezkoak.</li>
</ul></section>
<section class="card"><h2>4. Zertarako erabiltzen ditugun</h2>
<ul class="pal">
<li><b>Zu identifikatzeko eta zure kontuan sartu ahal izateko</b>.</li>
<li><b>Zure iragarpenak gordetzeko</b>, puntuak kalkulatzeko eta sailkapenetan eta zure ligetan erakusteko.</li>
<li>Idazten badiguzu, erantzuteko.</li>
</ul>
<p>Ez ditugu zure datuak publizitaterako edo profilak sortzeko erabiltzen, ez ditugu saltzen eta ez dizkiegu hirugarrenei
lagatzen. Beste parte-hartzaileek zure aliasa, puntuak eta, partida bat hasi ondoren, iragarri zenuena bakarrik ikusten
dituzte. Zure posta, benetako izena eta argazkia ez zaizkie inoiz beste erabiltzaileei erakusten.</p>
<p><b>Google-ren datuen erabilera mugatua:</b> Google-ren APIetatik jasotako informazioaren erabilera eta transferentzia
<a href="https://developers.google.com/terms/api-services-user-data-policy" rel="noopener">Google API Services User Data
Policy</a>-ra egokitzen dira, erabilera mugatuko baldintzak barne. Google-ren datuak hemen azaldutako saio-hasierarako
bakarrik erabiltzen dira eta ez zaizkio inori transferitzen, 6. ataleko hornitzaile teknikoei izan ezik, ezta publizitaterako
edo adimen artifizialeko ereduak entrenatzeko erabiltzen ere.</p></section>
<section class="card"><h2>5. Oinarri juridikoa</h2>
<p>Kontua sortzean ematen duzun <b>baimena</b> eta eskatzen duzun porraren zerbitzua emateko datuak tratatzeko beharra.
Edozein unetan erretira dezakezu zure kontua ezabatuta.</p></section>
<section class="card"><h2>6. Non gordetzen diren eta nola babesten diren</h2>
<ul class="pal">
<li><b>Supabasen</b>, <b>Europar Batasuneko</b> zerbitzarietan (Frankfurt, Alemania), tratamenduaren eragile gisa.</li>
<li>Konexioak <b>zifratuta</b> doaz (HTTPS/TLS) eta datuak zifratuta gordetzen dira.</li>
<li>Datu-basearen arauek erabiltzaile batek beste baten datuak irakurtzea edo aldatzea eragozten dute; webgunearen
arduradunak bakarrik du administrazio-sarbidea.</li>
<li>Google-rekin saioa hastea Google-k kudeatzen du bere <a href="https://policies.google.com/privacy" rel="noopener">pribatutasun
politikaren</a> arabera. Webgunea GitHub Pages eta Cloudflare bidez zerbitzatzen da.</li>
<li>Saioa irekita mantentzeko, zure nabigatzaileak saio-identifikatzaile bat gordetzen du bere biltegi lokalean. Ez dugu
publizitate- edo jarraipen-cookierik erabiltzen.</li>
</ul></section>
<section class="card"><h2>7. Zenbat denboraz gordetzen ditugun</h2>
<p>Kontua duzun bitartean. Ezabatzen baduzu, <b>berehala eta behin betiko</b> ezabatzen dira zure kontua, Google-ren datuak,
aliasa, iragarpenak, ligak eta kidetzak. Hornitzailearen segurtasun-kopia teknikoak gehienez 30 egunean gainidazten dira.</p></section>
<section class="card"><h2>8. Zure eskubideak eta datuak nola ezabatu</h2>
<ul class="pal">
<li><b>Zure kontua eta datu guztiak ezabatu</b> zeuk, nahi duzunean: porran, ireki «Arauak, puntuazioa eta zure datuak» eta
sakatu <b>«Nire kontua ezabatu»</b>.</li>
<li><b>Sarbidea, zuzenketa, ezabaketa, mugaketa, eramangarritasuna edo aurkaratzea</b> eskatu
<a href="mailto:euskopilotastats@gmail.com">euskopilotastats@gmail.com</a> helbidera idatzita. Gehienez hilabeteko epean erantzuten dugu.</li>
<li>EskupilotaStatsek zure Google kontura duen sarbidea kendu
<a href="https://myaccount.google.com/permissions" rel="noopener">myaccount.google.com/permissions</a> helbidean.</li>
<li>Datuak Babesteko Espainiako Agentziaren aurrean erreklamatu (<a href="https://www.aepd.es" rel="noopener">aepd.es</a>).</li>
</ul></section>
<section class="card"><h2>9. Adingabeak</h2>
<p>Porra ez dago 14 urtetik beherakoei zuzendua, eta ez dute konturik sortu behar.</p></section>
<section class="card"><h2>10. Aldaketak</h2>
<p>Politika hau aldatzen badugu, orri honetan adieraziko dugu eguneraketa-datarekin.</p></section>
"""),
}

PORRA_INFO = {
    'es': ('La porra de EskupilotaStats: pronósticos de pelota a mano',
           'Porra gratuita de pelota a mano profesional: pronostica los partidos de la cartelera, suma puntos, compite en el ranking mensual y anual y crea ligas privadas con tus amigos.',
           'La porra de EskupilotaStats', """
<p class="sub">Un juego gratuito de pronósticos de pelota a mano profesional, sin apuestas ni dinero.</p>
<a class="cta" href="/#/porra">Jugar a la porra →</a>
<section class="card"><h2>Qué es</h2>
<p>EskupilotaStats es una web de estadísticas de pelota a mano profesional: resultados desde 2022, fichas de cada
pelotari, ranking Elo, campeonatos, frontones y cartelera. La <b>porra</b> es la parte en la que los aficionados
pronostican los partidos de la cartelera y compiten entre ellos por acertar más.</p></section>
<section class="card"><h2>Cómo funciona</h2>
<ul class="pal">
<li>Antes de cada partido eliges quién gana y, si quieres, los tantos del perdedor. Puedes cambiarlo hasta <b>1 hora antes</b> del inicio; después, en las ligas se ve lo que puso cada uno.</li>
<li><b>3 puntos</b> por acertar el ganador y <b>+3</b> si aciertas el resultado exacto (o <b>+1</b> si te quedas a 2 tantos o menos).</li>
<li>Hay una <b>porra por cada mes</b> y una <b>por cada torneo</b> (por ejemplo, el Campeonato de 4 y medio, serie A, B o entero).</li>
<li><b>Ranking anual:</b> al cerrar cada mes, los 50 primeros suman de 50 a 1 puntos. Los empates se deshacen con los puntos en partidos oficiales.</li>
<li>En los torneos de <b>mano a mano y 4 y medio</b> se pronostica también el <b>podio</b> (campeón 15, subcampeón 9, finalista cambiado 5, cada semifinalista 3 y +6 por el pleno).</li>
<li><b>Ligas privadas</b> de hasta 20 personas, de un torneo o de un mes, para jugar con tu cuadrilla.</li>
<li>Los resultados se recogen automáticamente y la clasificación se actualiza sola.</li>
</ul></section>
<section class="card"><h2>Tu cuenta y tus datos</h2>
<p>Para guardar tus pronósticos hace falta una cuenta: puedes entrar con <b>Google</b> o con un enlace a tu correo. Solo
usamos tu correo, tu nombre y tu foto de Google para identificarte; en las clasificaciones solo aparece el alias que eliges.
No hay publicidad ni se comparten los datos, y puedes borrar tu cuenta cuando quieras. Todos los detalles, en la
<a href="/privacidad/">política de privacidad</a>.</p></section>
<section class="card"><h2>Contacto</h2>
<p><a href="mailto:euskopilotastats@gmail.com">euskopilotastats@gmail.com</a></p></section>
"""),
    'eu': ('EskupilotaStatsen porra: esku pilotako iragarpenak',
           'Esku pilota profesionaleko doako porra: iragarri kartelerako partidak, batu puntuak, lehiatu hileko eta urteko sailkapenean eta sortu liga pribatuak lagunekin.',
           'EskupilotaStatsen porra', """
<p class="sub">Esku pilota profesionaleko iragarpenen doako jokoa, apusturik eta dirurik gabe.</p>
<a class="cta" href="/?lang=eu#/porra">Porran jokatu →</a>
<section class="card"><h2>Zer da</h2>
<p>EskupilotaStats esku pilota profesionaleko estatistika webgunea da: 2022tik aurrerako emaitzak, pilotari bakoitzaren
fitxak, Elo sailkapena, txapelketak, frontoiak eta kartelera. <b>Porra</b> zaleek kartelerako partidak iragartzen dituzten
eta gehiago asmatzeko lehiatzen diren atala da.</p></section>
<section class="card"><h2>Nola funtzionatzen du</h2>
<ul class="pal">
<li>Partida bakoitzaren aurretik nork irabaziko duen aukeratzen duzu eta, nahi baduzu, galtzailearen tantoak. Hasiera baino <b>ordubete lehenago</b> arte alda dezakezu; gero, ligetan bakoitzak jarritakoa ikusten da.</li>
<li><b>3 puntu</b> irabazlea asmatzeagatik eta <b>+3</b> emaitza zehatza asmatzen baduzu (edo <b>+1</b> 2 tanto edo gutxiagora geratzen bazara).</li>
<li><b>Hilabete bakoitzeko</b> porra bat dago eta <b>txapelketa bakoitzeko</b> beste bat (adibidez, Lau t'erdiko Txapelketa, A, B seriea edo osoa).</li>
<li><b>Urteko sailkapena:</b> hilabete bakoitza ixtean, lehen 50ek 50etik 1era puntu batzen dituzte. Berdinketak partida ofizialetako puntuek hausten dituzte.</li>
<li><b>Buruz buruko eta lau t'erdiko</b> txapelketetan <b>podioa</b> ere iragartzen da (txapelduna 15, txapeldunordea 9, finalista trukatua 5, finalerdilari bakoitza 3 eta +6 betea).</li>
<li>20 lagun arteko <b>liga pribatuak</b>, txapelketa edo hilabete batekoak, zure koadrilarekin jokatzeko.</li>
<li>Emaitzak automatikoki jasotzen dira eta sailkapena berez eguneratzen da.</li>
</ul></section>
<section class="card"><h2>Zure kontua eta datuak</h2>
<p>Iragarpenak gordetzeko kontu bat behar da: <b>Google</b>-rekin edo zure postara bidalitako esteka batekin sar zaitezke.
Google-ren posta, izena eta argazkia zu identifikatzeko bakarrik erabiltzen ditugu; sailkapenetan aukeratzen duzun aliasa
bakarrik agertzen da. Ez dago publizitaterik, datuak ez dira partekatzen eta nahi duzunean ezaba dezakezu kontua. Xehetasun
guztiak <a href="/eu/privacidad/">pribatutasun politikan</a>.</p></section>
<section class="card"><h2>Harremana</h2>
<p><a href="mailto:euskopilotastats@gmail.com">euskopilotastats@gmail.com</a></p></section>
"""),
}


def generar_porra_info(ultima):
    url_es, url_eu = f'{WEB}/porra/', f'{WEB}/eu/porra/'
    for lang in ('es', 'eu'):
        titulo, desc, h1, cuerpo = PORRA_INFO[lang]
        carpeta = limpiar('porra', lang)
        escribir(carpeta, None, cabecera(lang, titulo, desc, url_es if lang == 'es' else url_eu, url_es, url_eu)
                 + barra(lang, url_eu if lang == 'es' else url_es)
                 + f'<main>\n<h1>{h1}</h1>\n{cuerpo}\n' + pie(lang, fecha_hoy(ultima, lang)))


def generar_privacidad(ultima):
    url_es, url_eu = f'{WEB}/privacidad/', f'{WEB}/eu/privacidad/'
    for lang in ('es', 'eu'):
        titulo, desc, h1, cuerpo = PRIVACIDAD[lang]
        carpeta = limpiar('privacidad', lang)
        escribir(carpeta, None, cabecera(lang, titulo, desc, url_es if lang == 'es' else url_eu, url_es, url_eu)
                 + barra(lang, url_eu if lang == 'es' else url_es)
                 + f'<main>\n<h1>{h1}</h1>\n{cuerpo}\n' + pie(lang, fecha_hoy(ultima, lang)))


if __name__ == '__main__':
    main()
