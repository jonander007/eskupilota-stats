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

    escribir_sitemap(sorted(slugs.values()))
    print(f'✓ {len(st)} pelotaris × 2 idiomas en pelotari/ y eu/pelotari/, sitemap.xml actualizado')


# ─────────────────────────────────────────────────────────────────
# HTML
# ─────────────────────────────────────────────────────────────────
CSS = """
:root{--bg:#f5f7f4;--surface:#fff;--border:#e3e8e1;--green:#007A3D;--green-dark:#005a2c;--red:#C8102E;--text:#1A1A1A;--muted:#6b7a70;color-scheme:light dark}
@media (prefers-color-scheme:dark){:root{--bg:#0e1411;--surface:#151d18;--border:#29362f;--green:#1f9a57;--green-dark:#12663a;--red:#ef5361;--text:#e4ebe6;--muted:#8d9d93}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.55 Inter,system-ui,sans-serif}
header{background:linear-gradient(135deg,var(--green),var(--green-dark));padding:.7rem 1rem}
header .in{max-width:960px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;gap:1rem}
header img{height:40px;display:block}header nav a{color:#fff;font-size:.8rem;margin-left:1rem;text-decoration:none;font-weight:600}
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
@media (max-width:600px){table.ult th:nth-child(3),table.ult td:nth-child(3){display:none}h1{font-size:1.6rem}}
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
    lista = '/pelotari/' if lang == 'es' else '/eu/pelotari/'
    return f"""<header><div class="in">
  <a href="{inicio}"><img src="/logo-header.png" alt="EskupilotaStats" width="126" height="40"></a>
  <nav><a href="{lista}">{T['todos']}</a><a href="{alt_url}" lang="{'eu' if lang == 'es' else 'es'}">{T['otro_idioma']}</a></nav>
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
        f"{e(t_comp(comp[p['competicion_id']]['nombre'], lang))}"
        f"{marca_deducida if p.get('fase_deducida') else ''}</li>"
        for p, g in pal)

    def fila(p, k):
        otro = 'equipo2' if k == 'equipo1' else 'equipo1'
        gana = p['ganador'] == k
        suyo, rival = ' / '.join(eq(p, k)), ' / '.join(eq(p, otro))
        pts = f"{p['puntos1' if k == 'equipo1' else 'puntos2']}–{p['puntos2' if k == 'equipo1' else 'puntos1']}"
        cn = comp[p['competicion_id']]['nombre']
        nom = T['festival'] if p.get('categoria') == 'festival' else t_comp(cn, lang)
        return (f"<tr><td class=n>{fecha_es(p['fecha'])}</td><td>{e(nom)}</td><td>{e(fro[p['fronton_id']]['nombre'])}</td>"
                f"<td>{e(suyo)} <span style=\"color:var(--muted)\">vs</span> {e(rival)}</td>"
                f"<td class=n><b class=\"{'g' if gana else 'r'}\">{T['v'] if gana else T['d']}</b> {pts}</td></tr>")
    ult = ''.join(fila(p, k) for p, k in s['ultimos'])

    base = '/pelotari/' if lang == 'es' else '/eu/pelotari/'
    comps = ''.join(f"<tr><td><a href=\"{base}{slugs[c]}/\">{e(c)}</a></td><td class=n>{pj}</td>"
                    f"<td class=n>{s['compg'][c]}</td><td class=n>{round(s['compg'][c] / pj * 100)}%</td></tr>"
                    for c, pj in s['comp'].most_common(6))
    frons = ''.join(f"<tr><td>{e(fro[f]['nombre'])}</td><td class=n>{pj}</td><td class=n>{s['frog'][f]}</td>"
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


def escribir_sitemap(slugs):
    def url(es, eu, freq='daily'):
        alt = (f'    <xhtml:link rel="alternate" hreflang="es" href="{es}"/>\n'
               f'    <xhtml:link rel="alternate" hreflang="eu" href="{eu}"/>\n'
               f'    <xhtml:link rel="alternate" hreflang="x-default" href="{es}"/>\n')
        return ''.join(f'  <url>\n    <loc>{loc}</loc>\n    <changefreq>{freq}</changefreq>\n{alt}  </url>\n'
                       for loc in (es, eu))
    cuerpo = url(f'{WEB}/', f'{WEB}/?lang=eu') + url(f'{WEB}/pelotari/', f'{WEB}/eu/pelotari/')
    cuerpo += ''.join(url(f'{WEB}/pelotari/{s}/', f'{WEB}/eu/pelotari/{s}/') for s in slugs)
    with open(os.path.join(RAIZ, 'sitemap.xml'), 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n'
                '        xmlns:xhtml="http://www.w3.org/1999/xhtml">\n' + cuerpo.replace('&', '&amp;') + '</urlset>\n')


if __name__ == '__main__':
    main()
