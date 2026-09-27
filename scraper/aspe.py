"""
Eskupilota Stats — Lectores de aspepelota.eus

Devuelven lo mismo que los lectores de Baiko, para que los scrapers puedan
fusionar ambas fuentes:
  - resultados_aspe(html) -> partidos "planos" (como scraper.parse_tokens)
  - cartelera_aspe(html)  -> eventos (como scraper_cartelera.parse_eventos)

Estructura de las páginas (septiembre de 2026):

  Resultados: un <div class="herrika"> por festival con una tabla:
      td.herria      'Adarraga (Logroño)' o solo el pueblo 'Bermeo'
      td.date        '26/09/2026'
      por partido:   td.campeonato (a veces vacío), dos filas
                     td.jokalari 'Zabala  - Zabaleta' + td.result '22',
                     y td.banatu como separador

  Cartelera: un <div class="veventGrande"> por evento:
      span.location  'Adarraga (Logroño)'
      p.date         '27/09/2026' + span.date '17:15'
      por partido (separados por <hr>): p.campeonatocartelera (0 o más:
      'Final San Mateo Serie B', 'Zortzirenak // Octavos') y dos
      p.jokalari, uno por equipo. Si aún no hay pelotaris, las dos líneas
      jokalari traen el nombre de la competición ('Campeonato Eusko Label
      4 1/2 - Serie B') o 'Finala' / 'Final'.
"""

import re

from bs4 import BeautifulSoup

from competiciones import es_texto_competicion

URL_RESULTADOS = "https://aspepelota.eus/resultados/"
URL_CARTELERA = "https://aspepelota.eus/cartelera/"

FECHA_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")
HORA_RE = re.compile(r"(\d{1,2}:\d{2})")
SIN_PELOTARIS_RE = re.compile(r"^(finala?|final|finalerdiak|semifinal|xxx?x?)$", re.IGNORECASE)


def _texto(el):
    return " ".join(el.get_text(" ", strip=True).split()) if el else ""


def partir_lugar(texto):
    """'Adarraga (Logroño)' -> ('Adarraga', 'Logroño'); 'Bermeo' -> ('Bermeo', 'Bermeo')."""
    m = re.match(r"^(.*?)\s*\((.*)\)\s*$", texto)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return texto.strip(), texto.strip()


def partir_equipo(texto):
    """'Zabala  - Zabaleta' -> ['Zabala', 'Zabaleta']; 'Darío' -> ['Darío'].
    'Darío - Albisu / Iztueta' -> ['Darío', 'Albisu o Iztueta'] (como Baiko)."""
    partes = [p.strip() for p in re.split(r"\s+-\s+|\s*–\s*", texto) if p.strip()]
    return [re.sub(r"\s*/\s*", " o ", p) for p in partes]


# ─────────────────────────────────────────────────────────────────
# Resultados
# ─────────────────────────────────────────────────────────────────
def resultados_aspe(html, norm=lambda n: n):
    """Partidos jugados. `norm` normaliza los nombres (scraper.norm)."""
    soup = BeautifulSoup(html, "html.parser")
    bloques = soup.select("div.herrika")
    if not bloques:
        raise RuntimeError("Aspe resultados: no encontré ningún div.herrika (¿ha cambiado la web?)")
    partidos = []
    for bloque in bloques:
        fronton, ciudad = partir_lugar(_texto(bloque.select_one("td.herria")))
        m = FECHA_RE.search(_texto(bloque.select_one("td.date")))
        if not m:
            continue
        fecha = m.group(1)
        comp, lados = None, []
        for tr in bloque.select("tr"):
            td_comp = tr.select_one("td.campeonato")
            if td_comp is not None:
                comp = _texto(td_comp) or None
                lados = []
                continue
            jug, res = tr.select_one("td.jokalari"), tr.select_one("td.result")
            if jug is None or res is None:
                continue
            try:
                tantos = int(_texto(res))
            except ValueError:
                continue
            lados.append(([norm(n) for n in partir_equipo(_texto(jug))], tantos))
            if len(lados) == 2:
                (e1, p1), (e2, p2) = lados
                lados = []
                if not e1 or not e2 or (p1 == 0 and p2 == 0):
                    continue
                partidos.append({
                    "fecha": fecha,
                    "fronton": fronton.upper(),
                    "ciudad": ciudad.upper(),
                    "comp_texto": comp,
                    "equipo1": {"delantero": e1[0], "zaguero": e1[1] if len(e1) > 1 else None},
                    "puntos1": p1,
                    "equipo2": {"delantero": e2[0], "zaguero": e2[1] if len(e2) > 1 else None},
                    "puntos2": p2,
                    "ganador": "equipo1" if p1 > p2 else ("equipo2" if p2 > p1 else None),
                })
    return partidos


# ─────────────────────────────────────────────────────────────────
# Cartelera
# ─────────────────────────────────────────────────────────────────
def cartelera_aspe(html):
    import scraper_cartelera as SC   # aquí para evitar la importación circular

    soup = BeautifulSoup(html, "html.parser")
    divs = soup.select("div.veventGrande")
    if not divs:
        raise RuntimeError("Aspe cartelera: no encontré ningún div.veventGrande (¿ha cambiado la web?)")
    eventos = []
    for div in divs:
        fronton, ciudad = partir_lugar(_texto(div.select_one(".location")))
        p_fecha = div.select_one("p.date")
        texto_fecha = _texto(p_fecha)
        m, h = FECHA_RE.search(texto_fecha), HORA_RE.search(texto_fecha)
        if not m:
            continue
        ev = {
            "fecha": m.group(1), "hora": h.group(1) if h else "",
            "fronton": fronton, "ciudad": ciudad,
            "fase": None, "competicion": None,
            "cartel": [], "precio": None, "agotado": False,
            "url": None, "tv": bool(div.find("img", src=re.compile("eitb|etb|tele", re.I))),
            "partidos": [],
        }
        enlace = div.find("a", href=re.compile("entradas|ticket", re.I))
        if enlace:
            ev["url"] = enlace["href"]
        notas = [t for t in (_texto(p) for p in div.select("p.extra")) if t and "entradas" not in t.lower()]
        if notas:
            ev["notas"] = notas

        # Trocear por <hr>: cada trozo es un partido
        trozos, actual = [], {"titulos": [], "jok": []}
        for el in div.find_all(["p", "hr"]):
            if el.name == "hr":
                trozos.append(actual)
                actual = {"titulos": [], "jok": []}
                continue
            clase = (el.get("class") or [""])[0]
            if clase == "campeonatocartelera":
                actual["titulos"].append(_texto(el))
            elif clase == "jokalari":
                actual["jok"].append(_texto(el))
        trozos.append(actual)

        for t in trozos:
            if not t["jok"] and not t["titulos"]:
                continue
            ev["cartel"].extend(t["titulos"] + ([" // ".join(t["jok"])] if t["jok"] else []))
            sin_pelotaris = t["jok"] and all(es_texto_competicion(j) or SIN_PELOTARIS_RE.match(j) for j in t["jok"])
            titulos = t["titulos"] + (t["jok"] if sin_pelotaris else [])
            texto = " · ".join(titulos) or None
            if texto and not ev["competicion"]:
                comp = next((x for x in titulos if es_texto_competicion(x)), None)
                ev["competicion"] = comp
            if sin_pelotaris or len(t["jok"]) < 2:
                es_pareja = not re.search(r"4\s*1/2|4½|manomanista", texto or "", re.I)
                vacio = ["?", "?"] if es_pareja else ["?"]
                ev["partidos"].append(SC._partido(list(vacio), list(vacio), " // ".join(t["jok"]) or texto or "",
                                                  texto, ev["fecha"], es_pareja, None, "", pendiente=True))
                continue
            eq1, f1 = SC.parse_lado(" – ".join(partir_equipo(t["jok"][0])))
            eq2, f2 = SC.parse_lado(" – ".join(partir_equipo(t["jok"][1])))
            mayus = lambda n: n if n == "?" else " o ".join(x.upper() for x in n.split(" o "))
            eq1, eq2 = [mayus(n) for n in eq1], [mayus(n) for n in eq2]
            es_pareja = len(eq1) >= 2 or len(eq2) >= 2
            ev["partidos"].append(SC._partido(eq1, eq2, " // ".join(t["jok"]), texto, ev["fecha"], es_pareja,
                                              None, "", pendiente=f1 or f2 or "?" in eq1 + eq2))
        if not ev["partidos"]:
            ev["pendiente"] = True
        eventos.append(ev)
    return eventos
