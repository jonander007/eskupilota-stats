#!/usr/bin/env python3
"""
scraper_cartelera.py — Eskupilota Stats
Ejecutar: python3 scraper/scraper_cartelera.py
Genera:   data/cartelera.json

Lee la cartelera de cada fuente (FUENTES) y junta sus eventos: los de la
primera fuente se guardan todos y de las demás solo los que no estén ya.

Cómo se leen los carteles (Baiko):
  - 'A – B // C – D'            partido de parejas
  - 'A // B (4 1/2)'            partido individual (siempre con '//')
  - 'A – B //' + 'C – D'        partido partido en dos líneas: se une
  - 'A – B // C' + '– D'        ídem
  - '(Serie A)' en línea aparte se une a la anterior
  - 'XX', 'XXXX'                pelotari por anunciar -> '?', pendiente
  - 'PAREJAS', 'GANADORES GRUPO A'  partido pendiente
  - líneas que son nombre de competición ('Torneo San Mateo',
    'Final Torneo San Mateo (Serie B)'...) no son partidos
Una línea 'A – B' suelta NUNCA se toma como individual A contra B: es una
pareja (delantero – zaguero) cuyo rival no se ha podido leer.
"""
import json
import os
import re
import sys
from datetime import datetime

from bs4 import BeautifulSoup

from competiciones import clasificar, es_texto_competicion
from red import descargar
import aspe

# El scraper está en /scraper/, los datos en /data/
DATA_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data'))
CARTELERA_FILE = os.path.join(DATA_DIR, 'cartelera.json')

URL_BAIKO = "https://www.baikopilota.eus/entradas/"

DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}\s*-\s*\d{2}:\d{2}h$")
PRICE_RE = re.compile(r"^(?:DESDE\s+)?([\d.,]+)\s*€$", re.IGNORECASE)
SERIE_FRAGMENT_RE = re.compile(r"^\(Serie\s+[AB]\)$", re.IGNORECASE)
SERIE_RE = re.compile(r"\(\s*serie\s+([ab])\s*\)", re.IGNORECASE)
ANOTACION_RE = re.compile(r"\(([^)]*)\)")
PAREJAS_LABELS = {"PAREJAS", "BINAKA", "BIKOTEKA"}
GANADORES_RE = re.compile(r"^GANADORES?\s+(GRUPO|ELIMINATORIA)", re.IGNORECASE)
XXXX_TOKENS = {"XX", "XXX", "XXXX", "X X X X", "?"}
# A partir de aquí la página ya no es cartelera (tablas de clasificación, pie)
FIN_CARTELERA = {"Frontón", "LA REVISTA DE LA PELOTA", "Clasificación"}
NOTA_RE = re.compile(r"^(Recibirás|Gratuito|Entrada|Abono|Válid)", re.IGNORECASE)


def clean(t):
    return re.sub(r"\s+", " ", t).strip()


# ─────────────────────────────────────────────────────────────────
# Baiko: HTML -> tokens -> eventos
# ─────────────────────────────────────────────────────────────────
def tokens_baiko(html):
    soup = BeautifulSoup(html, "html.parser")
    tokens = [clean(t) for t in soup.stripped_strings if clean(t)]
    try:
        start = tokens.index("COMPRA TUS ENTRADAS") + 1
    except ValueError:
        raise RuntimeError("No encontré el inicio de la cartelera (¿ha cambiado la web?).")
    end = next((i for i, t in enumerate(tokens[start:], start) if t in FIN_CARTELERA), len(tokens))
    links = [a["href"] for a in soup.find_all("a", href=True)
             if clean(a.get_text()).lower() == "comprar entradas"]
    return tokens[start:end], links


def parse_eventos(tokens, links=()):
    """Convierte la lista de textos de la cartelera en eventos."""
    eventos = []
    links = list(links)
    i = li = 0
    while i < len(tokens):
        if not DATE_RE.match(tokens[i]):
            i += 1
            continue
        fecha, _, hora = tokens[i].partition(" - ")
        ev = {
            "fecha": fecha.strip(), "hora": hora.replace("h", "").strip(),
            "fronton": None, "ciudad": None,
            "fase": None, "competicion": None,
            "cartel": [], "precio": None,
            "agotado": False, "url": None, "tv": False,
        }
        i += 1

        # Lugar: 'Frontón, Ciudad'
        if i < len(tokens) and not DATE_RE.match(tokens[i]):
            partes = [p.strip() for p in tokens[i].split(",", 1)]
            ev["fronton"] = partes[0]
            ev["ciudad"] = partes[1] if len(partes) > 1 else partes[0]
            i += 1
        # Fase (opcional) y competición (opcional)
        if i < len(tokens) and not DATE_RE.match(tokens[i]) and not tokens[i].startswith("Campeonato") \
                and "//" not in tokens[i]:
            ev["fase"] = tokens[i]
            i += 1
        if i < len(tokens) and tokens[i].startswith("Campeonato"):
            ev["competicion"] = tokens[i]
            i += 1

        notas = []
        while i < len(tokens) and not DATE_RE.match(tokens[i]):
            t = tokens[i]
            i += 1
            tl = t.lower()
            if t.upper() in ("TV", "DIFERIDO"):
                ev["tv"] = True
            elif t.upper() == "DESDE" and i < len(tokens) and PRICE_RE.match(tokens[i]):
                ev["precio"] = PRICE_RE.match(tokens[i]).group(1).replace(",", ".")
                i += 1
            elif PRICE_RE.match(t):
                ev["precio"] = PRICE_RE.match(t).group(1).replace(",", ".")
            elif tl.startswith("gratuito"):
                ev["precio"] = "0"
            elif tl == "agotadas":
                ev["agotado"] = True
            elif tl == "comprar entradas":
                if li < len(links):
                    ev["url"] = links[li]
                    li += 1
            elif NOTA_RE.match(t) and "//" not in t:
                notas.append(t)
            else:
                ev["cartel"].append(t)
        if notas:
            ev["notas"] = notas

        ev["partidos"] = parse_partidos(ev["cartel"], ev["fecha"], ev["fase"], ev["competicion"])
        if not ev["partidos"]:
            ev["pendiente"] = True
        eventos.append(ev)
    return eventos


def eventos_baiko(html):
    tokens, links = tokens_baiko(html)
    return parse_eventos(tokens, links)


# ─────────────────────────────────────────────────────────────────
# Líneas del cartel -> partidos
# ─────────────────────────────────────────────────────────────────
def merge_serie_fragments(lines):
    """'(Serie A)' en una línea aparte pertenece a la anterior."""
    merged = []
    for line in lines:
        if SERIE_FRAGMENT_RE.match(line) and merged:
            merged[-1] = merged[-1].rstrip() + " " + line
        else:
            merged.append(line)
    return merged


def _sin_anotaciones(s):
    return ANOTACION_RE.sub("", s).strip()


def unir_lineas_partidas(lines):
    """Une partidos que la web parte en dos líneas:
    'JAKA – MARIEZKURRENA II //' + 'LARRAZABAL – IZTUETA (Serie A)'
    'LASO – MARTIJA // ZABALA'   + '– IZTUETA (Serie B)'"""
    out = []
    for line in lines:
        empieza_guion = re.match(r"^[–-]\s", line)
        if out and (_sin_anotaciones(out[-1]).endswith("//") or empieza_guion):
            out[-1] = f"{out[-1].rstrip()} {line.lstrip()}"
        else:
            out.append(line)
    return out


def _es_linea_competicion(linea):
    # '(Serie A)' sola no hace que una línea sea una competición: 'LARRAZABAL – IZTUETA (Serie A)'
    return "//" not in linea and es_texto_competicion(SERIE_RE.sub("", linea))


MODALIDAD_PREFIJO_RE = re.compile(r"^(4\s*1/2|4½|MANO A MANO)\s+", re.IGNORECASE)
# Rival aún por decidir: 'GANADORES DÍA 21', 'PERDEDOR DÍA 18', 'GANADORES ELIMINATORIA'
PLAZA_RE = re.compile(r"^(GANADOR|PERDEDOR|VENCEDOR|IRABAZLE|GALTZAILE)", re.IGNORECASE)
# Palabras que a veces se pegan al final del cartel: 'XX -XX SEMIFINAL'
COLETILLA_RE = re.compile(r"\s+(SEMIFINAL|FINAL|FINALA)$", re.IGNORECASE)


def parse_lado(texto):
    """'PEÑA II – SALAVERRI II' -> (['PEÑA II', 'SALAVERRI II'], faltan)."""
    texto = COLETILLA_RE.sub("", MODALIDAD_PREFIJO_RE.sub("", texto.strip()))
    if PLAZA_RE.match(_sin_anotaciones(texto)):
        return [_sin_anotaciones(texto)], True
    jugadores, faltan = [], False
    for p in re.split(r"\s*–\s*|\s+-\s*|\s*-\s+", texto):
        p = _sin_anotaciones(p).strip(" -–")
        if not p:
            continue
        if p.upper() in XXXX_TOKENS or re.fullmatch(r"X{2,}", p.upper()):
            jugadores.append("?")
            faltan = True
        else:
            jugadores.append(p)
    return jugadores, faltan


def parse_partidos(cartel_lines, fecha, fase, competicion):
    lineas = unir_lineas_partidas(merge_serie_fragments(cartel_lines))

    # Texto de competición del evento: 'Campeonato...', 'Torneo San Mateo', 'Masters CaixaBank'
    texto_evento = competicion
    if not texto_evento:
        texto_evento = next((l for l in lineas if _es_linea_competicion(l)), None)
    if not texto_evento and fase and es_texto_competicion(fase):
        texto_evento = fase

    series = {m.group(1).lower() for l in lineas for m in [SERIE_RE.search(l)] if m and "//" in l}
    serie_comun = series.pop() if len(series) == 1 else None

    partidos = []
    for linea in lineas:
        if _es_linea_competicion(linea) and _sin_anotaciones(linea).upper().strip() not in PAREJAS_LABELS:
            continue
        m = SERIE_RE.search(linea)
        serie = m.group(1).lower() if m else None
        anot = " ".join(a.lower() for a in ANOTACION_RE.findall(linea))
        prefijo = MODALIDAD_PREFIJO_RE.match(linea.strip())
        if prefijo:
            anot += " " + prefijo.group(1).lower()
        limpia = SERIE_RE.sub("", linea).strip()
        up = _sin_anotaciones(limpia).upper()

        if up in PAREJAS_LABELS:
            partidos.append(_partido(["?", "?"], ["?", "?"], linea, texto_evento, fecha, True,
                                     serie or serie_comun, anot, pendiente=True, etiqueta=up))
            continue
        if GANADORES_RE.match(up):
            partes = [p.strip() for p in limpia.split("//", 1)]
            eq1 = [partes[0]]
            eq2 = [partes[1]] if len(partes) > 1 and partes[1] else ["?"]
            partidos.append(_partido(eq1, eq2, linea, texto_evento, fecha, "–" in limpia,
                                     serie or serie_comun, anot, pendiente=True, etiqueta=limpia))
            continue

        if "//" in limpia:
            izq, _, der = limpia.partition("//")
            eq1, f1 = parse_lado(izq)
            eq2, f2 = parse_lado(der)
            es_pareja = len(eq1) >= 2 or len(eq2) >= 2
            # Un lado de pareja con un solo nombre: falta el compañero
            # (salvo que sea una plaza por decidir: 'GANADORES DÍA 21')
            if es_pareja:
                for eq in (eq1, eq2):
                    if not (len(eq) == 1 and PLAZA_RE.match(eq[0])):
                        eq += ["?"] * (2 - len(eq))
            eq1 = eq1 or ["?"]
            eq2 = eq2 or ["?"]
            pendiente = f1 or f2 or "?" in eq1 + eq2
            partidos.append(_partido(eq1, eq2, linea, texto_evento, fecha, es_pareja,
                                     serie or (serie_comun if es_pareja else None), anot, pendiente))
            continue

        if re.search(r"\s[–-]\s", limpia):
            # Pareja suelta: el rival no se ha podido leer
            eq1, _ = parse_lado(limpia)
            if len(eq1) == 2:
                partidos.append(_partido(eq1, ["?", "?"], linea, texto_evento, fecha, True,
                                         serie or serie_comun, anot, pendiente=True))
        # Cualquier otra línea (textos, tablas) no es un partido

    return partidos


def _partido(eq1, eq2, raw, texto_evento, fecha, es_pareja, serie, anot, pendiente=False, etiqueta=None):
    texto = texto_evento or ""
    if "4 1/2" in anot or "4½" in anot:
        texto = (texto + " 4 1/2").strip()
    elif "mano a mano" in anot or "manomanista" in anot:
        texto = (texto + " manomanista").strip()
    try:
        tipo, competicion = clasificar(texto or None, fecha, es_pareja, serie)
    except ValueError:
        tipo, competicion = ("festival" if es_pareja else "festival-mano"), "Festival"
    p = {
        "eq1": eq1, "eq2": eq2, "raw": raw,
        "tipo": tipo, "competicion": competicion,
        "serie": serie or (tipo[-1] if tipo[-2:] in ("-a", "-b") else None),
    }
    if pendiente:
        p["pendiente"] = True
    if etiqueta:
        p["etiqueta"] = etiqueta
    return p


# ─────────────────────────────────────────────────────────────────
# Varias fuentes
# ─────────────────────────────────────────────────────────────────
def _clave(s):
    s = re.sub(r"\(.*?\)", "", s or "")
    return re.sub(r"[^A-Z0-9]", "", s.upper().translate(str.maketrans("ÁÉÍÓÚÜÑ", "AEIOUUN")))


_CIUDADES = None


def _ciudad(ev):
    """Ciudad del evento con un nombre único ('Bilbo', 'Bilbao' y el frontón
    'Bizkaia Frontoia' dan todos BILBAO), usando los catálogos de data/."""
    global _CIUDADES
    if _CIUDADES is None:
        _CIUDADES = {}
        try:
            with open(os.path.join(DATA_DIR, "ciudades.json"), encoding="utf-8") as f:
                ciudades = json.load(f)
            with open(os.path.join(DATA_DIR, "frontones.json"), encoding="utf-8") as f:
                frontones = json.load(f)
        except (OSError, ValueError):
            ciudades, frontones = [], []
        por_id = {c["id"]: c["nombre"] for c in ciudades}
        for c in ciudades:
            for k in ("nombre", "nombre_es", "nombre_eu"):
                if c.get(k):
                    _CIUDADES.setdefault(_clave(c[k]), c["nombre"])
        for fr in frontones:
            if fr.get("ciudad_id") in por_id:
                _CIUDADES.setdefault("FRONTON:" + _clave(fr["nombre"]), por_id[fr["ciudad_id"]])
    for texto in (ev.get("ciudad"), *re.split(r"\s*[-/]\s*", ev.get("ciudad") or "")):
        if texto and _clave(texto) in _CIUDADES:
            return _CIUDADES[_clave(texto)]
    return _CIUDADES.get("FRONTON:" + _clave(ev.get("fronton"))) or _clave(ev.get("ciudad"))


def mismo_evento(a, b):
    """Mismo día y además: mismo frontón, algún partido en común, o misma
    hora en la misma ciudad (cuando aún no hay pelotaris anunciados)."""
    if a["fecha"] != b["fecha"]:
        return False
    fa, fb = _clave(a.get("fronton")), _clave(b.get("fronton"))
    if fa and fb and (fa == fb or fa in fb or fb in fa):
        return True
    jug = lambda ev: [{_clave(n) for n in p["eq1"] + p["eq2"] if n != "?"} for p in ev.get("partidos", [])]
    if any(len(x & y) >= 2 for x in jug(a) for y in jug(b)):
        return True
    ca, cb = _ciudad(a), _ciudad(b)
    return bool(ca) and ca == cb and (a.get("hora") or "") == (b.get("hora") or "")


def fusionar_eventos(por_fuente):
    """por_fuente: [(nombre, eventos)]. Todos los de la primera fuente y, de
    las demás, solo los eventos que no estén ya."""
    todos = []
    for nombre, eventos in por_fuente:
        # Solo se compara con lo de otras fuentes: una misma web puede
        # anunciar dos eventos distintos a la misma hora en el mismo frontón
        previos = list(todos)
        for ev in eventos:
            if any(mismo_evento(ev, e) for e in previos):
                continue
            ev["fuente"] = nombre
            todos.append(ev)
    todos.sort(key=lambda e: (datetime.strptime(e["fecha"], "%d/%m/%Y"), e.get("hora") or ""))
    return todos


FUENTES = [
    ("baiko", URL_BAIKO, eventos_baiko),
    ("aspe", aspe.URL_CARTELERA, aspe.cartelera_aspe),
]


def main():
    por_fuente, fallos = [], []
    for nombre, url, parser in FUENTES:
        print(f"Cartelera {nombre}: {url}")
        try:
            eventos = parser(descargar(url))
            print(f"  {len(eventos)} eventos")
            por_fuente.append((nombre, eventos))
        except Exception as e:  # una fuente caída no debe tumbar a las demás
            print(f"  ✗ {nombre}: {e}")
            fallos.append(nombre)
    if not por_fuente:
        print("✗ Ninguna fuente de cartelera disponible; no se toca data/cartelera.json")
        sys.exit(1)

    eventos = fusionar_eventos(por_fuente)
    output = {
        "actualizado": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "fuente": URL_BAIKO,
        "fuentes": [url for n, url, _ in FUENTES if n not in fallos],
        "partidos": eventos,
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CARTELERA_FILE, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    total = sum(len(e["partidos"]) for e in eventos)
    pendientes = sum(1 for e in eventos for p in e["partidos"] if p.get("pendiente"))
    print(f"✓ {len(eventos)} eventos, {total} partidos ({pendientes} con pelotaris por anunciar)")
    for e in eventos:
        tag = " [PENDIENTE]" if e.get("pendiente") else ""
        print(f"  {e['fecha']} {e['hora']} | {e['fronton']} | {e.get('fuente')} | {len(e['partidos'])} partidos{tag}")
    if fallos:
        sys.exit(1)


if __name__ == "__main__":
    main()
