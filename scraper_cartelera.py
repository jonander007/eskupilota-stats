#!/usr/bin/env python3
"""
scraper_cartelera.py
Ejecutar: python3 scraper_cartelera.py
Genera: data/cartelera.json

Programar con cron cada hora:
  0 * * * * cd /ruta/proyecto && python3 scraper_cartelera.py

v2 — robustez mejorada:
  - Partidos con 'XXXX': se guardan con pendiente:true y lado desconocido = ['?']
  - Líneas sueltas 'PAREJAS'/'BINAKA': partido pendiente con eq=['?','?']
  - Líneas 'GANADORES GRUPO X': partido pendiente con etiqueta
  - Fragmentos '(Serie X)' se fusionan con la línea anterior
  - Parejas sin sufijo de serie heredan serie del resto si es homogénea
  - Eventos sin ningún partido válido se marcan pendiente:true
"""
import re, json, os
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

URL = "https://www.baikopilota.eus/entradas/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,eu;q=0.8,en;q=0.7",
}
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}\s*-\s*\d{2}:\d{2}h$")
PRICE_RE = re.compile(r"^DESDE\s+([\d.,]+)\s*€$", re.IGNORECASE)
SERIE_FRAGMENT_RE = re.compile(r"^\(Serie\s+[AB]\)$", re.IGNORECASE)
PAREJAS_LABELS = {"PAREJAS", "BINAKA", "BIKOTEKA"}
GANADORES_RE = re.compile(r"^GANADORES?\s+GRUPO\s+[A-Z0-9]+", re.IGNORECASE)
XXXX_TOKENS = {"XXXX", "XXX", "X X X X"}


def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def clean(t):
    return re.sub(r"\s+", " ", t).strip()


def merge_serie_fragments(lines):
    """
    Fusiona líneas que son solo '(Serie A)' o '(Serie B)' con la línea anterior.
    El HTML de Baiko a veces parte el texto en fragmentos.
    """
    merged = []
    for line in lines:
        if SERIE_FRAGMENT_RE.match(line) and merged:
            merged[-1] = merged[-1].rstrip() + " " + line
        else:
            merged.append(line)
    return merged


def parse_cartelera(soup):
    tokens = [clean(t) for t in soup.stripped_strings if clean(t)]

    try:
        start = tokens.index("COMPRA TUS ENTRADAS") + 1
    except ValueError:
        raise RuntimeError("No encontré el inicio de la cartelera.")

    end_markers = {"Frontón", "LA REVISTA DE LA PELOTA"}
    end = next((i for i, t in enumerate(tokens[start:], start) if t in end_markers), len(tokens))
    tokens = tokens[start:end]

    links = [a["href"] for a in soup.find_all("a", href=True)
             if clean(a.get_text()).lower() == "comprar entradas"]

    festivals = []
    i = 0
    li = 0

    while i < len(tokens):
        if not DATE_RE.match(tokens[i]):
            i += 1
            continue

        raw_fecha = tokens[i]
        parts_dt = raw_fecha.split(" - ")
        fecha = parts_dt[0].strip()
        hora = parts_dt[1].replace("h", "").strip() if len(parts_dt) > 1 else ""

        f = {
            "fecha": fecha, "hora": hora,
            "fronton": None, "ciudad": None,
            "fase": None, "competicion": None,
            "cartel": [], "precio": None,
            "agotado": False, "url": None,
            "tv": False,
        }
        i += 1

        # Lugar
        if i < len(tokens) and not DATE_RE.match(tokens[i]):
            raw_lugar = tokens[i]; i += 1
            partes = [p.strip() for p in raw_lugar.split(",", 1)]
            f["fronton"] = partes[0]
            f["ciudad"] = partes[1] if len(partes) > 1 else partes[0]

        # Fase
        if i < len(tokens) and not DATE_RE.match(tokens[i]):
            tok = tokens[i]
            if not tok.startswith("Campeonato") and not DATE_RE.match(tok):
                f["fase"] = tok; i += 1

        # Competición
        if i < len(tokens) and tokens[i].startswith("Campeonato"):
            f["competicion"] = tokens[i]; i += 1

        # Cartel (recolección bruta)
        while i < len(tokens):
            t = tokens[i]
            if DATE_RE.match(t):
                break
            if t.upper() == "TV":
                f["tv"] = True; i += 1; continue
            m = PRICE_RE.match(t)
            if m:
                f["precio"] = m.group(1).replace(",", "."); i += 1; continue
            if t.lower() == "agotadas":
                f["agotado"] = True; i += 1; continue
            if t.lower() == "comprar entradas":
                if li < len(links):
                    f["url"] = links[li]; li += 1
                i += 1; continue
            f["cartel"].append(t)
            i += 1

        # Fusionar fragmentos "(Serie X)" con la línea anterior (arreglo 4)
        f["cartel"] = merge_serie_fragments(f["cartel"])

        # Parsear líneas en partidos estructurados
        f["partidos"] = parse_partidos(f["cartel"], f["fase"], f["competicion"])

        # Si ningún partido parseado → evento pendiente (bonus)
        if not f["partidos"]:
            f["pendiente"] = True

        festivals.append(f)

    return festivals


def parse_equipo(raw):
    """
    Separa una cadena 'A – B' en ['A', 'B']. Filtra XXXX y devuelve lista limpia
    junto con un flag indicando si faltaba alguna pieza.
    Devuelve (jugadores, tenia_xxxx).
    """
    tiene_xxxx = False
    resultado = []
    for p in re.split(r"\s*[–-]\s*", raw):
        p = p.strip()
        if not p:
            continue
        if p.upper() in XXXX_TOKENS:
            tiene_xxxx = True
            continue
        resultado.append(p)
    return resultado, tiene_xxxx


def serie_del_padre(fase, competicion, serie_explicita):
    """Decide si un partido es serie a o b."""
    if serie_explicita in ("a", "b"):
        return serie_explicita
    comp = (competicion or "").lower()
    if "serie b" in comp:
        return "b"
    return "a"


def guess_tipo(es_pareja, fase, competicion, serie):
    comp = (competicion or "").lower()
    fase_l = (fase or "").lower()

    if es_pareja:
        return "campeonato-b" if serie == "b" else (
            "festival" if "festival" in fase_l else "campeonato-a"
        )

    # 1 jugador por lado
    if "cuatro" in comp or "4½" in comp or "4 1/2" in comp:
        return "cuatro-medio-b" if serie == "b" else "cuatro-medio-a"
    if "manomanista" in comp or "manomanista" in fase_l:
        return "manomanista-b" if serie == "b" else "manomanista-a"
    return "manomanista-b" if serie == "b" else "manomanista-a"


def parse_partidos(cartel_lines, fase, competicion):
    """
    Cada línea se intenta interpretar como partido. Maneja:
      - 'A // B' → partido directo (mano o parejas según separadores internos)
      - 'A – B'  → mano a mano
      - 'XXXX'   → lado desconocido → pendiente:true, ['?'] (arreglo 1)
      - 'PAREJAS' / 'BINAKA' → partido pendiente parejas (arreglo 2)
      - 'GANADORES GRUPO X' → partido pendiente con etiqueta (arreglo 3)
    La serie puede venir en el sufijo '(Serie A|B)' o heredarse (arreglo 5).
    """
    # Primera pasada: clasificar líneas y capturar series explícitas
    entradas = []
    for linea in cartel_lines:
        serie_match = re.search(r"\(serie ([ab])\)", linea, re.IGNORECASE)
        serie_explicita = serie_match.group(1).lower() if serie_match else None
        linea_limpia = re.sub(r"\s*\(serie [ab]\)", "", linea, flags=re.IGNORECASE).strip()
        entradas.append({
            "raw": linea,
            "limpia": linea_limpia,
            "serie": serie_explicita,
        })

    # Determinar serie mayoritaria explícita (arreglo 5)
    series_explicitas = [e["serie"] for e in entradas if e["serie"]]
    serie_mayoritaria = None
    if series_explicitas and len(set(series_explicitas)) == 1:
        serie_mayoritaria = series_explicitas[0]

    partidos = []

    for e in entradas:
        linea = e["raw"]
        lc = e["limpia"]
        serie_expl = e["serie"]

        up = lc.upper().strip()

        # Arreglo 2: PAREJAS / BINAKA sueltos
        if up in PAREJAS_LABELS:
            serie = serie_expl or serie_del_padre(fase, competicion, serie_mayoritaria)
            partidos.append({
                "eq1": ["?", "?"], "eq2": ["?", "?"],
                "raw": linea, "tipo": guess_tipo(True, fase, competicion, serie),
                "serie": serie, "pendiente": True, "etiqueta": up,
            })
            continue

        # Arreglo 3: GANADORES GRUPO X
        if GANADORES_RE.match(up):
            serie = serie_expl or serie_del_padre(fase, competicion, serie_mayoritaria)
            # Puede venir con '//': 'GANADORES GRUPO A // GANADORES GRUPO B'
            if " // " in lc:
                partes = [p.strip() for p in lc.split(" // ", 1)]
                eq1 = [partes[0]]
                eq2 = [partes[1]] if len(partes) > 1 else ["?"]
            else:
                eq1 = ["?"]
                eq2 = ["?"]
            partidos.append({
                "eq1": eq1, "eq2": eq2,
                "raw": linea, "tipo": guess_tipo(False, fase, competicion, serie),
                "serie": serie, "pendiente": True, "etiqueta": lc,
            })
            continue

        # Partido con '//': puede ser mano a mano (1 vs 1) o parejas (2 vs 2)
        if " // " in lc:
            partes = [p.strip() for p in lc.split(" // ", 1)]
            eq1, xx1 = parse_equipo(partes[0])
            eq2, xx2 = parse_equipo(partes[1]) if len(partes) > 1 else ([], True)

            pendiente = xx1 or xx2 or (not eq1) or (not eq2)
            if not eq1:
                eq1 = ["?"]
            if not eq2:
                eq2 = ["?"]
            # Si la línea original tenía XXXX, marcar el lado con '?'
            if xx1 and len(eq1) < 2 and ("–" in partes[0] or "-" in partes[0]):
                eq1.append("?")
            if xx2 and len(partes) > 1 and len(eq2) < 2 and ("–" in partes[1] or "-" in partes[1]):
                eq2.append("?")

            es_pareja = len(eq1) >= 2 or len(eq2) >= 2
            serie = serie_expl or (
                serie_mayoritaria if (es_pareja and not serie_expl) else None
            ) or serie_del_padre(fase, competicion, None)

            partido = {
                "eq1": eq1, "eq2": eq2,
                "raw": linea,
                "tipo": guess_tipo(es_pareja, fase, competicion, serie),
                "serie": serie,
            }
            if pendiente:
                partido["pendiente"] = True
            partidos.append(partido)
            continue

        # Mano a mano sin '//' ('A – B')
        if " – " in lc or re.search(r"\s-\s", lc):
            jugadores, tenia_xxxx = parse_equipo(lc)
            if len(jugadores) >= 2:
                eq1 = [jugadores[0]]
                eq2 = [jugadores[1]]
            elif len(jugadores) == 1:
                eq1 = [jugadores[0]]
                eq2 = ["?"]
                tenia_xxxx = True
            else:
                eq1 = ["?"]; eq2 = ["?"]; tenia_xxxx = True

            serie = serie_expl or serie_del_padre(fase, competicion, serie_mayoritaria)
            partido = {
                "eq1": eq1, "eq2": eq2,
                "raw": linea,
                "tipo": guess_tipo(False, fase, competicion, serie),
                "serie": serie,
            }
            if tenia_xxxx:
                partido["pendiente"] = True
            partidos.append(partido)
            continue

        # Línea no reconocida: la dejamos fuera (no generamos partido)

    return partidos


def main():
    print(f"Scraping {URL}...")
    soup = get_soup(URL)
    festivals = parse_cartelera(soup)

    from datetime import datetime
    output = {
        "actualizado": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "fuente": URL,
        "partidos": festivals,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/cartelera.json", "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)

    total_partidos = sum(len(f["partidos"]) for f in festivals)
    pendientes = sum(1 for f in festivals for p in f["partidos"] if p.get("pendiente"))
    eventos_pendientes = sum(1 for f in festivals if f.get("pendiente"))

    print(f"✓ {len(festivals)} eventos, {total_partidos} partidos totales")
    print(f"  {pendientes} partidos marcados pendiente")
    print(f"  {eventos_pendientes} eventos sin ningún partido válido")
    for f in festivals:
        tag = " [PENDIENTE]" if f.get("pendiente") else ""
        print(f"  {f['fecha']} {f['hora']} | {f['fronton']} | {len(f['partidos'])} partidos{tag}")


if __name__ == "__main__":
    main()
