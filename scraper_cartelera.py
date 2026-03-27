#!/usr/bin/env python3
"""
scraper_cartelera.py
Ejecutar: python3 scraper_cartelera.py
Genera: data/cartelera.json

Programar con cron cada hora:
  0 * * * * cd /ruta/proyecto && python3 scraper_cartelera.py
"""
import re, json, os
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

URL = "https://www.baikopilota.eus/entradas/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EskupilotaStats/1.0)"}
DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}\s*-\s*\d{2}:\d{2}h$")
PRICE_RE = re.compile(r"^DESDE\s+([\d.,]+)\s*€$", re.IGNORECASE)


def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def clean(t):
    return re.sub(r"\s+", " ", t).strip()


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

        raw_fecha = tokens[i]  # "27/03/2026 - 19:00h"
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

        # Cartel
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

        # Parsear líneas de cartel en partidos estructurados
        f["partidos"] = parse_partidos(f["cartel"], f["fase"], f["competicion"])
        festivals.append(f)

    return festivals


def guess_tipo(fase, competicion, linea):
    comp = (competicion or "").lower()
    fase_l = (fase or "").lower()
    if "manomanista" in comp or "manomanista" in fase_l:
        return "manomanista-a" if "serie b" not in comp else "manomanista-b"
    if "cuatro" in comp or "4½" in comp or "4 1/2" in comp:
        return "cuatro-medio-a" if "serie b" not in comp else "cuatro-medio-b"
    if "serie b" in comp:
        return "campeonato-b"
    if "festival" in fase_l:
        return "festival"
    return "campeonato-a"


def parse_equipo(raw):
    """
    Convierte 'ALTUNA III – EZKURDIA' en ['ALTUNA III', 'EZKURDIA'] (pareja)
    o 'ALTUNA III' en ['ALTUNA III'] (manomanista).
    """
    jugadores = [p.strip() for p in re.split(r"\s*[–-]\s*", raw)
                 if p.strip() and p.strip().upper() != "XXXX"]
    return jugadores


def guess_tipo_from_line(eq1, eq2, fase, competicion, serie):
    """
    Determina el tipo real del partido mirando cuántos jugadores tiene cada equipo:
    - 1 jugador por lado → manomanista o cuatro medio
    - 2 jugadores por lado → parejas
    Si el evento es manomanista pero la línea tiene 2 jugadores por lado,
    es un partido de parejas que acompaña al evento.
    """
    comp = (competicion or "").lower()
    fase_l = (fase or "").lower()
    es_pareja = len(eq1) >= 2 or len(eq2) >= 2

    if es_pareja:
        return "campeonato-b" if serie == "b" else "festival" if "festival" in fase_l else "campeonato-a"

    # Un jugador por lado
    if "cuatro" in comp or "4½" in comp or "4 1/2" in comp:
        return "cuatro-medio-b" if serie == "b" else "cuatro-medio-a"
    if "manomanista" in comp or "manomanista" in fase_l:
        return "manomanista-b" if serie == "b" else "manomanista-a"
    # Si no hay info de competición, asumir manomanista cuando es 1v1
    return "manomanista-b" if serie == "b" else "manomanista-a"


def parse_partidos(cartel_lines, fase, competicion):
    """
    Cada línea de cartel es UN partido:
      "EQ1 // EQ2"  →  equipo1 vs equipo2  (// separa los dos lados)
      "A – B"       →  mano a mano

    El tipo se determina mirando cuántos jugadores tiene cada equipo,
    no heredando ciegamente el tipo del evento padre.
    """
    partidos = []

    for linea in cartel_lines:
        serie = "b" if re.search(r"\(serie b\)", linea, re.IGNORECASE) else "a"
        linea_limpia = re.sub(r"\s*\(serie [ab]\)", "", linea, flags=re.IGNORECASE).strip()

        if " // " in linea_limpia:
            partes = [p.strip() for p in linea_limpia.split(" // ", 1)]
            eq1 = parse_equipo(partes[0])
            eq2 = parse_equipo(partes[1]) if len(partes) > 1 else []
            if eq1 or eq2:
                tipo = guess_tipo_from_line(eq1, eq2, fase, competicion, serie)
                partidos.append({
                    "eq1": eq1, "eq2": eq2,
                    "raw": linea, "tipo": tipo, "serie": serie,
                })
        elif " – " in linea_limpia or re.search(r"\s-\s", linea_limpia):
            jugadores = parse_equipo(linea_limpia)
            if len(jugadores) >= 2:
                eq1 = [jugadores[0]]
                eq2 = [jugadores[1]]
                tipo = guess_tipo_from_line(eq1, eq2, fase, competicion, serie)
                partidos.append({
                    "eq1": eq1, "eq2": eq2,
                    "raw": linea, "tipo": tipo, "serie": serie,
                })

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

    print(f"✓ {len(festivals)} partidos guardados en data/cartelera.json")
    for f in festivals:
        print(f"  {f['fecha']} {f['hora']} | {f['fronton']} | {f['fase']} | {len(f['partidos'])} partidos")


if __name__ == "__main__":
    main()
