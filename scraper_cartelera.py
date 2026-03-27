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


def parse_players(raw):
    """Convierte 'ALTUNA III – EZKURDIA' en ['ALTUNA III', 'EZKURDIA']"""
    return [p.strip() for p in re.split(r"\s*[–-]\s*", raw) if p.strip() and p.strip().upper() != "XXXX"]


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


def parse_partidos(cartel_lines, fase, competicion):
    partidos = []
    for linea in cartel_lines:
        # Formato: "EQ1 // EQ2" (dos partidos en una línea) o "EQ1" (solo un equipo/mano)
        if " // " in linea:
            partes = [p.strip() for p in linea.split(" // ")]
            for parte in partes:
                # Cada parte puede ser "A – B" (pareja) o "A" (mano)
                jugadores = parse_players(parte)
                if jugadores:
                    partidos.append({
                        "eq1": [jugadores[0]] if jugadores else [],
                        "eq2": [jugadores[1]] if len(jugadores) > 1 else [],
                        "raw": parte,
                        "tipo": guess_tipo(fase, competicion, parte),
                        "serie": "b" if re.search(r"\(serie b\)", parte, re.IGNORECASE) else "a",
                    })
        elif " – " in linea or " - " in linea:
            jugadores = parse_players(linea)
            # Una pareja: si tiene 4 jugadores es "D1/Z1 vs D2/Z2"
            # Si tiene 2 jugadores puede ser mano o pareja sin zaguero
            if len(jugadores) >= 4:
                partidos.append({
                    "eq1": jugadores[:2], "eq2": jugadores[2:4],
                    "raw": linea, "tipo": guess_tipo(fase, competicion, linea), "serie": "a",
                })
            elif len(jugadores) == 2:
                # Mano a mano o pareja sin zaguero
                partidos.append({
                    "eq1": [jugadores[0]], "eq2": [jugadores[1]],
                    "raw": linea, "tipo": guess_tipo(fase, competicion, linea), "serie": "a",
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
