"""
Eskupilota Stats — Scraper completo
Extrae partidos de TODOS los torneos de baikopilota.eus: 2024, 2025 y 2026.

Torneos incluidos:
  - Campeonato Parejas Serie A y B
  - Campeonato Manomanista Serie A y B
  - Campeonato 4 1/2 Serie A y B
  - Torneo San Fermín (Serie A, Serie B, 4 1/2)
  - Torneo Bizkaia
  - Masters CaixaBank (Serie A y B)
  - Torneo La Blanca / Aste Nagusia / San Mateo / Donostia Hiria

Requisitos:
    pip install selenium beautifulsoup4
    + Google Chrome instalado (ChromeDriver se descarga solo con Selenium 4)

Uso:
    python scraper/scraper.py
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import json, re, time, os
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────

DATA_FILE = os.path.join(os.path.dirname(__file__), "../data/partidos.json")
TEMPORADAS = [2026, 2025, 2024]

# (tipo, nombre_base, url)
# El scraper iterará cada URL x cada temporada en TEMPORADAS
TORNEOS = [
    # ── CAMPEONATOS PRINCIPALES ───────────────────────────────────────────
    ("campeonato-a",   "Campeonato Parejas Serie A",
     "https://www.baikopilota.eus/campeonatos/campeonato-parejas-lep-m/"),

    ("campeonato-b",   "Campeonato Parejas Serie B",
     "https://www.baikopilota.eus/campeonatos/campeonato-parejas-serie-b/"),

    ("manomanista-a",  "Campeonato Manomanista Serie A",
     "https://www.baikopilota.eus/campeonatos/campeonato-manomanista-lep-m/"),

    ("manomanista-b",  "Campeonato Manomanista Serie B",
     "https://www.baikopilota.eus/campeonatos/campeonato-manomanista-serie-b/"),

    ("cuatro-medio-a", "Campeonato 4 1/2 Serie A",
     "https://www.baikopilota.eus/campeonatos/campeonato-4-1-2-lep-m/"),

    ("cuatro-medio-b", "Campeonato 4 1/2 Serie B",
     "https://www.baikopilota.eus/campeonatos/campeonato-cuatro-y-medio-serie-b/"),

    # ── TORNEO SAN FERMÍN ─────────────────────────────────────────────────
    ("festival",       "Torneo San Fermín Serie A",
     "https://www.baikopilota.eus/campeonatos/torneo-san-fermin-es/"),

    ("festival",       "Torneo San Fermín Serie B",
     "https://www.baikopilota.eus/campeonatos/torneo-san-fermin-serie-b/"),

    ("festival",       "Torneo San Fermín 4 1/2",
     "https://www.baikopilota.eus/campeonatos/torneo-san-fermin-cuatro-y-medio/"),

    # ── TORNEO BIZKAIA ────────────────────────────────────────────────────
    ("festival",       "Torneo Bizkaia",
     "https://www.baikopilota.eus/campeonatos/torneo-bizkaia-es/"),

    # ── MASTERS CAIXABANK ─────────────────────────────────────────────────
    ("festival",       "Masters CaixaBank Serie A",
     "https://www.baikopilota.eus/campeonatos/masters-caixabank/"),

    ("festival",       "Masters CaixaBank Serie B",
     "https://www.baikopilota.eus/campeonatos/masters-caixabank-serie-b/"),

    # ── TORNEOS DE VERANO / FIESTAS ───────────────────────────────────────
    ("festival",       "Torneo La Blanca Serie A",
     "https://www.baikopilota.eus/campeonatos/torneo-la-blanca-es/"),

    ("festival",       "Torneo La Blanca Serie B",
     "https://www.baikopilota.eus/campeonatos/torneo-la-blanca-serie-b/"),

    ("festival",       "Torneo Aste Nagusia Serie A",
     "https://www.baikopilota.eus/campeonatos/torneo-aste-nagusia-es/"),

    ("festival",       "Torneo Aste Nagusia Serie B",
     "https://www.baikopilota.eus/campeonatos/torneo-aste-nagusia-serie-b/"),

    ("festival",       "Torneo San Mateo Serie A",
     "https://www.baikopilota.eus/campeonatos/torneo-san-mateo-es/"),

    ("festival",       "Torneo San Mateo Serie B",
     "https://www.baikopilota.eus/campeonatos/torneo-san-mateo-serie-b/"),

    ("festival",       "Torneo Donostia Hiria Serie A",
     "https://www.baikopilota.eus/campeonatos/torneo-donostia-hiria-es/"),

    ("festival",       "Torneo Donostia Hiria Serie B",
     "https://www.baikopilota.eus/campeonatos/torneo-donostia-hiria-serie-b/"),

    # ── RESULTADOS RECIENTES (festivales sueltos) ─────────────────────────
    ("festival",       "Resultados recientes",
     "https://www.baikopilota.eus/resultados/"),
]

# ── HELPERS ───────────────────────────────────────────────────────────────────

def clave(p):
    return (f"{p['fecha']}_{p['fronton']}_"
            f"{p['equipo1']['delantero']}_{p['equipo2']['delantero']}")

# ── PARSER ────────────────────────────────────────────────────────────────────
#
# Estructura real de baikopilota.eus (confirmada inspeccionando el DOM):
#
#   <div class="... sombrabox ...">          ← un partido
#     <span class="fw-semibold">DD/MM/YYYY</span>
#     <span class="nombrefrontonsmall">Frontón - Ciudad</span>
#     <div class="... text-red ...">
#       <li class="list-inline-item">DELANTERO</li>    ← equipo 1
#       <li class="list-inline-item">ZAGUERO</li>
#     </div>
#     <div class="col-2 ... text-red"> 22 </div>       ← tantos equipo 1
#     <div class="... text-blue ...">
#       <li class="list-inline-item">DELANTERO</li>    ← equipo 2
#       <li class="list-inline-item">ZAGUERO</li>
#     </div>
#     <div class="col-2 ... text-blue"> 13 </div>      ← tantos equipo 2
#   </div>

def parsear_cards(html, tipo, competicion):
    soup = BeautifulSoup(html, "html.parser")
    partidos = []

    for card in soup.find_all("div", class_=lambda c: c and "sombrabox" in c):
        try:
            # ── Fecha ──────────────────────────────────────────────────────
            fecha_span = card.find("span", class_="fw-semibold")
            if not fecha_span:
                continue
            fecha_txt = fecha_span.get_text(strip=True)
            if not re.match(r"\d{2}/\d{2}/\d{4}", fecha_txt):
                continue
            fecha = fecha_txt

            # ── Frontón / ciudad ───────────────────────────────────────────
            fronton_span = card.find("span", class_="nombrefrontonsmall")
            fronton_txt  = fronton_span.get_text(strip=True) if fronton_span else ""
            partes  = [x.strip() for x in fronton_txt.split(" - ")]
            fronton = partes[0].upper() if partes        else ""
            ciudad  = partes[1].upper() if len(partes) > 1 else ""

            # ── Equipo rojo (equipo 1) ─────────────────────────────────────
            divs_red   = card.find_all("div", class_=lambda c: c and "text-red"  in c.split())
            names_red  = [li.get_text(strip=True).upper()
                          for d in divs_red for li in d.find_all("li")]
            tantos_red = card.find("div", class_=lambda c:
                                   c and "col-2"   in c.split()
                                   and "text-red"  in c.split())
            p1 = int(tantos_red.get_text(strip=True)) if tantos_red else None

            # ── Equipo azul (equipo 2) ─────────────────────────────────────
            divs_blue   = card.find_all("div", class_=lambda c: c and "text-blue" in c.split())
            names_blue  = [li.get_text(strip=True).upper()
                           for d in divs_blue for li in d.find_all("li")]
            tantos_blue = card.find("div", class_=lambda c:
                                    c and "col-2"    in c.split()
                                    and "text-blue"  in c.split())
            p2 = int(tantos_blue.get_text(strip=True)) if tantos_blue else None

            # ── Validar ────────────────────────────────────────────────────
            if not names_red or not names_blue or p1 is None or p2 is None:
                continue
            if p1 == 0 and p2 == 0:
                continue  # sin resultado todavía

            ganador = "equipo1" if p1 > p2 else ("equipo2" if p2 > p1 else None)

            # Manomanista: un pelotari por equipo (sin zaguero)
            partidos.append({
                "fecha":       fecha,
                "fronton":     fronton,
                "ciudad":      ciudad,
                "provincia":   "",
                "tipo":        tipo,
                "competicion": competicion,
                "equipo1": {
                    "delantero": names_red[0],
                    "zaguero":   names_red[1] if len(names_red)  > 1 else None,
                },
                "puntos1": p1,
                "equipo2": {
                    "delantero": names_blue[0],
                    "zaguero":   names_blue[1] if len(names_blue) > 1 else None,
                },
                "puntos2": p2,
                "ganador": ganador,
            })

        except Exception:
            continue

    return partidos

# ── SELENIUM ──────────────────────────────────────────────────────────────────

def crear_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)

def cerrar_cookies(driver):
    for txt in ["Aceptar", "Accept", "Aceptar todo", "Aceptar cookies"]:
        try:
            btn = driver.find_element(
                By.XPATH, f"//button[contains(normalize-space(),'{txt}')]"
            )
            btn.click()
            time.sleep(1)
            return
        except Exception:
            pass

def seleccionar_temporada(driver, anio):
    """Hace clic en el botón de temporada y espera la recarga AJAX."""
    xpaths = [
        f"//button[normalize-space()='Temporada {anio}']",
        f"//a[normalize-space()='Temporada {anio}']",
        f"//button[contains(normalize-space(),'{anio}')]",
        f"//a[contains(normalize-space(),'{anio}') and contains(normalize-space(),'Temporada')]",
        f"//li[contains(normalize-space(),'Temporada {anio}')]",
    ]
    for xpath in xpaths:
        try:
            el = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView();", el)
            driver.execute_script("arguments[0].click();", el)
            time.sleep(3)   # esperar recarga AJAX
            return True
        except Exception:
            continue
    return False

# ── MAIN ──────────────────────────────────────────────────────────────────────

def scrape():
    print("=" * 65)
    print("  ESKUPILOTA STATS — Scraper completo")
    print(f"  Torneos: {len(TORNEOS)}  |  Temporadas: {TEMPORADAS}")
    print("=" * 65)

    # Cargar existentes
    existentes = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            existentes = json.load(f)
    claves_ex = {clave(p) for p in existentes}
    print(f"  Partidos existentes: {len(existentes)}\n")

    driver = crear_driver()
    todos_nuevos = []
    cookies_cerradas = False

    try:
        for tipo, nombre, url in TORNEOS:

            # La página de resultados no tiene selector de temporada
            es_resultados = "resultados" in url
            iters = [None] if es_resultados else TEMPORADAS

            for anio in iters:
                etiqueta = f"{nombre} {anio}" if anio else nombre
                print(f"  [{tipo.upper()[:12]:12}] {etiqueta}")

                driver.get(url)
                time.sleep(3)

                # Cerrar cookies solo la primera vez
                if not cookies_cerradas:
                    cerrar_cookies(driver)
                    cookies_cerradas = True

                # Seleccionar temporada
                if anio:
                    ok = seleccionar_temporada(driver, anio)
                    if not ok:
                        print(f"     ⚠️  No encontré botón para {anio}, usando vista actual")

                html     = driver.page_source
                partidos = parsear_cards(html, tipo, etiqueta)
                nuevos   = [p for p in partidos if clave(p) not in claves_ex]
                todos_nuevos.extend(nuevos)
                claves_ex.update(clave(p) for p in nuevos)

                print(f"     Encontrados: {len(partidos):3d}  |  Nuevos: {len(nuevos):3d}")
                time.sleep(0.8)

    finally:
        driver.quit()

    # Guardar
    total = existentes + todos_nuevos
    total.sort(
        key=lambda p: datetime.strptime(p["fecha"], "%d/%m/%Y"),
        reverse=True
    )
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(total, f, ensure_ascii=False, indent=2)

    print()
    print("=" * 65)
    print(f"  ✅  Nuevos partidos añadidos : {len(todos_nuevos)}")
    print(f"  📊  Total en base de datos   : {len(total)}")
    print("=" * 65)


if __name__ == "__main__":
    scrape()
