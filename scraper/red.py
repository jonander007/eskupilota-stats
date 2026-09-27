"""
Eskupilota Stats — Descarga de páginas para los scrapers

Una sola función con los reintentos, el tiempo de espera y las cabeceras que
usan todos los scrapers, para que un fallo puntual de la red o del servidor
no deje un día sin datos.
"""

import time

import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,eu;q=0.8,en;q=0.7",
}


def descargar(url, intentos=3, espera=5, timeout=30):
    """Devuelve el HTML de `url`. Reintenta errores de red y respuestas 5xx/429
    con espera creciente (5 s, 10 s...). Un 4xx distinto de 429 no se reintenta."""
    ultimo = None
    for n in range(1, intentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            # Algunas webs no declaran el charset y requests asume latin-1
            if not r.encoding or r.encoding.lower() == 'iso-8859-1':
                r.encoding = r.apparent_encoding or 'utf-8'
            return r.text
        except requests.HTTPError as e:
            codigo = e.response.status_code if e.response is not None else None
            if codigo and 400 <= codigo < 500 and codigo != 429:
                raise
            ultimo = e
        except requests.RequestException as e:
            ultimo = e
        if n < intentos:
            print(f"  ! {url}: {ultimo} — reintento {n}/{intentos - 1} en {espera * n} s")
            time.sleep(espera * n)
    raise ultimo
