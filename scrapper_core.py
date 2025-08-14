# scrapper_core.py

import os
import re
import pandas as pd
from pandas.errors import EmptyDataError

import scrappers.elpatron as elpatron
import scrappers.touche    as touche

# ————— Utilitarios —————

def sanitize_filename(name: str) -> str:
    """
    Quita caracteres inválidos y espacios de un nombre,
    dando un string seguro para ficheros y carpetas.
    """
    # Elimina caracteres \ / * ? : " < > | y reemplaza espacios por guiones bajos
    cleaned = re.sub(r'[\\/*?:"<>|]', "", name)
    cleaned = cleaned.strip().replace(" ", "_")
    return cleaned

# ————— Mapeo de scrapers —————

SCRAPERS = {
    "elpatron": elpatron,
    "touche":   touche,
}

def get_scraper(site: str):
    try:
        return SCRAPERS[site]
    except KeyError:
        raise ValueError(f"Site desconocido: {site!r}, elige uno de {list(SCRAPERS)}")


# ————— Categorías —————

def fetch_categories(site: str) -> list[dict]:
    scraper = get_scraper(site)
    return scraper.fetch_categories()


# ————— Productos —————

def fetch_all_products(site: str) -> list[dict]:
    scraper = get_scraper(site)
    cats = scraper.fetch_categories()
    allp = []
    for c in cats:
        prods = scraper.fetch_products_for_category(c["url"])
        for p in prods:
            p["categoria"] = c["nombre"]
            allp.append(p)
    return allp

def save_products(site: str, products: list[dict]) -> str:
    path = f"{site}_products.csv"
    df = pd.DataFrame(products)
    df.to_csv(path, index=False, encoding='utf-8-sig')
    return path

def load_products(site: str) -> list[dict]:
    path = f"{site}_products.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No hay datos guardados para '{site}'. Ejecuta POST /api/{site}/scrape primero."
        )
    try:
        df = pd.read_csv(path)
    except EmptyDataError:
        return []

    df = df.where(pd.notnull(df), None)
    if df.shape[1] == 0:
        return []
    return df.to_dict(orient="records")


# ————— Assets —————

def update_assets(site: str, category_name: str) -> None:
    scraper = get_scraper(site)
    scraper.update_assets_for_category(category_name)
