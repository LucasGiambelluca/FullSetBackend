# scrapper_core.py

from typing import Literal
import scrapers.elpatron as elpatron
import scrapers.touche    as touche

SCRAPERS = {
    "elpatron": elpatron,
    "touche":   touche,
}

def get_scraper(site: str):
    try:
        return SCRAPERS[site]
    except KeyError:
        raise ValueError(f"Site desconocido: {site!r}, elige uno de {list(SCRAPERS)}")

def fetch_categories(site: str) -> list[dict]:
    scraper = get_scraper(site)
    return scraper.fetch_categories()

def fetch_products(site: str, category_name: str) -> list[dict]:
    scraper = get_scraper(site)
    # todas las categorías para resolver la URL
    cats = scraper.fetch_categories()
    match = next((c for c in cats if c["nombre"] == category_name), None)
    if not match:
        raise ValueError(f"Categoría {category_name!r} no encontrada en site {site}")
    return scraper.fetch_products_for_category(match["url"])

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

def update_assets(site: str, category_name: str) -> None:
    scraper = get_scraper(site)
    scraper.update_assets_for_category(category_name)

