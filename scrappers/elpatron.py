# scrappers/elpatron.py
# Scraper El Patrón - maneja categorías en BD, guarda provider y category_id
# Incluye perfil de Chrome temporal y limpieza para evitar colisiones entre sesiones.

import os
import re
import json
import time
import shutil
import tempfile
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from sqlalchemy import text
from connection import engine

# ————— Configuración —————
BASE_URL    = "https://elpatronimport.mitiendanube.com/"
HEADERS     = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
ASSETS_ROOT = "product_assets"
ASSETS_DIR  = os.path.join(ASSETS_ROOT, "elpatron")
PROVIDER    = "elpatron"

# Ruta del chromedriver instalada en el server
CHROMEDRIVER_PATH = "/srv/api/FullSetBackend/chromedriver-linux64/chromedriver"

# Carpeta base para perfiles temporales de Chrome (evita colisiones entre hilos/requests)
TMP_PROFILES_BASE = "/srv/api/FullSetBackend/.tmp/chrome-profiles"
os.makedirs(TMP_PROFILES_BASE, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)


# ————— Auxiliares —————

def sanitize_filename(name: str) -> str:
    """Convierte nombre a algo apto para filesystem."""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip().replace(" ", "_")


def get_or_create_category(provider: str, name: str, url: str) -> int:
    """
    Inserta en categories si no existe y devuelve el id.
    (MySQL/MariaDB usando ON DUPLICATE KEY con índice único en provider+name)
    """
    insert_stmt = text(
        "INSERT INTO categories(provider, name, url) "
        "VALUES(:provider, :name, :url) "
        "ON DUPLICATE KEY UPDATE url = VALUES(url)"
    )
    select_stmt = text(
        "SELECT id FROM categories WHERE provider = :provider AND name = :name"
    )
    with engine.begin() as conn:
        conn.execute(insert_stmt, {"provider": provider, "name": name, "url": url})
        row = conn.execute(select_stmt, {"provider": provider, "name": name}).first()
        return row.id


def fetch_categories() -> list[dict]:
    """Obtiene categorías visibles del menú principal del sitio."""
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    menu = soup.find("ul", class_="megamenu-list")
    categorias: list[dict] = []
    if menu:
        for a in menu.select("a.nav-list-link.desktop-nav-link.position-relative"):
            nombre = a.get_text(strip=True)
            href = (a.get("href") or "").strip()
            if not href or href == "#" or "javascript:" in href:
                continue
            url = href if "://" in href else urljoin(BASE_URL, href)
            categorias.append({"nombre": nombre, "url": url})
    return categorias


def get_driver():
    """
    Crea un Chrome headless con perfil temporal único y lo retorna.
    El path del perfil se cuelga en driver._user_data_dir para poder limpiarlo luego.
    """
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--remote-debugging-pipe")  # evita puertos fijos

    # ✅ perfil único por request/hilo para que no se pisen
    user_data_dir = tempfile.mkdtemp(prefix="profile-", dir=TMP_PROFILES_BASE)
    opts.add_argument(f"--user-data-dir={user_data_dir}")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)

    # Guardamos para limpieza posterior
    driver._user_data_dir = user_data_dir
    return driver


def fetch_products_for_category(category_url: str) -> list[dict]:
    """
    Usa Selenium para obtener los productos listados en una categoría.
    Devuelve [{nombre, precio, link}, ...]
    """
    driver = get_driver()
    try:
        driver.get(category_url)
        time.sleep(3)  # dejar cargar listados/render
        soup = BeautifulSoup(driver.page_source, "html.parser")

        productos: list[dict] = []
        for product in soup.select(".product"):
            try:
                name_el = product.select_one(".product-name")
                price_el = product.select_one(".price")
                link_el = product.select_one("a")

                if not name_el or not link_el:
                    continue

                name = name_el.get_text(strip=True)
                price = price_el.get_text(strip=True) if price_el else ""
                href = (link_el.get("href") or "").strip()
                link = href if "://" in href else urljoin(BASE_URL, href)

                productos.append({"nombre": name, "precio": price, "link": link})
            except Exception as e:
                print(f"⚠️ Error procesando producto: {e}")
                continue

        return productos
    finally:
        # Cerrar y limpiar el perfil temporal
        try:
            driver.quit()
        finally:
            ud = getattr(driver, "_user_data_dir", None)
            if ud:
                shutil.rmtree(ud, ignore_errors=True)


def save_scraped_product(provider: str, provider_sku: str, category_id: int, payload: dict) -> None:
    """Inserta un registro del scrapeo (como snapshot JSON)"""
    stmt = text(
        "INSERT INTO scraped_products(provider, provider_sku, category_id, fetched_at, data) "
        "VALUES(:provider, :sku, :cat_id, :fetched_at, :data_json)"
    )
    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "provider": provider,
                "sku": provider_sku,
                "cat_id": category_id,
                "fetched_at": datetime.utcnow(),
                "data_json": json.dumps(payload, ensure_ascii=False),
            },
        )


def update_assets_for_category(category_name: str) -> None:
    """
    Busca la categoría (por nombre exacto), la registra en BD si hace falta,
    scrapea sus productos, baja imágenes y guarda snapshot en scraped_products.
    """
    cats = fetch_categories()
    match = next((c for c in cats if c["nombre"] == category_name), None)
    if not match:
        raise ValueError(f"Categoría '{category_name}' no encontrada")

    # Registrar/obtener category_id en BD
    category_id = get_or_create_category(PROVIDER, category_name, match["url"])

    # Listado de productos
    productos = fetch_products_for_category(match["url"])

    for p in productos:
        nombre = p["nombre"]
        link   = p["link"]

        # Detalle del producto (sin Selenium: HTML estático suele alcanzar)
        resp = requests.get(link, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Descripción
        desc_el = soup.select_one(".description.product-description-desktop.visible-when-content-ready")
        descripcion = desc_el.get_text(separator="\n", strip=True) if desc_el else ""

        # Imágenes (miniaturas o slides)
        imgs = soup.select(".js-swiper-product-thumbnails img") or soup.select("img.js-product-slide-img")
        image_paths: list[str] = []

        for img in imgs:
            src = img.get("data-src") or img.get("src")
            if not src:
                continue
            url = urljoin(BASE_URL, src)
            fname = sanitize_filename(nombre) + "_" + os.path.basename(urlparse(src).path)
            local_path = os.path.join(ASSETS_DIR, fname)

            if not os.path.exists(local_path):
                try:
                    data = requests.get(url, headers=HEADERS, timeout=20).content
                    with open(local_path, "wb") as f:
                        f.write(data)
                    time.sleep(0.2)
                except Exception as e:
                    print(f"⚠️ No se pudo bajar imagen {url}: {e}")
                    continue

            image_paths.append(local_path)

        # Guardar en DB
        sku = sanitize_filename(nombre)
        payload = {"nombre": nombre, "descripcion": descripcion, "images": image_paths}
        save_scraped_product(PROVIDER, sku, category_id, payload)

        # Evita overloading del sitio
        time.sleep(0.4)


# ————— Ejecución manual —————
if __name__ == "__main__":
    # Ajusta el nombre según categoría existente (por ejemplo, 'ANILLOS', 'CAMPERAS', etc.)
    update_assets_for_category("Todos")
