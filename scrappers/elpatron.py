# scrapers/elpatron.py
# Actualizado: maneja categorías en BD, guarda provider y category_id

import os
import re
import json
import time
import requests
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from sqlalchemy import text
from connection import engine

# ————— Configuración —————
BASE_URL    = 'https://elpatronimport.mitiendanube.com/'
HEADERS     = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
ASSETS_ROOT = 'product_assets'
ASSETS_DIR  = os.path.join(ASSETS_ROOT, 'elpatron')
PROVIDER    = 'elpatron'

os.makedirs(ASSETS_DIR, exist_ok=True)

# ————— Auxiliares —————

import tempfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def get_driver():
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    # ✅ usar un directorio temporal único para cada sesión
    user_data_dir = tempfile.mkdtemp()
    opts.add_argument(f"--user-data-dir={user_data_dir}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver




def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '', name).strip().replace(' ', '_')


def get_or_create_category(provider: str, name: str, url: str) -> int:
    """
    Inserta en categories si no existe y devuelve el id.
    """
    insert_stmt = text(
        "INSERT INTO categories(provider, name, url)"
        " VALUES(:provider, :name, :url)"
        " ON DUPLICATE KEY UPDATE url = VALUES(url)"
    )
    select_stmt = text(
        "SELECT id FROM categories"
        " WHERE provider = :provider AND name = :name"
    )
    with engine.begin() as conn:
        conn.execute(
            insert_stmt,
            {'provider': provider, 'name': name, 'url': url}
        )
        result = conn.execute(
            select_stmt,
            {'provider': provider, 'name': name}
        )
        row = result.first()
        return row.id


def fetch_categories() -> list[dict]:
    resp = requests.get(BASE_URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    menu = soup.find('ul', class_='megamenu-list')
    categorias = []
    if menu:
        for a in menu.select('a.nav-list-link.desktop-nav-link.position-relative'):
            nombre = a.get_text(strip=True)
            href = a.get('href','').strip()
            if not href or href == '#' or 'javascript:' in href:
                continue
            url = href if '://' in href else urljoin(BASE_URL, href)
            categorias.append({'nombre': nombre, 'url': url})
    return categorias

""" def fetch_products_for_category(category_url):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup
    import time

    service = Service("/usr/local/bin/chromedriver")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--log-level=3")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={HEADERS['User-Agent']}")

    driver = webdriver.Chrome(service=service, options=opts)

    try:
        driver.get(category_url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        productos = []
        for product in soup.select(".product"):
            try:
                name = product.select_one(".product-name").get_text(strip=True)
                price = product.select_one(".price").get_text(strip=True)
                link = product.select_one("a")["href"]
                productos.append({
                    "name": name,
                    "price": price,
                    "link": link
                })
            except Exception as e:
                print(f"Error procesando producto: {e}")
                continue

        return productos
    finally:
        driver.quit()
 """



def fetch_products_for_category(category_url):
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from bs4 import BeautifulSoup
    import time

    service = Service("/usr/local/bin/chromedriver")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--log-level=3")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={HEADERS['User-Agent']}")

    driver = get_driver()

    try:
        driver.get(category_url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")

        productos = []
        for product in soup.select(".product"):
            try:
                name = product.select_one(".product-name").get_text(strip=True)
                price = product.select_one(".price").get_text(strip=True)
                link = product.select_one("a")["href"]
                productos.append({
                    "name": name,
                    "price": price,
                    "link": link
                })
            except Exception as e:
                print(f"Error procesando producto: {e}")
                continue

        return productos
    finally:
        driver.quit()


def save_scraped_product(provider: str, provider_sku: str, category_id: int, payload: dict) -> None:
    stmt = text(
        "INSERT INTO scraped_products(provider, provider_sku, category_id, fetched_at, data)"
        " VALUES(:provider, :sku, :cat_id, :fetched_at, :data_json)"
    )
    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                'provider':   provider,
                'sku':        provider_sku,
                'cat_id':     category_id,
                'fetched_at': datetime.utcnow(),
                'data_json':  json.dumps(payload, ensure_ascii=False)
            }
        )


def update_assets_for_category(category_name: str) -> None:
    cats = fetch_categories()
    match = next((c for c in cats if c['nombre'] == category_name), None)
    if not match:
        raise ValueError(f"Categoría '{category_name}' no encontrada")
    # Registrar categoría en BD
    category_id = get_or_create_category(
        PROVIDER, category_name, match['url']
    )
    productos = fetch_products_for_category(match['url'])
    for p in productos:
        nombre = p['nombre']
        link   = p['link']
        # Detalle
        resp = requests.get(link, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        # Descripción
        desc_el = soup.select_one(
            '.description.product-description-desktop.visible-when-content-ready'
        )
        descripcion = desc_el.get_text(separator='\n', strip=True) if desc_el else ''
        # Imágenes
        imgs = soup.select('.js-swiper-product-thumbnails img') or soup.select('img.js-product-slide-img')
        image_paths = []
        for img in imgs:
            src = img.get('data-src') or img.get('src')
            if not src:
                continue
            url = urljoin(BASE_URL, src)
            fname = sanitize_filename(nombre) + '_' + os.path.basename(urlparse(src).path)
            local_path = os.path.join(ASSETS_DIR, fname)
            if not os.path.exists(local_path):
                data = requests.get(url, headers=HEADERS).content
                with open(local_path, 'wb') as f:
                    f.write(data)
                time.sleep(0.2)
            image_paths.append(local_path)
        # Guardar en DB
        sku = sanitize_filename(nombre)
        payload = {
            'nombre':      nombre,
            'descripcion': descripcion,
            'images':      image_paths
        }
        save_scraped_product(PROVIDER, sku, category_id, payload)
        time.sleep(0.5)


# ————— Ejecución manual —————
if __name__ == '__main__':
    update_assets_for_category('Todos')  # Ajusta el nombre según categoría existente
