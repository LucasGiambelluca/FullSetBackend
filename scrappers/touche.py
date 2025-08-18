# scrapers/touche.py
# Actualizado: maneja categorías en BD, guarda provider y category_id
# Compatibilidad Selenium 4: usa Selenium Manager (sin webdriver_manager)

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

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# 👇 NO usamos webdriver_manager en Selenium 4 (Selenium Manager resuelve el driver)
# from webdriver_manager.chrome import ChromeDriverManager

# ————— Configuración —————
PROVIDER    = 'touche'
BASE_URL    = 'https://toucheimport.mitiendanube.com/'
HEADERS     = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
ASSETS_ROOT = 'product_assets'
ASSETS_DIR  = os.path.join(ASSETS_ROOT, PROVIDER)
os.makedirs(ASSETS_DIR, exist_ok=True)

# ————— Auxiliares —————

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '', name).strip().replace(' ', '_')

def _norm(s: str) -> str:
    """Normaliza strings para comparación (insensible a mayúsculas/espacios)."""
    return (s or "").strip().casefold()

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
        conn.execute(insert_stmt, {'provider': provider, 'name': name, 'url': url})
        result = conn.execute(select_stmt, {'provider': provider, 'name': name})
        row = result.first()
        return row.id

# ————— Categorías —————

def fetch_categories() -> list[dict]:
    r = requests.get(BASE_URL, headers=HEADERS)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    # Ajustá el selector si cambia el sitio
    menu = soup.find('ul', class_='desktop-list-subitems')
    categorias = []
    if menu:
        for a in menu.find_all('a', href=True):
            nombre = a.get_text(strip=True)
            href = a['href'].strip()
            if not href or href == '#' or 'javascript:' in href:
                continue
            url = href if '://' in href else urljoin(BASE_URL, href)
            categorias.append({'nombre': nombre, 'url': url})
    return categorias

# ————— Productos por categoría —————

def _new_chrome_driver() -> webdriver.Chrome:
    """
    Crea un Chrome headless compatible con Selenium 4 (Selenium Manager).
    No pasa argumentos posicionales (evita TypeError en __init__).
    """
    opts = Options()
    # Headless moderno en Chrome
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--log-level=3")
    opts.add_argument("--window-size=1920,1080")
    # Alinear user-agent con requests
    ua = HEADERS.get('User-Agent')
    if ua:
        opts.add_argument(f"--user-agent={ua}")
    # Si querés forzar binario (no suele hacer falta):
    # opts.binary_location = "/usr/bin/google-chrome-stable"

    # ✅ Selenium Manager resuelve el driver automáticamente
    return webdriver.Chrome(options=opts)

def fetch_products_for_category(category_url: str) -> list[dict]:
    driver = _new_chrome_driver()
    try:
        driver.get(category_url)

        # Esperar el contenedor de productos
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.js-product-table"))
        )

        # Scroll inicial para cargar
        for _ in range(12):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
            time.sleep(0.5)

        # Cargar más (si existe botón)
        while True:
            try:
                btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".js-load-more-btn"))
                )
                btn.click()
                time.sleep(1)
            except Exception:
                break

        html = driver.page_source
    finally:
        # Asegura cierre aunque falle algo
        try:
            driver.quit()
        except Exception:
            pass

    soup = BeautifulSoup(html, 'html.parser')
    cont = soup.find('div', class_='js-product-table')
    if not cont:
        return []

    items = cont.select('div.js-product-item-image-container-private')
    resultados = []
    for it in items:
        a = it.find('a', href=True)
        if not a:
            continue
        link   = urljoin(BASE_URL, a['href'])
        nombre = a.get('title') or a.get_text(strip=True)
        resultados.append({'nombre': nombre, 'link': link})
    return resultados

# ————— Guardar en DB —————

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

# ————— Descarga de assets y registro en DB —————

def update_assets_for_category(category_name: str) -> None:
    cats = fetch_categories()

    # match tolerante por nombre de categoría
    wanted = _norm(category_name)
    match = next((c for c in cats if _norm(c['nombre']) == wanted), None)
    if not match:
        raise ValueError(f"Categoría '{category_name}' no encontrada")

    # Registrar categoría y obtener ID
    category_id = get_or_create_category(PROVIDER, match['nombre'], match['url'])

    productos = fetch_products_for_category(match['url'])
    for p in productos:
        nombre = p['nombre']
        link   = p['link']

        # Detalle del producto
        resp = requests.get(link, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Descripción principal
        desc_el = soup.select_one('.description.product-description')
        descripcion = desc_el.get_text('\n', strip=True) if desc_el else ''

        # Variaciones
        vars_el = soup.select('a.js-insta-variant.btn-variant-color')
        variaciones = [a['data-option'] for a in vars_el if a.has_attr('data-option')]

        # Imágenes de alta resolución
        thumbs = soup.select('.js-swiper-product-thumbnails img') or soup.select('img.js-product-slide-img')
        image_paths = []
        for img in thumbs:
            src = img.get('data-src') or img.get('src')
            if not src:
                continue
            # intentar subir a 1024x1024 si viene con sufijo WxH
            if '-1024-1024' not in src:
                src = re.sub(r'-(\d+)-(\d+)(\.\w+)$', r'-1024-1024\3', src)
            url = src if src.startswith('http') else urljoin(BASE_URL, src)

            fname = sanitize_filename(nombre) + '_' + os.path.basename(urlparse(url).path)
            local_path = os.path.join(ASSETS_DIR, fname)

            if not os.path.exists(local_path):
                data = requests.get(url, headers=HEADERS, timeout=20).content
                with open(local_path, 'wb') as f:
                    f.write(data)
                time.sleep(0.2)  # leve backoff para no golpear el sitio

            image_paths.append(local_path)

        # Insertar en BD
        sku = sanitize_filename(nombre)
        payload = {
            'nombre':       nombre,
            'descripcion':  descripcion,
            'variaciones':  variaciones,
            'images':       image_paths
        }
        save_scraped_product(PROVIDER, sku, category_id, payload)
        time.sleep(0.5)

# ————— Ejecución manual —————
if __name__ == '__main__':
    # Ajustá el nombre según categoría existente (ver fetch_categories())
    update_assets_for_category('Ropa')
