# main.py

import os
import json
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query, Body, UploadFile, File
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from urllib.parse import urlparse
from uuid import uuid4
from pydantic import BaseModel

import scrapper_core as core
from connection import engine

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # ajustar en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Montar carpeta de assets estáticos
app.mount("/assets", StaticFiles(directory="product_assets"), name="assets")

# =========================
#  Helper de comparación de provider (tolerante)
# =========================
# Evita fallos por mayúsculas/espacios en provider (p.ej. "El Patrón" vs "elpatron").
PROVIDER_MATCH = """
LOWER(REPLACE(provider,' ','')) = LOWER(REPLACE(:prov,' ',''))
"""

# =========================
#  CATEGORÍAS
# =========================
@app.get("/api/{site}/categories")
def list_categories(site: str):
    qry = text("""
        SELECT id, name, url
          FROM categories
         WHERE provider = :prov
         ORDER BY name
    """)
    with engine.connect() as conn:
        rows = conn.execute(qry, {"prov": site}).fetchall()
    if not rows:
        raise HTTPException(404, f"No hay categorías para '{site}'")
    return [{"id": r.id, "name": r.name, "url": r.url} for r in rows]


@app.post("/api/{site}/categories/refresh")
def refresh_categories(site: str):
    try:
        live = core.fetch_categories(site)
    except ValueError as e:
        raise HTTPException(404, str(e))

    upsert = text("""
        INSERT INTO categories(provider, name, url)
        VALUES (:prov, :name, :url)
        ON DUPLICATE KEY UPDATE url = VALUES(url)
    """)
    with engine.begin() as conn:
        for c in live:
            conn.execute(upsert, {"prov": site, "name": c["nombre"], "url": c["url"]})
        rows = conn.execute(
            text("SELECT id, name, url FROM categories WHERE provider = :prov ORDER BY name"),
            {"prov": site}
        ).fetchall()

    return [{"id": r.id, "name": r.name, "url": r.url} for r in rows]


# Solo categorías que tienen publicados (para navegación pública)
@app.get("/api/{site}/categories/with-published")
def categories_with_published(site: str):
    qry = text(f"""
        SELECT c.id, c.name, c.url, COUNT(*) AS count
          FROM products p
          JOIN categories c ON c.id = p.category_id
         WHERE {PROVIDER_MATCH.replace("provider","p.provider")}
           AND p.status = 'published'
           AND {PROVIDER_MATCH.replace("provider","c.provider")}
         GROUP BY c.id, c.name, c.url
         ORDER BY c.name
    """)
    with engine.connect() as conn:
        rows = conn.execute(qry, {"prov": site}).fetchall()
    return [{"id": r.id, "name": r.name, "url": r.url, "count": int(r.count)} for r in rows]

# =========================
#  ASSETS (scraper)
# =========================
@app.post("/api/{site}/assets/{category_name}")
def scrape_assets(site: str, category_name: str):
    try:
        core.update_assets(site, category_name)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"site": site, "category": category_name, "status": "assets downloaded"}


@app.get("/api/{site}/assets/{category_name}")
def list_assets(site: str, category_name: str):
    scraper = core.get_scraper(site)
    safe_cat = scraper.sanitize_filename(category_name)
    base = os.path.join("product_assets", site, safe_cat)
    if not os.path.isdir(base):
        raise HTTPException(404, f"No hay assets en product_assets/{site}/{safe_cat}")

    result = []
    for prod in os.listdir(base):
        pdir = os.path.join(base, prod)
        if not os.path.isdir(pdir):
            continue
        files = os.listdir(pdir)
        urls  = [f"/assets/{site}/{safe_cat}/{prod}/{fname}" for fname in files]
        result.append({"producto": prod, "archivos": urls})

    return {"site": site, "category": category_name, "productos": result}

# =========================
#  SCRAPED PRODUCTS
# =========================
@app.get("/api/{site}/scraped-products")
def list_scraped(
    site: str,
    category_id: Optional[int] = Query(None, description="Filtrar por ID de categoría")
):
    base_q = """
        SELECT sp.id,
               sp.provider_sku   AS sku,
               sp.fetched_at,
               sp.category_id,
               c.name           AS category,
               sp.data          AS payload
          FROM scraped_products AS sp
     LEFT JOIN categories AS c ON sp.category_id = c.id
         WHERE sp.provider = :prov
    """
    params = {"prov": site}
    if category_id:
        base_q += " AND sp.category_id = :cid"
        params["cid"] = category_id

    with engine.connect() as conn:
        rows = conn.execute(text(base_q), params).fetchall()

    return [
        {
            "id":          r.id,
            "sku":         r.sku,
            "fetched_at":  r.fetched_at,
            "category_id": r.category_id,
            "category":    r.category,
            "data":        json.loads(r.payload)
        }
        for r in rows
    ]

# =========================
#  CATÁLOGO (products)
# =========================

def _catalog_rows_to_json(rows):
    out = []
    for r in rows:
        # other_data puede venir NULL o no-JSON
        raw = r.other_data
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except Exception:
            data = {}
        out.append({
            "id":           r.id,
            "sku":          r.sku,
            "name":         r.name,
            "status":       r.status,
            "category_id":  r.category_id,
            "category_name": r.category_name,
            "stock":        r.stock if r.stock is not None else 0,
            "data":         data,
            "updated_at":   r.updated_at,
        })
    return out


# --- Listar catálogo final (alias legacy) ---
@app.get("/api/{site}/catalog-products")
def list_catalog_products(
    site: str,
    status: Optional[str] = Query("published", description="published|hidden")
):
    qry = text(f"""
        SELECT p.id,
               p.provider_sku AS sku,
               p.name,
               p.status,
               p.category_id,
               c.name AS category_name,
               p.stock,
               p.other_data,
               p.updated_at
          FROM products p
     LEFT JOIN categories c
            ON c.id = p.category_id
           AND {PROVIDER_MATCH.replace("provider","c.provider")}
         WHERE {PROVIDER_MATCH.replace("provider","p.provider")}
           AND p.status = :st
         ORDER BY p.updated_at DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(qry, {"prov": site, "st": status}).fetchall()
    return _catalog_rows_to_json(rows)


# --- Alias dinámico para listar por estado (published|hidden) ---
@app.get("/api/{site}/catalog")
def list_catalog_by_status(
    site: str,
    status: Optional[str] = Query("published", description="published|hidden")
):
    if status not in ("published", "hidden"):
        raise HTTPException(400, "status debe ser 'published' o 'hidden'")

    qry = text(f"""
        SELECT p.id,
               p.provider_sku AS sku,
               p.name,
               p.status,
               p.category_id,
               c.name AS category_name,
               p.stock,
               p.other_data,
               p.updated_at
          FROM products p
     LEFT JOIN categories c
            ON c.id = p.category_id
           AND {PROVIDER_MATCH.replace("provider","c.provider")}
         WHERE {PROVIDER_MATCH.replace("provider","p.provider")}
           AND p.status = :st
         ORDER BY p.updated_at DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(qry, {"prov": site, "st": status}).fetchall()
    return _catalog_rows_to_json(rows)


# --- Publicar scraped al catálogo ---
@app.post("/api/{site}/catalog/publish/{scrape_id}")
def publish_product(site: str, scrape_id: int):
    sel = text("""
        SELECT provider_sku, data, category_id
          FROM scraped_products
         WHERE id = :sid AND provider = :prov
    """)
    with engine.begin() as conn:
        row = conn.execute(sel, {"sid": scrape_id, "prov": site}).first()
        if not row:
            raise HTTPException(404, "Scraped product no encontrado")
        payload = json.loads(row.data)
        ins = text("""
            INSERT INTO products
                (provider, provider_sku, name, other_data, status, category_id, stock)
            VALUES
                (:prov, :sku, :name, :other, 'published', :cat, :stock)
        """)
        conn.execute(ins, {
            "prov":  site,
            "sku":   row.provider_sku,
            "name":  payload.get("nombre"),
            "other": json.dumps(payload, ensure_ascii=False),
            "cat":   row.category_id,
            "stock": 0,  # default
        })
    return {"ok": True, "published_from": scrape_id}


# --- Ocultar producto del catálogo (robusto) ---
@app.delete("/api/{site}/catalog/{product_id}")
def hide_product(site: str, product_id: int):
    upd = text(f"""
        UPDATE products
           SET status = 'hidden',
               updated_at = CURRENT_TIMESTAMP
         WHERE id = :pid
           AND {PROVIDER_MATCH}
           AND COALESCE(status, 'published') <> 'hidden'
    """)
    with engine.begin() as conn:
        res = conn.execute(upd, {"pid": product_id, "prov": site})
        if res.rowcount == 0:
            raise HTTPException(404, "Producto no encontrado o ya oculto")
    return {"ok": True, "hidden": product_id}


# --- Cambiar SOLO el estado de un producto ---
@app.patch("/api/{site}/catalog/{product_id}/status")
def set_product_status(site: str, product_id: int, body: dict = Body(...)):
    """
    Cambia el estado de un producto.
    JSON esperado: { "status": "published" | "hidden" }
    """
    status = (body.get("status") or "").strip().lower()
    if status not in ("published", "hidden"):
        raise HTTPException(400, "status debe ser 'published' o 'hidden'")

    upd = text(f"""
        UPDATE products
           SET status = :st,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = :pid
           AND {PROVIDER_MATCH}
    """)
    with engine.begin() as conn:
        res = conn.execute(upd, {"st": status, "pid": product_id, "prov": site})
        if res.rowcount == 0:
            raise HTTPException(404, "Producto no encontrado")

    return {"ok": True, "id": product_id, "provider": site, "status": status}


# --- Editar producto (agrega soporte de stock) ---
@app.put("/api/{site}/catalog/{product_id}")
def edit_product(site: str, product_id: int, body: dict = Body(...)):
    name       = body.get("name")
    status     = body.get("status")
    other_data = body.get("other_data")
    stock      = body.get("stock")

    if name is None and status is None and other_data is None and stock is None:
        raise HTTPException(400, "Nada para actualizar")

    sets, params = [], {"pid": product_id, "prov": site}
    if name is not None:
        sets.append("name = :name");     params["name"] = name
    if status is not None:
        sets.append("status = :status"); params["status"] = status
    if other_data is not None:
        sets.append("other_data = :other"); params["other"] = json.dumps(other_data, ensure_ascii=False)
    if stock is not None:
        try:
            stock = int(stock)
        except Exception:
            raise HTTPException(400, "stock debe ser entero")
        sets.append("stock = :stock"); params["stock"] = stock

    sql = f"""
        UPDATE products SET {', '.join(sets)}
         WHERE id = :pid AND {PROVIDER_MATCH}
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), params)
        if res.rowcount == 0:
            raise HTTPException(404, "Producto no encontrado")

    updated_fields = [k for k in params.keys() if k not in ("pid", "prov")]
    return {"ok": True, "updated_fields": updated_fields}

# =========================
#  MEDIA por producto
# =========================
@app.post("/api/{site}/catalog/{product_id}/assets")
@app.post("/api/{site}/catalog/{product_id}/media")
async def upload_assets(
    site: str,
    product_id: int,
    files: List[UploadFile] = File(..., description="Archivos a subir (imágenes/videos)")
):
    if not files:
        raise HTTPException(400, "No se recibieron archivos")

    # 1) Obtener SKU y categoría (pueden venir NULL)
    sel = text("""
        SELECT p.provider_sku, c.name AS category_name
          FROM products p
     LEFT JOIN categories c ON p.category_id = c.id
         WHERE p.id = :pid AND p.provider = :prov
    """)
    with engine.connect() as conn:
        row = conn.execute(sel, {"pid": product_id, "prov": site}).first()
    if not row:
        raise HTTPException(404, "Producto no encontrado")

    sku = row.provider_sku or f"PID-{product_id}"
    category = row.category_name or "UNCATEGORIZED"

    # 2) Carpeta destino (normalizando valores None)
    from scrapper_core import sanitize_filename
    safe_cat = sanitize_filename(str(category))
    safe_sku = sanitize_filename(str(sku))
    base_dir = os.path.join("product_assets", site, safe_cat, safe_sku)
    os.makedirs(base_dir, exist_ok=True)

    saved: List[str] = []
    for up in files:
        fname = sanitize_filename((up.filename or f"file-{uuid4().hex}"))
        content = await up.read()
        dest = os.path.join(base_dir, fname)
        with open(dest, "wb") as f:
            f.write(content)
        rel = f"/assets/{site}/{safe_cat}/{safe_sku}/{fname}"
        saved.append(rel)

    # 3) Actualizar other_data de forma segura aunque esté vacío
    sel2 = text("SELECT other_data FROM products WHERE id = :pid AND provider = :prov")
    with engine.begin() as conn:
        row2 = conn.execute(sel2, {"pid": product_id, "prov": site}).first()
        raw_other = (row2.other_data if row2 and row2.other_data else "{}")

        try:
            data = json.loads(raw_other) if isinstance(raw_other, str) else (raw_other or {})
        except Exception:
            data = {}

        imgs = list(data.get("images") or [])
        vids = list(data.get("videos") or [])

        for p in saved:
            lower = p.lower()
            if lower.endswith((".mp4", ".mov", ".webm", ".mkv", ".avi")):
                vids.append(p)
            else:
                imgs.append(p)

        data["images"] = imgs
        data["videos"] = vids

        conn.execute(
            text("""
                UPDATE products
                   SET other_data = :other
                 WHERE id = :pid AND provider = :prov
            """),
            {"other": json.dumps(data, ensure_ascii=False), "pid": product_id, "prov": site}
        )

    return {"ok": True, "assets": saved}


@app.delete("/api/{site}/catalog/{product_id}/images")
def delete_media(
    site: str,
    product_id: int,
    image_path: str = Query(..., description="Ruta del asset a eliminar; acepta /assets/... o product_assets/...")
):
    def to_assets_path(p: str) -> str:
        """Normaliza cualquier variante a '/assets/<...>'."""
        if not p:
            return ""
        p = p.replace("\\", "/").strip()

        # Si es URL completa (p.ej. http://localhost:8000/assets/...)
        try:
            parsed = urlparse(p)
            if parsed.scheme and parsed.netloc:
                p = parsed.path
        except Exception:
            pass

        if p.startswith("/assets/"):
            return p
        if p.startswith("assets/"):
            return "/" + p
        if p.startswith("product_assets/"):
            return "/assets/" + p[len("product_assets/"):]
        return "/assets/" + p.lstrip("/")

    assets_path = to_assets_path(image_path)  # forma canónica
    if not assets_path.startswith("/assets/"):
        raise HTTPException(400, "Parámetro image_path inválido")

    # Ruta física en disco
    rel = assets_path[len("/assets/"):]
    fs_path = os.path.normpath(os.path.join("product_assets", rel))

    # 1) Cargar y actualizar other_data
    sel = text("SELECT other_data FROM products WHERE id = :pid AND provider = :prov")
    with engine.begin() as conn:
        row = conn.execute(sel, {"pid": product_id, "prov": site}).first()
        if not row:
            raise HTTPException(404, "Producto no encontrado")

        try:
            data = json.loads(row.other_data) if row.other_data else {}
        except Exception:
            data = {}

        imgs = data.get("images", []) or []
        vids = data.get("videos", []) or []

        product_assets_variant = "product_assets/" + rel
        candidates = {assets_path, product_assets_variant}

        def keep(item: str) -> bool:
            if not item:
                return True
            norm = to_assets_path(item)
            return norm not in {to_assets_path(c) for c in candidates}

        new_imgs = [i for i in imgs if keep(i)]
        new_vids = [v for v in vids if keep(v)]

        data["images"] = new_imgs
        data["videos"] = new_vids
        conn.execute(
            text("UPDATE products SET other_data = :other WHERE id = :pid AND provider = :prov"),
            {"other": json.dumps(data, ensure_ascii=False), "pid": product_id, "prov": site}
        )

    # 2) Intentar borrar el archivo físico
    fs_removed = False
    try:
        if os.path.isfile(fs_path):
            os.remove(fs_path)
            fs_removed = True
    except Exception:
        pass

    return {
        "ok": True,
        "removed": assets_path,
        "fs_removed": fs_removed,
        "fs_path": fs_path
    }

# =========================
#  NUEVO: productos manuales
# =========================

def _gen_manual_sku() -> str:
    return f"MAN-{uuid4().hex[:10].upper()}"

def _get_or_create_category(provider: str, category_name: str) -> int:
    """Devuelve id de categoría; si no existe para ese provider, la crea."""
    sel = text("SELECT id FROM categories WHERE provider=:prov AND name=:name LIMIT 1")
    ins = text("INSERT INTO categories(provider, name, url) VALUES(:prov, :name, '')")
    with engine.begin() as conn:
        row = conn.execute(sel, {"prov": provider, "name": category_name}).first()
        if row:
            return row.id
        res = conn.execute(ins, {"prov": provider, "name": category_name})
        try:
            new_id = res.lastrowid
        except Exception:
            new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        return int(new_id)

@app.post("/api/manual/catalog")
def create_manual_product(body: dict = Body(...)):
    """
    Crea un producto con provider='manual'.
    JSON:
    {
      "name": "Nombre",                # requerido
      "sku": "SKU-OPCIONAL",           # opcional (si falta se genera)
      "category_id": 12,               # opcional
      "category_name": "Lentes",       # opcional si no pasas category_id
      "status": "published|hidden",    # opcional (default 'published')
      "stock": 0,                      # opcional (default 0)
      "other_data": { ... }            # opcional; normaliza images/videos
    }
    """
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Campo 'name' es obligatorio")

    sku = (body.get("sku") or "").strip() or _gen_manual_sku()
    status = (body.get("status") or "published").strip().lower()
    if status not in ("published", "hidden"):
        raise HTTPException(400, "status debe ser 'published' o 'hidden'")

    # Resolver categoría
    category_id = body.get("category_id")
    category_name = body.get("category_name")
    if not category_id and not category_name:
        raise HTTPException(400, "Debes enviar 'category_id' o 'category_name'")
    if not category_id and category_name:
        category_id = _get_or_create_category("manual", category_name.strip())

    # Normalizar other_data
    other_data = body.get("other_data") or {}
    if not isinstance(other_data, dict):
        raise HTTPException(400, "other_data debe ser un objeto JSON")
    images = other_data.get("images") or []
    videos = other_data.get("videos") or []
    if not isinstance(images, list) or not isinstance(videos, list):
        raise HTTPException(400, "other_data.images y other_data.videos deben ser listas")
    other_data["images"] = images
    other_data["videos"] = videos

    stock = int(body.get("stock") or 0)

    ins = text("""
        INSERT INTO products (provider, provider_sku, name, other_data, status, category_id, stock)
        VALUES (:prov, :sku, :name, :other, :status, :cat, :stock)
    """)
    with engine.begin() as conn:
        res = conn.execute(ins, {
            "prov":  "manual",
            "sku":   sku,
            "name":  name,
            "other": json.dumps(other_data, ensure_ascii=False),
            "status": status,
            "cat":   category_id,
            "stock": stock
        })
        try:
            product_id = res.lastrowid
        except Exception:
            product_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()

    return {
        "ok": True,
        "id": int(product_id),
        "provider": "manual",
        "sku": sku,
        "name": name,
        "status": status,
        "category_id": int(category_id),
        "stock": stock,
        "other_data": other_data
    }
class BannerIn(BaseModel):
    image_url: str
    title: Optional[str] = None
    link_url: Optional[str] = None
    sort_order: int = 0
    is_active: bool = True

def _banners_has_provider() -> bool:
    """Devuelve True si la tabla banners tiene columna 'provider'."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SHOW COLUMNS FROM banners LIKE 'provider'")).first()
            return bool(row)
    except Exception:
        return False

@app.get("/api/banners")
def list_banners_global(only_active: Optional[bool] = True):
    """
    Lista banners globales.
    Param: only_active=true/false (por defecto devuelve solo activos).
    Si existe columna 'provider', devuelve solo provider IN ('global','',NULL).
    """
    act = 1 if (only_active is None or only_active) else None
    conds = ["(:act IS NULL) OR (is_active = :act)"]
    if _banners_has_provider():
        conds.append("(provider = 'global' OR provider = '' OR provider IS NULL)")

    qry = text(f"""
        SELECT id, image_url, title, link_url, sort_order, is_active, created_at, updated_at
          FROM banners
         WHERE {' AND '.join(conds)}
         ORDER BY sort_order ASC, created_at DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(qry, {"act": act}).fetchall()
    return [dict(r._mapping) for r in rows]

@app.post("/api/banners")
def create_banner_global(body: BannerIn):
    """
    Crea un banner global. Si la tabla tiene 'provider', se guarda como 'global'.
    """
    if _banners_has_provider():
        ins = text("""
            INSERT INTO banners(provider, image_url, title, link_url, sort_order, is_active)
            VALUES(:prov, :url, :title, :link, :ord, :act)
        """)
        params = {
            "prov": "global",
            "url": body.image_url,
            "title": body.title,
            "link": body.link_url,
            "ord": body.sort_order,
            "act": 1 if body.is_active else 0,
        }
    else:
        ins = text("""
            INSERT INTO banners(image_url, title, link_url, sort_order, is_active)
            VALUES(:url, :title, :link, :ord, :act)
        """)
        params = {
            "url": body.image_url,
            "title": body.title,
            "link": body.link_url,
            "ord": body.sort_order,
            "act": 1 if body.is_active else 0,
        }

    with engine.begin() as conn:
        res = conn.execute(ins, params)
        try:
            new_id = res.lastrowid
        except Exception:
            new_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
    return {"ok": True, "id": int(new_id)}


@app.post("/api/banners/upload")
async def upload_banner_file(file: UploadFile = File(...)):
    from scrapper_core import sanitize_filename
    base = os.path.join("product_assets", "banners")
    os.makedirs(base, exist_ok=True)
    safe = sanitize_filename(file.filename or f"banner-{uuid4().hex}.png")
    dest = os.path.join(base, safe)
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)
    # lo servís desde /assets/banners/...
    return {"ok": True, "url": f"/assets/banners/{safe}"}


@app.put("/api/banners/{banner_id}")
def update_banner_global(banner_id: int, body: BannerIn):
    """
    Actualiza un banner. No toca 'provider' (si existiera).
    """
    upd = text("""
        UPDATE banners
           SET image_url=:url, title=:title, link_url=:link,
               sort_order=:ord, is_active=:act, updated_at=CURRENT_TIMESTAMP
         WHERE id=:bid
    """)
    with engine.begin() as conn:
        res = conn.execute(upd, {
            "url": body.image_url, "title": body.title, "link": body.link_url,
            "ord": body.sort_order, "act": 1 if body.is_active else 0,
            "bid": banner_id
        })
        if res.rowcount == 0:
            raise HTTPException(404, "Banner no encontrado")
    return {"ok": True}

@app.delete("/api/banners/{banner_id}")
def delete_banner_global(banner_id: int):
    with engine.begin() as conn:
        res = conn.execute(text("DELETE FROM banners WHERE id=:bid"), {"bid": banner_id})
        if res.rowcount == 0:
            raise HTTPException(404, "Banner no encontrado")
    return {"ok": True}

@app.post("/api/{site}/catalog/{product_id}/stock/adjust")
def adjust_product_stock(site: str, product_id: int, body: dict = Body(...)):
    """
    JSON: { "delta": <int>, "clamp_min_zero": true|false }
    delta positivo suma, negativo resta.
    """
    if "delta" not in body:
        raise HTTPException(400, "Falta 'delta'")
    try:
        delta = int(body["delta"])
    except Exception:
        raise HTTPException(400, "delta debe ser entero")

    clamp = bool(body.get("clamp_min_zero", True))

    # Un solo UPDATE atómico (MySQL/MariaDB)
    expr = "GREATEST(stock + :delta, 0)" if clamp else "stock + :delta"
    upd = text(f"""
        UPDATE products
           SET stock = {expr},
               updated_at = CURRENT_TIMESTAMP
         WHERE id = :pid
           AND {PROVIDER_MATCH}
    """)
    with engine.begin() as conn:
        res = conn.execute(upd, {"delta": delta, "pid": product_id, "prov": site})
        if res.rowcount == 0:
            raise HTTPException(404, "Producto no encontrado")

        # devolver el valor final
        new_stock = conn.execute(
            text("SELECT stock FROM products WHERE id=:pid AND provider=:prov"),
            {"pid": product_id, "prov": site}
        ).scalar()

    return {"ok": True, "id": product_id, "provider": site, "stock": int(new_stock)}
