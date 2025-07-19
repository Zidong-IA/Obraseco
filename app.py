from flask import Flask, request, jsonify
from decimal import Decimal
import pyodbc
import os
import requests
import re
import schedule
import time
from threading import Thread
from datetime import datetime

app = Flask(__name__)

# ================== CONFIG ==================
sql_host = os.environ.get('SQLSERVER_HOST')
sql_db   = os.environ.get('SQLSERVER_DB')
sql_user = os.environ.get('SQLSERVER_USER')
sql_pass = os.environ.get('SQLSERVER_PASS')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Modo de sincronización:
#   "truncate" -> borra todo y re-inserta (rápido, simple)
#   "upsert"   -> hace upsert por codigo (requiere UNIQUE(codigo))
SYNC_MODE = os.environ.get('SYNC_MODE', 'truncate').lower()  # truncate | upsert

# Cada cuántas horas volver a sincronizar
SYNC_EVERY_HOURS = int(os.environ.get('SYNC_EVERY_HOURS', '8'))

if not all([sql_host, sql_db, sql_user, sql_pass]):
    raise RuntimeError("Faltan variables de entorno SQLSERVER_*")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[WARN] Faltan variables de Supabase; la sync fallará.")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_host};"
    f"DATABASE={sql_db};"
    f"UID={sql_user};"
    f"PWD={sql_pass};"
    "TrustServerCertificate=yes;"
)

# ================== HELPERS ==================
_keyword_regex = re.compile(r'[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]+', re.UNICODE)

def normalize_text(text):
    if not text:
        return ''
    t = text.strip().lower()
    # Podés agregar reemplazos puntuales si hace falta
    return re.sub(r'\s+', ' ', t)

def extract_keywords(text):
    if not text:
        return []
    return list({m.group(0).lower() for m in _keyword_regex.finditer(text) if len(m.group(0)) > 2})

def decimal_to_float(v):
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except:
        return 0.0

def log(msg, *extra):
    print(f"[{datetime.utcnow().isoformat()}] {msg}", *extra, flush=True)

# ================== SYNC CORE ==================
def fetch_source_products():
    """
    Lee productos de SQL Server.
    Ajustá la query según la tabla real.
    """
    with pyodbc.connect(conn_str, timeout=45) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT Codigo, Descri, PrecioFinal
            FROM dbo.ConsStock
            WHERE PrecioFinal > 0
            ORDER BY Codigo
        """)
        rows = cur.fetchall()

    products = []
    for codigo, descri, precio in rows:
        products.append({
            "codigo": str(codigo).strip(),
            "descripcion": descri.strip() if descri else "",
            "descripcion_normalizada": normalize_text(descri),
            "precio_final": decimal_to_float(precio),
            "keywords": extract_keywords(descri)
        })
    return products

def truncate_target(headers):
    """
    Borra TODO usando criterio siempre verdadero (id > 0).
    """
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo"
    resp = requests.delete(url, headers=headers, params={"id": "gt.0"})
    log(f"TRUNCATE DELETE status={resp.status_code}")
    if resp.status_code not in (200, 204):
        log("TRUNCATE DELETE body:", resp.text[:500])

def upsert_batch(products, headers, batch_size=1000):
    """
    Upsert por codigo (requiere constraint UNIQUE en 'codigo').
    """
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo?on_conflict=codigo"
    total = len(products)
    for i in range(0, total, batch_size):
        batch = products[i:i+batch_size]
        resp = requests.post(url, headers=headers, json=batch)
        if resp.status_code not in (200, 201, 204):
            log(f"UPSERT ERROR lote={i//batch_size+1} status={resp.status_code} body={resp.text[:400]}")
            raise RuntimeError("Fallo upsert lote")
        log(f"UPSERT OK lote={i//batch_size+1} size={len(batch)}")
    return total

def insert_full(products, headers, batch_size=1000):
    """
    Inserta todo (sin on_conflict). Requiere TRUNCATE previo.
    """
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo"
    total = len(products)
    for i in range(0, total, batch_size):
        batch = products[i:i+batch_size]
        resp = requests.post(url, headers=headers, json=batch)
        if resp.status_code not in (200, 201, 204):
            log(f"INSERT ERROR lote={i//batch_size+1} status={resp.status_code} body={resp.text[:400]}")
            raise RuntimeError("Fallo insert lote")
        log(f"INSERT OK lote={i//batch_size+1} size={len(batch)}")
    return total

def sync_catalog_to_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("SYNC ABORT: Supabase no configurado.")
        return False
    start = time.time()
    log(f"SYNC START mode={SYNC_MODE}")

    try:
        products = fetch_source_products()
        log(f"FETCH OK count={len(products)}")

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        }

        if SYNC_MODE == 'truncate':
            truncate_target(headers)
            inserted = insert_full(products, headers)
        elif SYNC_MODE == 'upsert':
            # Para upsert el header Prefer puede ayudar a merges:
            headers["Prefer"] = "resolution=merge-duplicates"
            inserted = upsert_batch(products, headers)
        else:
            log(f"Modo desconocido: {SYNC_MODE}")
            return False

        elapsed = round(time.time() - start, 2)
        log(f"SYNC DONE inserted={inserted} elapsed={elapsed}s")
        return True
    except Exception as e:
        log("SYNC EXCEPTION", repr(e))
        return False

# ================== SCHEDULER ==================
def run_scheduler():
    schedule.every(SYNC_EVERY_HOURS).hours.do(sync_catalog_to_supabase)
    # primera ejecución inmediata
    sync_catalog_to_supabase()
    while True:
        schedule.run_pending()
        time.sleep(60)

# ================== ROUTES ==================
@app.route("/")
def health():
    return {
        "status": "ok",
        "mode": SYNC_MODE,
        "next_runs": [str(job.next_run) for job in schedule.jobs]
    }

@app.route("/search-multi")
def search_multi():
    token = request.args.get('token')
    query = request.args.get('query', '').strip()

    if token != os.environ.get('ACCESS_TOKEN'):
        return jsonify({"error": "Token inválido"}), 401

    if not query:
        return jsonify({"error": "Falta parámetro query"}), 400

    # Ejemplo simple de búsqueda directa a SQL Server
    try:
        with pyodbc.connect(conn_str, timeout=45) as conn:
            cur = conn.cursor()
            # Búsqueda básica - ajustar a tus necesidades.
            cur.execute("""
                SELECT TOP 50 Codigo, Descri, PrecioFinal
                FROM dbo.ConsStock
                WHERE Descri LIKE ?
                ORDER BY Codigo
            """, f"%{query}%")
            rows = cur.fetchall()
        results = []
        for codigo, descri, precio in rows:
            results.append({
                "Codigo": str(codigo).strip(),
                "Descri": descri.strip() if descri else "",
                "PrecioFinal": decimal_to_float(precio)
            })
        return jsonify({"results": results})
    except Exception as e:
        log("SEARCH EXCEPTION", repr(e))
        return jsonify({"error": "Error interno"}), 500

# ================== MAIN ==================
if __name__ == "__main__":
    log(f"BOOT ENV sql_host={sql_host} sql_db={sql_db} supabase_set={bool(SUPABASE_URL)}")
    Thread(target=run_scheduler, daemon=True).start()
    # Producción real: usar gunicorn, uwsgi, etc. Esto es dev server.
    app.run(host="0.0.0.0", port=5000)
