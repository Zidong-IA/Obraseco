from flask import Flask, request, jsonify, abort
from decimal import Decimal
import pyodbc, os, requests, re, schedule, time
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
ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')  # unificamos

SYNC_EVERY_HOURS = int(os.environ.get('SYNC_EVERY_HOURS', '8'))

if not all([sql_host, sql_db, sql_user, sql_pass]):
    raise RuntimeError("Faltan variables SQLSERVER_*")

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={sql_host};DATABASE={sql_db};UID={sql_user};PWD={sql_pass};"
    "Encrypt=no;TrustServerCertificate=yes;"
)

# ================== HELPERS ==================
WORD_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]+")

def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = text.lower().strip()
    t = re.sub(r'\s+', ' ', t)
    # (Si querés quitar acentos, hacelo)
    return t

def extract_keywords(description: str):
    if not description:
        return []
    stop = {'de','la','el','en','con','para','por','una','uno','del','las','los','un','y','o'}
    words = [w.lower() for w in WORD_RE.findall(description)]
    base = [w for w in words if len(w) > 2 and w not in stop]
    out = set(base)
    for w in base:
        if w.endswith('s') and len(w) > 3:
            out.add(w[:-1])
        else:
            out.add(w + 's')
    return list(out)

def dec_to_float(v):
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except:
        return 0.0

def log(msg, *extra):
    print(f"[{datetime.utcnow().isoformat()}] {msg}", *extra, flush=True)

# ================== DATA FETCH ==================
def fetch_source_products():
    """
    Lee productos de SQL Server.
    Dedup por codigo (el último gana).
    """
    with pyodbc.connect(conn_str, timeout=45) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT Codigo, Descri, PrecioFinal
            FROM dbo.ConsStock
            WHERE PrecioFinal > 0
        """)
        rows = cur.fetchall()

    dedup = {}
    for codigo, descri, precio in rows:
        key = str(codigo).strip()
        descri = descri or ""
        dedup[key] = {
            "codigo": key,
            "descripcion": descri.strip(),
            "descripcion_normalizada": normalize_text(descri),
            "precio_final": dec_to_float(precio),
            "keywords": extract_keywords(descri)
        }
    return list(dedup.values())

def get_existing_codes():
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo?select=codigo"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        log(f"GET existing codes error {r.status_code} {r.text[:200]}")
        return set()
    return {item["codigo"] for item in r.json()}

def purge_missing(existing_codes, new_codes):
    """
    Borra SOLO los códigos que ya no están en la fuente.
    Evita violar FKs de tablas hijas que referencian código inexistente? (Ellas deberían referenciar uno válido;
    si hay FKs, este delete también podría fallar si hay dependencias no diseñadas para eliminar).
    """
    to_delete = existing_codes - new_codes
    if not to_delete:
        log("PURGE: nada para borrar")
        return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    base_url = f"{SUPABASE_URL}/rest/v1/productos_catalogo"
    # PostgREST limita largo de query; armamos en lotes
    codes_list = list(to_delete)
    chunk = 150
    for i in range(0, len(codes_list), chunk):
        subset = codes_list[i:i+chunk]
        in_list = ",".join(subset)
        params = {"codigo": f"in.({in_list})"}
        resp = requests.delete(base_url, headers=headers, params=params)
        log(f"PURGE subset {i//chunk+1} status={resp.status_code} size={len(subset)}")
        if resp.status_code not in (200,204):
            log("PURGE ERROR body:", resp.text[:300])

def upsert_products(products):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo?on_conflict=codigo"
    batch = 1000
    total = len(products)
    for i in range(0, total, batch):
        part = products[i:i+batch]
        resp = requests.post(url, headers=headers, json=part)
        if resp.status_code not in (200,201,204):
            log(f"UPSERT ERROR lote={i//batch+1} status={resp.status_code} body={resp.text[:400]}")
            raise RuntimeError("Upsert failed")
        log(f"UPSERT OK lote={i//batch+1} size={len(part)}")

# ================== SYNC ==================
def sync_catalog_to_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("SYNC ABORT: Supabase no configurado")
        return False
    start = time.time()
    log("SYNC START")
    try:
        new_products = fetch_source_products()
        new_codes = {p["codigo"] for p in new_products}
        log(f"FETCH OK count={len(new_products)} unique_codes={len(new_codes)}")
        existing = get_existing_codes()
        log(f"EXISTING codes={len(existing)}")
        purge_missing(existing, new_codes)
        upsert_products(new_products)
        elapsed = round(time.time() - start, 2)
        log(f"SYNC DONE upserted={len(new_products)} elapsed={elapsed}s")
        return True
    except Exception as e:
        log("SYNC EXCEPTION", repr(e))
        return False

# ================== SCHEDULER ==================
def run_scheduler():
    schedule.every(SYNC_EVERY_HOURS).hours.do(sync_catalog_to_supabase)
    # Primera ejecución inmediata dentro del mismo thread
    sync_catalog_to_supabase()
    while True:
        schedule.run_pending()
        time.sleep(60)

# ================== ROUTES ==================
@app.route("/")
def health():
    return {
        "status": "ok",
        "next_runs": [str(job.next_run) for job in schedule.jobs],
        "jobs": len(schedule.jobs)
    }

@app.route("/sync-now", methods=["POST"])
def sync_now():
    tok = request.headers.get("X-Access-Token") or request.args.get("token")
    if tok != ACCESS_TOKEN:
        return abort(403)
    ok = sync_catalog_to_supabase()
    return {"ok": ok}, (200 if ok else 500)

@app.route("/search-multi", methods=["GET"])
def search_multi():
    token = request.args.get("token")
    if token != ACCESS_TOKEN:
        return abort(403, "Unauthorized")

    query_param = request.args.get("query", "").strip()
    if not query_param:
        return jsonify({"error": "Query parameter is required."}), 400

    terms = [t.strip() for t in query_param.split(",") if t.strip()]
    if not terms:
        return jsonify({"error": "No valid terms provided."}), 400

    try:
        with pyodbc.connect(conn_str, timeout=45) as conn:
            cur = conn.cursor()
            like_clause = " OR ".join(["Descri LIKE ?" for _ in terms])
            params = [f"%{t}%" for t in terms]
            cur.execute(f"""
                SELECT TOP 200 Codigo, Descri, PrecioFinal
                FROM dbo.ConsStock
                WHERE ({like_clause}) AND PrecioFinal > 0
                ORDER BY PrecioFinal ASC
            """, params)
            rows = cur.fetchall()

        results = []
        for codigo, descri, precio in rows:
            results.append({
                "Codigo": str(codigo).strip(),
                "Descri": (descri or "").strip(),
                "PrecioFinal": dec_to_float(precio)
            })

        return jsonify({"total": len(results), "results": results})
    except Exception as e:
        log("SEARCH EXCEPTION", repr(e))
        return jsonify({"error": "Internal error"}), 500

# ================== MAIN ==================
if __name__ == "__main__":
    log(f"BOOT sql_host={sql_host} db={sql_db} supabase_set={bool(SUPABASE_URL)}")
    Thread(target=run_scheduler, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
