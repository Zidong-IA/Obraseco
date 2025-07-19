from flask import Flask, request, jsonify, abort
from decimal import Decimal
import pyodbc, os, requests, re, time, schedule
from threading import Thread
from datetime import datetime

app = Flask(__name__)

# ================== CONFIG ==================
SQL_HOST = os.environ.get('SQLSERVER_HOST')
SQL_DB   = os.environ.get('SQLSERVER_DB')
SQL_USER = os.environ.get('SQLSERVER_USER')
SQL_PASS = os.environ.get('SQLSERVER_PASS')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

API_TOKEN    = os.environ.get('API_TOKEN')   # usamos la variable que ya tenés
SYNC_EVERY_HOURS = int(os.environ.get('SYNC_EVERY_HOURS', '8'))

BATCH_SIZE = 1000

if not all([SQL_HOST, SQL_DB, SQL_USER, SQL_PASS]):
    raise RuntimeError("Faltan variables SQLSERVER_*")

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SQL_HOST};DATABASE={SQL_DB};UID={SQL_USER};PWD={SQL_PASS};"
    "Encrypt=no;TrustServerCertificate=yes;"
)

WORD_RE = re.compile(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]+")

def log(msg):
    print(f"[{datetime.utcnow().isoformat()}] {msg}", flush=True)

# ================== HELPERS ==================
def normalize_text(text):
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip().lower())

def extract_keywords(description):
    if not description:
        return []
    stop = {'de','la','el','en','con','para','por','una','uno','del','las','los','un','y','o'}
    words = [w.lower() for w in WORD_RE.findall(description)]
    base = [w for w in words if len(w) > 2 and w not in stop]
    out = set(base)
    # variación singular/plural simple
    for w in base:
        if w.endswith('s') and len(w) > 3:
            out.add(w[:-1])
        else:
            out.add(w + 's')
    return list(out)

def dec_to_float(v):
    if isinstance(v, Decimal):
        return float(v)
    try: return float(v)
    except: return 0.0

def norm_code(c):
    return "" if c is None else str(c).strip()

# ================== FETCH ORIGEN ==================
def fetch_products():
    with pyodbc.connect(CONN_STR, timeout=60) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT Codigo, Descri, PrecioFinal
            FROM dbo.ConsStock
            WHERE PrecioFinal > 0
        """)
        rows = cur.fetchall()

    dedup = {}
    now_iso = datetime.utcnow().isoformat()
    for codigo, descri, precio in rows:
        code = norm_code(codigo)
        descri = (descri or "").strip()
        dedup[code] = {
            "codigo": code,
            "descripcion": descri,
            "descripcion_normalizada": normalize_text(descri),
            "precio_final": dec_to_float(precio),
            "keywords": extract_keywords(descri),
            "updated_at": now_iso  # por si no hay trigger
        }
    return list(dedup.values())

# ================== UPSERT ==================
def upsert_products(products):
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("Supabase no configurado.")
        return False
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo?on_conflict=codigo"
    total = len(products)
    for i in range(0, total, BATCH_SIZE):
        batch = products[i:i+BATCH_SIZE]
        r = requests.post(url, headers=headers, json=batch)
        if r.status_code not in (200,201,204):
            log(f"UPSERT ERROR lote={i//BATCH_SIZE+1} status={r.status_code} body={r.text[:300]}")
            raise RuntimeError("Upsert failed")
        log(f"UPSERT OK lote={i//BATCH_SIZE+1} size={len(batch)}")
    return True

# ================== SYNC ==================
def sync_catalog():
    if not SUPABASE_URL or not SUPABASE_KEY:
        log("SYNC ABORT: Supabase no configurado")
        return False
    start = time.time()
    log("SYNC START (solo upsert)")
    try:
        products = fetch_products()
        log(f"FETCH OK count={len(products)}")
        upsert_products(products)
        log(f"SYNC DONE elapsed={round(time.time()-start,2)}s")
        return True
    except Exception as e:
        log(f"SYNC EXCEPTION {repr(e)}")
        return False

# ================== SCHEDULER ==================
def scheduler_loop():
    schedule.every(SYNC_EVERY_HOURS).hours.do(sync_catalog)
    sync_catalog()   # primera ejecución inmediata
    while True:
        schedule.run_pending()
        time.sleep(60)

# ================== ROUTES ==================
@app.route("/")
def health():
    return {
        "status": "ok",
        "jobs": len(schedule.jobs),
        "next_runs": [str(j.next_run) for j in schedule.jobs]
    }

@app.route("/sync-now", methods=["POST"])
def sync_now():
    token = request.headers.get("X-Api-Token") or request.args.get("token")
    if token != API_TOKEN:
        return abort(403)
    ok = sync_catalog()
    return {"ok": ok}, (200 if ok else 500)

@app.route("/search-multi")
def search_multi():
    token = request.args.get("token")
    if token != API_TOKEN:
        return abort(403)
    q = request.args.get("query","").strip()
    if not q:
        return {"error":"query vacio"}, 400
    terms = [t.strip() for t in q.split(",") if t.strip()]
    if not terms:
        return {"error":"sin terminos"}, 400
    try:
        with pyodbc.connect(CONN_STR, timeout=45) as conn:
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
        out = []
        for codigo, descri, precio in rows:
            out.append({
                "Codigo": norm_code(codigo),
                "Descri": (descri or "").strip(),
                "PrecioFinal": dec_to_float(precio)
            })
        return jsonify({"total": len(out), "results": out})
    except Exception as e:
        log(f"SEARCH EXCEPTION {repr(e)}")
        return {"error":"internal"}, 500

# ================== MAIN ==================
if __name__ == "__main__":
    log(f"BOOT sql_host={SQL_HOST} db={SQL_DB} supabase={bool(SUPABASE_URL)} mode=upsert_only")
    Thread(target=scheduler_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
