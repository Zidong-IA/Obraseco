import os, re
from datetime import datetime
import pyodbc, requests
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# ================== ENTORNO ==================
SQL_HOST = os.getenv("SQLSERVER_HOST")          # 168.205.92.17
SQL_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQL_DB   = os.getenv("SQLSERVER_DB")
SQL_USER = os.getenv("SQLSERVER_USER")
SQL_PASS = os.getenv("SQLSERVER_PASS")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_TOKEN    = os.getenv("API_TOKEN")

if not all([SQL_HOST, SQL_DB, SQL_USER, SQL_PASS]):
    raise RuntimeError("Faltan variables SQL (HOST/DB/USER/PASS).")

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_HOST},{SQL_PORT};"
    f"DATABASE={SQL_DB};UID={SQL_USER};PWD={SQL_PASS};"
    "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
)

def norm(txt: str) -> str:
    if not txt: return ""
    txt = txt.lower()
    for a,b in {'á':'a','é':'e','í':'i','ó':'o','ú':'u','ñ':'n','ü':'u'}.items():
        txt = txt.replace(a,b)
    return re.sub(r'\s+',' ',txt).strip()

# ============== OBTENER DESDE SQL SERVER ==============
def fetch_from_sql():
    sql = """
        SELECT Codigo, Descri, PrecioFinal
        FROM dbo.ConsStock
        WHERE PrecioFinal > 0
        ORDER BY Codigo
    """
    items = []
    with pyodbc.connect(CONN_STR, timeout=30) as c:
        cur = c.cursor()
        cur.execute(sql)
        for codigo, descri, precio in cur.fetchall():
            items.append({
                "codigo": codigo,
                "descripcion": descri,
                "descripcion_normalizada": norm(descri),
                "precio_final": float(precio or 0),
                "updated_at": datetime.utcnow().isoformat()
            })
    return items

# ============== SUPABASE ==============
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def sb_delete_all():
    # borra todo usando filtro universal
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo"
    r = requests.delete(url, headers=sb_headers(), params={"codigo":"not.is.null"})
    return r.status_code in (200,204)

def sb_upsert(batch):
    url = f"{SUPABASE_URL}/rest/v1/productos_catalogo"
    r = requests.post(url, headers=sb_headers(), json=batch)
    return r.status_code in (200,201,204)

# ============== RUTAS ==============
@app.route("/health")
def health():
    return {"ok": True, "time": datetime.utcnow().isoformat()}

@app.route("/sync", methods=["POST"])
def sync():
    if request.headers.get("X-API-TOKEN") != API_TOKEN:
        abort(403)
    if not (SUPABASE_URL and SUPABASE_KEY):
        return {"error":"Supabase no configurado"}, 500
    try:
        items = fetch_from_sql()
        if not sb_delete_all():
            return {"error":"delete falló"}, 500
        total = 0
        size = 700
        for i in range(0, len(items), size):
            chunk = items[i:i+size]
            if not sb_upsert(chunk):
                return {"error": f"upsert falló bloque {i}"}, 500
            total += len(chunk)
        return {"ok": True, "total": total}
    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/search")
def search():
    if request.args.get("token") != API_TOKEN:
        abort(403)
    q = request.args.get("q","").strip()
    if not q:
        return {"error":"falta q"}, 400
    try:
        sql = """
            SELECT TOP 50 Codigo, Descri, PrecioFinal
            FROM dbo.ConsStock
            WHERE Descri LIKE ? AND PrecioFinal > 0
            ORDER BY PrecioFinal ASC
        """
        with pyodbc.connect(CONN_STR, timeout=10) as c:
            cur = c.cursor()
            cur.execute(sql, f"%{q}%")
            rows = [{"Codigo":r[0], "Descri":r[1], "PrecioFinal":float(r[2] or 0)} for r in cur.fetchall()]
        return {"total": len(rows), "results": rows}
    except Exception as e:
        return {"error": str(e)}, 500

# SOLO local
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")))
