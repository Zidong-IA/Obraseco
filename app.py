from flask import Flask, jsonify, request
from decimal import Decimal
import pyodbc
import os
import requests
import re
from datetime import datetime

app = Flask(__name__)

# Configuración SQL Server
sql_host = os.environ.get('SQLSERVER_HOST')
sql_db   = os.environ.get('SQLSERVER_DB')
sql_user = os.environ.get('SQLSERVER_USER')
sql_pass = os.environ.get('SQLSERVER_PASS')

# Configuración Supabase
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

if not all([sql_host, sql_db, sql_user, sql_pass, SUPABASE_URL, SUPABASE_KEY]):
    raise RuntimeError("Faltan variables de entorno necesarias.")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_host};"
    f"DATABASE={sql_db};"
    f"UID={sql_user};"
    f"PWD={sql_pass}"
)

# Función para normalizar descripciones
def normalizar_descripcion(texto):
    if texto is None:
        return ''
    texto = texto.upper()
    texto = re.sub(r'\s+', ' ', texto)
    texto = re.sub(r'[^\w\s]', '', texto)
    return texto.strip()

# Sincronización
def sync_catalogo():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT codigo, descripcion, precio_final FROM productos_catalogo")
        rows = cursor.fetchall()

        productos = []
        for row in rows:
            codigo = row[0]
            descripcion = row[1]
            descripcion_norm = normalizar_descripcion(descripcion)
            precio_final = float(row[2]) if isinstance(row[2], Decimal) else 0

            productos.append({
                "codigo": codigo,
                "descripcion": descripcion,
                "descripcion_normalizada": descripcion_norm,
                "precio_final": precio_final,
            })

        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

        res = requests.post(f"{SUPABASE_URL}/rest/v1/productos_catalogo", headers=headers, json=productos)

        return {
            "status": res.status_code,
            "message": res.text,
            "count": len(productos)
        }

    except Exception as e:
        print(f"[ERROR SYNC] {e}")
        return {
            "status": 500,
            "error": str(e)
        }

# Ruta para ejecución manual
@app.route('/sync', methods=['GET'])
def trigger_sync():
    result = sync_catalogo()
    return jsonify(result)

# Al iniciar el servidor
@app.before_first_request
def auto_sync():
    print("Ejecutando sincronización automática al iniciar...")
    sync_catalogo()

# Manejador global de errores
@app.errorhandler(Exception)
def handle_error(e):
    print(f"[ERROR GENERAL] {e}")
    return jsonify({'error': str(e)}), 500

# Ejecutar local
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
