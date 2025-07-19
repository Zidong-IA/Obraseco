from flask import Flask, request, jsonify
import requests
import os
from datetime import datetime
from supabase import create_client, Client

app = Flask(__name__)

# Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Catálogo externo
CATALOGO_URL = "https://api.prod.catalogopvc.ar/catalogo"

def actualizar_catalogo_interno():
    try:
        response = requests.get(CATALOGO_URL)
        response.raise_for_status()
        data = response.json()

        # Borrar anterior
        supabase.table("productos_catalogo").delete().neq("id", 0).execute()

        # Insertar nuevo
        for item in data:
            supabase.table("productos_catalogo").insert({
                "codigo": item.get("codigo"),
                "descripcion": item.get("descripcion"),
                "descripcion_normalizada": item.get("descripcion_normalizada"),
                "precio_final": float(item.get("precio_final", 0)),
                "keywords": item.get("keywords", []),
                "updated_at": datetime.utcnow().isoformat()
            }).execute()
        return f"Actualizado correctamente ({len(data)} registros)"
    except Exception as e:
        return f"Error: {str(e)}"

@app.route("/")
def index():
    msg = actualizar_catalogo_interno()
    return f"Deploy OK - {msg}", 200

@app.route("/actualizar_catalogo", methods=["POST", "GET"])
def actualizar_catalogo():
    msg = actualizar_catalogo_interno()
    return jsonify({"status": msg}), 200
