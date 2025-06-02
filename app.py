from flask import Flask, request, jsonify
import pyodbc

app = Flask(__name__)

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=168.205.92.17\\SQLEXPRESS;"
    "DATABASE=ObraSeco;"
    "UID=invitado;PWD=Invi2025;"
    "Encrypt=no;TrustServerCertificate=yes;"
)

@app.route("/search", methods=["GET"])
def search():
    term = request.args.get("query", "").strip()
    if not term:
        return jsonify({"error": "Query parameter is required."}), 400

    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            cursor = conn.cursor()
            query = f"""
                SELECT TOP 5 Codigo, Descri, PrecioFinal
                FROM dbo.ConsStock
                WHERE Descri LIKE ?
            """
            cursor.execute(query, f"%{term}%")
            columns = [c[0] for c in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
