from flask import Flask, request, jsonify, abort
import pyodbc
import os

app = Flask(__name__)

sql_host = os.environ.get('SQLSERVER_HOST')
sql_db   = os.environ.get('SQLSERVER_DB')
sql_user = os.environ.get('SQLSERVER_USER')
sql_pass = os.environ.get('SQLSERVER_PASS')

if not all([sql_host, sql_db, sql_user, sql_pass]):
    raise RuntimeError("⛔ Faltan variables de entorno necesarias para la conexión SQL Server.")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_host};"
    f"DATABASE={sql_db};"
    f"UID={sql_user};"
    f"PWD={sql_pass};"
    "Encrypt=no;TrustServerCertificate=yes;"
)


@app.route("/search", methods=["GET"])
def search():
    token = request.args.get("token")
    if token != os.environ.get("API_TOKEN"):
        return abort(403, "Unauthorized")

    term = request.args.get("query", "").strip()
    if not term:
        return jsonify({"error": "Query parameter is required."}), 400

    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            cursor = conn.cursor()
            query = """
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
