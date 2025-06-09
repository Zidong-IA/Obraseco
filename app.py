from flask import Flask, request, jsonify, abort
from decimal import Decimal
import pyodbc
import os

app = Flask(__name__)

sql_host = os.environ.get('SQLSERVER_HOST')
sql_db   = os.environ.get('SQLSERVER_DB')
sql_user = os.environ.get('SQLSERVER_USER')
sql_pass = os.environ.get('SQLSERVER_PASS')

if not all([sql_host, sql_db, sql_user, sql_pass]):
    raise RuntimeError("Faltan variables de entorno necesarias para la conexión SQL Server.")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_host};"
    f"DATABASE={sql_db};"
    f"UID={sql_user};"
    f"PWD={sql_pass};"
    "Encrypt=no;TrustServerCertificate=yes;"
)


@app.route("/search-multi", methods=["GET"])
def search_multi():
    token = request.args.get("token")
    if token != os.environ.get("API_TOKEN"):
        return abort(403, "Unauthorized")

    query_param = request.args.get("query", "").strip()
    if not query_param:
        return jsonify({"error": "Query parameter is required."}), 400

    terms = [t.strip() for t in query_param.split(",") if t.strip()]
    if not terms:
        return jsonify({"error": "No valid terms provided."}), 400

    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            cursor = conn.cursor()
            like_clauses = " OR ".join(["Descri LIKE ?" for _ in terms])
            params = [f"%{t}%" for t in terms]

            query = f"""
                SELECT TOP 100 Codigo, Descri, PrecioFinal
                FROM dbo.ConsStock
                WHERE {like_clauses}
            """
            cursor.execute(query, params)
            columns = [c[0] for c in cursor.description]
            results = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                # Convertir Decimal a float si aplica
                for key, value in record.items():
                    if isinstance(value, Decimal):
                        record[key] = float(value)
                results.append(record)

            return jsonify({
                "total": len(results),
                "results": results
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
