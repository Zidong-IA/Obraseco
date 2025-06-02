from flask import Flask, request, jsonify, abort
import pyodbc
import os

app = Flask(__name__)

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.environ['SQLSERVER_HOST']};"
    f"DATABASE={os.environ['SQLSERVER_DB']};"
    f"UID={os.environ['SQLSERVER_USER']};"
    f"PWD={os.environ['SQLSERVER_PASS']};"
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
