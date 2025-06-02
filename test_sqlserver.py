import pyodbc, pprint, sys

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=168.205.92.17\\SQLEXPRESS;"
    "DATABASE=ObraSeco;"
    "UID=invitado;PWD=Invi2025;"
    "Encrypt=no;TrustServerCertificate=yes;"
)

with pyodbc.connect(conn_str, timeout=5) as conn:
    cur = conn.cursor()
    print("Conexion OK")

    # 1. ¿La tabla existe?
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = 'ConsStock'
    """)
    if not cur.fetchall():
        sys.exit("La tabla ConsStock no existe o no es visible.")

    # 2. Cuántas filas hay
    cur.execute("SELECT COUNT(*) FROM dbo.ConsStock")
    total = cur.fetchone()[0]
    print("Filas en ConsStock:", total)

    # 3. Muestra hasta 10 filas
    if total:
        cur.execute("SELECT TOP 10 * FROM dbo.ConsStock")
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            data = dict(zip(cols, r))
            safe_str = str(data).encode('ascii', 'replace').decode()
            print(safe_str)

    else:
        print("La tabla está vacia.")
