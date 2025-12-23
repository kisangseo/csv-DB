import pyodbc
import os

server = "bsco-sql-server.database.windows.net"
database = "bcsodb"
username = "bscoit"
password = os.getenv("AZURE_SQL_PASSWORD")

conn = pyodbc.connect(
    f"DRIVER={{SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

cursor = conn.cursor()
cursor.execute("SELECT 1")
print(cursor.fetchone())

def search_by_name(conn, name_query, limit=100):
    cursor = conn.cursor()

    sql = """
    SELECT TOP (?)
        record_id,
        department,
        full_name,
        case_number,
        sid,
        date_of_birth,
        address,
        warrant_type,
        warrant_status,
        issue_date,
        notes
    FROM search.records
    WHERE full_name LIKE ?
    ORDER BY created_at DESC
    """

    cursor.execute(
        sql,
        limit,
        f"%{name_query}%"
    )

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    return [dict(zip(columns, row)) for row in rows]