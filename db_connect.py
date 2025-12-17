import pyodbc
import os

server = "bsco-sql-server.database.windows.net"
database = "bscodb"
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