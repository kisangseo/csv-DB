from db_connect import get_sql_connection

try:
    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    row = cursor.fetchone()
    print("Connected! Result:", row[0])
    conn.close()
except Exception as e:
    print("ERROR:", e)