from db_connect import conn
from search_sql import search_by_name

results = search_by_name(conn, "Terrence")

for r in results[:3]:
    print(r)