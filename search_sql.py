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