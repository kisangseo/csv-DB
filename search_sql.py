def search_by_name(conn, name_query, dob=None, sex=None, race=None, issuing_county=None, last_x_days=None, sid=None, limit=100):
    
    cursor = conn.cursor()

    sql = """
    SELECT TOP (?)
        full_name      AS name,
        sid            AS sid,
        case_number    AS case_number,
        address        AS address,
        warrant_type   AS warrant_type,
        issue_date     AS issue_date,
        warrant_status AS warrant_status,
        department
    FROM search.records
    WHERE full_name LIKE ?
    """

    params = [limit, f"%{name_query}%"]

    if sex:
        sql += " AND sex = ?"
        params.append(sex)

    if last_x_days:
        sql += " AND issue_date >= DATEADD(day, -?, CAST(GETDATE() AS date))"
        params.append(int(last_x_days))
    if race:
        sql += " AND LOWER(race) = ?"
        params.append(race.lower())
    if issuing_county:
        sql += " AND issuing_county IS NOT NULL AND LOWER(issuing_county) LIKE ?"
        params.append(f"%{issuing_county.lower()}%")
    if sid:
        sql += " AND sid = ?"
        params.append(str(sid))
    if dob:
        sql += " AND date_of_birth = CAST(? AS date)"
        params.append(dob)

    sql += " ORDER BY created_at DESC"

    print("DEBUG FINAL SQL:")
    print(sql)
    print("DEBUG PARAMS:")
    print(params)
    cursor.execute(sql, params)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    return [dict(zip(columns, row)) for row in rows]