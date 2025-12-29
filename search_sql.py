def search_by_name(conn, name_query, case_number=None, dob=None, sex=None, race=None, date_start=None, date_end=None, issuing_county=None, last_x_days=None, sid=None, limit=100):
    
    cursor = conn.cursor()

    sql = """
    SELECT TOP (?)
        full_name      AS name,
        sid            AS sid,
        case_number    AS case_number,
        address        AS address,
        warrant_type   AS warrant_type,
        COALESCE(issue_date, intake_date) AS record_date,
        warrant_status AS warrant_status,
        disposition    AS disposition,
        department
    FROM search.records
    WHERE full_name LIKE ?
    """

    params = [limit, f"%{name_query}%"]

    if case_number:
        sql += " AND case_number LIKE ?"
        params.append(f"%{case_number}%")
    if date_start and date_end:
        sql += """
        AND COALESCE(issue_date, intake_date)
            BETWEEN CAST(? AS date) AND CAST(? AS date)
        """
        params.extend([date_start, date_end])

    if sex:
        sql += " AND sex = ?"
        params.append(sex)

    if last_x_days:
        sql += """
        AND COALESCE(issue_date, intake_date) >=
            DATEADD(day, -?, CAST(GETDATE() AS date))
        """
        params.append(int(last_x_days))
    if race:
        sql += " AND LOWER(race) = ?"
        params.append(race.lower())
    if issuing_county:
        sql += """
        AND issuing_county IS NOT NULL
        AND LTRIM(RTRIM(issuing_county)) != ''
        AND LOWER(issuing_county) LIKE ?
        """
        params.append(f"%{issuing_county.lower()}%")
    if sid:
        sql += " AND sid = ?"
        params.append(str(sid))
    if dob:
        sql += " AND date_of_birth = CAST(? AS date)"
        params.append(dob)

    sql += " ORDER BY created_at DESC"

    
    cursor.execute(sql, params)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    return [dict(zip(columns, row)) for row in rows]