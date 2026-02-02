def search_by_name(conn, name_query, case_number=None, dob=None, sex=None, race=None, date_start=None, date_end=None, issuing_county=None, last_x_days=None, sid=None, limit=100):
    name_tokens = [t for t in name_query.strip().split() if t]
    cursor = conn.cursor()

    sql = """
    SELECT
        full_name AS name,
        sid AS sid,
        FORMAT(date_of_birth, 'yyyy-MM-dd') AS date_of_birth,
        facility AS facility,
        case_number AS case_number,
        address AS address,
        
        notes AS notes,
        warrant_type AS warrant_type,
        court_document_type,
        intake_date,
        FORMAT(COALESCE(issue_date, intake_date), 'yyyy-MM-dd') AS record_date,
        warrant_status AS warrant_status,
        disposition AS disposition,
        warrant_id_number,
        
        sex AS sex,
        race AS race,
        issuing_county AS issuing_county,
        CASE
            WHEN source_file = 'AllActiveWarrants_0.csv'
                THEN 'Active Warrants'
            WHEN department = 'BCSO_ACTIVE_WARRANTS'
                THEN 'BCSO Active Warrants'
            ELSE department
        END AS department
    FROM search.records
    WHERE 1=1
    
    """

    params = []

    for token in name_tokens:
        sql += " AND full_name LIKE ?"
        params.append(f"%{token}%")

    if case_number:
        sql += " AND case_number LIKE ?"
        params.append(f"%{case_number}%")
    if date_start and date_end:
        sql += """
        AND COALESCE(issue_date, intake_date)
            BETWEEN CAST(? AS date) AND CAST(? AS date)
        """
        params.extend([date_start, date_end])
        print("DEBUG SQL =", sql)
        print("DEBUG params =", params)

    if sex:
        sql += " AND sex = ?"
        params.append(sex)

    if last_x_days:
        sql += """
        AND (
            issue_date IS NOT NULL
            OR intake_date IS NOT NULL
        )
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

print("🔥 USING CAST DATE VERSION 🔥")