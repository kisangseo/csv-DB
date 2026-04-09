from typing import List, Optional, Tuple


def _build_filters_sql(
    name_query: str,
    case_number: Optional[str] = None,
    dob: Optional[str] = None,
    sex: Optional[str] = None,
    race: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    issuing_county: Optional[str] = None,
    last_x_days: Optional[str] = None,
    sid: Optional[str] = None,
) -> Tuple[str, List[object]]:
    name_tokens = [t for t in (name_query or "").strip().split() if t]
    where_clauses = ["1=1"]
    params: List[object] = []

    for token in name_tokens:
        where_clauses.append("full_name LIKE ?")
        params.append(f"%{token}%")

    if case_number:
        normalized_case_number = "".join(
            ch for ch in str(case_number) if ch not in {"/", " ", "-"}
        )
        where_clauses.append(
            "REPLACE(REPLACE(REPLACE(case_number, '/', ''), ' ', ''), '-', '') LIKE ?"
        )
        params.append(f"%{normalized_case_number}%")

    if date_start and date_end:
        where_clauses.append(
            """
            COALESCE(issue_date, intake_date)
                BETWEEN CAST(? AS date) AND CAST(? AS date)
            """.strip()
        )
        params.extend([date_start, date_end])

    if sex:
        normalized_sex = sex.strip().lower()
        if normalized_sex == "male":
            where_clauses.append("LOWER(sex) IN (?, ?)")
            params.extend(["male", "m"])
        elif normalized_sex == "female":
            where_clauses.append("LOWER(sex) IN (?, ?)")
            params.extend(["female", "f"])
        else:
            where_clauses.append("LOWER(sex) = ?")
            params.append(normalized_sex)

    if last_x_days:
        where_clauses.append(
            """
            (
                issue_date IS NOT NULL
                OR intake_date IS NOT NULL
            )
            """.strip()
        )
        where_clauses.append(
            "COALESCE(issue_date, intake_date) >= DATEADD(day, -?, CAST(GETDATE() AS date))"
        )
        params.append(int(last_x_days))

    if race:
        where_clauses.append("LOWER(race) = ?")
        params.append(race.lower())

    if issuing_county:
        where_clauses.append(
            """
            issuing_county IS NOT NULL
            AND LTRIM(RTRIM(issuing_county)) != ''
            AND LOWER(issuing_county) LIKE ?
            """.strip()
        )
        params.append(f"%{issuing_county.lower()}%")

    if sid:
        where_clauses.append("sid = ?")
        params.append(str(sid))

    if dob:
        where_clauses.append("date_of_birth = CAST(? AS date)")
        params.append(dob)

    return "\n    AND ".join(where_clauses), params


def build_search_sql(
    select_sql: str,
    from_sql: str,
    name_query: str,
    case_number: Optional[str] = None,
    dob: Optional[str] = None,
    sex: Optional[str] = None,
    race: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    issuing_county: Optional[str] = None,
    last_x_days: Optional[str] = None,
    sid: Optional[str] = None,
    order_by: str = "created_at DESC",
    extra_where: Optional[List[str]] = None,
) -> Tuple[str, List[object]]:
    where_sql, params = _build_filters_sql(
        name_query=name_query,
        case_number=case_number,
        dob=dob,
        sex=sex,
        race=race,
        date_start=date_start,
        date_end=date_end,
        issuing_county=issuing_county,
        last_x_days=last_x_days,
        sid=sid,
    )

    if extra_where:
        where_sql = "\n    AND ".join([where_sql] + [clause for clause in extra_where if clause])

    sql = f"""
    SELECT
        {select_sql}
    FROM {from_sql}
    WHERE {where_sql}
    ORDER BY {order_by}
    """
    return sql, params


def search_by_name(conn, name_query, case_number=None, dob=None, sex=None, race=None, date_start=None, date_end=None, issuing_county=None, last_x_days=None, sid=None, limit=100):
    cursor = conn.cursor()

    select_sql = """
        record_id,
        COALESCE(full_name, tenant_defendant_or_respondent, resp_name) AS name,
        sid AS sid,
        FORMAT(date_of_birth, 'yyyy-MM-dd') AS date_of_birth,
        facility AS facility,
        case_number AS case_number,
        COALESCE(address, tenant_defendant_or_respondent_address, doc_address, location_of_prior_attempt) AS address,
        apt AS apt,
        city AS city,
        postal_code AS postal_code,
        COALESCE(petitioner_name, petitioner_or_plaintiff_name) AS petitioner_name,
        global_id,
        COALESCE(served_by, serving_or_attempting_deputy, member_reporting, return_deputy) AS served_by,
        x AS x,
        y AS y,
        state AS state,
        COALESCE(notes, notes_from_attempt) AS notes,
        warrant_type AS warrant_type,
        court_document_type,
        FORMAT(COALESCE(issue_date, court_issued_date), 'yyyy-MM-dd') AS issue_date,
        FORMAT(intake_date, 'yyyy-MM-dd') AS intake_date,
        FORMAT(date_time_attempted, 'yyyy-MM-dd') AS date_time_attempted,
        FORMAT(date_time_attempted, 'MM-dd-yyyy hh:mm tt') AS date_time_attempted_display,
        FORMAT(prior_attempt_date, 'yyyy-MM-dd') AS prior_attempt_date,
        FORMAT(date_received, 'yyyy-MM-dd') AS date_received,
        FORMAT(COALESCE(issue_date, court_issued_date, intake_date, date_time_attempted, prior_attempt_date, date_received), 'yyyy-MM-dd') AS record_date,
        warrant_status AS warrant_status,
        COALESCE(disposition, administrative_status, service_disp) AS disposition,
        warrant_id_number,
        sex AS sex,
        race AS race,
        issuing_county AS issuing_county,
        source_file,
        CASE
            WHEN source_file = 'AllActiveWarrants_0.csv'
                THEN 'Active Warrants'
            WHEN department = 'BCSO_ACTIVE_WARRANTS'
                THEN 'BCSO Active Warrants'
            ELSE department
        END AS department
    """

    sql, params = build_search_sql(
        select_sql=select_sql,
        from_sql="search.records",
        name_query=name_query,
        case_number=case_number,
        dob=dob,
        sex=sex,
        race=race,
        date_start=date_start,
        date_end=date_end,
        issuing_county=issuing_county,
        last_x_days=last_x_days,
        sid=sid,
    )

    cursor.execute(sql, params)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    if limit:
        rows = rows[:limit]

    return [dict(zip(columns, row)) for row in rows]


print("🔥 USING CAST DATE VERSION 🔥")
