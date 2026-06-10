def search_daily_logs(conn, filters, limit=2000):
    """Return Daily Logs records from dbo.esri_events using applicable search filters."""
    where_clauses = ["1 = 1"]
    params = []

    if filters["query"]:
        where_clauses.append("LOWER(COALESCE(e.[name], '')) LIKE ?")
        params.append(f"%{filters['query'].lower()}%")

    if filters["case_number"]:
        where_clauses.append("LOWER(COALESCE(e.event_number, '')) LIKE ?")
        params.append(f"%{filters['case_number'].lower()}%")

    if filters["date_start"] and filters["date_end"]:
        where_clauses.append("CAST(e.received_at AS date) BETWEEN CAST(? AS date) AND CAST(? AS date)")
        params.extend([filters["date_start"], filters["date_end"]])
    elif filters["last_x_days"]:
        try:
            last_x_days = int(filters["last_x_days"])
        except (TypeError, ValueError):
            last_x_days = None
        if last_x_days is not None and last_x_days >= 0:
            where_clauses.append("e.received_at >= DATEADD(day, -?, CAST(GETDATE() AS date))")
            params.append(last_x_days)

    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT TOP {int(limit)}
            e.event_number,
            CONVERT(varchar(19), e.received_at, 126) AS received_at,
            e.event_status,
            e.activity_type,
            e.address,
            e.city,
            e.state,
            e.postal_code,
            e.additional_report,
            e.[name] AS name,
            e.radio_id
        FROM dbo.esri_events AS e
        WHERE {' AND '.join(where_clauses)}
        ORDER BY e.received_at DESC, e.id DESC
        """,
        params,
    )

    columns = [
        "event_number",
        "received_at",
        "event_status",
        "activity_type",
        "address",
        "city",
        "state",
        "postal_code",
        "additional_report",
        "name",
        "radio_id",
    ]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
