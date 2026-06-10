def search_daily_logs(conn, filters, limit=2000):
    """Return Daily Logs records from dbo.esri_events using applicable search filters."""
    where_clauses = ["1 = 1"]
    params = []

    if filters["query"]:
        where_clauses.append("LOWER(COALESCE(name, '')) LIKE ?")
        params.append(f"%{filters['query'].lower()}%")

    if filters["case_number"]:
        where_clauses.append("LOWER(COALESCE(event_number, '')) LIKE ?")
        params.append(f"%{filters['case_number'].lower()}%")

    if filters["date_start"] and filters["date_end"]:
        where_clauses.append("CAST(received_at AS date) BETWEEN CAST(? AS date) AND CAST(? AS date)")
        params.extend([filters["date_start"], filters["date_end"]])
    elif filters["last_x_days"]:
        try:
            last_x_days = int(filters["last_x_days"])
        except (TypeError, ValueError):
            last_x_days = None
        if last_x_days is not None and last_x_days >= 0:
            where_clauses.append("received_at >= DATEADD(day, -?, CAST(GETDATE() AS date))")
            params.append(last_x_days)

    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT TOP {int(limit)}
            event_number,
            CONVERT(varchar(19), received_at, 126) AS received_at,
            event_status,
            activity_type,
            address,
            city,
            state,
            postal_code,
            additional_report,
            name
        FROM dbo.esri_events
        WHERE {' AND '.join(where_clauses)}
        ORDER BY received_at DESC, id DESC
        """,
        params,
    )

    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
