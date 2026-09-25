from search_sql import ADDRESS_TOKEN_ALTERNATIVES, address_search_tokens


ARRIVAL_TIME_EASTERN_SQL = """
CAST(
    DATEADD(SECOND, TRY_CONVERT(INT, TRY_CONVERT(BIGINT, e.arrival_time) / 1000), '1970-01-01')
    AT TIME ZONE 'UTC'
    AT TIME ZONE 'Eastern Standard Time'
    AS DATETIME2
)
""".strip()

EVENT_NUMBER_DISPLAY_SQL = """
COALESCE(
    NULLIF(LTRIM(RTRIM(e.event_number)), ''),
    CASE
        WHEN LOWER(COALESCE(e.activity_type, '')) LIKE '%peace%'
          OR LOWER(COALESCE(e.activity_type, '')) LIKE '%protective%'
        THEN e.generated_event_number
        ELSE NULL
    END
)
""".strip()


def search_daily_logs(conn, filters, limit=2000):
    """Return Daily Logs records from dbo.esri_events using applicable search filters."""
    where_clauses = ["1 = 1"]
    params = []

    if filters["query"]:
        where_clauses.append("LOWER(COALESCE(e.[name], '')) LIKE ?")
        params.append(f"%{filters['query'].lower()}%")

    if filters["case_number"]:
        where_clauses.append(f"LOWER(COALESCE({EVENT_NUMBER_DISPLAY_SQL}, '')) LIKE ?")
        params.append(f"%{filters['case_number'].lower()}%")

    for token in address_search_tokens(filters.get("address")):
        alternatives = ADDRESS_TOKEN_ALTERNATIVES.get(token, (token,))
        haystack = "LOWER(CONCAT_WS(' ', COALESCE(e.address, ''), COALESCE(e.city, ''), COALESCE(e.state, ''), COALESCE(e.postal_code, '')))"
        where_clauses.append("(" + " OR ".join(f"{haystack} LIKE ?" for _ in alternatives) + ")")
        params.extend(f"%{alternative}%" for alternative in alternatives)

    if filters["date_start"] and filters["date_end"]:
        where_clauses.append(f"CAST({ARRIVAL_TIME_EASTERN_SQL} AS date) BETWEEN CAST(? AS date) AND CAST(? AS date)")
        params.extend([filters["date_start"], filters["date_end"]])
    elif filters["last_x_days"]:
        try:
            last_x_days = int(filters["last_x_days"])
        except (TypeError, ValueError):
            last_x_days = None
        if last_x_days is not None and last_x_days >= 0:
            where_clauses.append(f"{ARRIVAL_TIME_EASTERN_SQL} >= DATEADD(day, -?, CAST(GETDATE() AS date))")
            params.append(last_x_days)

    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT TOP {int(limit)}
            {EVENT_NUMBER_DISPLAY_SQL} AS event_number,
            CONVERT(varchar(19), {ARRIVAL_TIME_EASTERN_SQL}, 120) AS arrival_time,
            e.event_status,
            e.activity_type,
            e.address,
            e.city,
            e.state,
            e.postal_code,
            e.notes_or_narrative,
            e.additional_report,
            e.[name] AS name,
            e.radio_id
        FROM dbo.esri_events AS e
        WHERE {' AND '.join(where_clauses)}
        ORDER BY {ARRIVAL_TIME_EASTERN_SQL} DESC, e.id DESC
        """,
        params,
    )

    columns = [
        "event_number",
        "arrival_time",
        "event_status",
        "activity_type",
        "address",
        "city",
        "state",
        "postal_code",
        "notes_or_narrative",
        "additional_report",
        "name",
        "radio_id",
    ]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
