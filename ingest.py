from db_connect import get_conn
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os
import pyodbc
import re
import requests
from datetime import datetime
print("USING INGEST.PY FROM:", __file__)



def geocode_address(address, city=None, state=None, postal_code=None):
    if not address:
        return None, None

    address_text = str(address).strip().strip('"').strip("'")

    parts = [p.strip() for p in address_text.split(",") if p and p.strip()]
    if len(parts) >= 2:
        first_part = parts[0]
        second_part = parts[1]
        looks_like_leading_code = (
            bool(re.fullmatch(r"[A-Z0-9 ]{6,}", first_part, flags=re.IGNORECASE))
            and any(ch.isdigit() for ch in first_part)
            and bool(re.match(r"^\d+\s+\S+", second_part))
        )
        if looks_like_leading_code:
            parts = parts[1:]

    cleaned_text = ", ".join(parts) if parts else address_text

    def _extract_postal5(value):
        m = re.search(r"\b(\d{5})(?:-\d{4})?\b", str(value or ""))
        return m.group(1) if m else None

    city_text = str(city or "").strip()
    state_text = str(state or "").strip()
    input_zip = _extract_postal5(postal_code) or _extract_postal5(cleaned_text)

    url = "https://atlas.microsoft.com/search/address/json"
    key = os.getenv("AZURE_MAPS_KEY")

    if not key:
        print("ERROR: AZURE_MAPS_KEY is missing")
        return None, None

    query_candidates = [cleaned_text]
    if city_text or state_text or input_zip:
        expanded_parts = [cleaned_text]
        if city_text:
            expanded_parts.append(city_text)
        if state_text:
            expanded_parts.append(state_text)
        if input_zip:
            expanded_parts.append(input_zip)
        expanded_query = ", ".join([part for part in expanded_parts if part])
        if expanded_query not in query_candidates:
            query_candidates.insert(0, expanded_query)
    if len(parts) >= 4:
        state_part = parts[-2].upper()
        city_part = parts[-3].upper()
        if ("MARYLAND" in state_part or state_part == "MD") and city_part != "BALTIMORE":
            baltimore_parts = parts[:]
            baltimore_parts[-3] = "Baltimore"
            forced_baltimore = ", ".join(baltimore_parts)
            if forced_baltimore not in query_candidates:
                query_candidates.append(forced_baltimore)

    try:
        for query in query_candidates:
            params = {
                "api-version": "1.0",
                "subscription-key": key,
                "query": query,
                "countrySet": "US",
                "limit": 5,
            }
            r = requests.get(url, params=params, timeout=5)
            print("GEOCODE STATUS:", r.status_code)

            if r.status_code != 200:
                print("GEOCODE ERROR:", r.text[:300])
                continue

            data = r.json()

            results = data.get("results") or []
            if results:
                def _is_md(addr):
                    state_code = str(addr.get("countrySubdivisionCode") or "").upper()
                    state_name = str(addr.get("countrySubdivision") or "").upper()
                    return state_code in {"MD", "US-MD"} or "MARYLAND" in state_name

                def _score(result):
                    addr = result.get("address") or {}
                    score = 0
                    if input_zip and _extract_postal5(addr.get("postalCode")) == input_zip:
                        score += 100
                    if _is_md(addr):
                        score += 50
                    return score + float(result.get("score") or 0.0)

                best = max(results, key=_score)
                pos = best["position"]
                return pos["lon"], pos["lat"]

        print("NO RESULTS FOR:", address)

    except Exception as e:
        print("GEOCODE EXCEPTION:", str(e))

    return None, None


def geocode_postal_code(address, city=None, state=None):
    if not address:
        return None

    address_text = str(address).strip().strip('"').strip("'")
    city_text = str(city or "").strip()
    state_text = str(state or "").strip()

    query_parts = [address_text]
    if city_text:
        query_parts.append(city_text)
    if state_text:
        query_parts.append(state_text)
    query = ", ".join([part for part in query_parts if part])

    url = "https://atlas.microsoft.com/search/address/json"
    key = os.getenv("AZURE_MAPS_KEY")
    if not key:
        print("ERROR: AZURE_MAPS_KEY is missing")
        return None

    try:
        params = {
            "api-version": "1.0",
            "subscription-key": key,
            "query": query,
            "countrySet": "US",
            "limit": 5,
        }
        r = requests.get(url, params=params, timeout=5)
        if r.status_code != 200:
            print("GEOCODE ZIP ERROR:", r.text[:300])
            return None

        data = r.json()
        results = data.get("results") or []
        if not results:
            return None

        state_norm = state_text.upper()
        city_norm = city_text.upper()

        def _is_md(addr):
            state_code = str(addr.get("countrySubdivisionCode") or "").upper()
            state_name = str(addr.get("countrySubdivision") or "").upper()
            return state_code in {"MD", "US-MD"} or "MARYLAND" in state_name

        def _score(result):
            addr = result.get("address") or {}
            score = float(result.get("score") or 0.0)
            if _is_md(addr):
                score += 25
            if state_norm:
                addr_state_code = str(addr.get("countrySubdivisionCode") or "").upper()
                addr_state_name = str(addr.get("countrySubdivision") or "").upper()
                if state_norm in {addr_state_code, addr_state_name, f"US-{state_norm}"}:
                    score += 15
            if city_norm:
                addr_city = str(addr.get("municipality") or addr.get("localName") or "").upper()
                if addr_city and addr_city == city_norm:
                    score += 15
            return score

        best = max(results, key=_score)
        addr = best.get("address") or {}
        return _normalize_postal_code(addr.get("postalCode"))
    except Exception as exc:
        print("GEOCODE ZIP EXCEPTION:", str(exc))
        return None

print(geocode_address("3900 Kimble Rd Baltimore MD"))


def ingest_dv_csv_one_time(csv_file_name="dv_pdf_records.csv"):
    """
    One-time ingest for DV CSV files that:
      1) stores display columns in search.dv_pdf_records
      2) stores the full original CSV row JSON in SQL (source_row_json)
    """
    csv_path = os.path.join(os.path.dirname(__file__), csv_file_name)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    normalized_cols = {str(c).strip().lower(): c for c in df.columns}

    def col(*candidates):
        for c in candidates:
            hit = normalized_cols.get(str(c).strip().lower())
            if hit:
                return hit
        return None

    case_col = col("Case Number", "case_number", "Warrant Case Number", "orderID and Version", "ObjectID")
    respondent_col = col("Respondent Name", "respondent_name", "Name")
    issue_col = col("Date Order was Issued", "Issue Date", "issue_date", "Issue Date Update")
    type_col = col("Order Type", "type", "CourtType", "Order Classification")
    status_col = col("Order Status", "order_status", "Order Status ID")
    pdf_col = col("pdf_download", "PDF Download")

    if not case_col or not respondent_col:
        raise ValueError(
            "CSV must include case/respondent columns (e.g. 'Case Number' and 'Respondent Name')."
        )

    def parse_recency(row_dict):
        recency_candidates = [
            "Date and Time of Reissue",
            "Date and Time of Dispostion",
            "Issue Date Update",
            "EditDate",
            "CreationDate",
            "uploaded_at",
            "Date Order was Issued",
            "Issue Date",
            "issue_date",
        ]
        best = pd.NaT
        for candidate in recency_candidates:
            c = col(candidate)
            if not c:
                continue
            value = str(row_dict.get(c, "")).strip()
            if not value:
                continue
            parsed = pd.to_datetime(value, errors="coerce")
            if pd.isna(parsed):
                continue
            if pd.isna(best) or parsed > best:
                best = parsed
        return best

    def info_score(row_dict):
        return sum(1 for _, v in row_dict.items() if str(v).strip())

    # collapse duplicates in the CSV first:
    # keep row with most filled-in columns; tie-break by most recent timestamp
    best_rows_by_key = {}
    duplicate_rows_dropped = 0
    for _, row in df.iterrows():
        case_number = str(row.get(case_col, "")).strip()
        respondent_name = str(row.get(respondent_col, "")).strip()
        if not case_number or not respondent_name:
            continue
        key = (case_number.lower(), respondent_name.lower())
        row_dict = row.to_dict()
        candidate_score = info_score(row_dict)
        candidate_recency = parse_recency(row_dict)

        current = best_rows_by_key.get(key)
        if current is None:
            best_rows_by_key[key] = (row_dict, candidate_score, candidate_recency)
            continue

        _, current_score, current_recency = current
        replace = False
        if candidate_score > current_score:
            replace = True
        elif candidate_score == current_score:
            if pd.isna(current_recency) and not pd.isna(candidate_recency):
                replace = True
            elif not pd.isna(current_recency) and not pd.isna(candidate_recency) and candidate_recency > current_recency:
                replace = True

        if replace:
            best_rows_by_key[key] = (row_dict, candidate_score, candidate_recency)
            duplicate_rows_dropped += 1
        else:
            duplicate_rows_dropped += 1

    conn = get_conn()
    inserted = 0
    skipped = 0
    updated_existing = 0

    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'search' AND TABLE_NAME = 'dv_pdf_records'
            """
        )
        if cur.fetchone() is None:
            raise RuntimeError("SQL table search.dv_pdf_records does not exist.")

        cur.execute(
            """
            IF COL_LENGTH('search.dv_pdf_records', 'source_row_json') IS NULL
                ALTER TABLE search.dv_pdf_records ADD source_row_json NVARCHAR(MAX) NULL;
            IF COL_LENGTH('search.dv_pdf_records', 'source_csv_name') IS NULL
                ALTER TABLE search.dv_pdf_records ADD source_csv_name NVARCHAR(260) NULL;
            IF COL_LENGTH('search.dv_pdf_records', 'order_status') IS NULL
                ALTER TABLE search.dv_pdf_records ADD order_status NVARCHAR(255) NULL;
            """
        )

        csv_to_sql_col = {}
        used_sql_cols = set()
        for original_col in df.columns:
            normalized = re.sub(r"[^a-z0-9]+", "_", str(original_col).strip().lower()).strip("_")
            if not normalized:
                normalized = "column"
            if normalized[0].isdigit():
                normalized = f"c_{normalized}"
            base_sql_col = f"csv_{normalized}"
            sql_col = base_sql_col
            suffix = 2
            while sql_col in used_sql_cols:
                sql_col = f"{base_sql_col}_{suffix}"
                suffix += 1
            used_sql_cols.add(sql_col)
            csv_to_sql_col[original_col] = sql_col

        for sql_col in csv_to_sql_col.values():
            escaped = sql_col.replace("]", "]]")
            cur.execute(
                f"""
                IF COL_LENGTH('search.dv_pdf_records', '{escaped}') IS NULL
                    ALTER TABLE search.dv_pdf_records ADD [{escaped}] NVARCHAR(MAX) NULL;
                """
            )

        cur.execute(
            """
            SELECT
                LOWER(LTRIM(RTRIM(case_number))) AS case_number_norm,
                LOWER(LTRIM(RTRIM(respondent_name))) AS respondent_name_norm
            FROM search.dv_pdf_records
            WHERE is_reissue = 0
            """
        )
        existing_non_reissues = {(r[0] or "", r[1] or "") for r in cur.fetchall()}

        dynamic_column_sql = ", ".join(f"[{name.replace(']', ']]')}]" for name in csv_to_sql_col.values())
        dynamic_placeholders = ", ".join("?" for _ in csv_to_sql_col)

        insert_sql = f"""
            INSERT INTO search.dv_pdf_records
                (
                    case_number,
                    respondent_name,
                    issue_date,
                    order_type,
                    order_status,
                    blob_name,
                    pdf_download,
                    uploaded_at,
                    is_reissue,
                    source_row_json,
                    source_csv_name,
                    {dynamic_column_sql}
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), ?, CAST(? AS NVARCHAR(MAX)), CAST(? AS NVARCHAR(260)), {dynamic_placeholders})
        """

        dynamic_update_set = ", ".join(
            f"[{name.replace(']', ']]')}] = CASE "
            f"WHEN NULLIF(LTRIM(RTRIM([{name.replace(']', ']]')}])), '') IS NULL THEN ? "
            f"ELSE [{name.replace(']', ']]')}] END"
            for name in csv_to_sql_col.values()
        )
        update_sql = f"""
            UPDATE search.dv_pdf_records
            SET
                issue_date = CASE WHEN issue_date IS NULL THEN ? ELSE issue_date END,
                order_type = CASE WHEN NULLIF(LTRIM(RTRIM(order_type)), '') IS NULL THEN ? ELSE order_type END,
                order_status = CASE WHEN NULLIF(LTRIM(RTRIM(order_status)), '') IS NULL THEN ? ELSE order_status END,
                blob_name = CASE WHEN NULLIF(LTRIM(RTRIM(blob_name)), '') IS NULL THEN ? ELSE blob_name END,
                pdf_download = CASE WHEN NULLIF(LTRIM(RTRIM(pdf_download)), '') IS NULL THEN ? ELSE pdf_download END,
                source_row_json = CASE WHEN NULLIF(LTRIM(RTRIM(source_row_json)), '') IS NULL THEN CAST(? AS NVARCHAR(MAX)) ELSE source_row_json END,
                source_csv_name = CASE WHEN NULLIF(LTRIM(RTRIM(source_csv_name)), '') IS NULL THEN CAST(? AS NVARCHAR(260)) ELSE source_csv_name END,
                {dynamic_update_set}
            WHERE LOWER(LTRIM(RTRIM(case_number))) = ?
              AND LOWER(LTRIM(RTRIM(respondent_name))) = ?
              AND is_reissue = 0
        """

        for key, (row_dict, _, _) in best_rows_by_key.items():
            case_number = str(row_dict.get(case_col, "")).strip()
            respondent_name = str(row_dict.get(respondent_col, "")).strip()
            if not case_number or not respondent_name:
                skipped += 1
                continue

            issue_text = str(row_dict.get(issue_col, "")).strip() if issue_col else ""
            issue_date = pd.to_datetime(issue_text, errors="coerce")
            issue_date_value = None if pd.isna(issue_date) else issue_date.strftime("%Y-%m-%d")

            order_type = str(row_dict.get(type_col, "")).strip() if type_col else ""
            order_status = str(row_dict.get(status_col, "")).strip() if status_col else ""
            pdf_download = str(row_dict.get(pdf_col, "")).strip() if pdf_col else ""
            blob_name = pdf_download.replace("/dv-pdf/file/", "", 1).strip("/")
            source_row_json = json.dumps(row_dict, ensure_ascii=False, default=str)
            dynamic_values = [str(row_dict.get(original_col, "")).strip() for original_col in csv_to_sql_col]

            if key in existing_non_reissues:
                cur.execute(
                    update_sql,
                    issue_date_value,
                    order_type,
                    order_status,
                    blob_name,
                    pdf_download,
                    source_row_json,
                    os.path.basename(csv_path),
                    *dynamic_values,
                    key[0],
                    key[1],
                )
                if cur.rowcount and cur.rowcount > 0:
                    updated_existing += 1
                continue

            cur.execute(
                insert_sql,
                case_number,
                respondent_name,
                issue_date_value,
                order_type,
                order_status,
                blob_name,
                pdf_download,
                0,
                source_row_json,
                os.path.basename(csv_path),
                *dynamic_values,
            )
            inserted += 1

        conn.commit()
        print(
            f"DV one-time ingest complete for {os.path.basename(csv_path)}: "
            f"inserted={inserted}, skipped={skipped}, "
            f"duplicate_rows_dropped={duplicate_rows_dropped}, updated_existing={updated_existing}"
        )
    finally:
        conn.close()


def insert_search_record_warrants(cursor, record):
    
    sql = """
        INSERT INTO search.records (
            department,
            source_file,
            first_name,
            last_name,
            full_name,
            date_of_birth,
            sid,
            case_number,
            warrant_type,
            warrant_status,
            issue_date,
            intake_date,
            address,
            city,
            state,
            postal_code,
            court_document_type,
            disposition,
            notes,
            sex,
            race,
            issuing_county
        )
        OUTPUT INSERTED.record_id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    values = (
        record.get("department"),
        record.get("source_file"),
        record.get("first_name"),
        record.get("last_name"),
        record.get("full_name"),
        record.get("date_of_birth"),
        record.get("sid"),
        record.get("case_number"),
        record.get("warrant_type"),
        record.get("warrant_status"),
        record.get("issue_date"),
        record.get("intake_date"),
        record.get("address"),
        record.get("city"),
        record.get("state"),
        record.get("postal_code"),
        record.get("court_document_type"),
        record.get("disposition"),
        record.get("notes"),
        record.get("sex"),
        record.get("race"),
        record.get("issuing_county"),
            )
    
    cursor.execute(sql, *values)
    

    
    
    return cursor.fetchone()[0]

def insert_search_record_active_warrants(cursor, record):
    sql = """
        INSERT INTO search.records (
            department,
            source_file,
            full_name,
            case_number,
            warrant_id_number,
            warrant_type,
            issue_date,
            warrant_status,
            sid,
            date_of_birth,
            race,
            sex,
            issuing_county,
            address,
            x,
            y
        )
        OUTPUT INSERTED.record_id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        record.get("department"),
        record.get("source_file"),
        record.get("full_name"),
        record.get("case_number"),
        record.get("warrant_id_number"),
        record.get("warrant_type"),
        record.get("issue_date"),
        record.get("warrant_status"),
        record.get("sid"),
        record.get("date_of_birth"),
        record.get("race"),
        record.get("sex"),
        record.get("issuing_county"),
        record.get("address"),
        record.get("x"),
        record.get("y"),
    )

    cursor.execute(sql, values)
    return cursor.fetchone()[0]

def insert_search_record_population(cursor, record):
    sql = """
        INSERT INTO search.records (
            department,
            source_file,
            first_name,
            last_name,
            full_name,
            date_of_birth,
            sid,
            facility
        )
        OUTPUT INSERTED.record_id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = (
        record.get("department"),
        record.get("source_file"),
        record.get("first_name"),
        record.get("last_name"),
        record.get("full_name"),
        record.get("date_of_birth"),
        record.get("sid"),
        record.get("facility"),
    )

    cursor.execute(sql, *values)
    return cursor.fetchone()[0]


def insert_search_record_fsdw(cursor, record):
    sql = """
    INSERT INTO search.records (
        department,
        source_file,
        full_name,
        case_number,
        issue_date,
        intake_date,
        address,
        city,
        state,
        postal_code,
        court_document_type,
        disposition,
        notes
    )
    OUTPUT INSERTED.record_id
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def clean(v):
        return None if v is None or (isinstance(v, float) and pd.isna(v)) else v

    values = tuple(clean(v) for v in (
        record.get("department"),
        record.get("source_file"),
        record.get("full_name"),
        record.get("case_number"),
        record.get("issue_date"),
        record.get("intake_date"),
        record.get("address"),
        record.get("city"),
        record.get("state"),
        record.get("postal_code"),
        record.get("court_document_type"),
        record.get("disposition"),
        record.get("notes"),
    ))

    cursor.execute(sql, *values)
    return cursor.fetchone()[0]





def insert_search_record_odyssey(cursor, record):
    sql = """
    INSERT INTO search.records (
        department,
        source_file,
        full_name,
        case_number,
        intake_date,
        address,
        apt,
        x,
        y,
        city,
        state,
        court_document_type,
        disposition,
        notes
    )
    OUTPUT INSERTED.record_id
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def clean(v):
        return None if v is None or (isinstance(v, float) and pd.isna(v)) else v

    values = tuple(clean(v) for v in (
        record.get("department"),
        record.get("source_file"),
        record.get("full_name"),
        record.get("case_number"),
        record.get("intake_date"),
        record.get("address"),
        record.get("apt"),
        record.get("x"),
        record.get("y"),
        record.get("city"),
        record.get("state"),
        record.get("court_document_type"),
        record.get("disposition"),
        record.get("notes"),
    ))
    cursor.execute(sql, *values)
    return cursor.fetchone()[0]


def insert_search_record_civil_papers(cursor, record):
    
    sql = """
    INSERT INTO search.records (
        department,
        source_file,

        case_number,
        court_document_type,

        type_of_rfs,
        type_of_child_support,

        doc_address,
        unit,

        resp_name,
        agid,
        unit_id,

        return_deputy,
        return_rank,
        return_sequence,
        return_email,

        member_reporting,

        date_time_attempted,
        service_disp,

        prior_attempt_date_admin,
        prior_attempt_date,
        location_of_prior_attempt,

        method_of_service,

        two_prior,
        name_of_adult,
        relationship_to_respondent,

        reason_for_non_est,
        reason_for_non_est_other,

        notes_from_attempt,

        parent_document,

        date_received,

        globalid,
        objectid
    )
    OUTPUT INSERTED.record_id
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def clean(v):
        return None if v is None or (isinstance(v, float) and pd.isna(v)) else v

    values = tuple(clean(v) for v in (
    "Civil Papers",
    "survey123",

    record.get("Doc"),
    record.get("type"),

    record.get("Type of RFS"),
    record.get("Type of Child Support"),

    record.get("doc address"),
    record.get("unit"),

    record.get("Resp Name"),
    record.get("AgId"),
    record.get("Unit ID"),

    record.get("return Deputy"),
    record.get("return Rank"),
    record.get("return Sequence"),
    record.get("return email"),

    record.get("Member Reporting"),

    record.get("Date and Time Attempted"),
    record.get("Service Disp"),

    record.get("Prior Attempt Date - Admin"),
    record.get("Prior Attempt Date"),
    record.get("Location of Prior Attempt"),

    record.get("method of service"),

    record.get("Two prior: Yes"),
    record.get("Name of Adult"),
    record.get("Relationship to Respondent"),

    record.get("Reason for Non Est"),
    record.get("reason_for_non_est_other"),

    record.get("Notes from Attempt"),

    record.get("Parent Document"),

    record.get("Date Received"),

    record.get("globalid"),
    record.get("objectid"),
))

    cursor.execute(sql, *values)
    return cursor.fetchone()[0]

def ensure_civil_papers_columns(cursor):
    cursor.execute("""
        IF COL_LENGTH('search.records', 'global_id') IS NULL
            ALTER TABLE search.records ADD global_id NVARCHAR(255) NULL;
        IF COL_LENGTH('search.records', 'petitioner_name') IS NULL
            ALTER TABLE search.records ADD petitioner_name NVARCHAR(500) NULL;
        IF COL_LENGTH('search.records', 'served_by') IS NULL
            ALTER TABLE search.records ADD served_by NVARCHAR(500) NULL;
    """)


def _pick_row_value(row, *candidates):
    def normalize_key(value):
        return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").strip().lower().split())

    normalized_columns = {
        normalize_key(col): col
        for col in getattr(row, "index", [])
    }

    for key in candidates:
        direct = row.get(key) if key in row else None
        if pd.notna(direct):
            text = str(direct).strip()
            if text:
                return text

        match_col = normalized_columns.get(normalize_key(key))
        if match_col is None:
            continue
        value = row.get(match_col)
        if pd.notna(value):
            text = str(value).strip()
            if text:
                return text
    return None


def _normalize_postal_code(value):
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", str(value or ""))
    return m.group(1) if m else None


def ingest_civil_papers_one_time(file_name="survey_0.csv"):
    """
    One-time ingest for CIVIL PAPERS from a local CSV file located next to ingest.py.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    candidate_paths = [
        os.path.join(base_dir, file_name),
        os.path.join(base_dir, "survey_0"),
    ]
    csv_path = next((p for p in candidate_paths if os.path.exists(p)), None)
    if not csv_path:
        raise FileNotFoundError(
            f"Could not find '{file_name}' (or 'survey_0') in {base_dir}"
        )

    df = pd.read_csv(csv_path, low_memory=False)
    source_file = os.path.basename(csv_path)

    conn = get_conn()
    try:
        cursor = conn.cursor()
        ensure_civil_papers_columns(cursor)
        cursor.execute("""
            DELETE FROM search.records
            WHERE department = 'CIVIL PAPERS' AND source_file = ?
        """, source_file)

        inserted = 0
        for _, row in df.iterrows():
            record = {
                "department": "CIVIL PAPERS",
                "source_file": source_file,
                "global_id": _pick_row_value(row, "GlobalID"),
                "intake_date": safe_sql_date(_pick_row_value(row, "Intake Date")),
                "case_number": _pick_row_value(row, "Case Number"),
                "court_document_type": _pick_row_value(row, "Court Document Type"),
                "issue_date": safe_sql_date(_pick_row_value(row, "Court Issued Date")),
                "full_name": _pick_row_value(row, "Tenant, Defendant, or Respondent Name"),
                "address": _pick_row_value(row, "Tenant, Defendant or Respondent Address"),
                "petitioner_name": _pick_row_value(row, "Petitioner or Plaintiff Name"),
                "disposition": _pick_row_value(row, "Administrative Status"),
                "served_by": _pick_row_value(row, "Served By"),
                "notes": _pick_row_value(row, "Comments"),
            }
            record_id = insert_search_record_civil_papers(cursor, record)
            insert_raw_record(cursor, record_id, source_file, row.to_dict())
            inserted += 1

        conn.commit()
        print(f"CIVIL PAPERS one-time ingest complete. Inserted {inserted} rows from {source_file}.")
    finally:
        conn.close()


APT_RE = re.compile(r"(?i)\bapt\.?\s*#?\s*([A-Za-z0-9-]+)\b")
STREET_SUFFIXES = (
    "aly", "allee", "ave", "avenue", "blvd", "boulevard", "cir", "circle",
    "court", "ct", "dr", "drive", "hwy", "highway", "lane", "ln", "parkway",
    "pkwy", "pl", "place", "rd", "road", "st", "streat", "street", "ter",
    "terrace", "way",
)
STREET_SUFFIX_RE = re.compile(
    r"(?i)^(.*\b(?:"
    + "|".join(STREET_SUFFIXES)
    + r")\.?)(?:\s*,\s*|\s+)(.+)$"
)
ADDRESS_COLUMN_CANDIDATES = (
    "TenantAddress",
    "Address",
    "address",
    "Street Address",
    "street_address",
)
APT_COPY_SUFFIX = "_with_apt_unit.csv"
LATEST_LT_WITH_APT_BLOB_NAME = "latest_landlord_tenant_with_apt.csv"
ODYSSEY_FILE_DATE_RE = re.compile(r"(?i)^Odyssey-JobOutput-([A-Za-z]+ \d{1,2}, \d{4})")


def split_address_and_apt(address):
    if address is None:
        return None, None
    text = str(address).strip()
    if not text:
        return None, None

    match = APT_RE.search(text)
    if match:
        apt_value = match.group(1).strip().lstrip("#")
        cleaned = (text[:match.start()] + " " + text[match.end():]).strip()
        cleaned = re.sub(r"\s+,", ",", cleaned)
        cleaned = re.sub(r",\s*,", ", ", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" ,")
        return cleaned or None, (apt_value if apt_value else None)

    suffix_match = STREET_SUFFIX_RE.match(text)
    if not suffix_match:
        return text, None

    street = re.sub(r"\s{2,}", " ", suffix_match.group(1)).strip(" ,")
    trailing = suffix_match.group(2).strip(" ,")
    return (street or None), (trailing or None)


def ensure_records_apt_column(cursor):
    cursor.execute("""
        IF COL_LENGTH('search.records', 'apt') IS NULL
        BEGIN
            ALTER TABLE search.records ADD apt NVARCHAR(100) NULL
        END
    """)


def normalize_existing_fsd_apt_records(cursor):
    ensure_records_apt_column(cursor)
    cursor.execute("""
        SELECT record_id, address, apt
        FROM search.records
        WHERE LOWER(LTRIM(RTRIM(department))) = 'field services department'
    """)
    rows = cursor.fetchall()

    for record_id, address, existing_apt in rows:
        normalized_address, parsed_apt = split_address_and_apt(address)
        clean_existing_apt = None
        if existing_apt is not None:
            existing_match = APT_RE.search(str(existing_apt))
            if existing_match:
                clean_existing_apt = existing_match.group(1).strip().lstrip("#")
            else:
                clean_existing_apt = str(existing_apt).strip() or None
        apt_to_store = clean_existing_apt or parsed_apt
        if normalized_address != address or apt_to_store != existing_apt:
            cursor.execute(
                """
                UPDATE search.records
                SET address = ?, apt = ?
                WHERE record_id = ?
                """,
                normalized_address,
                apt_to_store,
                record_id,
            )
    

def insert_raw_record(cursor, record_id, source_file, raw_row_dict):
    sql = """
        INSERT INTO search.raw_records (
            record_id,
            source_file,
            raw_payload
        )
        VALUES (?, ?, ?)
    """

    values = (
        record_id,
        source_file,
        json.dumps(raw_row_dict, default=str)
    )

    
    cursor.execute(sql, *values)
def safe_sql_date(v):
    ts = pd.to_datetime(v, errors="coerce")
    return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")


def _normalize_dedupe_text(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.upper().split())


def _odyssey_dedupe_key(event_date, case_number, defendant_name):
    return (
        _normalize_dedupe_text(event_date),
        _normalize_dedupe_text(case_number),
        _normalize_dedupe_text(defendant_name),
    )


def safe_sql_date_epoch(v):
    """
    Converts Survey123 epoch milliseconds OR normal date strings to SQL date.
    """
    if v is None:
        return None

    try:
        v = str(v).strip()

        # Survey123 sends milliseconds since epoch
        if v.isdigit() and len(v) >= 13:
            ts = pd.to_datetime(int(v), unit="ms", errors="coerce")
        else:
            ts = pd.to_datetime(v, errors="coerce")

        return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")

    except Exception:
        return None

def read_csv_from_blob(container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]
    return df


def _pick_address_column(columns):
    for candidate in ADDRESS_COLUMN_CANDIDATES:
        if candidate in columns:
            return candidate
    return None


def _move_column_right_of(df, column_name, anchor_column):
    if column_name not in df.columns or anchor_column not in df.columns:
        return df

    columns = list(df.columns)
    columns.remove(column_name)
    anchor_idx = columns.index(anchor_column)
    columns.insert(anchor_idx + 1, column_name)
    return df[columns]


def create_apt_split_copy_for_blob(blob_service_client, container_name, blob_name):
    if not blob_name.lower().endswith(".csv"):
        return False, "not_csv"
    if blob_name.lower().endswith(APT_COPY_SUFFIX):
        return False, "already_copy"

    out_blob_name = f"{blob_name[:-4]}{APT_COPY_SUFFIX}"
    out_blob_client = blob_service_client.get_blob_client(container=container_name, blob=out_blob_name)
    if out_blob_client.exists():
        return False, "copy_exists"

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))

    out_df = df.copy()
    address_col = _pick_address_column(out_df.columns)

    if "AptUnit" not in out_df.columns:
        out_df["AptUnit"] = None

    if address_col:
        parsed = out_df[address_col].apply(split_address_and_apt)
        out_df[address_col] = parsed.apply(lambda x: x[0])
        out_df["AptUnit"] = out_df["AptUnit"].where(out_df["AptUnit"].notna(), parsed.apply(lambda x: x[1]))
        anchor_col = "TenantAddress" if "TenantAddress" in out_df.columns else address_col
        out_df = _move_column_right_of(out_df, "AptUnit", anchor_col)

    payload = out_df.to_csv(index=False).encode("utf-8")
    out_blob_client.upload_blob(payload, overwrite=False)
    return True, out_blob_name


def reorder_aptunit_in_existing_copies(container_name="fscsv"):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(container_name)

    updated = 0
    skipped = 0
    for blob in container_client.list_blobs():
        blob_name = blob.name
        if not blob_name.lower().endswith(APT_COPY_SUFFIX):
            skipped += 1
            continue

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(data))

        if "AptUnit" not in df.columns or "TenantAddress" not in df.columns:
            skipped += 1
            continue

        tenant_idx = list(df.columns).index("TenantAddress")
        apt_idx = list(df.columns).index("AptUnit")
        if apt_idx == tenant_idx + 1:
            skipped += 1
            continue

        df = _move_column_right_of(df, "AptUnit", "TenantAddress")
        payload = df.to_csv(index=False).encode("utf-8")
        blob_client.upload_blob(payload, overwrite=True)
        updated += 1
        print(f"Reordered AptUnit next to TenantAddress in: {blob_name}")

    print(f"AptUnit reorder finished. updated={updated}, skipped={skipped}, container={container_name}")


def create_apt_split_copies_for_all_csv_blobs(container_name="fscsv"):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(container_name)

    copied = 0
    skipped = 0
    for blob in container_client.list_blobs():
        made_copy, detail = create_apt_split_copy_for_blob(
            blob_service_client=blob_service_client,
            container_name=container_name,
            blob_name=blob.name,
        )
        if made_copy:
            copied += 1
            print(f"Created apt/unit split copy: {detail}")
        else:
            skipped += 1

    print(f"APT copy job finished. copied={copied}, skipped={skipped}, container={container_name}")


def _is_landlord_tenant_df(df):
    if "CaseNumber" in df.columns:
        case_numbers = df["CaseNumber"].fillna("").astype(str)
        if case_numbers.str.contains("-LT-", case=False).any():
            return True
    if "CourtDocumentType" in df.columns:
        doc_types = df["CourtDocumentType"].fillna("").astype(str)
        if doc_types.str.contains("landlord|tenant", case=False, regex=True).any():
            return True
    return False


def _extract_odyssey_file_date_from_name(blob_name):
    match = ODYSSEY_FILE_DATE_RE.match(blob_name)
    if not match:
        return None
    raw_date = match.group(1).strip()
    try:
        return datetime.strptime(raw_date, "%B %d, %Y").date()
    except ValueError:
        try:
            return datetime.strptime(raw_date, "%B %e, %Y").date()
        except ValueError:
            return None


def build_latest_landlord_tenant_with_apt_blob(container_name="fscsv"):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(container_name)

    candidates = []
    for blob in container_client.list_blobs():
        blob_name = blob.name
        if not blob_name.startswith("Odyssey-JobOutput-"):
            continue
        if not blob_name.lower().endswith(APT_COPY_SUFFIX):
            continue
        candidates.append(blob)

    if not candidates:
        print("No apt-split Odyssey copies found; skipping latest landlord/tenant export.")
        return None

    candidate_dates = [
        _extract_odyssey_file_date_from_name(blob.name) or (blob.last_modified.date() if blob.last_modified else None)
        for blob in candidates
    ]
    candidate_dates = [d for d in candidate_dates if d is not None]
    if not candidate_dates:
        print("No usable dates on Odyssey apt-split files; skipping latest landlord/tenant export.")
        return None

    newest_blob_date = max(candidate_dates)
    newest_day_blobs = [
        blob for blob in candidates
        if (
            _extract_odyssey_file_date_from_name(blob.name)
            or (blob.last_modified.date() if blob.last_modified else None)
        ) == newest_blob_date
    ]

    xy_lookup = None
    merged_frames = []
    for blob in newest_day_blobs:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        data = blob_client.download_blob().readall()
        df = pd.read_csv(io.BytesIO(data))

        if "AptUnit" not in df.columns:
            continue
        if not _is_landlord_tenant_df(df):
            continue

        needs_xy_hydration = (
            "x" not in df.columns
            or "y" not in df.columns
            or df["x"].isna().any()
            or df["y"].isna().any()
        )
        if not needs_xy_hydration:
            merged_frames.append(df)
            continue

        if xy_lookup is None:
            conn = get_conn()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT intake_date, case_number, full_name, x, y
                    FROM search.records
                    WHERE x IS NOT NULL AND y IS NOT NULL
                """)
                xy_lookup = {}
                for intake_date, case_number, full_name, x, y in cursor.fetchall():
                    key = _odyssey_dedupe_key(intake_date, case_number, full_name)
                    if key not in xy_lookup:
                        xy_lookup[key] = (x, y)
            finally:
                conn.close()

        geocode_cache = {}

        def _xy_from_records_or_geocode(row):
            event_date = safe_sql_date(row.get("EventDate") or row.get("intake_date"))
            case_number = row.get("CaseNumber") or row.get("case_number")
            full_name = row.get("DefendantName") or row.get("full_name")
            key = _odyssey_dedupe_key(event_date, case_number, full_name)
            x, y = xy_lookup.get(key, (None, None))
            if x is not None and y is not None:
                return x, y

            # Fallback path:
            # if lookup in search.records misses, geocode the row address directly.
            address = row.get("TenantAddress") or row.get("Address") or row.get("address")
            city = _pick_row_value(row, "TenantCity", "city")
            state = _pick_row_value(row, "TenantState", "state")
            tenant_zip = _normalize_postal_code(_pick_row_value(row, "TenantZip", "postal_code", "Zip", "ZipCode"))
            address_text = str(address).strip() if address is not None else ""
            if not address_text:
                return None, None

            cache_key = "|".join([
                address_text,
                str(city or "").strip(),
                str(state or "").strip(),
                str(tenant_zip or "").strip(),
            ])
            if cache_key not in geocode_cache:
                geocode_cache[cache_key] = geocode_address(address_text, city=city, state=state, postal_code=tenant_zip)
            return geocode_cache[cache_key]

        coords = df.apply(_xy_from_records_or_geocode, axis=1)
        df["x"] = coords.apply(lambda c: c[0])
        df["y"] = coords.apply(lambda c: c[1])

        merged_frames.append(df)

    if not merged_frames:
        print("No matching landlord/tenant apt files found for latest day; skipping export.")
        return None

    combined_df = pd.concat(merged_frames, ignore_index=True)
    output_blob = blob_service_client.get_blob_client(
        container=container_name,
        blob=LATEST_LT_WITH_APT_BLOB_NAME
    )
    output_payload = combined_df.to_csv(index=False).encode("utf-8")
    output_blob.upload_blob(output_payload, overwrite=True)
    print(
        f"Wrote {LATEST_LT_WITH_APT_BLOB_NAME} with {len(combined_df)} rows "
        f"from {len(merged_frames)} files dated {newest_blob_date}."
    )
    return LATEST_LT_WITH_APT_BLOB_NAME


def backfill_landlord_tenant_xy():
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT record_id, address, city, state, postal_code
            FROM search.records
            WHERE LOWER(LTRIM(RTRIM(department))) = 'field services department'
            AND address IS NOT NULL
            AND (
                x IS NULL
                OR y IS NULL
            )
        """)
        rows = cursor.fetchall()

        cache = {}
        count = 0
        updated = 0
        for record_id, address, city, state, postal_code in rows:
            count += 1
            if count % 100 == 0:
                print(f"Processed {count} rows...")
            address_text = (str(address).strip() if address is not None else "")
            if not address_text:
                continue
            cache_key = "|".join([
                address_text,
                str(city or "").strip(),
                str(state or "").strip(),
                str(postal_code or "").strip(),
            ])
            if cache_key in cache:
                x, y = cache[cache_key]
            else:
                x, y = geocode_address(address_text, city=city, state=state, postal_code=postal_code)
                cache[cache_key] = (x, y)
            if x is None or y is None:
                continue
            cursor.execute(
                """
                UPDATE search.records
                SET x = ?, y = ?
                WHERE record_id = ?
                """,
                x,
                y,
                record_id,
            )
            updated += 1
            if updated % 100 == 0:
                conn.commit()
                print(f"Committed {updated} rows...")

        conn.commit()
        print(f"Backfilled landlord/tenant x,y for {updated} rows.")
    finally:
        conn.close()


def backfill_landlord_tenant_postal_code():
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT r.record_id, rr.raw_payload
            FROM search.records r
            LEFT JOIN search.raw_records rr ON rr.record_id = r.record_id
            WHERE LOWER(LTRIM(RTRIM(r.department))) = 'field services department'
              AND (
                    r.postal_code IS NULL
                    OR LTRIM(RTRIM(CAST(r.postal_code AS NVARCHAR(20)))) = ''
              )
              AND rr.raw_payload IS NOT NULL
        """)
        rows = cursor.fetchall()

        updated = 0
        for record_id, raw_payload in rows:
            payload = raw_payload
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8", errors="ignore")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = None
            if not isinstance(payload, dict):
                continue

            normalized = {
                str(k).replace("\ufeff", "").strip().lower(): v
                for k, v in payload.items()
            }
            candidate = (
                normalized.get("tenantzip")
                or normalized.get("tenant_zip")
                or normalized.get("zip")
                or normalized.get("zipcode")
                or normalized.get("postal_code")
            )
            zip5 = _normalize_postal_code(candidate)
            if not zip5:
                continue

            cursor.execute(
                "UPDATE search.records SET postal_code = ? WHERE record_id = ?",
                zip5,
                record_id,
            )
            updated += 1
            if updated % 500 == 0:
                conn.commit()
                print(f"Updated postal_code for {updated} rows...")

        conn.commit()
        print(f"Backfilled landlord/tenant postal_code for {updated} rows.")
    finally:
        conn.close()


def backfill_landlord_tenant_postal_code_from_latest_blob(
    container_name="fscsv",
    blob_name=LATEST_LT_WITH_APT_BLOB_NAME,
):
    try:
        df = read_csv_from_blob(container_name, blob_name)
    except Exception as exc:
        print(f"Unable to read {blob_name} for postal backfill: {exc}")
        return

    key_to_zip = {}
    for _, row in df.iterrows():
        zip5 = _normalize_postal_code(_pick_row_value(row, "TenantZip", "postal_code", "Zip", "ZipCode"))
        if not zip5:
            continue
        event_date = safe_sql_date(_pick_row_value(row, "EventDate", "intake_date"))
        case_number = _pick_row_value(row, "CaseNum", "CaseNumber", "case_number")
        full_name = _pick_row_value(row, "Defendant", "DefendantName", "full_name")
        key = _odyssey_dedupe_key(event_date, case_number, full_name)
        if key not in key_to_zip:
            key_to_zip[key] = zip5

    if not key_to_zip:
        print(f"No usable TenantZip values found in {blob_name}; skipping blob-based postal backfill.")
        return

    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT record_id, intake_date, case_number, full_name
            FROM search.records
            WHERE LOWER(LTRIM(RTRIM(department))) = 'field services department'
              AND (
                    postal_code IS NULL
                    OR LTRIM(RTRIM(CAST(postal_code AS NVARCHAR(20)))) = ''
              )
        """)
        rows = cursor.fetchall()

        updated = 0
        for record_id, intake_date, case_number, full_name in rows:
            key = _odyssey_dedupe_key(intake_date, case_number, full_name)
            zip5 = key_to_zip.get(key)
            if not zip5:
                continue
            cursor.execute(
                "UPDATE search.records SET postal_code = ? WHERE record_id = ?",
                zip5,
                record_id,
            )
            updated += 1
            if updated % 500 == 0:
                conn.commit()
                print(f"Blob postal backfill updated {updated} rows...")

        conn.commit()
        print(f"Backfilled landlord/tenant postal_code from {blob_name} for {updated} rows.")
    finally:
        conn.close()


def backfill_landlord_tenant_postal_code_from_geocode(limit=None):
    conn = get_conn()
    try:
        cursor = conn.cursor()
        top_clause = ""
        if isinstance(limit, int) and limit > 0:
            top_clause = f"TOP {limit}"

        cursor.execute(f"""
            SELECT {top_clause} record_id, address, city, state
            FROM search.records
            WHERE LOWER(LTRIM(RTRIM(department))) = 'field services department'
              AND address IS NOT NULL
              AND LTRIM(RTRIM(CAST(address AS NVARCHAR(500)))) <> ''
              AND (
                    postal_code IS NULL
                    OR LTRIM(RTRIM(CAST(postal_code AS NVARCHAR(20)))) = ''
              )
            ORDER BY record_id
        """)
        rows = cursor.fetchall()

        cache = {}
        updated = 0
        scanned = 0
        for record_id, address, city, state in rows:
            scanned += 1
            address_text = str(address or "").strip()
            city_text = str(city or "").strip()
            state_text = str(state or "").strip()
            if not address_text:
                continue

            cache_key = "|".join([address_text.lower(), city_text.lower(), state_text.lower()])
            if cache_key in cache:
                zip5 = cache[cache_key]
            else:
                zip5 = geocode_postal_code(address_text, city=city_text, state=state_text)
                cache[cache_key] = zip5

            if not zip5:
                continue

            cursor.execute(
                "UPDATE search.records SET postal_code = ? WHERE record_id = ?",
                zip5,
                record_id,
            )
            updated += 1
            if updated % 200 == 0:
                conn.commit()
                print(f"Geocode postal backfill updated {updated} rows (scanned {scanned})...")

        conn.commit()
        print(f"Backfilled landlord/tenant postal_code from geocode for {updated} rows (scanned {scanned}).")
    finally:
        conn.close()


def backfill_bcso_active_warrants_xy():
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT record_id, address
            FROM search.records
            WHERE department = 'BCSO_ACTIVE_WARRANTS'
            AND address IS NOT NULL
            AND LTRIM(RTRIM(address)) <> ''
            AND (
                x IS NULL
                OR y IS NULL
            )
        """)
        rows = cursor.fetchall()

        cache = {}
        scanned = 0
        updated = 0

        for record_id, address in rows:
            scanned += 1
            if scanned % 100 == 0:
                print(f"Scanned {scanned} BCSO rows...")

            address_text = str(address).strip()
            cache_key = address_text.lower()
            if cache_key in cache:
                x, y = cache[cache_key]
            else:
                x, y = geocode_address(address_text)
                cache[cache_key] = (x, y)

            if x is None or y is None:
                continue

            cursor.execute(
                """
                UPDATE search.records
                SET x = ?, y = ?
                WHERE record_id = ?
                """,
                x,
                y,
                record_id,
            )
            updated += 1
            if updated % 100 == 0:
                conn.commit()
                print(f"Committed {updated} BCSO rows...")

        conn.commit()
        print(f"Backfilled BCSO Active Warrants x,y for {updated} rows.")
    finally:
        conn.close()


def backfill_bcso_active_warrants_xy_force():
    """
    Re-geocode ALL BCSO active warrant rows that have an address,
    even when x/y are already populated.
    """
    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT record_id, address
            FROM search.records
            WHERE department = 'BCSO_ACTIVE_WARRANTS'
            AND address IS NOT NULL
            AND LTRIM(RTRIM(address)) <> ''
        """)
        rows = cursor.fetchall()

        cache = {}
        scanned = 0
        updated = 0

        for record_id, address in rows:
            scanned += 1
            if scanned % 100 == 0:
                print(f"Scanned {scanned} BCSO rows...")

            address_text = str(address).strip()
            cache_key = address_text.lower()
            if cache_key in cache:
                x, y = cache[cache_key]
            else:
                x, y = geocode_address(address_text)
                cache[cache_key] = (x, y)

            if x is None or y is None:
                continue

            cursor.execute(
                """
                UPDATE search.records
                SET x = ?, y = ?
                WHERE record_id = ?
                """,
                x,
                y,
                record_id,
            )
            updated += 1
            if updated % 100 == 0:
                conn.commit()
                print(f"Committed {updated} BCSO rows...")

        conn.commit()
        print(f"Force backfilled BCSO Active Warrants x,y for {updated} rows.")
    finally:
        conn.close()


def ingest_odyssey_civil_from_blob(blob_name, container_name="fscsv", existing_keys=None):
    df = read_csv_from_blob(container_name, blob_name)

    conn = get_conn()
    try:
        cursor = conn.cursor()
        ensure_records_apt_column(cursor)
        cursor.execute("""
            DELETE FROM search.records
            WHERE department = 'FIELD SERVICES DEPARTMENT'
            AND source_file = ?
        """, blob_name)

        if existing_keys is None:
            cursor.execute("""
                SELECT intake_date, case_number, full_name
                FROM search.records
                WHERE department = 'FIELD SERVICES DEPARTMENT'
            """)
            existing_keys = {
                _odyssey_dedupe_key(event_date, case_number, defendant_name)
                for event_date, case_number, defendant_name in cursor.fetchall()
            }

        for _, row in df.iterrows():
            event_date = safe_sql_date(row.get("EventDate"))
            case_number = row.get("CaseNumber")
            defendant_name = row.get("DefendantName")
            dedupe_key = _odyssey_dedupe_key(event_date, case_number, defendant_name)
            if dedupe_key in existing_keys:
                continue

            address, apt = split_address_and_apt(row.get("TenantAddress"))
            tenant_city = _pick_row_value(row, "TenantCity")
            tenant_state = _pick_row_value(row, "TenantState")
            tenant_zip = _normalize_postal_code(_pick_row_value(row, "TenantZip", "Zip", "ZipCode"))
            x, y = geocode_address(address, city=tenant_city, state=tenant_state, postal_code=tenant_zip) if address else (None, None)
            record = {
                "department": "FIELD SERVICES DEPARTMENT",
                "source_file": blob_name,
                "full_name": defendant_name,
                "case_number": case_number,
                "court_document_type": row.get("CaseType"),
                "intake_date": event_date,
                "address": address,
                "apt": apt,
                "x": x,
                "y": y,
                "city": tenant_city,
                "state": tenant_state,
                "postal_code": tenant_zip,
                "disposition": row.get("EventType"),
                "notes": row.get("EventComment"),
            }

            record_id = insert_search_record_odyssey(cursor, record)
            insert_raw_record(cursor, record_id, blob_name, row.to_dict())
            existing_keys.add(dedupe_key)

        conn.commit()
    finally:
        conn.close()

def ingest_all_odyssey_civil_blobs(container_name="fscsv"):
    create_apt_split_copies_for_all_csv_blobs(container_name)
    reorder_aptunit_in_existing_copies(container_name)

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    container_client = blob_service_client.get_container_client(container_name)

    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT source_file
            FROM search.records
            WHERE department = 'FIELD SERVICES DEPARTMENT'
        """)
        already_ingested = {row[0] for row in cursor.fetchall() if row[0]}

        cursor.execute("""
            SELECT intake_date, case_number, full_name
            FROM search.records
            WHERE department = 'FIELD SERVICES DEPARTMENT'
        """)
        existing_keys = {
            _odyssey_dedupe_key(event_date, case_number, defendant_name)
            for event_date, case_number, defendant_name in cursor.fetchall()
        }
    finally:
        conn.close()

    for blob in container_client.list_blobs():
        name = blob.name

        if not (name.startswith("Odyssey-JobOutput-") and name.endswith(".csv")):
            continue

        if name in already_ingested:
            print("SKIPPING (already ingested):", name)
            continue

        print("Ingesting:", name)
        ingest_odyssey_civil_from_blob(name, container_name, existing_keys=existing_keys)

    conn = get_conn()
    try:
        cursor = conn.cursor()
        normalize_existing_fsd_apt_records(cursor)
        conn.commit()
    finally:
        conn.close()

    # Keep the "latest_landlord_tenant_with_apt.csv" blob fresh even when a
    # later backfill step (for example geocoding) is slow or interrupted.
    # This also ensures blob-based postal backfill reads the newest file.
    build_latest_landlord_tenant_with_apt_blob(container_name)
    backfill_landlord_tenant_postal_code_from_latest_blob(container_name=container_name)
    backfill_landlord_tenant_postal_code()
    backfill_landlord_tenant_postal_code_from_geocode()
    backfill_landlord_tenant_xy()

def ingest_population_from_table(table_name, display_department, source_file):
    """
    Copies rows from a staging table (jail_population or doc_population)
    into search.records so it becomes searchable in the site.
    """

    conn = get_conn()
    try:
        cursor = conn.cursor()

        # OPTIONAL safety: remove prior inserts for this same source_file + department
        cursor.execute("""
            DELETE FROM search.records
            WHERE department = ? AND source_file = ?
        """, display_department, source_file)

        cursor.execute(f"""
            SELECT
                sid,
                last_name,
                first_name,
                middle_initial,
                date_of_birth,
                facility
            FROM {table_name}
            WHERE source_file = ?
        """, source_file)

        rows = cursor.fetchall()

        for sid, last_name, first_name, mi, dob, facility in rows:
            full_name = f"{last_name}, {first_name}".strip(", ").strip()

            record = {
                "department": display_department,
                "source_file": source_file,
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "date_of_birth": dob,      # already a date in SQL
                "sid": str(sid) if sid is not None else None,
                "facility": facility
            }

            insert_search_record_population(cursor, record)

        conn.commit()
        print(f"Inserted {len(rows)} rows into search.records from {table_name}")

    finally:
        conn.close()

def ingest_warrants_csv():
    from azure.storage.blob import BlobServiceClient
    import io
    import os
    import pandas as pd
    

    


    

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )


    container_name = "warrantscsv"
    blob_name = "warrants_1.csv"

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data))
    print("DF SHAPE:", df.shape)
   
    print("WARRANTS DF ROWS:", len(df))

    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")

        for _, row in df.iterrows():
           
            record = {
                "department": "WARRANTS",
                "source_file": blob_name,

                "first_name": (
                    None
                    if pd.isna(row.get("First Name"))
                    else str(row.get("First Name"))
                ),

                "last_name": (
                    None
                    if pd.isna(row.get("Last Name"))
                    else str(row.get("Last Name"))
                ),

                "full_name": (
                    None
                    if pd.isna(row.get("First Name")) and pd.isna(row.get("Last Name"))
                    else f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
                ),

                "date_of_birth": (
                    None
                    if pd.isna(row.get("Date of Birth"))
                    else safe_sql_date(row.get("Date of Birth"))
                ),

                "sid": (
                    None
                    if pd.isna(row.get("SID"))
                    else str(int(row.get("SID")))
                ),

                "case_number": (
                    None
                    if pd.isna(row.get("Case Number"))
                    else str(row.get("Case Number"))
                ),

                "warrant_type": (
                    None
                    if pd.isna(row.get("Warrant Type"))
                    else str(row.get("Warrant Type"))
                ),

                "warrant_status": (
                    None
                    if pd.isna(row.get("Warrant Status"))
                    else str(row.get("Warrant Status"))
                ),

                "issue_date": (
                    None
                    if pd.isna(row.get("Issue Date"))
                    else safe_sql_date(row.get("Issue Date"))
                ),

                "intake_date": None,

                "address": (
                    None
                    if pd.isna(row.get("Address"))
                    else str(row.get("Address"))
                ),

                "city": None,
                "state": None,
                "postal_code": None,

                "notes": (
                    None
                    if pd.isna(row.get("Notes or Alias"))
                    else str(row.get("Notes or Alias"))
                ),
                "sex": (
                    None if pd.isna(row.get("Sex")) else str(row.get("Sex")).strip()
                ),
                "race": (
                    None if pd.isna(row.get("Race")) else str(row.get("Race")).strip()
                ),
                "issuing_county": (
                    None if pd.isna(row.get("Issuing County")) else str(row.get("Issuing County")).strip()
                ),
            }
            

            record_id = insert_search_record_warrants(cursor, record)
            insert_raw_record(cursor, record_id, blob_name, row.to_dict())
        
    
        conn.commit()
    finally:
        conn.close()

def ingest_bcso_active_warrants_csv(_=None):
    from azure.storage.blob import BlobServiceClient
    import io
    import pandas as pd
    import os

    container_name = "bcsoactivewarrants"

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(container_name)

    conn = get_conn()
    try:
        cursor = conn.cursor()

        # 1) Build a set of blob filenames we've already ingested
        cursor.execute("""
            SELECT DISTINCT source_file
            FROM search.records
            WHERE department = 'BCSO_ACTIVE_WARRANTS'
        """)
        already_ingested = {row[0] for row in cursor.fetchall()}

        inserted = 0

        # 2) Loop through blobs and only ingest NEW ones
        for blob in container_client.list_blobs():
            if not blob.name.endswith(".csv"):
                continue

            if blob.name in already_ingested:
                print("SKIPPING (already ingested):", blob.name)
                continue

            print("INGESTING:", blob.name)

            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()

            if not data.strip():
                print("SKIPPING (empty file):", blob.name)
                continue

            # 3) Read headerless CSV (your Make output)
            text = data.decode("utf-8").strip().splitlines()
            rows = []

            for line in text:
                parts = [p.strip() for p in line.split(",")]
                print("RAW PARTS:", parts)

                if len(parts) < 13:
                    print("SKIPPING malformed line:", line)
                    continue

                # Columns 0–11 are fixed (ending with sex)
                fixed = parts[0:13]

                # Columns 12+ are the address (street, city, state, zip, etc)
                lka = ", ".join(parts[13:]).strip()

                fixed.append(lka)
                rows.append(fixed)

            df = pd.DataFrame(rows, columns=[
                "issuing_county",
                "case_number",
                "warrant_id",
                "warrant_type",
                "date_issued",
                "warrant_status",
                "last_name",
                "first_name",
                "sid",
                "dob",
                "race",
                "sex",
                "notes",
                "lka"
            ])

            # IMPORTANT: Skip old/wrong-format files (like survey_0.csv with 46 columns)
            if df.shape[1] != 14:
                print(f"SKIPPING (unexpected column count {df.shape[1]}):", blob.name)
                continue

            df.columns = [
                "issuing_county",
                "case_number",
                "warrant_id",
                "warrant_type",
                "date_issued",
                "warrant_status",
                "last_name",
                "first_name",
                "sid",
                "dob",
                "race",
                "sex",
                "notes",
                "lka"
            ]
          
            # 4) Insert each row into SQL
            geocode_cache = {}
            for _, row in df.iterrows():
                row = row.where(pd.notna(row), None)
                record_type = row.get("Record Type") or row.get("record_type")

                is_update = (
                    isinstance(record_type, str)
                    and record_type.strip().lower() == "update warrant"
                )

                print("DEBUG record_type:", record_type, "is_update =", is_update)

                full_name = f"{row.get('last_name')}, {row.get('first_name')}".strip(", ")
                
                address_value = row.get("lka")
                address_text = str(address_value).strip() if address_value is not None else ""
                cache_key = address_text.lower()
                if address_text:
                    if cache_key not in geocode_cache:
                        geocode_cache[cache_key] = geocode_address(address_text)
                    x, y = geocode_cache[cache_key]
                else:
                    x, y = (None, None)

                record = {
                    "department": "BCSO_ACTIVE_WARRANTS",
                    "source_file": blob.name,

                    "full_name": full_name,
                    "case_number": row.get("case_number"),
                    "warrant_id_number": row.get("warrant_id"),
                    "warrant_type": row.get("warrant_type"),
                    "issue_date": safe_sql_date_epoch(row.get("date_issued")),
                    "warrant_status": row.get("warrant_status"),

                    "sid": row.get("sid"),
                    "date_of_birth": safe_sql_date_epoch(row.get("dob")),
                    "race": row.get("race"),
                    "sex": row.get("sex"),

                    "issuing_county": row.get("issuing_county"),
                    "notes": row.get("notes"),
                    "address": row.get("lka"),
                    "x": x,
                    "y": y,
                }

                # 1) Try to find existing Active Warrant by case_number (update scenario)
                cursor.execute("""
                    SELECT TOP 1 record_id
                    FROM search.records
                    WHERE department = 'BCSO_ACTIVE_WARRANTS'
                    AND case_number = ?
                    ORDER BY record_id DESC
                """, record.get("case_number"))

                existing = cursor.fetchone()
                

                if existing:
                    record_id = existing[0]
                    print(
                        f"DEBUG UPDATE START | case_number={record.get('case_number')} "
                        f"| record_id={record_id} "
                        f"| source_file={blob.name}"
                    )
                    

                    # 2) UPDATE rule:
                    # - if incoming is empty/None, keep existing
                    # - if incoming differs and is non-empty, overwrite
                    cursor.execute("""
                        UPDATE search.records
                        SET
                            source_file       = COALESCE(NULLIF(?, ''), source_file),
                            full_name         = COALESCE(NULLIF(?, ''), full_name),
                            warrant_id_number = COALESCE(NULLIF(?, ''), warrant_id_number),
                            warrant_type      = COALESCE(NULLIF(?, ''), warrant_type),
                            issue_date        = COALESCE(?, issue_date),
                            warrant_status    = COALESCE(NULLIF(?, ''), warrant_status),
                            sid               = COALESCE(NULLIF(?, ''), sid),
                            date_of_birth     = COALESCE(?, date_of_birth),
                            race              = COALESCE(NULLIF(?, ''), race),
                            sex               = COALESCE(NULLIF(?, ''), sex),
                            issuing_county    = COALESCE(NULLIF(?, ''), issuing_county),
                            notes             = COALESCE(NULLIF(?, ''), notes),
                            address           = COALESCE(NULLIF(?, ''), address),
                            x                 = COALESCE(?, x),
                            y                 = COALESCE(?, y)
                        WHERE record_id = ?
                    """,
                        record.get("source_file") or "",
                        record.get("full_name") or "",
                        record.get("warrant_id_number") or "",
                        record.get("warrant_type") or "",
                        record.get("issue_date"),                 # dates: pass None or YYYY-MM-DD
                        record.get("warrant_status") or "",
                        (str(record.get("sid")) if record.get("sid") is not None else ""),
                        record.get("date_of_birth"),
                        record.get("race") or "",
                        record.get("sex") or "",
                        record.get("issuing_county") or "",
                        record.get("notes") or "",
                        record.get("address") or "",
                        record.get("x"),
                        record.get("y"),
                        record_id
                    )

                else:
                    # 3) No existing case_number -> insert new
                    record_id = insert_search_record_active_warrants(cursor, record)
                print(
                    f"DEBUG UPDATE COMPLETE | case_number={record.get('case_number')} "
                    f"| record_id={record_id}"
                )

                # Keep raw payload for traceability either way
                insert_raw_record(cursor, record_id, blob.name, row.to_dict())
                inserted += 1

                if inserted % 1000 == 0:
                    print(f"Inserted {inserted} records...")

        conn.commit()
        print(f"Done. Inserted {inserted} new BCSO Active Warrants records.")

    finally:
        conn.close()

def ingest_new_warrant_csv():
    from azure.storage.blob import BlobServiceClient
    import io
    import pandas as pd
    import os

    container_name = "warrantscsv"
    blob_name = "AllActiveWarrants_0.csv"

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data), dtype=str, low_memory=False)
    
    

    conn = get_conn()
    try:
        cursor = conn.cursor()

        for i, (_, row) in enumerate(df.iterrows(), start=1):
            row = row.where(pd.notna(row), None)
            full_name = (
                row.get("Full Name")
                if pd.notna(row.get("Full Name"))
                else " ".join(
                    str(x) for x in [
                        row.get("Last_Name"),
                        row.get("First_Name"),
                        row.get("Middle_Name")
                    ]
                    if pd.notna(x)
                )
            )

            record = {
                "department": "WARRANTS",
                "source_file": blob_name,
                "full_name": full_name,
                "case_number": row.get("Case Number"),
                "issue_date": safe_sql_date(row.get("Warrant_Issue_Date")),
                "date_of_birth": safe_sql_date(row.get("DOB")),

                # force FLOAT-backed columns to NULL for this CSV
                "sid": None,
                "sex": row.get("Sex"),
                "race": row.get("Race"),


                "issuing_county": row.get("County"),
                "disposition": (
                    row.get("1st_Charge")
                    if pd.notna(row.get("1st_Charge"))
                    else None
                ),
            }

            record_id = insert_search_record_warrants(cursor, record)
            insert_raw_record(cursor, record_id, blob_name, row.to_dict())

            if i % 1000 == 0:
                print(f"Inserted {i} records...")

        conn.commit()
        print(f"Finished ingesting {i} records.")

    finally:
        conn.close()

def ingest_wor(_=None):
    from azure.storage.blob import BlobServiceClient
    import os
    import pandas as pd
    import io

    container_name = "warrantscsv"

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )
    container_client = blob_service_client.get_container_client(container_name)

    conn = get_conn()

    try:
        cursor = conn.cursor()

        # 1) Get already ingested files
        cursor.execute("""
            SELECT DISTINCT source_file
            FROM search.records
            WHERE department = 'Warrant of Restitution'
        """)
        already_ingested = {row[0] for row in cursor.fetchall()}

        inserted = 0

        # 2) Loop through blobs
        for blob in container_client.list_blobs():
            if not blob.name.endswith(".csv"):
                continue

            if blob.name in already_ingested:
                print("SKIPPING (already ingested):", blob.name)
                continue

            print("INGESTING:", blob.name)

            blob_client = container_client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()

            if not data.strip():
                print("SKIPPING (empty file):", blob.name)
                continue

            # 3) Headerless CSV from MAKE
            df = pd.read_csv(io.BytesIO(data), header=None)

            # Expecting 13 columns based on your MAKE order
            if df.shape[1] < 13:
                print(f"SKIPPING (unexpected column count {df.shape[1]}):", blob.name)
                continue

            for _, row in df.iterrows():
                row = row.where(pd.notna(row), None)

                record = {
                    "department": "Warrant of Restitution",
                    "source_file": blob.name,

                    "full_name": row[6],
                    "case_number": row[3],

                    "intake_date": safe_sql_date_epoch(row[1]),
                    "issue_date": safe_sql_date_epoch(row[2]),

                    "court_document_type": row[4],
                    "disposition": row[10],

                    "address": row[7],
                    "notes": row[12],
                }

                record_id = insert_search_record_fsdw(cursor, record)
                insert_raw_record(cursor, record_id, blob.name, row.to_dict())

                inserted += 1

                if inserted % 1000 == 0:
                    print(f"Inserted {inserted} records...")

        conn.commit()
        print(f"Done. Inserted {inserted} Warrant of Restitution records.")

    finally:
        conn.close()

def ingest_baltimore_jail_population():
    ingest_population_from_table(
        table_name="jail_population",
        display_department="Baltimore Jail Population",
        source_file="baltimorejailpopulation_20260128.pdf"
    )

def ingest_doc_jail_population():
    ingest_population_from_table(
        table_name="doc_population",
        display_department="DOC Jail Population",
        source_file="docpopulation_20251228.pdf"
    )
