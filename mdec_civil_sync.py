import json
import os
import re
import time
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qs, unquote, urljoin, urlparse

MDEC_SOURCE_SYSTEM = "mdec"
MDEC_MATCH_WINDOW_DAYS = 10
MDEC_ACTIVE_LINK_DAYS = 60
MDEC_COMBINED_FORMAT_VERSION = 2
MDEC_RETRY_MINUTES = 10
MDEC_FAILED_RETRY_MINUTES = 30
MDEC_BATCH_SIZE = 10
MDEC_RUN_TIME_BUDGET_SECONDS = 150
MDEC_DOWNLOAD_TIMEOUT_SECONDS = 15
MDEC_COMBINED_JOB_TIMEOUT_SECONDS = 45
MDEC_SERVICE_BASE_URL = os.getenv(
    "MDEC_SERVICE_BASE_URL",
    "https://bcso-service-case-docs-e7hfcmdva0gpgphd.centralus-01.azurewebsites.net",
).rstrip("/")


def normalize_case_number(value):
    return re.sub(r"[^A-Za-z0-9]+", "", str(value or "")).upper()


def parse_submission_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    normalized = re.sub(
        r"\s+(?:EST|EDT|CST|CDT|MST|MDT|PST|PDT|UTC|GMT)$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    normalized = normalized.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).replace(tzinfo=None)
    except ValueError:
        pass
    for fmt in (
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def civil_priority_sql(alias=None):
    prefix = f"{alias}." if alias else ""
    return f"""
        CASE
            WHEN LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) LIKE '%served%'
             AND LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) NOT LIKE '%non est%'
             AND LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) NOT LIKE '%not served%'
             AND LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) NOT LIKE '%unserved%'
                THEN 0
            WHEN LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) LIKE '%non est%'
              OR LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) LIKE '%not served%'
              OR LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) LIKE '%unserved%'
                THEN 1
            WHEN LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) LIKE '%attempt%'
              OR LOWER(COALESCE({prefix}source_file, '')) = 'civil-paper-attempts'
                THEN 2
            WHEN LOWER(COALESCE({prefix}administrative_status, {prefix}service_disp, {prefix}disposition, '')) LIKE '%received%'
                THEN 3
            ELSE 4
        END
    """


def find_best_civil_record(cur, case_number, submission_at):
    normalized_case = normalize_case_number(case_number)
    if not normalized_case or not submission_at:
        return None
    # Matching is calendar-date based; any source time or timezone is ignored.
    submission_date = submission_at.date().isoformat()
    priority = civil_priority_sql()
    issued_date = """
        COALESCE(
            TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(50), issue_date))), '')),
            TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(50), court_issued_date))), ''))
        )
    """
    intake_date_value = """
        TRY_CONVERT(date, NULLIF(LTRIM(RTRIM(CONVERT(nvarchar(50), intake_date))), ''))
    """
    cur.execute(
        f"""
        SELECT TOP 1 record_id
        FROM search.records
        WHERE LOWER(LTRIM(RTRIM(COALESCE(department, '')))) = 'civil papers'
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(COALESCE(case_number, '')), '-', ''), ' ', ''), '/', ''), '.', '') = ?
          AND (
                ABS(DATEDIFF(day, CAST(? AS date), {issued_date})) <= ?
             OR ABS(DATEDIFF(day, CAST(? AS date), {intake_date_value})) <= ?
          )
        ORDER BY
          {priority},
          CASE
            WHEN {issued_date} IS NULL THEN ABS(DATEDIFF(day, CAST(? AS date), {intake_date_value}))
            WHEN {intake_date_value} IS NULL THEN ABS(DATEDIFF(day, CAST(? AS date), {issued_date}))
            WHEN ABS(DATEDIFF(day, CAST(? AS date), {issued_date}))
               <= ABS(DATEDIFF(day, CAST(? AS date), {intake_date_value}))
              THEN ABS(DATEDIFF(day, CAST(? AS date), {issued_date}))
            ELSE ABS(DATEDIFF(day, CAST(? AS date), {intake_date_value}))
          END,
          COALESCE(date_time_served, date_time_attempted, prior_attempt_date, date_received,
                   intake_date, issue_date, court_issued_date, created_at) DESC,
          record_id DESC
        """,
        normalized_case,
        submission_date,
        MDEC_MATCH_WINDOW_DAYS,
        submission_date,
        MDEC_MATCH_WINDOW_DAYS,
        submission_date,
        submission_date,
        submission_date,
        submission_date,
        submission_date,
        submission_date,
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def fetch_mdec_documents(conn):
    cur = conn.cursor()
    priority = civil_priority_sql("r")
    cur.execute(f"""
        WITH source_documents AS (
            SELECT cd.*,
                   REPLACE(REPLACE(REPLACE(REPLACE(UPPER(COALESCE(cd.case_number, '')), '-', ''), ' ', ''), '/', ''), '.', '') AS normalized_case_number,
                   COALESCE(
                       TRY_CONVERT(date, cd.submission_datetime),
                       TRY_CONVERT(date, parsed.submission_date_text, 23),
                       TRY_CONVERT(date, parsed.submission_date_text, 101),
                       TRY_CONVERT(date, parsed.submission_date_text, 1)
                   ) AS parsed_submission_date
            FROM dbo.case_documents AS cd
            CROSS APPLY (
                SELECT LEFT(
                    LTRIM(RTRIM(CONVERT(nvarchar(100), cd.submission_datetime))),
                    CHARINDEX(' ', LTRIM(RTRIM(CONVERT(nvarchar(100), cd.submission_datetime))) + ' ') - 1
                ) AS submission_date_text
            ) AS parsed
            WHERE NULLIF(LTRIM(RTRIM(COALESCE(cd.case_number, ''))), '') IS NOT NULL
        ),
        ranked_documents AS (
            SELECT cd.*,
                   MAX(cd.id) OVER (
                       PARTITION BY cd.normalized_case_number
                   ) AS source_version,
                   ROW_NUMBER() OVER (
                       PARTITION BY cd.normalized_case_number
                       ORDER BY cd.id DESC
                   ) AS case_row_number
            FROM source_documents AS cd
            WHERE cd.parsed_submission_date BETWEEN
                  DATEADD(day, -{MDEC_ACTIVE_LINK_DAYS}, CAST(SYSUTCDATETIME() AS date))
                  AND CAST(SYSUTCDATETIME() AS date)
        )
        SELECT TOP ({MDEC_BATCH_SIZE}) cd.source_version, cd.case_number, cd.submission_datetime,
               cd.document_name, cd.lead_document, cd.filing_description, cd.normalized_case_number
        FROM ranked_documents AS cd
        LEFT JOIN search.mdec_civil_sync_status AS sync
          ON sync.source_document_id = CONCAT('combined:', cd.normalized_case_number)
        LEFT JOIN search.civil_return_pdfs AS pdf
          ON pdf.source_system = 'mdec'
         AND pdf.source_document_id = CONCAT('combined:', cd.normalized_case_number)
        LEFT JOIN search.records AS r
          ON r.record_id = pdf.record_id
        WHERE cd.case_row_number = 1
          AND (
                (sync.source_document_id IS NULL AND pdf.id IS NULL)
             OR (pdf.id IS NOT NULL
                 AND COALESCE(TRY_CONVERT(BIGINT, JSON_VALUE(pdf.source_json, '$.source_version')), 0) < cd.source_version)
             OR (pdf.id IS NOT NULL
                 AND COALESCE(TRY_CONVERT(INT, JSON_VALUE(pdf.source_json, '$.combined_format_version')), 0) < {MDEC_COMBINED_FORMAT_VERSION})
             OR (sync.sync_status IN ('unmatched', 'failed')
                 AND (sync.next_retry_at IS NULL OR sync.next_retry_at <= SYSUTCDATETIME()))
             OR (pdf.id IS NOT NULL
                 AND {priority} > 1
                 AND (sync.next_retry_at IS NULL OR sync.next_retry_at <= SYSUTCDATETIME()))
          )
        ORDER BY cd.parsed_submission_date DESC,
                 cd.source_version DESC
    """)
    documents = []
    for row in cur.fetchall():
        documents.append({
            "source_document_id": f"combined:{str(row[6] or '').strip()}",
            "source_version": int(row[0]),
            "case_number": str(row[1] or "").strip(),
            "submission_at": parse_submission_datetime(row[2]),
            "document_name": str(row[3] or "").strip(),
            "lead_document": str(row[4] or "").strip(),
            "filing_description": str(row[5] or "").strip(),
            "download_url": f"{MDEC_SERVICE_BASE_URL}/download-case/{str(row[1] or '').strip()}/combined/start",
        })
    return documents


def get_civil_record_priority(cur, record_id):
    priority = civil_priority_sql()
    cur.execute(f"SELECT {priority} FROM search.records WHERE record_id = ?", record_id)
    row = cur.fetchone()
    return int(row[0]) if row else 4


def record_sync_status(conn, document, status, record_id=None, pdf_id=None, error=None, terminal=False):
    cur = conn.cursor()
    retry_minutes = MDEC_FAILED_RETRY_MINUTES if status == "failed" else MDEC_RETRY_MINUTES
    cur.execute("""
        MERGE search.mdec_civil_sync_status AS target
        USING (SELECT ? AS source_document_id) AS source
           ON target.source_document_id = source.source_document_id
        WHEN MATCHED THEN UPDATE SET
            case_number = ?, sync_status = ?, matched_record_id = ?, matched_pdf_id = ?,
            attempt_count = target.attempt_count + 1,
            last_attempt_at = SYSUTCDATETIME(),
            next_retry_at = CASE WHEN ? = 1 THEN NULL ELSE DATEADD(minute, ?, SYSUTCDATETIME()) END,
            last_error = ?, completed_at = CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE NULL END,
            updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            source_document_id, case_number, sync_status, matched_record_id, matched_pdf_id,
            attempt_count, last_attempt_at, next_retry_at, last_error, completed_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, 1, SYSUTCDATETIME(),
            CASE WHEN ? = 1 THEN NULL ELSE DATEADD(minute, ?, SYSUTCDATETIME()) END,
            ?, CASE WHEN ? = 1 THEN SYSUTCDATETIME() ELSE NULL END, SYSUTCDATETIME()
        );
    """,
        document["source_document_id"], document.get("case_number"), status, record_id, pdf_id,
        1 if terminal else 0, retry_minutes, error, 1 if terminal else 0,
        document["source_document_id"], document.get("case_number"), status, record_id, pdf_id,
        1 if terminal else 0, retry_minutes, error, 1 if terminal else 0,
    )


def source_filename(document):
    return f"Civil Papers {document.get('case_number') or 'MDEC Case'}.pdf"


class _DocumentLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        for key in ("href", "src", "data-href", "data-url", "onclick"):
            value = values.get(key)
            if value:
                self.links.append(value)


def _unwrap_secure_web_url(url):
    parsed = urlparse(url)
    if "secure-web.cisco.com" not in parsed.netloc.lower():
        return url
    query = parse_qs(parsed.query)
    for key in ("url", "u", "target"):
        if query.get(key):
            return unquote(query[key][0])
    match = re.search(r"https?%3A%2F%2F[^&]+", url, re.IGNORECASE)
    return unquote(match.group(0)) if match else url


def _is_probable_document_link(candidate):
    parsed = urlparse(str(candidate or ""))
    lowered = str(candidate or "").lower()
    if any(token in lowered for token in ("servedocument", "download", "fileid", "docid")):
        return True
    extension = os.path.splitext(parsed.path.lower())[1]
    return extension in {
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".txt",
        ".rtf", ".zip", ".tif", ".tiff", ".png", ".jpg", ".jpeg",
    }


def _extract_document_links(page_url, html_text):
    parser = _DocumentLinkParser()
    parser.feed(html_text)
    candidates = list(parser.links)
    candidates.extend(re.findall(r"https?://[^\s\"'<>]+", html_text, re.IGNORECASE))
    candidates.extend(re.findall(r"(?:href|url)\s*[:=]\s*[\"']([^\"']+)", html_text, re.IGNORECASE))
    candidates.extend(
        re.findall(r"ServeDocument\.ashx\?[^\s\"'<>]+", html_text, re.IGNORECASE)
    )

    links = []
    seen = set()
    for raw in candidates:
        raw = unescape(str(raw or "").strip().strip("\"'"))
        js_match = re.search(r"(?:window\.open|location(?:\.href)?)\s*\(?\s*[\"']([^\"']+)", raw, re.IGNORECASE)
        if js_match:
            raw = js_match.group(1)
        if not raw or raw.lower().startswith(("javascript:", "mailto:", "data:")):
            continue
        if raw.lower().startswith("servedocument.ashx"):
            raw = f"/{raw}"
        resolved = _unwrap_secure_web_url(urljoin(page_url, raw))
        parsed = urlparse(resolved)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            continue
        if not _is_probable_document_link(resolved):
            continue
        key = resolved.lower()
        if key not in seen:
            seen.add(key)
            links.append(resolved)
    return links


def download_pdf(url, timeout=MDEC_DOWNLOAD_TIMEOUT_SECONDS, session=None, max_depth=3):
    if session is None:
        import requests
        session = requests.Session()
    client = session
    queue = [(_unwrap_secure_web_url(url), 0, None)]
    visited = set()
    child_errors = []
    while queue:
        current_url, depth, referer = queue.pop(0)
        if current_url.lower() in visited or depth > max_depth:
            continue
        visited.add(current_url.lower())
        headers = {"Referer": referer} if referer else None
        try:
            response = client.get(current_url, timeout=timeout, allow_redirects=True, headers=headers)
            response.raise_for_status()
        except Exception as exc:
            if depth == 0:
                raise
            child_errors.append(f"{current_url}: {exc}")
            continue
        content = response.content or b""
        content_type = (response.headers.get("content-type") or "").lower()
        if content.startswith(b"%PDF-") or "application/pdf" in content_type:
            return content
        if depth < max_depth and ("html" in content_type or b"<html" in content[:2048].lower()):
            page_url = getattr(response, "url", None) or current_url
            text = getattr(response, "text", None)
            if text is None:
                text = content.decode("utf-8", errors="ignore")
            for link in _extract_document_links(page_url, text):
                if link.lower() not in visited:
                    queue.append((link, depth + 1, page_url))
    detail = f" Tried child links: {'; '.join(child_errors[:3])}" if child_errors else ""
    raise ValueError(f"MDEC download link did not resolve to a PDF.{detail}")


def download_combined_pdf(start_url, timeout=MDEC_COMBINED_JOB_TIMEOUT_SECONDS, session=None):
    if session is None:
        import requests
        session = requests.Session()
    response = session.post(start_url, timeout=MDEC_DOWNLOAD_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    job_id = str(payload.get("job_id") or "").strip()
    if not payload.get("ok") or not job_id:
        raise ValueError("MDEC combined-PDF job did not return a job_id")

    parsed = urlparse(start_url)
    file_url = f"{parsed.scheme}://{parsed.netloc}/download-case/jobs/{job_id}/file"
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            file_response = session.get(
                file_url,
                timeout=MDEC_DOWNLOAD_TIMEOUT_SECONDS,
                allow_redirects=True,
            )
            if file_response.status_code == 200:
                content = file_response.content or b""
                content_type = (file_response.headers.get("content-type") or "").lower()
                if content.startswith(b"%PDF-") or "application/pdf" in content_type:
                    return content
            last_error = f"HTTP {file_response.status_code}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise TimeoutError(f"MDEC combined PDF was not ready within {timeout} seconds: {last_error}")


def mdec_blob_name(document):
    case_key = normalize_case_number(document.get("case_number")) or "UNKNOWN"
    filename = re.sub(r"[^A-Za-z0-9._-]+", "_", source_filename(document)).strip("_")
    return f"mdec/{case_key}/{document['source_document_id']}_{filename}"


def upsert_mdec_document(target_conn, container, document, record_id, pdf_loader=download_combined_pdf):
    from azure.storage.blob import ContentSettings

    cur = target_conn.cursor()
    source_id = document["source_document_id"]
    cur.execute("""
        SELECT id, blob_name, source_json
        FROM search.civil_return_pdfs
        WHERE source_system = ? AND source_document_id = ?
    """, MDEC_SOURCE_SYSTEM, source_id)
    existing = cur.fetchone()
    blob_name = existing[1] if existing and existing[1] else mdec_blob_name(document)
    existing_version = 0
    existing_format_version = 0
    if existing and len(existing) > 2 and existing[2]:
        try:
            existing_source = json.loads(existing[2])
            existing_version = int(existing_source.get("source_version") or 0)
            existing_format_version = int(existing_source.get("combined_format_version") or 0)
        except (TypeError, ValueError, json.JSONDecodeError):
            existing_version = 0
            existing_format_version = 0

    blob_client = container.get_blob_client(blob_name)
    if (
        not existing
        or not existing[1]
        or int(document.get("source_version") or 0) > existing_version
        or existing_format_version < MDEC_COMBINED_FORMAT_VERSION
    ):
        pdf_bytes = pdf_loader(document["download_url"])
        blob_client.upload_blob(
            pdf_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
            metadata={
                "source_system": MDEC_SOURCE_SYSTEM,
                "source_document_id": source_id,
                "case_number": document["case_number"][:100],
                "original_filename": source_filename(document),
            },
        )

    source_json = {
        "source_system": MDEC_SOURCE_SYSTEM,
        "source_document_id": source_id,
        "source_version": document.get("source_version"),
        "combined_format_version": MDEC_COMBINED_FORMAT_VERSION,
        "download_url": document.get("download_url"),
        "document_name": document.get("document_name"),
        "lead_document": document.get("lead_document"),
        "filing_description": document.get("filing_description"),
    }
    if existing:
        cur.execute("""
            UPDATE search.civil_return_pdfs
            SET record_id = ?, case_number = ?, intake_date = CAST(? AS date),
                original_filename = ?, blob_name = ?, content_type = 'application/pdf',
                source_submission_at = ?, source_download_url = ?, source_json = ?,
                parse_status = 'matched', parse_error = NULL
            WHERE id = ?
        """,
            record_id,
            document["case_number"],
            document["submission_at"],
            source_filename(document),
            blob_name,
            document["submission_at"],
            document["download_url"],
            json.dumps(source_json),
            int(existing[0]),
        )
        return int(existing[0]), False

    message_key = f"mdec:{source_id}"
    cur.execute("""
        INSERT INTO search.civil_return_pdfs (
            record_id, case_number, intake_date, mailbox, message_id, email_subject,
            email_received_at, attachment_id, original_filename, blob_name, content_type,
            pdf_case_number, parse_status, source_json, source_system,
            source_document_id, source_submission_at, source_download_url
        )
        OUTPUT INSERTED.id
        VALUES (?, ?, CAST(? AS date), 'mdec', ?, ?, ?, ?, ?, ?, 'application/pdf', ?,
                'matched', ?, ?, ?, ?, ?)
    """,
        record_id,
        document["case_number"],
        document["submission_at"],
        message_key,
        f"MDEC document for {document['case_number']}",
        document["submission_at"],
        message_key,
        source_filename(document),
        blob_name,
        document["case_number"],
        json.dumps(source_json),
        MDEC_SOURCE_SYSTEM,
        source_id,
        document["submission_at"],
        document["download_url"],
    )
    return int(cur.fetchone()[0]), True


def sync_mdec_civil_documents(target_conn, container, source_conn=None, pdf_loader=download_combined_pdf):
    # MDEC and the BCSO Search Portal share bcsodb.  Keeping source_conn as an
    # optional argument makes isolated tests possible without requiring a
    # second production database connection.
    source_conn = source_conn or target_conn
    summary = {"status": "ok", "scanned": 0, "matched": 0, "inserted": 0, "updated": 0, "unmatched": 0, "failed": 0, "deferred": 0, "errors": []}
    started_at = time.monotonic()
    try:
        documents = fetch_mdec_documents(source_conn)
        summary["scanned"] = len(documents)
        target_cur = target_conn.cursor()
        for index, document in enumerate(documents):
            if time.monotonic() - started_at >= MDEC_RUN_TIME_BUDGET_SECONDS:
                summary["deferred"] = len(documents) - index
                break
            if not document.get("submission_at"):
                summary["unmatched"] += 1
                record_sync_status(target_conn, document, "unmatched", error="Missing or invalid submission date")
                target_conn.commit()
                continue
            try:
                record_id = find_best_civil_record(target_cur, document["case_number"], document["submission_at"])
                if not record_id:
                    summary["unmatched"] += 1
                    record_sync_status(target_conn, document, "unmatched", error="No eligible Civil Papers record")
                    target_conn.commit()
                    continue
                pdf_id, inserted = upsert_mdec_document(target_conn, container, document, record_id, pdf_loader=pdf_loader)
                priority = get_civil_record_priority(target_cur, record_id)
                record_sync_status(
                    target_conn, document, "matched", record_id=record_id, pdf_id=pdf_id,
                    terminal=priority <= 1,
                )
                summary["matched"] += 1
                summary["inserted" if inserted else "updated"] += 1
                target_conn.commit()
            except Exception as exc:
                target_conn.rollback()
                record_sync_status(target_conn, document, "failed", error=str(exc))
                target_conn.commit()
                summary["failed"] += 1
                summary["errors"].append({"source_document_id": document.get("source_document_id"), "error": str(exc)})
        if summary["failed"]:
            summary["status"] = "partial"
        return summary
    finally:
        # The caller owns the shared bcsodb connection.
        pass
