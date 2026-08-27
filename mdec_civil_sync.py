import json
import os
import re
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qs, unquote, urljoin, urlparse

MDEC_SOURCE_SYSTEM = "mdec"
MDEC_MATCH_WINDOW_DAYS = 10


def normalize_case_number(value):
    return re.sub(r"[^A-Za-z0-9]+", "", str(value or "")).upper()


def parse_submission_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).replace(tzinfo=None)
    except ValueError:
        pass
    for fmt in (
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def civil_priority_sql():
    return """
        CASE
            WHEN LOWER(COALESCE(administrative_status, service_disp, disposition, '')) LIKE '%served%'
             AND LOWER(COALESCE(administrative_status, service_disp, disposition, '')) NOT LIKE '%non est%'
             AND LOWER(COALESCE(administrative_status, service_disp, disposition, '')) NOT LIKE '%not served%'
             AND LOWER(COALESCE(administrative_status, service_disp, disposition, '')) NOT LIKE '%unserved%'
                THEN 0
            WHEN LOWER(COALESCE(administrative_status, service_disp, disposition, '')) LIKE '%non est%'
              OR LOWER(COALESCE(administrative_status, service_disp, disposition, '')) LIKE '%not served%'
              OR LOWER(COALESCE(administrative_status, service_disp, disposition, '')) LIKE '%unserved%'
                THEN 1
            WHEN LOWER(COALESCE(administrative_status, service_disp, disposition, '')) LIKE '%attempt%'
              OR LOWER(COALESCE(source_file, '')) = 'civil-paper-attempts'
                THEN 2
            WHEN LOWER(COALESCE(administrative_status, service_disp, disposition, '')) LIKE '%received%'
                THEN 3
            ELSE 4
        END
    """


def find_best_civil_record(cur, case_number, submission_at):
    normalized_case = normalize_case_number(case_number)
    if not normalized_case or not submission_at:
        return None
    submission_date = submission_at.date().isoformat()
    priority = civil_priority_sql()
    cur.execute(
        f"""
        SELECT TOP 1 record_id
        FROM search.records
        WHERE LOWER(LTRIM(RTRIM(COALESCE(department, '')))) = 'civil papers'
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(COALESCE(case_number, '')), '-', ''), ' ', ''), '/', ''), '.', '') = ?
          AND (
                ABS(DATEDIFF(day, CAST(? AS date), CAST(COALESCE(issue_date, court_issued_date) AS date))) <= ?
             OR ABS(DATEDIFF(day, CAST(? AS date), CAST(intake_date AS date))) <= ?
          )
        ORDER BY
          {priority},
          CASE
            WHEN COALESCE(issue_date, court_issued_date) IS NULL THEN ABS(DATEDIFF(day, CAST(? AS date), CAST(intake_date AS date)))
            WHEN intake_date IS NULL THEN ABS(DATEDIFF(day, CAST(? AS date), CAST(COALESCE(issue_date, court_issued_date) AS date)))
            WHEN ABS(DATEDIFF(day, CAST(? AS date), CAST(COALESCE(issue_date, court_issued_date) AS date)))
               <= ABS(DATEDIFF(day, CAST(? AS date), CAST(intake_date AS date)))
              THEN ABS(DATEDIFF(day, CAST(? AS date), CAST(COALESCE(issue_date, court_issued_date) AS date)))
            ELSE ABS(DATEDIFF(day, CAST(? AS date), CAST(intake_date AS date)))
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
    cur.execute("""
        SELECT id, case_number, submission_datetime, document_name, lead_document,
               filing_description, download_link
        FROM dbo.case_documents
        WHERE NULLIF(LTRIM(RTRIM(COALESCE(case_number, ''))), '') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(COALESCE(download_link, ''))), '') IS NOT NULL
        ORDER BY id
    """)
    documents = []
    for row in cur.fetchall():
        documents.append({
            "source_document_id": str(row[0]),
            "case_number": str(row[1] or "").strip(),
            "submission_at": parse_submission_datetime(row[2]),
            "document_name": str(row[3] or "").strip(),
            "lead_document": str(row[4] or "").strip(),
            "filing_description": str(row[5] or "").strip(),
            "download_url": str(row[6] or "").strip(),
        })
    return documents


def source_filename(document):
    for value in (document.get("document_name"), document.get("lead_document")):
        name = os.path.basename(urlparse(str(value or "")).path).strip()
        if name:
            return name if name.lower().endswith(".pdf") else f"{name}.pdf"
    return f"mdec_{document['source_document_id']}.pdf"


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


def _extract_document_links(page_url, html_text):
    parser = _DocumentLinkParser()
    parser.feed(html_text)
    candidates = list(parser.links)
    candidates.extend(re.findall(r"https?://[^\s\"'<>]+", html_text, re.IGNORECASE))
    candidates.extend(re.findall(r"(?:href|url)\s*[:=]\s*[\"']([^\"']+)", html_text, re.IGNORECASE))
    candidates.extend(re.findall(r"(?:ServeDocument\.ashx|Download[^\s\"'<>]*)[^\s\"'<>]*", html_text, re.IGNORECASE))

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
        key = resolved.lower()
        if key not in seen:
            seen.add(key)
            links.append(resolved)
    return links


def download_pdf(url, timeout=60, session=None, max_depth=3):
    if session is None:
        import requests
        session = requests.Session()
    client = session
    queue = [(_unwrap_secure_web_url(url), 0, None)]
    visited = set()
    while queue:
        current_url, depth, referer = queue.pop(0)
        if current_url.lower() in visited or depth > max_depth:
            continue
        visited.add(current_url.lower())
        headers = {"Referer": referer} if referer else None
        response = client.get(current_url, timeout=timeout, allow_redirects=True, headers=headers)
        response.raise_for_status()
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
    raise ValueError("MDEC download link did not resolve to a PDF")


def mdec_blob_name(document):
    case_key = normalize_case_number(document.get("case_number")) or "UNKNOWN"
    filename = re.sub(r"[^A-Za-z0-9._-]+", "_", source_filename(document)).strip("_")
    return f"mdec/{case_key}/{document['source_document_id']}_{filename}"


def upsert_mdec_document(target_conn, container, document, record_id, pdf_loader=download_pdf):
    from azure.storage.blob import ContentSettings

    cur = target_conn.cursor()
    source_id = document["source_document_id"]
    cur.execute("""
        SELECT id, blob_name
        FROM search.civil_return_pdfs
        WHERE source_system = ? AND source_document_id = ?
    """, MDEC_SOURCE_SYSTEM, source_id)
    existing = cur.fetchone()
    blob_name = existing[1] if existing and existing[1] else mdec_blob_name(document)

    blob_client = container.get_blob_client(blob_name)
    if not existing or not existing[1]:
        pdf_bytes = pdf_loader(document["download_url"])
        blob_client.upload_blob(
            pdf_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
            metadata={
                "source_system": MDEC_SOURCE_SYSTEM,
                "source_document_id": source_id,
                "case_number": document["case_number"][:100],
            },
        )

    source_json = {
        "source_system": MDEC_SOURCE_SYSTEM,
        "source_document_id": source_id,
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


def sync_mdec_civil_documents(target_conn, container, source_conn=None, pdf_loader=download_pdf):
    # MDEC and the BCSO Search Portal share bcsodb.  Keeping source_conn as an
    # optional argument makes isolated tests possible without requiring a
    # second production database connection.
    source_conn = source_conn or target_conn
    summary = {"status": "ok", "scanned": 0, "matched": 0, "inserted": 0, "updated": 0, "unmatched": 0, "failed": 0, "errors": []}
    try:
        documents = fetch_mdec_documents(source_conn)
        summary["scanned"] = len(documents)
        target_cur = target_conn.cursor()
        for document in documents:
            if not document.get("submission_at"):
                summary["unmatched"] += 1
                continue
            try:
                record_id = find_best_civil_record(target_cur, document["case_number"], document["submission_at"])
                if not record_id:
                    summary["unmatched"] += 1
                    continue
                _, inserted = upsert_mdec_document(target_conn, container, document, record_id, pdf_loader=pdf_loader)
                summary["matched"] += 1
                summary["inserted" if inserted else "updated"] += 1
                target_conn.commit()
            except Exception as exc:
                target_conn.rollback()
                summary["failed"] += 1
                summary["errors"].append({"source_document_id": document.get("source_document_id"), "error": str(exc)})
        if summary["failed"]:
            summary["status"] = "partial"
        return summary
    finally:
        # The caller owns the shared bcsodb connection.
        pass
