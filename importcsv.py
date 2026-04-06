import csv
from datetime import datetime
from pathlib import Path
from db_connect import get_conn

CSV_PATH = Path("static/uploads/dv_pdf_records.csv")

def parse_issue_date(v):
    v = (v or "").strip()
    if not v:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            pass
    return None

def parse_uploaded_at(v):
    v = (v or "").strip()
    if not v:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            pass
    return None

rows = []
with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        rows.append(r)

conn = get_conn()
cur = conn.cursor()

inserted = 0
skipped = 0

for r in rows:
    case_number = (r.get("case_number") or "").strip()
    respondent_name = (r.get("respondent_name") or "").strip()
    if not case_number or not respondent_name:
        skipped += 1
        continue

    issue_date = parse_issue_date(r.get("issue_date"))
    order_type = (r.get("type") or "").strip() or None
    pdf_download = (r.get("pdf_download") or "").strip()
    blob_name = pdf_download.replace("/dv-pdf/file/", "", 1).strip("/") if pdf_download else ""
    uploaded_at = parse_uploaded_at(r.get("uploaded_at"))

    # detect duplicate non-reissue
    cur.execute("""
        SELECT TOP 1 id
        FROM search.dv_pdf_records
        WHERE LOWER(LTRIM(RTRIM(case_number))) = LOWER(LTRIM(RTRIM(?)))
          AND LOWER(LTRIM(RTRIM(respondent_name))) = LOWER(LTRIM(RTRIM(?)))
          AND is_reissue = 0
        ORDER BY id
    """, case_number, respondent_name)
    existing = cur.fetchone()

    is_reissue = 1 if existing else 0

    cur.execute("""
        INSERT INTO search.dv_pdf_records
        (case_number, respondent_name, issue_date, order_type, blob_name, pdf_download, uploaded_at, is_reissue)
        VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, SYSUTCDATETIME()), ?)
    """, case_number, respondent_name, issue_date, order_type, blob_name, pdf_download, uploaded_at, is_reissue)
    inserted += 1

conn.commit()
conn.close()

print(f"Inserted: {inserted}, Skipped: {skipped}")