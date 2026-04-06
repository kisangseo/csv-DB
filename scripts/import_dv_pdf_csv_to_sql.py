import csv
from datetime import datetime
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_connect import get_conn


CSV_PATH = Path("static/uploads/dv_pdf_records.csv")


def parse_issue_date(value):
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def parse_uploaded_at(value):
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def main():
    if not CSV_PATH.exists():
        raise SystemExit(f"CSV not found: {CSV_PATH}")

    conn = get_conn()
    inserted = 0
    skipped = 0
    try:
        cur = conn.cursor()
        with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                case_number = (row.get("case_number") or "").strip()
                respondent_name = (row.get("respondent_name") or "").strip()
                if not case_number or not respondent_name:
                    skipped += 1
                    continue

                issue_date = parse_issue_date(row.get("issue_date"))
                uploaded_at = parse_uploaded_at(row.get("uploaded_at"))
                pdf_download = (row.get("pdf_download") or "").strip()
                blob_name = pdf_download.replace("/dv-pdf/file/", "", 1).strip("/")
                order_type = (row.get("type") or "").strip() or None

                cur.execute(
                    """
                    SELECT TOP 1 id
                    FROM search.dv_pdf_records
                    WHERE LOWER(LTRIM(RTRIM(case_number))) = LOWER(LTRIM(RTRIM(?)))
                      AND LOWER(LTRIM(RTRIM(respondent_name))) = LOWER(LTRIM(RTRIM(?)))
                      AND is_reissue = 0
                    ORDER BY id ASC
                    """,
                    case_number,
                    respondent_name,
                )
                existing = cur.fetchone()
                is_reissue = 1 if existing else 0

                if uploaded_at is None:
                    cur.execute(
                        """
                        INSERT INTO search.dv_pdf_records
                            (case_number, respondent_name, issue_date, order_type, blob_name, pdf_download, uploaded_at, is_reissue)
                        VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), ?)
                        """,
                        case_number,
                        respondent_name,
                        issue_date,
                        order_type,
                        blob_name,
                        pdf_download,
                        is_reissue,
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO search.dv_pdf_records
                            (case_number, respondent_name, issue_date, order_type, blob_name, pdf_download, uploaded_at, is_reissue)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        case_number,
                        respondent_name,
                        issue_date.strftime("%Y-%m-%d") if issue_date else None,
                        order_type,
                        blob_name,
                        pdf_download,
                        uploaded_at.strftime("%Y-%m-%d %H:%M:%S"),
                        is_reissue,
                    )
                inserted += 1

        conn.commit()
    finally:
        conn.close()

    print(f"Imported DV PDF rows: inserted={inserted}, skipped={skipped}")


if __name__ == "__main__":
    main()
