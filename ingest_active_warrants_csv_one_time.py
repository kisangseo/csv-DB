import csv
from datetime import datetime

from db_connect import get_conn
from ingest import insert_search_record_active_warrants


CSV_PATH = "active_warrants.csv"
DEPARTMENT = "BCSO_ACTIVE_WARRANTS"
SOURCE_FILE = "active_warrants_csv_one_time"


# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------

def clean(val):
    if val is None:
        return None
    val = str(val).strip()
    return val if val != "" else None


def parse_date(val):
    val = clean(val)
    if not val:
        return None

    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue

    return None


# ------------------------------------------------------------
# db helpers
# ------------------------------------------------------------

def get_existing_active_warrant(cursor, warrant_id):
    cursor.execute(
        """
        SELECT record_id
        FROM search.records
        WHERE department = ?
          AND warrant_id_number = ?
        """,
        (DEPARTMENT, warrant_id),
    )
    return cursor.fetchone()


def update_active_warrant_if_present(cursor, record_id, record):
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
            address           = COALESCE(NULLIF(?, ''), address)
        WHERE record_id = ?
    """,
        record.get("source_file") or "",
        record.get("full_name") or "",
        record.get("warrant_id_number") or "",
        record.get("warrant_type") or "",
        record.get("issue_date"),
        record.get("warrant_status") or "",
        (str(record.get("sid")) if record.get("sid") is not None else ""),
        record.get("date_of_birth"),
        record.get("race") or "",
        record.get("sex") or "",
        record.get("issuing_county") or "",
        record.get("address") or "",
        record_id
    )


# ------------------------------------------------------------
# record builder (maps CSV → search.records)
# ------------------------------------------------------------

def build_active_warrant_record(row):
    first = clean(row.get("First Name"))
    last = clean(row.get("Last Name"))

    full_name = None
    if first and last:
        full_name = f"{last}, {first}"
    elif last:
        full_name = last

    address = clean(row.get("Last Known Address"))
    apt = clean(row.get("Apt or Unit"))
    if address and apt:
        address = f"{address}, {apt}"

    return {
        "department": DEPARTMENT,
        "source_file": SOURCE_FILE,
        "full_name": full_name,
        "case_number": clean(row.get("Case Number")),
        "warrant_id_number": clean(row.get("Warrant_ID")),
        "warrant_type": clean(row.get("Court Type")),
        "issue_date": parse_date(row.get("Date Issued")),
        "warrant_status": clean(row.get("Warrant Status")),
        "sid": clean(row.get("Sid Number")),
        "date_of_birth": parse_date(row.get("Date of Birth")),
        "race": clean(row.get("Race")),
        "sex": clean(row.get("Sex")),
        "issuing_county": clean(row.get("Issuing County")),
        "address": address,
    }


# ------------------------------------------------------------
# main ingest
# ------------------------------------------------------------

def run():
    conn = get_conn()
    cursor = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            record = build_active_warrant_record(row)
            warrant_id = record["warrant_id_number"]

            if not warrant_id:
                skipped += 1
                continue

            existing = get_existing_active_warrant(cursor, warrant_id)

            if existing:
                update_active_warrant_if_present(
                    cursor,
                    existing[0],
                    record
                )
                updated += 1
            else:
                insert_search_record_active_warrants(cursor, record)
                inserted += 1

            processed += 1

            if processed % 1000 == 0:
                print(
                    f"[{processed:,}] processed | "
                    f"inserted={inserted:,} | "
                    f"updated={updated:,} | "
                    f"skipped={skipped:,}"
                )

    conn.commit()
    cursor.close()
    conn.close()

    print("=== ACTIVE WARRANTS CSV INGEST COMPLETE ===")
    print(f"Inserted: {inserted}")
    print(f"Updated : {updated}")
    print(f"Skipped : {skipped}")


if __name__ == "__main__":
    run()