import os
import io
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

print("=== START ingest_doc_csv.py ===")

# ============================
# CONFIG
# ============================

CONTAINER_NAME = "jailpopulation"

DEPARTMENT_NAME = "DOC Jail Population"

# ============================
# SQL CONNECTION
# ============================

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = os.getenv("ODBC_DRIVER", "SQL Server")

def get_conn():
    return pyodbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

# ============================
# BLOB CONNECTION
# ============================

blob_service = BlobServiceClient.from_connection_string(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
)

container_client = blob_service.get_container_client(CONTAINER_NAME)

def already_ingested(cursor, department_name, source_file):
    cursor.execute("""
        SELECT COUNT(*)
        FROM search.records
        WHERE department = ? AND source_file = ?
    """, department_name, source_file)

    count = cursor.fetchone()[0]
    return count > 0


# ============================
# INGEST ONE DOC CSV
# ============================

def ingest_one_doc_csv(blob_name):

    print("\nINGESTING:", blob_name)

    # Download CSV from blob
    blob_client = container_client.get_blob_client(blob_name)
    csv_bytes = blob_client.download_blob().readall()

    df = pd.read_csv(io.BytesIO(csv_bytes))

    conn = get_conn()
    cursor = conn.cursor()
    # ✅ SKIP if already ingested
    if already_ingested(cursor, DEPARTMENT_NAME, blob_name):
        print("SKIP (already ingested):", blob_name)
        conn.close()
        return

    # Delete old rows for this same snapshot file
    cursor.execute("""
        DELETE FROM search.records
        WHERE department = ?
          AND source_file = ?
    """, DEPARTMENT_NAME, blob_name)

    inserted = 0

    for _, row in df.iterrows():

        # ============================
        # SAFE SID CLEANING
        # ============================

        sid_raw = str(row.get("sid", "")).strip()

        if not sid_raw.isdigit():
            print("⚠️ SKIP BAD ROW (SID NOT NUMERIC)")
            print("SID =", sid_raw)
            print("ROW =", row.to_dict())
            continue

        sid_clean = sid_raw

        # ============================
        # BUILD RECORD
        # ============================

        full_name = f"{row.get('last_name','')}, {row.get('first_name','')}".strip(", ")

        cursor.execute("""
            INSERT INTO search.records (
                department,
                source_file,
                sid,
                first_name,
                last_name,
                full_name,
                date_of_birth,
                facility
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        DEPARTMENT_NAME,
        blob_name,
        sid_clean,
        str(row.get("first_name", "")).strip(),
        str(row.get("last_name", "")).strip(),
        full_name,
        row.get("date_of_birth"),
        str(row.get("facility", "")).strip()
        )

        inserted += 1

    conn.commit()
    conn.close()

    print("Inserted rows:", inserted)

# ============================
# DEDUPE: KEEP NEWEST PER SID
# ============================

def dedupe_doc():

    print("\n=== DEDUPING DOC RECORDS ===")

    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        WITH ranked AS (
            SELECT
                record_id,
                sid,
                ROW_NUMBER() OVER (
                    PARTITION BY sid
                    ORDER BY source_file DESC, record_id DESC
                ) AS rn
            FROM search.records
            WHERE department = ?
        )
        DELETE FROM ranked
        WHERE rn > 1;
    """, DEPARTMENT_NAME)

    conn.commit()
    conn.close()

    print("DEDUPED: kept newest row per SID")

# ============================
# INGEST ALL DOC CSVs
# ============================

def ingest_all_doc_csvs():

    for blob in container_client.list_blobs():

        name = blob.name

        # Only DOC CSVs
        if name.startswith("docpopulation_") and name.endswith(".csv"):
            ingest_one_doc_csv(name)

    dedupe_doc()

# ============================
# MAIN
# ============================

if __name__ == "__main__":
    ingest_all_doc_csvs()
    print("\n=== DONE ingest_doc_csv.py ===")
