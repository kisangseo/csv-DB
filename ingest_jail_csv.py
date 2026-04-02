import os
import re
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient

# =========================
# CONFIG
# =========================
CONTAINER_NAME = "jailpopulation"
DEPARTMENT_NAME = "Baltimore Jail Population"

# =========================
# SQL CONNECTION
# =========================
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


# =========================
# READ CSV FROM BLOB
# =========================
def read_csv_from_blob(container_name, blob_name):
    blob_service = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    blob_client = blob_service.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    csv_bytes = blob_client.download_blob().readall()

    return pd.read_csv(pd.io.common.BytesIO(csv_bytes))




def extract_snapshot_date(blob_name):
    """Return YYYYMMDD as int from filenames like baltimorejailpopulation_20260128.csv."""
    match = re.search(r"_(\d{8})\.csv$", blob_name.lower())
    return int(match.group(1)) if match else -1


def get_newest_jail_csv_blob_name(container_client):
    jail_blobs = [
        blob.name
        for blob in container_client.list_blobs()
        if blob.name.lower().endswith(".csv")
        and "baltimorejailpopulation" in blob.name.lower()
    ]

    if not jail_blobs:
        return None

    return max(jail_blobs, key=extract_snapshot_date)

# =========================
# INGEST ONE CSV
# =========================
def ingest_one_jail_csv(blob_name):
    print("\nINGESTING:", blob_name)

    df = read_csv_from_blob(CONTAINER_NAME, blob_name)

    conn = get_conn()
    cursor = conn.cursor()

    # Snapshot reset: keep ONLY newest Baltimore jail population snapshot
    cursor.execute("""
        DELETE FROM search.records
        WHERE department = ?
    """, DEPARTMENT_NAME)

    inserted = 0

    # insert all rows from this CSV
    for _, row in df.iterrows():
        full_name = f"{row['last_name']}, {row['first_name']}"

        cursor.execute("""
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        DEPARTMENT_NAME,
        blob_name,
        row["first_name"],
        row["last_name"],
        full_name,
        row["date_of_birth"],
        str(row["sid"]),
        row["facility"],
        
        )

        inserted += 1

    conn.commit()
    print("Inserted rows:", inserted)

    # =========================
    # ONLY NEW CHANGE: DEDUPE BY SID
    # Keep newest snapshot per SID
    # =========================
    
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
    print("Deduped: kept newest row per SID")

    conn.close()


# =========================
# INGEST ALL JAIL CSVs
# =========================
def ingest_all_jail_csvs():
    blob_service = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    container_client = blob_service.get_container_client(CONTAINER_NAME)
    newest_blob_name = get_newest_jail_csv_blob_name(container_client)

    if not newest_blob_name:
        print("No Baltimore jail population CSV files found to ingest.")
        return

    print("Newest Baltimore jail CSV selected:", newest_blob_name)
    ingest_one_jail_csv(newest_blob_name)

def dedupe_jail_population():
    print("=== DEDUPING: Keep newest row per SID ===")

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
          AND sid IS NOT NULL
    )
    DELETE FROM ranked
    WHERE rn > 1;
    """, "Baltimore Jail Population")

    conn.commit()
    conn.close()

    print("=== DONE: Deduped newest SID rows only ===")


# =========================
# RUN
# =========================

if __name__ == "__main__":
    ingest_all_jail_csvs()
    dedupe_jail_population()
