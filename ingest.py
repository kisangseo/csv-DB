from db_connect import conn
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os



def insert_search_record(cursor, record):
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
            notes
        )
        OUTPUT INSERTED.record_id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        record.get("notes"),
    )

    cursor.execute(sql, values)
    return cursor.fetchone()[0]

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

    cursor.execute(sql, values)

def read_csv_from_blob(container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    data = blob_client.download_blob().readall()
    return pd.read_csv(io.BytesIO(data))

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
   
    

    cursor = conn.cursor()
    for i, (_, row) in enumerate(df.iterrows()):
        if i == 0:
            print("DEBUG ROW KEYS:")
            print(list(row.index))
            print("DEBUG SAMPLE VALUES:")
            print({
                "First Name": row.get("First Name"),
                "Last Name": row.get("Last Name"),
                "SID": row.get("SID"),
                "Case Number": row.get("Case Number"),
                "Warrant Type": row.get("Warrant Type"),
                "Warrant Status": row.get("Warrant Status"),
                "Issue Date": row.get("Issue Date"),
            })

    for _, row in df.iterrows():
        record = {
            "department": "WARRANTS",
            "source_file": blob_name,

            "first_name": row.get("First Name"),
            "last_name": row.get("Last Name"),
            "full_name": (
                f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
            ),

            "date_of_birth": row.get("Date of Birth"),
            "sid": (
                None
                if pd.isna(row.get("SID"))
                else str(int(row.get("SID")))
            ),

            "case_number": row.get("Case Number"),
            "warrant_type": row.get("Warrant Type"),
            "warrant_status": row.get("Warrant Status"),

            "issue_date": row.get("Issue Date"),
            "intake_date": None,

            "address": row.get("Address"),
            "city": None,
            "state": None,
            "postal_code": None,

            "notes": row.get("Notes or Alias"),
        }

        record_id = insert_search_record(cursor, record)
        insert_raw_record(cursor, record_id, blob_name, row.to_dict())

    conn.commit()