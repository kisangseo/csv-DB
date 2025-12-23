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
            sex,
            race,
            issuing_county,
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
            notes
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
        record.get("sex"),
        record.get("race"),
        record.get("issuing_county"),
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
        None,  # court_document_type
        None,  # disposition
        record.get("notes"),
    )
    
    cursor.execute(sql, *values)
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
                else pd.to_datetime(row.get("Date of Birth"), errors="coerce").date()
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
                else pd.to_datetime(row.get("Issue Date"), errors="coerce")
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


        record_id = insert_search_record(cursor, record)
        insert_raw_record(cursor, record_id, blob_name, row.to_dict())
        

    conn.commit()