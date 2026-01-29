from db_connect import get_conn
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os
import pyodbc
print("USING INGEST.PY FROM:", __file__)



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
            address
        )
        OUTPUT INSERTED.record_id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        city,
        state,
        disposition,
        notes
    )
    OUTPUT INSERTED.record_id
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        record.get("city"),
        record.get("state"),
        record.get("disposition"),
        record.get("notes"),
    ))
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

    
    cursor.execute(sql, *values)
def safe_sql_date(v):
    ts = pd.to_datetime(v, errors="coerce")
    return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")
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
    return pd.read_csv(io.BytesIO(data))

def ingest_odyssey_civil_from_blob(blob_name, container_name="fscsv"):
    df = read_csv_from_blob(container_name, blob_name)

    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM search.records
            WHERE department = 'FIELD SERVICES DEPARTMENT'
            AND source_file = ?
        """, blob_name)
        for _, row in df.iterrows():
            record = {
                "department": "FIELD SERVICES DEPARTMENT",
                "source_file": blob_name,
                "full_name": row.get("DefendantName"),
                "case_number": row.get("CaseNumber"),
                "intake_date": safe_sql_date(row.get("EventDate")),
                "address": row.get("TenantAddress"),
                "city": row.get("TenantCity"),
                "state": row.get("TenantState"),
                "postal_code": None,
                "disposition": row.get("EventType"),
                "notes": row.get("EventComment"),
            }

            record_id = insert_search_record_odyssey(cursor, record)
            insert_raw_record(cursor, record_id, blob_name, row.to_dict())

        conn.commit()
    finally:
        conn.close()

def ingest_all_odyssey_civil_blobs(container_name="fscsv"):
    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    container_client = blob_service_client.get_container_client(container_name)

    for blob in container_client.list_blobs():
        name = blob.name

        if name.startswith("Odyssey-JobOutput-") and name.endswith(".csv"):
            print("Ingesting:", name)
            ingest_odyssey_civil_from_blob(name, container_name)

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
            for _, row in df.iterrows():
                row = row.where(pd.notna(row), None)
                record_type = row.get("Record Type") or row.get("record_type")

                is_update = (
                    isinstance(record_type, str)
                    and record_type.strip().lower() == "update warrant"
                )

                print("DEBUG record_type:", record_type, "is_update =", is_update)

                full_name = f"{row.get('last_name')}, {row.get('first_name')}".strip(", ")
                
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
                            address           = COALESCE(NULLIF(?, ''), address)
                            
                            
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

def ingest_wor_csv():
    from azure.storage.blob import BlobServiceClient
    import io
    import os
    import pandas as pd

    blob_service_client = BlobServiceClient.from_connection_string(
        os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    )

    container_name = "fscsv"
    blob_name = "Warrant_of_Restitution_Data_Management_Table_for_size_est(survey).csv"

    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data), low_memory=False)

    print("WOR DF ROWS:", len(df))

    conn = get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")

        batch_size = 1000

        for i, (_, row) in enumerate(df.iterrows(), start=1):
            record = {
                "department": "Warrant of Restitution",
                "source_file": blob_name,

                "full_name": (
                    None
                    if pd.isna(row.get("Tenant, Defendant, or Respondent Name"))
                    else str(row.get("Tenant, Defendant, or Respondent Name")).strip()
                ),

                "case_number": (
                    None
                    if pd.isna(row.get("Case Number"))
                    else str(row.get("Case Number")).strip()
                ),

                "intake_date": safe_sql_date(row.get("Intake Date")),
                "issue_date": safe_sql_date(row.get("Court Issued Date")),

                "address": (
                    None
                    if pd.isna(row.get("Tenant, Defendant or Respondent Address"))
                    else str(row.get("Tenant, Defendant or Respondent Address")).strip()
                ),

                "city": None,
                "state": None,
                "postal_code": None,

                "court_document_type": (
                    None
                    if pd.isna(row.get("Court Document Type"))
                    else str(row.get("Court Document Type")).strip()
                ),

                "disposition": (
                    None
                    if pd.isna(row.get("Adminstrative Status"))
                    else str(row.get("Adminstrative Status")).strip()
                ),

                "notes": (
                    None
                    if pd.isna(row.get("Comments"))
                    else str(row.get("Comments")).strip()
                ),
            }

            record_id = insert_search_record_fsdw(cursor, record)
            insert_raw_record(cursor, record_id, blob_name, row.to_dict())

            if i % batch_size == 0:
                conn.commit()
                print(f">>> WOR committed {i} rows")

        conn.commit()
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