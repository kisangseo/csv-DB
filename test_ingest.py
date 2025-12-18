from db_connect import conn
from ingest import insert_search_record, insert_raw_record

cursor = conn.cursor()

fake_record = {
    "department": "TEST",
    "source_file": "test_file.csv",
    "first_name": "John",
    "last_name": "Doe",
    "full_name": "John Doe",
    "date_of_birth": "1990-01-01",
    "sid": "TEST123",
    "case_number": "CASE-TEST-001",
    "warrant_type": "TEST WARRANT",
    "warrant_status": "OPEN",
    "issue_date": "2025-01-01",
    "intake_date": None,
    "address": "123 Test St",
    "city": "Baltimore",
    "state": "MD",
    "postal_code": "21201",
    "notes": "This is a test record"
}

record_id = insert_search_record(cursor, fake_record)

insert_raw_record(
    cursor,
    record_id,
    fake_record["source_file"],
    fake_record
)

conn.commit()

print("Inserted test record_id:", record_id)
