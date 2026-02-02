import os
import re
import tempfile
from datetime import datetime

import pdfplumber
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ============================
# CONFIG
# ============================
CONTAINER_NAME = "jailpopulation"
REPORT_DATE = datetime.today().date()

print("=== START extract_doc_pop.py ===")
print("Container:", CONTAINER_NAME)

# ============================
# DEBUG: Connection String
# ============================
conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

print("DEBUG: AZURE_STORAGE_CONNECTION_STRING exists?:", conn_str is not None)

if not conn_str:
    print("ERROR: No Azure Storage connection string found.")
    print("Set AZURE_STORAGE_CONNECTION_STRING before running.")
    raise SystemExit()

# ============================
# HELPERS
# ============================
def is_date(token: str) -> bool:
    return re.match(r"\d{2}/\d{2}/\d{4}", token) is not None


def download_blob_pdf_to_temp(blob_name: str) -> str:
    print("DEBUG: Downloading PDF:", blob_name)

    blob_client = blob_service.get_blob_client(
        container=CONTAINER_NAME,
        blob=blob_name
    )

    pdf_bytes = blob_client.download_blob().readall()

    tmp_path = os.path.join(tempfile.gettempdir(), blob_name)

    with open(tmp_path, "wb") as f:
        f.write(pdf_bytes)

    print("DEBUG: Saved temp PDF to:", tmp_path)
    return tmp_path


def upload_csv_to_blob(csv_name: str, df: pd.DataFrame):
    print("DEBUG: Uploading CSV back to blob:", csv_name)

    blob_client = blob_service.get_blob_client(
        container=CONTAINER_NAME,
        blob=csv_name
    )

    csv_text = df.to_csv(index=False)

    blob_client.upload_blob(csv_text, overwrite=True)

    print("DEBUG: Upload successful:", csv_name)


# ============================
# CONNECT TO AZURE BLOB
# ============================
print("DEBUG: Connecting to BlobServiceClient...")

blob_service = BlobServiceClient.from_connection_string(conn_str)

container_client = blob_service.get_container_client(CONTAINER_NAME)

print("DEBUG: Connected. Listing blobs now...")

# ============================
# MAIN LOOP
# ============================
found_any = False
processed_any = False

for blob in container_client.list_blobs():
    found_any = True
    print("\nFOUND BLOB:", blob.name)

    name_lower = blob.name.lower()

    # Only DOC PDFs
    if not name_lower.endswith(".pdf"):
        print("  SKIP: Not a PDF")
        continue

    if "docpopulation" not in name_lower:
        print("  SKIP: PDF but not DOC population")
        continue

    processed_any = True

    SOURCE_FILE = blob.name
    print("PROCESSING PDF:", SOURCE_FILE)

    # Download PDF
    PDF_PATH = download_blob_pdf_to_temp(SOURCE_FILE)

    records = []

    # ============================
    # PARSE PDF (EXACT SAME LOGIC)
    # ============================
    print("DEBUG: Extracting text rows...")

    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            for line in text.split("\n"):
                line = line.strip()

                if not line:
                    continue
                if not line[0].isdigit():
                    continue

                tokens = line.split()
                i = 0

                while i < len(tokens):
                    if i + 4 >= len(tokens):
                        break

                    sid = tokens[i]
                    last_name = tokens[i + 1]
                    first_name = tokens[i + 2]

                    possible_mi = tokens[i + 3]

                    if len(possible_mi) == 1 and possible_mi.isalpha():
                        if i + 5 >= len(tokens):
                            break
                        mi = possible_mi
                        dob = tokens[i + 4]
                        facility = tokens[i + 5]
                        i += 6
                    else:
                        mi = None
                        dob = tokens[i + 3]
                        facility = tokens[i + 4]
                        i += 5

                    if not is_date(dob):
                        continue

                    records.append({
                        "sid": sid,
                        "last_name": last_name,
                        "first_name": first_name,
                        "middle_initial": mi,
                        "date_of_birth": datetime.strptime(dob, "%m/%d/%Y").date(),
                        "facility": facility,
                        "report_date": REPORT_DATE,
                        "source_file": SOURCE_FILE
                    })

    print("DEBUG: Extracted rows:", len(records))

    if not records:
        print("WARNING: No records extracted. Skipping CSV upload.")
        continue

    # ============================
    # UPLOAD CSV BACK TO BLOB
    # ============================
    df = pd.DataFrame(records)

    csv_name = SOURCE_FILE.replace(".pdf", ".csv")

    upload_csv_to_blob(csv_name, df)

# ============================
# FINAL DEBUG SUMMARY
# ============================
print("\n=== DONE ===")

if not found_any:
    print("ERROR: No blobs found at all. Container may be empty or wrong.")

if found_any and not processed_any:
    print("ERROR: Blobs exist, but none matched docpopulation PDFs.")

if processed_any:
    print("SUCCESS: DOC PDFs processed and CSVs uploaded.")
