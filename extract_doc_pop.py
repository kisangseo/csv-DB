import pdfplumber
import re
from datetime import datetime
import pyodbc
import os

# =====================
# CONFIG
# =====================
REPORT_DATE = datetime(2025, 12, 28).date()
SOURCE_FILE = "docpopulation_20251228.pdf"
PDF_PATH = "docpopulation_20251228.pdf"

# =====================
# HELPERS
# =====================
def is_date(token):
    return re.match(r"\d{2}/\d{2}/\d{4}", token)

records = []

# =====================
# PDF PARSE
# =====================
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
                    mi = possible_mi
                    dob = tokens[i + 4]
                    facility = tokens[i + 5] if i + 5 < len(tokens) else None
                    i += 6
                else:
                    mi = None
                    dob = tokens[i + 3]
                    facility = tokens[i + 4]
                    i += 5

                if not facility or not is_date(dob):
                    continue

                records.append({
                    "sid": sid,
                    "last_name": last_name,
                    "first_name": first_name,
                    "mi": mi,
                    "dob": datetime.strptime(dob, "%m/%d/%Y").date(),
                    "facility": facility
                })

print(f"Total extracted DOC records: {len(records)}")

# =====================
# DB INSERT
# =====================
server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = os.getenv("ODBC_DRIVER", "SQL Server")

conn = pyodbc.connect(
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
)

cursor = conn.cursor()

sql = """
INSERT INTO doc_population (
    sid,
    last_name,
    first_name,
    middle_initial,
    date_of_birth,
    facility,
    report_date,
    source_file
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

for r in records:
    cursor.execute(
        sql,
        r["sid"],
        r["last_name"],
        r["first_name"],
        r["mi"],
        r["dob"].strftime("%Y-%m-%d"),
        r["facility"],
        REPORT_DATE.strftime("%Y-%m-%d"),
        SOURCE_FILE
    )

conn.commit()
cursor.close()
conn.close()

print("DOC population insert complete.")
