from flask import Flask, request, jsonify, render_template, redirect, session, send_file
from flask_cors import CORS
import os
import re
import string
import csv
import json
import tempfile
import threading
import uuid
import io
import requests
import importlib.util
from difflib import SequenceMatcher
import pandas as pd
import chardet
from pypdf import PdfReader
from azure.storage.blob import ContainerClient
from db_connect import get_conn
from search_sql import search_by_name, build_search_sql
from datetime import timedelta, datetime, UTC
from werkzeug.utils import secure_filename


# ============================================================
# USER LOGIN
# ============================================================

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")
app.permanent_session_lifetime = timedelta(hours=12)
APT_SPLIT_RE = re.compile(r"(?i)\bapt\.?\s*#?\s*([A-Za-z0-9-]+)\b")
STREET_SUFFIXES = (
    "aly", "allee", "ave", "avenue", "blvd", "boulevard", "cir", "circle",
    "court", "ct", "dr", "drive", "hwy", "highway", "lane", "ln", "parkway",
    "pkwy", "pl", "place", "rd", "road", "st", "streat", "street", "ter",
    "terrace", "way",
)
STREET_SUFFIX_SPLIT_RE = re.compile(
    r"(?i)^(.*\b(?:"
    + "|".join(STREET_SUFFIXES)
    + r")\.?)(?:\s*,\s*|\s+)(.+)$"
)
ODYSSEY_FILE_DATE_RE = re.compile(r"(?i)^Odyssey-JobOutput-([A-Za-z]+ \d{1,2}, \d{4})")
_apt_backfill_attempted = False
ENABLE_APT_BACKFILL_ON_SEARCH = os.environ.get("ENABLE_APT_BACKFILL_ON_SEARCH", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

def geocode_address(address):
    if not address:
        return None, None

    url = "https://atlas.microsoft.com/search/address/json"
    key = os.getenv("AZURE_MAPS_KEY")

    if not key:
        print("ERROR: AZURE_MAPS_KEY is missing")
        return None, None

    params = {
        "api-version": "1.0",
        "subscription-key": key,
        "query": address
    }

    try:
        r = requests.get(url, params=params, timeout=5)
        print("GEOCODE STATUS:", r.status_code)

        if r.status_code != 200:
            print("GEOCODE ERROR:", r.text[:300])
            return None, None

        data = r.json()

        if data.get("results"):
            pos = data["results"][0]["position"]
            return pos["lon"], pos["lat"]

        print("NO RESULTS FOR:", address)

    except Exception as e:
        print("GEOCODE EXCEPTION:", str(e))

    return None, None


def split_address_and_apt(address):
    if address is None:
        return None, None
    text = str(address).strip()
    if not text:
        return None, None

    match = APT_SPLIT_RE.search(text)
    if match:
        apt_value = match.group(1).strip().lstrip("#")
        cleaned = (text[:match.start()] + " " + text[match.end():]).strip()
        cleaned = re.sub(r"\s+,", ",", cleaned)
        cleaned = re.sub(r",\s*,", ", ", cleaned)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" ,")
        return cleaned or None, (apt_value if apt_value else None)

    suffix_match = STREET_SUFFIX_SPLIT_RE.match(text)
    if not suffix_match:
        return text, None

    street = re.sub(r"\s{2,}", " ", suffix_match.group(1)).strip(" ,")
    trailing = suffix_match.group(2).strip(" ,")
    return (street or None), (trailing or None)


def backfill_landlord_tenant_apt(conn):
    cur = conn.cursor()
    cur.timeout = 5
    cur.execute("""
        SELECT TOP 2000 record_id, address, apt
        FROM search.records
        WHERE LOWER(LTRIM(RTRIM(department))) = 'field services department'
        AND address IS NOT NULL
        AND (
            apt IS NULL
            OR LTRIM(RTRIM(CAST(apt AS NVARCHAR(100)))) = ''
        )
        ORDER BY record_id DESC
    """)

    for record_id, address, existing_apt in cur.fetchall():
        normalized_address, parsed_apt = split_address_and_apt(address)
        clean_existing_apt = (str(existing_apt).strip() if existing_apt is not None else "")
        existing_match = APT_SPLIT_RE.search(clean_existing_apt)
        if existing_match:
            clean_existing_apt = existing_match.group(1).strip().lstrip("#")
        clean_existing_apt = clean_existing_apt or None
        apt_to_store = clean_existing_apt or parsed_apt
        if normalized_address != address or apt_to_store != existing_apt:
            cur.execute(
                "UPDATE search.records SET address = ?, apt = ? WHERE record_id = ?",
                normalized_address,
                apt_to_store,
                record_id,
            )


def get_current_permission() -> str:
    permission = (session.get("permission") or "").strip().lower()
    if permission:
        return permission

    user_id = session.get("user_id")
    if not user_id:
        return ""

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT permission FROM search.users WHERE user_id = ?", user_id)
        row = cur.fetchone()
    finally:
        conn.close()

    permission = (row[0] if row and row[0] else "").strip().lower()
    session["permission"] = permission
    return permission


def can_edit_records() -> bool:
    return get_current_permission() in {"admin", "edit"}


def can_delete_records() -> bool:
    return get_current_permission() == "admin"


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return '''
        <form method="post">
            Email: <input name="email"><br>
            Password: <input name="password" type="password"><br>
            <button type="submit">Login</button>
        </form>
        '''

    email = request.form["email"]
    password = request.form["password"]

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_id, password_hash, must_change_password, permission
        FROM search.users
        WHERE email = ? AND is_active = 1
    """, email)

    row = cur.fetchone()

    if not row:
        return "Invalid login", 401

    user_id, pw, must_change, permission = row

    if password != pw:
        return "Invalid login", 401
    session.permanent = True
    session["user_id"] = user_id
    session["permission"] = (permission or "").strip().lower()

    if must_change:
        return redirect("/change-password")

    return redirect("/")



def require_login():
    if "user_id" not in session:
        return redirect("/login")


def extract_odyssey_date_label(blob_name: str) -> str:
    match = ODYSSEY_FILE_DATE_RE.match(blob_name or "")
    return match.group(1).strip() if match else ""


def get_latest_landlord_tenant_file_date_label() -> str:
    if not CONNECTION_STRING:
        return ""

    container = ContainerClient.from_connection_string(CONNECTION_STRING, "fscsv")
    newest_blob = None
    newest_date = None
    for blob in container.list_blobs():
        name = blob.name
        if not name.startswith("Odyssey-JobOutput-"):
            continue
        if not name.lower().endswith("_with_apt_unit.csv"):
            continue

        label = extract_odyssey_date_label(name)
        parsed_date = None
        if label:
            try:
                parsed_date = datetime.strptime(label, "%B %d, %Y").date()
            except ValueError:
                parsed_date = None
        if parsed_date is None and blob.last_modified:
            parsed_date = blob.last_modified.date()
            label = parsed_date.strftime("%B %d, %Y")

        if parsed_date is None:
            continue

        if newest_date is None or parsed_date > newest_date:
            newest_date = parsed_date
            newest_blob = (name, label)

    return newest_blob[1] if newest_blob else ""


def ensure_dv_pdf_storage():
    os.makedirs(DV_PDF_UPLOAD_DIR, exist_ok=True)
    if not os.path.exists(DV_PDF_CSV_PATH):
        with open(DV_PDF_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["case_number", "respondent_name", "issue_date", "type", "pdf_download", "uploaded_at"],
            )
            writer.writeheader()


def read_dv_pdf_records():
    ensure_dv_pdf_storage()
    rows = []
    with open(DV_PDF_CSV_PATH, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def append_dv_pdf_record(record):
    ensure_dv_pdf_storage()
    with open(DV_PDF_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["case_number", "respondent_name", "issue_date", "type", "pdf_download", "uploaded_at"],
        )
        writer.writerow(record)


def get_pdf_reader_class():
    if importlib.util.find_spec("pypdf") is not None:
        return __import__("pypdf").PdfReader, "pypdf"
    if importlib.util.find_spec("PyPDF2") is not None:
        return __import__("PyPDF2").PdfReader, "PyPDF2"
    raise RuntimeError(
        "Missing dependency: install `pypdf` or `PyPDF2` before parsing DV PDFs."
    )


def extract_text_with_ocr(pdf_path):
    if importlib.util.find_spec("pdf2image") is None or importlib.util.find_spec("pytesseract") is None:
        raise RuntimeError(
            "OCR dependencies missing: install `pdf2image` and `pytesseract`."
        )

    convert_from_path = __import__("pdf2image", fromlist=["convert_from_path"]).convert_from_path
    pytesseract = __import__("pytesseract")

    try:
        images = convert_from_path(pdf_path)
    except Exception as exc:
        raise RuntimeError(
            "Unable to render PDF pages for OCR. Ensure Poppler is installed and in PATH."
        ) from exc

    page_texts = []
    for img in images:
        page_texts.append(pytesseract.image_to_string(img) or "")
    return page_texts


def extract_dv_pdf_data(pdf_path):
    PdfReader, _reader_name = get_pdf_reader_class()
    reader = PdfReader(pdf_path)
    page_text = []
    for page in reader.pages:
        extracted = page.extract_text() or ""
        if extracted.strip():
            page_text.append(extracted)
            continue

        page_text.append("")

    full_text = "\n".join(page_text)
    if not full_text.strip():
        page_text = extract_text_with_ocr(pdf_path)
        full_text = "\n".join(page_text)
    if not full_text.strip():
        raise RuntimeError("No extractable text found after OCR.")
    page1_text = page_text[0] if page_text else ""
    page5_text = page_text[4] if len(page_text) >= 5 else full_text

    case_match = re.search(r"Case No\.\s*([A-Z0-9-]+)", full_text, flags=re.IGNORECASE)
    case_number = case_match.group(1).strip().upper() if case_match else ""

    respondent_match = re.search(r"RESPONDENT\s+([A-Z][A-Z\s.'-]+)", page1_text)
    respondent_name = ""
    if respondent_match:
        respondent_name = respondent_match.group(1).split("\n")[0].strip()

    type_patterns = [
        r"(TEMPORARY PROTECTIVE ORDER)",
        r"(INTERIM PROTECTIVE ORDER)",
        r"(FINAL PROTECTIVE ORDER)",
    ]
    order_type = ""
    for pattern in type_patterns:
        match = re.search(pattern, page1_text, flags=re.IGNORECASE)
        if match:
            order_type = match.group(1).upper().strip()
            break
    if not order_type:
        match = re.search(r"CERTIFICATION OF\s+([A-Z ]+ORDER)", page5_text, flags=re.IGNORECASE)
        if match:
            order_type = re.sub(r"\s+", " ", match.group(1)).upper().strip()

    issue_match = re.search(r"Date:\s*(\d{2}/\d{2}/\d{4})", page5_text, flags=re.IGNORECASE)
    issue_date = issue_match.group(1) if issue_match else ""

    return {
        "case_number": case_number,
        "respondent_name": respondent_name,
        "issue_date": issue_date,
        "type": order_type,
    }


def filter_dv_pdf_records(records, filters):
    query = (filters.get("query") or "").strip().lower()
    case_number = (filters.get("case_number") or "").strip().lower()
    filtered = []
    for row in records:
        row_case = (row.get("case_number") or "").lower()
        row_name = (row.get("respondent_name") or "").lower()
        if case_number and case_number not in row_case:
            continue
        if query and query not in row_name:
            continue
        filtered.append(row)
    return filtered


@app.route("/")
def home():
    if "user_id" not in session:
        return redirect("/login")

    return render_template(
        "index.html",
        user_permission=get_current_permission(),
        latest_lt_file_date=get_latest_landlord_tenant_file_date_label(),
    )
@app.route("/change-password", methods=["GET","POST"])
def change_password():
    if "user_id" not in session:
        return redirect("/login")

    if request.method == "GET":
        return '''
        <form method="post">
            New password: <input name="pw1" type="password"><br>
            Confirm: <input name="pw2" type="password"><br>
            <button type="submit">Set Password</button>
        </form>
        '''

    pw1 = request.form["pw1"]
    pw2 = request.form["pw2"]

    if pw1 != pw2:
        return "Passwords do not match"

    conn = get_conn()
    cur = conn.cursor()

   

  

    cur.execute("""
        UPDATE search.users
        SET password_hash = ?, must_change_password = 0
        WHERE user_id = ?
    """, pw1, session["user_id"])

    conn.commit()

    return redirect("/")

# ============================================================
# NORMALIZATION HELPERS
# ============================================================

def date_only(series):
    return pd.to_datetime(series, errors="coerce").dt.date.astype(str).replace("NaT", "")

def normalize_col(col: str) -> str:
    col = col.lower().strip()
    return re.sub(r"[^a-z0-9 ]", "", col)


def detect_encoding(blob_bytes: bytes) -> str:
    result = chardet.detect(blob_bytes)
    return result.get("encoding") or "utf-8"


def clean_str(s):
    if s is None:
        return ""
    s = str(s).lower().strip()
    for p in string.punctuation:
        s = s.replace(p, "")
    s = re.sub(r"\s+", "", s)
    return s
#test
 
def fuzzy_match(a, b, threshold=0.75):
    if not a or not b:
        return False
    return SequenceMatcher(None, a, b).ratio() >= threshold


# ============================================================
# COLUMN MAP
# ============================================================

COLUMN_MAP = {
    "name": [
        "civil respondent",
        "tenant defendant or respondent name",
        "respondent name",
        "defendant name",
        "tenant name",
        "name",
    ],
    "address": [
        "tenant defendant or respondent address",
        "address addressaddress",
        "respondent address",
        "address",
        "street address",
    ],
    "case number": [
        "case number",
        "casenumber",
        "case_number",
    ],
    "court document type": [
        "court document type",
        "document type",
        "doctype",
        "doc type",
    ],
    "hearing date": [
        "hearing date",
        "hearingdate",
        "court issued date",
        "trial date",
        "court date",
        "arrival"
    ],
    "intake date": [
        "intake date",
        "intakedate",
        "intake_date",
        "entry date",
        "filed date",
        "date",
    ],
    "current disposition": [
        "current disposition",
        "adminstrative status",
        "administrative status",
        
        
        "civil process service disposition",
        "eviction disposition",
    ],
    "order type": [
        "order type",
        "ordertype",
        "court document type",
    ],
    "order status": [
        "order status",
        "orderstatus",
        "civil process service disposition",
    ],
}

# Flexible column resolver
def get_col(subdf: pd.DataFrame, logical_key: str):
    candidates = COLUMN_MAP.get(logical_key, [])
    cols = list(subdf.columns)

    # exact match
    for cand in candidates:
        if cand in cols:
            return cand

    # relaxed match
    for cand in candidates:
        for col in cols:
            if cand in col or col in cand:
                return col

    return None


# ============================================================
# FLASK + AZURE SETUP
# ============================================================




TABLE_DEFINITIONS = {
    "Warrants to Audit": {
        "department": "Active Warrants",
        "fields": [
            "full_name", "case_number", "issue_date", "date_of_birth",
            "sex", "race", "issuing_county", "disposition"
        ]
    },
    "Baltimore Jail Population": {
        "department": "Baltimore Jail Population",
        "fields": ["sid", "full_name", "date_of_birth", "facility"]
    },
    "BCSO Active Warrants": {
        "department": "BCSO_ACTIVE_WARRANTS",
        "fields": [
            "issuing_county", "case_number", "warrant_id_number", "warrant_type",
            "issue_date", "warrant_status", "full_name", "sid", "date_of_birth",
            "race", "sex", "address", "notes"
        ]
    },
    "Warrant of Restitution - MDEC": {
        "department": "WARRANT OF RESTITUTION - MDEC",
        "fields": ["full_name", "case_number", "address", "court_document_type", "intake_date", "disposition"]
    },
    "Field Services Department": {
        "department": "Field Services Department",
        "fields": ["full_name", "case_number", "address", "apt", "intake_date", "disposition", "notes"]
    }
}

ALL_EDITABLE_COLUMNS = {
    "department", "source_file", "first_name", "last_name", "full_name", "date_of_birth", "sid",
    "case_number", "warrant_id_number", "warrant_type", "warrant_status", "issue_date", "intake_date",
    "address", "apt", "city", "state", "postal_code", "court_document_type", "disposition", "notes",
    "sex", "race", "issuing_county", "facility"
}

DATE_FIELDS = {"date_of_birth", "issue_date", "intake_date"}

REQUIRED_FIELDS_BY_TABLE = {
    "BCSO Active Warrants": {"case_number", "warrant_type", "issue_date", "full_name", "warrant_status"}
}

EDITABLE_DEPARTMENTS = {
    "active warrants",
    "bcso_active_warrants",
    "field services department",
    "field services department - civil intake",
    "field services department - civil survey",
    "field services department - warrants",
}

CORS(app)

CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
LATEST_LT_WITH_APT_BLOB_NAME = "latest_landlord_tenant_with_apt.csv"
EXPORT_CONTAINER_NAME = os.environ.get("EXPORT_CONTAINER_NAME", "fscsv")
EXPORTS_BLOB_PREFIX = os.environ.get("EXPORTS_BLOB_PREFIX", "exports")
DV_PDF_UPLOAD_DIR = os.path.join("static", "uploads", "dv_pdf")
DV_PDF_CSV_PATH = os.path.join("static", "uploads", "dv_pdf_records.csv")
ALLOWED_DV_PDF_EXTENSIONS = {".pdf"}


# Your actual containers:
CONTAINERS = {
    "dvcsv": "domestic violence department",
    "fscsv": "mixed",  # civil + warrants
    "warrantscsv": "warrants",
}
"""
# ============================================================
# LOAD ALL DATA FROM AZURE
# ============================================================

def load_all_data():
    all_dfs = []
   

    for container_name, dept_type in CONTAINERS.items():
        container = ContainerClient.from_connection_string(
            CONNECTION_STRING, container_name
        )

        for blob in container.list_blobs():
            
            
            name = blob.name.lower()
            

            if not name.endswith(".csv"):
                continue

            blob_bytes = container.download_blob(blob.name).readall()
            enc = detect_encoding(blob_bytes)

            df = pd.read_csv(
                pd.io.common.BytesIO(blob_bytes),
                encoding=enc,
                low_memory=False
            )

            df.columns = [normalize_col(c) for c in df.columns]
             
            
           
            
            # Department detection
            blob_name_lower = blob.name.lower()
            

            if "dv" in blob_name_lower:
                dept = "domestic violence department"
            elif "civil" in blob_name_lower and "intake" in blob_name_lower:
                dept = "field services department - civil intake"
            elif "civil" in blob_name_lower and "intake" not in blob_name_lower:
                dept = "field services department - civil survey"
            # SPECIAL WARRANTS (new section)
            elif "warrants_1.csv" in blob_name_lower:
                dept = "warrants"
            elif "warrant" in blob_name_lower and "intake" not in blob_name_lower:
                dept = "field services department - warrants"

            elif "warrant" in blob_name_lower or "rest" in blob_name_lower:
                dept = "field services department - warrants"

            
            #debug to see which dept the csvs are attached to
            print("ASSIGNED DEPT:", dept)   

            df["department"] = dept
            all_dfs.append(df)
            print("ASSIGNING:", blob.name, "FROM CONTAINER:", container_name, "→", dept)

    if not all_dfs:
        return pd.DataFrame()

    out = pd.concat(all_dfs, ignore_index=True)
    out["department"] = out["department"].astype(str).str.lower().str.strip()
    

    return out


df = load_all_data()
"""

# ============================================================
# DEPARTMENT FIELD BUILDERS
# ============================================================

def build_name(subdf, dept_norm):
    cols = list(subdf.columns)
    

    if dept_norm == "field services department - civil intake":
        priority = [
            "tenant defendant or respondent name",
            "civil respondent",
            
            "respondent name",
            
        ]

    elif dept_norm == "field services department - civil survey":
        priority = [
            "civil respondent",
            "respondent name",
            "tenant defendant or respondent name"
        ]
        
        

    elif dept_norm == "domestic violence department":
        priority = ["respondent name", "name"]

    elif dept_norm == "field services department - warrants":
        priority = [
            "tenant defendant or respondent name",
            "respondent name",
            "name",
        ]

    else:
        priority = ["name"]

    for p in priority:
        if p in cols:
            return subdf[p].astype(str)

    col = get_col(subdf, "name")
    return subdf[col].astype(str) if col else pd.Series([""] * len(subdf))


def build_address(subdf, dept_norm):
    cols = list(subdf.columns)

    if dept_norm == "field services department - civil intake":
        if "tenant defendant or respondent address" in cols:
            return subdf["tenant defendant or respondent address"].astype(str)
        if "address" in cols:
            return subdf["address"].astype(str)
    elif dept_norm == "field services department - civil survey":
        priority = [
            "address",]
        for cand in priority:
            if cand in cols:
                return subdf[cand].astype(str)
        return ""


    elif dept_norm == "domestic violence department":
        if "address addressaddress" in cols:
            return subdf["address addressaddress"].astype(str)
        if "respondent address" in cols:
            return subdf["respondent address"].astype(str)
        if "address" in cols:
            return subdf["address"].astype(str)
        
    elif dept_norm == "warrants":
        if "address" in cols:
            return subdf["address"].astype(str)
        return pd.Series([""] * len(subdf))


    col = get_col(subdf, "address")
    if col:
        return subdf[col].astype(str)

    if {"address", "city", "subregion"}.issubset(cols):
        return (
            subdf["address"].fillna("") + ", " +
            subdf["city"].fillna("") + ", " +
            subdf["subregion"].fillna("")
        ).astype(str)

    return pd.Series([""] * len(subdf))



def build_disposition(subdf, dept_norm):
    cols = list(subdf.columns)

    # --- CIVIL INTAKE ---
    if dept_norm == "field services department - civil intake":
        priority = [
            "administrative status",
            "current disposition",
        ]

    # --- CIVIL SURVEY ---
    elif dept_norm == "field services department - civil survey":
        priority = [
            "civil process service disposition",
        ]

    # --- WARRANTS ---
    elif dept_norm == "field services department - warrants":
        priority = [
            "adminstrative status",   # misspelled column in actual warrants CSV
        ]

    # --- DV (Domestic Violence) ---
    elif dept_norm == "domestic violence department":
        priority = [
            "order status",
        ]

    # --- DEFAULT (fallback) ---
    else:
        priority = [
            "current disposition",
        ]

    # Select the first matching column
    for cand in priority:
        if cand in cols:
            return subdf[cand]

    return pd.Series([""] * len(subdf), index=subdf.index)

# ============================================================
# TRANSFORM RAW DF → FRONTEND STRUCTURE
# ============================================================

def enforce_department_columns(df):
    out = {}

    for dept, subdf in df.groupby("department", dropna=False):
        dept_norm = dept.lower().strip()
        sub = subdf.copy()

        name_series = build_name(sub, dept_norm)
        addr_series = build_address(sub, dept_norm)

        case_col = get_col(sub, "case number")
        case_series = sub[case_col].astype(str) if case_col else ""
        

        intake_col = get_col(sub, "intake date")
        intake_series = date_only(sub[intake_col]) if intake_col else ""

        court_col = get_col(sub, "court document type")
        court_series = sub[court_col].astype(str) if court_col else ""
        #trying out new thing
        #disp_col = get_col(sub, "current disposition")
        #disp_series = sub[disp_col].astype(str) if disp_col else ""
        disp_series = build_disposition(sub, dept_norm).astype(str)

        if dept_norm == "domestic violence department":
            order_type_col = get_col(sub, "order type")
            order_type_series = sub[order_type_col].astype(str) if order_type_col else ""

            hearing_col = get_col(sub, "hearing date")
            hearing_series = date_only(sub[hearing_col]) if hearing_col else ""

            order_status_col = get_col(sub, "order status")
            order_status_series = (
                sub[order_status_col].astype(str) if order_status_col else ""
            )

            clean = pd.DataFrame({
                "Name": name_series,
                "Case Number": case_series,
                "Address": addr_series,
                "Order Type": order_type_series,
                "Hearing Date": hearing_series,
                "Order Status": order_status_series,
            })
        elif dept_norm == "warrants":
            
            first_col = get_col(sub, "first name")
            last_col = get_col(sub, "last name")

            if first_col and last_col:
                name_series = (
                    sub[first_col].fillna("").astype(str) + " " +
                    sub[last_col].fillna("").astype(str)
                ).str.strip()
            else:
                name_series = build_name(sub, dept_norm)

            sid_series = sub["sid"].astype(str) if "sid" in sub.columns else ""

            warrant_type_series = sub["warrant type"].astype(str) if "warrant type" in sub.columns else ""
            issue_date_series = date_only(sub["issue date"]) if "issue date" in sub.columns else ""
            warrant_status_series = sub["warrant status"].astype(str) if "warrant status" in sub.columns else ""

            

            clean = pd.DataFrame({
                "Name": name_series,
                "SID": sid_series,
                "Case Number": case_series,
                "Address": addr_series,
                "Warrant Type": warrant_type_series,
                "Issue Date": issue_date_series,
                "Warrant Status": warrant_status_series,
            })

        else:
            clean = pd.DataFrame({
                "Name": name_series,
                "Case Number": case_series,
                "Address": addr_series,
                "Court Document Type": court_series,
                "Intake Date": intake_series,
                "Current Disposition": disp_series,
            })
        

        clean = clean.fillna("")

        out[dept.title()] = clean.to_dict(orient="records")

    return out


"""
PROCESSED = enforce_department_columns(df)
print("\nCIVIL RECORDS:")
for rec in PROCESSED.get("Field Services Department - Civil Intake", []):
    print(rec)
    break  # print just first row
"""


@app.route("/table_definitions")
def table_definitions():
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify(TABLE_DEFINITIONS)


@app.route("/downloads/latest-landlord-tenant-with-apt.csv")
def download_latest_landlord_tenant_with_apt():
    if "user_id" not in session:
        return redirect("/login")

    container = ContainerClient.from_connection_string(CONNECTION_STRING, "fscsv")
    blob = container.get_blob_client(LATEST_LT_WITH_APT_BLOB_NAME)
    if not blob.exists():
        return jsonify({"error": "Latest landlord/tenant file is not ready yet."}), 404

    data = blob.download_blob().readall()
    return send_file(
        pd.io.common.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=LATEST_LT_WITH_APT_BLOB_NAME,
    )


@app.route("/dv-pdf/upload", methods=["POST"])
def upload_dv_pdf():
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    uploaded = request.files.get("pdf_file")
    if not uploaded or not uploaded.filename:
        return jsonify({"error": "Missing PDF file"}), 400

    ext = os.path.splitext(uploaded.filename)[1].lower()
    if ext not in ALLOWED_DV_PDF_EXTENSIONS:
        return jsonify({"error": "Only PDF files are supported"}), 400

    ensure_dv_pdf_storage()
    safe_name = secure_filename(uploaded.filename)
    stamped_name = f"{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}_{safe_name}"
    target_path = os.path.join(DV_PDF_UPLOAD_DIR, stamped_name)
    uploaded.save(target_path)

    try:
        extracted = extract_dv_pdf_data(target_path)
    except RuntimeError as exc:
        os.remove(target_path)
        return jsonify({"error": str(exc)}), 500
    if not extracted.get("case_number"):
        os.remove(target_path)
        return jsonify({"error": "Unable to parse this PDF format. Case number not found."}), 422

    record = {
        "case_number": extracted.get("case_number", ""),
        "respondent_name": extracted.get("respondent_name", ""),
        "issue_date": extracted.get("issue_date", ""),
        "type": extracted.get("type", ""),
        "pdf_download": f"/static/uploads/dv_pdf/{stamped_name}",
        "uploaded_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
    }
    append_dv_pdf_record(record)
    return jsonify({"status": "success", "record": record})


@app.route("/downloads/dv-pdf.csv")
def download_dv_pdf_csv():
    if "user_id" not in session:
        return redirect("/login")
    ensure_dv_pdf_storage()
    return send_file(
        DV_PDF_CSV_PATH,
        mimetype="text/csv",
        as_attachment=True,
        download_name="dv_pdf_records.csv",
    )


@app.route("/records", methods=["POST"])
def create_record():
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401
    if not can_edit_records():
        return jsonify({"error": "You do not have permission to add records"}), 403

    payload = request.get_json(silent=True) or {}
    table_name = (payload.get("table") or "").strip()
    table_info = TABLE_DEFINITIONS.get(table_name)
    if not table_info:
        return jsonify({"error": "Invalid table selection"}), 400

    fields = payload.get("fields") or {}
    required_fields = REQUIRED_FIELDS_BY_TABLE.get(table_name, set())
    missing_fields = [field for field in required_fields if not str(fields.get(field, "")).strip()]
    if missing_fields:
        return jsonify({"error": f"Missing required fields: {', '.join(sorted(missing_fields))}"}), 400

    insert_data = {"department": table_info["department"], "source_file": "manual_entry"}

    for column in table_info["fields"]:
        if column not in ALL_EDITABLE_COLUMNS:
            continue
        value = fields.get(column)
        if value is None:
            continue
        clean_value = str(value).strip()
        if clean_value == "":
            continue
        if column in DATE_FIELDS:
            parsed = pd.to_datetime(clean_value, errors="coerce")
            clean_value = None if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")
            if clean_value is None:
                continue
        insert_data[column] = clean_value

    if "full_name" not in insert_data:
        first = insert_data.get("first_name", "")
        last = insert_data.get("last_name", "")
        combined = f"{first} {last}".strip()
        if combined:
            insert_data["full_name"] = combined

    columns = list(insert_data.keys())
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"""
        INSERT INTO search.records ({", ".join(columns)})
        OUTPUT INSERTED.record_id
        VALUES ({placeholders})
    """

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(insert_data[c] for c in columns))
        record_id = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    return jsonify({"status": "success", "record_id": record_id})


@app.route("/records/<int:record_id>", methods=["PATCH"])
def update_record(record_id):
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401
    if not can_edit_records():
        return jsonify({"error": "You do not have permission to edit records"}), 403

    payload = request.get_json(silent=True) or {}
    updates = payload.get("fields") or {}

    set_parts = []
    values = []

    for column, value in updates.items():
        if column not in ALL_EDITABLE_COLUMNS:
            continue

        clean_value = ("" if value is None else str(value)).strip()
        if clean_value == "":
            db_value = None
        elif column in DATE_FIELDS:
            parsed = pd.to_datetime(clean_value, errors="coerce")
            db_value = None if pd.isna(parsed) else parsed.strftime("%Y-%m-%d")
        else:
            db_value = clean_value

        set_parts.append(f"{column} = ?")
        values.append(db_value)

    if not set_parts:
        return jsonify({"error": "No valid fields provided"}), 400

    values.append(record_id)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT department FROM search.records WHERE record_id = ?", record_id)
        existing = cur.fetchone()
        if not existing:
            return jsonify({"error": "Record not found"}), 404
        if str(existing[0] or "").strip().lower() not in EDITABLE_DEPARTMENTS:
            return jsonify({"error": "Editing is not allowed for this department"}), 403

        cur.execute(
            f"UPDATE search.records SET {', '.join(set_parts)} WHERE record_id = ?",
            tuple(values)
        )
        conn.commit()
    finally:
        conn.close()

    return jsonify({"status": "success"})


@app.route("/records/<int:record_id>", methods=["DELETE"])
def delete_record(record_id):
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401
    if not can_delete_records():
        return jsonify({"error": "Only admins can delete records"}), 403

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT department FROM search.records WHERE record_id = ?", record_id)
        existing = cur.fetchone()
        if not existing:
            return jsonify({"error": "Record not found"}), 404
        if str(existing[0] or "").strip().lower() not in EDITABLE_DEPARTMENTS:
            return jsonify({"error": "Deleting is not allowed for this department"}), 403

        cur.execute("DELETE FROM search.records WHERE record_id = ?", record_id)
        conn.commit()
    finally:
        conn.close()

    return jsonify({"status": "success"})

@app.route("/run-active-warrants", methods=["POST"])
def run_active_warrants():
    
    from ingest import ingest_bcso_active_warrants_csv
    ingest_bcso_active_warrants_csv()
    return "OK"

@app.route("/run-warrant-of-restitution", methods=["POST"])
def run_warrant_of_restitution():
    return {"OK"}


from ingest import ingest_wor
@app.route("/ingest-wor", methods=["POST"])
def ingest_wor_route():
    ingest_wor()
    return {"status": "success"}


def parse_search_filters(source):
    query = source.get("name", "").strip()
    case_number = source.get("case_number", "").strip()
    date_start = None
    date_end = None

    intake_date = source.get("intake_date", "").strip()
    if " to " in intake_date:
        parts = intake_date.split(" to ")
        if len(parts) == 2:
            date_start = parts[0].strip()
            date_end = parts[1].strip()

    last_x_days = source.get("last_x_days", "").strip()
    sex = source.get("sex", "").strip()
    race = source.get("race", "").strip()
    issuing_county = source.get("issuing_county", "").strip()
    sid = source.get("sid", "").strip()
    dob = source.get("dob", "").strip()

    return {
        "query": query,
        "case_number": case_number or None,
        "date_start": date_start,
        "date_end": date_end,
        "last_x_days": last_x_days or None,
        "sex": sex or None,
        "race": race or None,
        "issuing_county": issuing_county or None,
        "sid": sid or None,
        "dob": dob or None,
    }


def ensure_exports_table(conn):
    cur = conn.cursor()
    cur.execute("""
        IF OBJECT_ID('search.exports', 'U') IS NULL
        BEGIN
            CREATE TABLE search.exports (
                token NVARCHAR(100) NOT NULL PRIMARY KEY,
                url NVARCHAR(2000) NULL,
                status NVARCHAR(50) NOT NULL,
                error NVARCHAR(MAX) NULL,
                created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
        END
    """)
    conn.commit()


def _sanitize_column_name(name, used):
    safe = re.sub(r"[^0-9a-zA-Z_]+", "_", str(name)).strip("_").lower()
    if not safe:
        safe = "raw"
    candidate = safe
    idx = 2
    while candidate in used:
        candidate = f"{safe}_{idx}"
        idx += 1
    used.add(candidate)
    return candidate


def _iter_export_rows(cursor, filters):
    select_sql = """
        r.record_id,
        r.full_name,
        r.sid,
        r.date_of_birth,
        r.facility,
        r.case_number,
        r.address,
        r.apt,
        r.x,
        r.y,
        r.city,
        r.state,
        r.postal_code,
        r.notes,
        r.court_document_type AS case_type,
        r.intake_date,
        COALESCE(r.issue_date, r.intake_date) AS record_date,
        r.warrant_status,
        r.disposition,
        r.warrant_id_number,
        r.sex,
        r.race,
        r.issuing_county,
        r.department,
        r.source_file,
        r.created_at,
        rr.raw_payload
    """
    from_sql = """
        search.records r
        LEFT JOIN search.raw_records rr ON rr.record_id = r.record_id
    """
    sql, params = build_search_sql(
        select_sql=select_sql,
        from_sql=from_sql,
        name_query=filters["query"],
        case_number=filters["case_number"],
        dob=filters["dob"],
        sex=filters["sex"],
        race=filters["race"],
        date_start=filters["date_start"],
        date_end=filters["date_end"],
        issuing_county=filters["issuing_county"],
        last_x_days=filters["last_x_days"],
        sid=filters["sid"],
        extra_where=["LOWER(LTRIM(RTRIM(r.department))) = 'field services department'"],
    )
    cursor.execute(sql, params)

    columns = [col[0] for col in cursor.description]
    while True:
        batch = cursor.fetchmany(500)
        if not batch:
            break
        for row in batch:
            mapped = dict(zip(columns, row))
            raw_payload = mapped.pop("raw_payload", None)
            flattened = {}
            if raw_payload:
                if isinstance(raw_payload, (bytes, bytearray)):
                    raw_payload = raw_payload.decode("utf-8", errors="ignore")
                if isinstance(raw_payload, str):
                    try:
                        raw_payload = json.loads(raw_payload)
                    except Exception:
                        raw_payload = {}
                if isinstance(raw_payload, dict):
                    for k, v in raw_payload.items():
                        flattened[f"raw_{k}"] = v
            yield mapped, flattened


def run_export_csv_job(token, filters):
    conn = get_conn()
    tmp_path = None
    try:
        ensure_exports_table(conn)
        cur = conn.cursor()
        cur.execute("UPDATE search.exports SET status = 'processing', updated_at = SYSUTCDATETIME() WHERE token = ?", token)
        conn.commit()

        base_headers = [
            "record_id", "full_name", "case_number",
            "address", "apt", "city", "state", "postal_code", "notes", "case_type", "intake_date",
            "record_date", "Event Type", "x", "y"
        ]

        headers = base_headers

        with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name
            writer = csv.DictWriter(tmp, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()

            write_cur = conn.cursor()
            for base_row, _ in _iter_export_rows(write_cur, filters):
                out = dict(base_row)
                out["Event Type"] = out.pop("disposition", "")
                writer.writerow(out)

        container = ContainerClient.from_connection_string(CONNECTION_STRING, EXPORT_CONTAINER_NAME)
        blob_name = f"{EXPORTS_BLOB_PREFIX}/landlord_tenant_export_{token}.csv"
        blob_client = container.get_blob_client(blob_name)
        with open(tmp_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        blob_url = f"/export-download?token={token}"
        cur.execute(
            "UPDATE search.exports SET status = 'ready', url = ?, updated_at = SYSUTCDATETIME() WHERE token = ?",
            blob_url, token
        )
        conn.commit()
    except Exception as exc:
        cur = conn.cursor()
        cur.execute(
            "UPDATE search.exports SET status = 'failed', error = ?, updated_at = SYSUTCDATETIME() WHERE token = ?",
            str(exc), token
        )
        conn.commit()
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        conn.close()


@app.route("/export-csv", methods=["POST"])
def export_csv():
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    filters = parse_search_filters(payload)

    conn = get_conn()
    try:
        ensure_exports_table(conn)
        token = uuid.uuid4().hex
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO search.exports (token, status) VALUES (?, 'started')",
            token
        )
        conn.commit()
    finally:
        conn.close()

    thread = threading.Thread(target=run_export_csv_job, args=(token, filters), daemon=True)
    thread.start()

    return jsonify({"status": "started", "token": token})


@app.route("/export-status")
def export_status():
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    token = request.args.get("token", "").strip()
    if not token:
        return jsonify({"error": "token is required"}), 400

    conn = get_conn()
    try:
        ensure_exports_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT status, url, error FROM search.exports WHERE token = ?", token)
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"error": "token not found"}), 404

    status, url, error = row
    if status == "ready":
        return jsonify({"status": "ready", "url": url})
    if status == "failed":
        return jsonify({"status": "failed", "error": error})
    return jsonify({"status": "processing"})


@app.route("/export-download")
def export_download():
    if "user_id" not in session:
        return jsonify({"error": "Unauthorized"}), 401

    token = request.args.get("token", "").strip()
    if not token:
        return jsonify({"error": "token is required"}), 400

    conn = get_conn()
    try:
        ensure_exports_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT status FROM search.exports WHERE token = ?", token)
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"error": "token not found"}), 404
    if row[0] != "ready":
        return jsonify({"error": "export not ready"}), 409

    container = ContainerClient.from_connection_string(CONNECTION_STRING, EXPORT_CONTAINER_NAME)
    blob_name = f"{EXPORTS_BLOB_PREFIX}/landlord_tenant_export_{token}.csv"
    blob_client = container.get_blob_client(blob_name)
    if not blob_client.exists():
        return jsonify({"error": "export file not found"}), 404

    data = blob_client.download_blob().readall()
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"landlord_tenant_export_{token}.csv",
    )


# ============================================================
# SEARCH ENDPOINT
# ============================================================



@app.route("/search_all")
def search_all():
    global _apt_backfill_attempted
    filters = parse_search_filters(request.args)

    conn = get_conn()
    try:
        if ENABLE_APT_BACKFILL_ON_SEARCH and not _apt_backfill_attempted:
            try:
                backfill_landlord_tenant_apt(conn)
                conn.commit()
            except Exception as exc:
                print(f"WARN apt backfill skipped due to error: {exc}")
            finally:
                _apt_backfill_attempted = True
        records = search_by_name(
            conn,
            filters["query"],
            date_start=filters["date_start"],
            date_end=filters["date_end"],
            case_number=filters["case_number"],
            sid=filters["sid"],
            dob=filters["dob"],
            sex=filters["sex"],
            race=filters["race"],
            issuing_county=filters["issuing_county"],
            last_x_days=filters["last_x_days"],
            limit=None
        )
    finally:
        conn.close()
    
    grouped = {}
    for r in records:
        dept = r["department"].title()
        grouped.setdefault(dept, []).append(r)

    response = {}
    for dept, rows in grouped.items():
        response[dept] = {
            "count": len(rows),
            "records": rows,
        }

    dv_records = filter_dv_pdf_records(read_dv_pdf_records(), filters)
    if dv_records:
        response["DV PDF"] = {
            "count": len(dv_records),
            "records": dv_records,
        }

    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)
