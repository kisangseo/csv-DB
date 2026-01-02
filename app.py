from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import os
import re
import string
from difflib import SequenceMatcher
import pandas as pd
import chardet
from azure.storage.blob import ContainerClient
from db_connect import get_conn
from search_sql import search_by_name

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

app = Flask(__name__)
CORS(app)

CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

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


# ============================================================
# SEARCH ENDPOINT
# ============================================================

@app.route("/")
def home():
    return render_template("index.html")


# LEGACY CSV SEARCH (parked, not deleted)
def legacy_search_all():
    if df.empty:
        return jsonify({"error": "No data loaded"}), 500

    params = {
        k: request.args.get(k, "").strip().lower()
        for k in ["name", "address", "case_number", "intake_date"]
    }

    all_filtered = {}

    for dept, records in PROCESSED.items():
        dept_df = pd.DataFrame(records)
        search_df = dept_df.copy()

        search_df.columns = [normalize_col(c) for c in search_df.columns]

       

        # TEXT FILTERS
        for key, val in params.items():
            if key == "intake_date":
                continue
            if not val:
                continue

            user_tokens = [clean_str(t) for t in val.split() if t.strip()]

            possible_cols = [
                c for c in search_df.columns if key.replace("_", " ") in c
            ]

            if not possible_cols:
                continue

            col = possible_cols[0]

            def matches(cell):
                if cell is None:
                    return False

                raw = str(cell).lower()
                for p in string.punctuation:
                    raw = raw.replace(p, " ")

                cell_tokens = [clean_str(t) for t in raw.split() if t.strip()]

                for ut in user_tokens:
                    if not any(ut in ct or fuzzy_match(ct, ut) for ct in cell_tokens):
                        return False
                return True

            search_df = search_df[search_df[col].apply(matches)]

        # DATE FILTERING
        dr = params["intake_date"]
        if dr:
            raw = dr.replace("  ", " ").strip()

            if "to" in raw:
                parts = [p.strip() for p in raw.split("to")]
                if len(parts) == 2 and parts[0] and parts[1]:
                    start_str, end_str = parts
                else:
                    start_str = end_str = parts[0]
            else:
                start_str = end_str = raw

            start = pd.to_datetime(start_str, errors="coerce")
            end = pd.to_datetime(end_str, errors="coerce")

            dept_lower = dept.lower()

            if dept_lower == "domestic violence department":
                date_col = "hearing date"
            else:
                date_col = "intake date"

            if normalize_col(date_col) in search_df.columns:
                clean = (
                    search_df[normalize_col(date_col)]
                    .astype(str)
                    .str.replace(",", "")
                    .str.strip()
                )
                parsed = pd.to_datetime(clean, errors="coerce")
                mask = (parsed >= start) & (parsed <= end)
                search_df = search_df[mask.fillna(False)]

        if not search_df.empty:
            filtered = dept_df.loc[search_df.index]
            limited = filtered.head(200)

            all_filtered[dept] = {
                "count": len(limited),
                "records": limited.to_dict(orient="records"),
            }

    return jsonify(all_filtered)




@app.route("/search_all")
def search_all():
    query = request.args.get("name", "").strip()
    case_number = request.args.get("case_number", "").strip()
    date_start = None
    date_end = None

    intake_date = request.args.get("intake_date", "").strip()
    if " to " in intake_date:
        parts = intake_date.split(" to ")
        if len(parts) == 2:
            date_start = parts[0].strip()
            date_end = parts[1].strip()
    last_x_days = request.args.get("last_x_days", "").strip()
    sex = request.args.get("sex", "").strip()
    race = request.args.get("race", "").strip()
    race = race if race else None
    issuing_county = request.args.get("issuing_county", "").strip()
    sid = request.args.get("sid", "").strip()
    dob = request.args.get("dob", "").strip()
    print("DEBUG dob:", dob)

    
    conn = get_conn()
    try:
        records = search_by_name(
            conn,
            query,
            date_start=date_start,
            date_end=date_end,
            case_number=case_number or None,
            sid=sid or None,
            dob=dob or None,
            sex=sex or None,
            race=race,
            issuing_county=issuing_county or None,
            last_x_days=last_x_days or None,
            limit=200
        )
    finally:
        conn.close()
    
    sex = sex if sex in ("M", "F") else None

    
    

    grouped = {}
    for r in records:
        dept = r["department"].title()
        grouped.setdefault(dept, []).append(r)
    MAX_PER_DEPT = 200
    for dept in grouped:
        grouped[dept] = grouped[dept][:MAX_PER_DEPT]

    response = {}
    for dept, rows in grouped.items():
        response[dept] = {
            "count": len(rows),
            "records": rows,
        }
    
   
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)