from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
from azure.storage.blob import ContainerClient
import chardet
import re
import os

app = Flask(__name__)
CORS(app)

# --- Load once at startup (from Azure Blob Storage) ---
from io import StringIO
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)



def read_blob(container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    data = blob_client.download_blob().readall().decode('utf-8')
    return pd.read_csv(StringIO(data))

# ✅ Load CSVs once from your Azure containers
dv_df = read_blob("dvcsv", "DV Sample.csv")
civil_df = read_blob("fscsv", "Civil_Intake_Data(survey).csv")
warrants_df = read_blob("csv", "sample_warrants.csv")

# Optional: Label for clarity
dv_df["department"] = "domestic violence department"
civil_df["department"] = "field services department - civil intake"
warrants_df["department"] = "field services department - warrants"


# --- Azure Storage ---
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
CONTAINERS = ["csv", "dvcsv", "fscsv"]

# --- Detect encoding of blobs ---
def detect_encoding(blob_bytes):
    result = chardet.detect(blob_bytes)
    return result["encoding"] or "utf-8"
#load csv from blob
def load_all_data():
    all_dfs = []
    for container_name in CONTAINERS:
        container_client = ContainerClient.from_connection_string(CONNECTION_STRING, container_name)
        
        for blob in container_client.list_blobs():
            if blob.name.lower().endswith(".csv"):
                blob_data = container_client.download_blob(blob.name).readall()
                encoding = detect_encoding(blob_data)
                df = pd.read_csv(pd.io.common.BytesIO(blob_data), encoding=encoding, low_memory=False)
                df.columns = [c.strip().lower() for c in df.columns]

                # ✅ Determine department by file name (smarter than container-only)
                blob_name_lower = blob.name.lower()
                if "dv" in blob_name_lower:
                    df["department"] = "domestic violence department"
                elif "civil" in blob_name_lower:
                    df["department"] = "field services department - civil intake"
                elif "warrant" in blob_name_lower:
                    df["department"] = "field services department - warrants"
                else:
                    # fallback by container if unknown
                    if "dv" in container_name.lower():
                        df["department"] = "domestic violence department"
                    elif "fs" in container_name.lower():
                        df["department"] = "field services department"
                    else:
                        df["department"] = "warrants department"

                all_dfs.append(df)
                

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


# Load all data at startup
df = load_all_data()
df.columns = df.columns.str.strip().str.lower()

def enforce_department_columns(df):
    import re

    filtered = {}

    for dept, subdf in df.groupby("department", dropna=False):
        # normalize department name
        dept = str(dept).lower().strip()

        # 🔧 Ensure all subdf columns are normalized and copied from df
        original_cols = df.columns
        subdf.columns = [re.sub(r"[^a-z0-9 ]", "", c.lower().strip()) for c in subdf.columns]

        # 🩵 Restore Intake Date early (before trimming or display filtering)
        if "intake date" not in subdf.columns or subdf["intake date"].isna().all():
            # Prefer columns already present in this department slice
            for col in subdf.columns:
                if "intake" in col.lower() and "date" in col.lower():
                    subdf["intake date"] = subdf[col]
                    print(f"🩵 Restored '{col}' -> 'intake date' for {dept} (from subdf)")
                    break
            else:
                # Fallback: pull from the original df, but align rows to this subdf
                for col in df.columns:
                    if "intake" in col.lower() and "date" in col.lower():
                        subdf["intake date"] = df.loc[subdf.index, col]
                        print(f"🩵 Restored '{col}' -> 'intake date' for {dept} (from df, aligned)")
                        break

        

        # --- COMMON NAME + ADDRESS HANDLING ---
        if "respondent name" in subdf.columns:
            subdf["name"] = subdf["respondent name"]
        elif "tenant defendant or respondent name" in subdf.columns:
            subdf["name"] = subdf["tenant defendant or respondent name"]
        elif {"first name", "last name"}.issubset(subdf.columns):
            subdf["name"] = subdf["first name"].fillna("") + " " + subdf["last name"].fillna("")
        else:
            subdf["name"] = ""

        if "tenant defendant or respondent address" in subdf.columns:
            subdf["address"] = subdf["tenant defendant or respondent address"]
        elif {"address", "city", "subregion"}.issubset(subdf.columns):
            subdf["address"] = (
                subdf["address"].fillna("") + ", " +
                subdf["city"].fillna("") + ", " +
                subdf["subregion"].fillna("")
            ).str.replace(r"\s+", " ", regex=True).str.strip()
        else:
            subdf["address"] = ""

        # --- DEPARTMENT SPECIFIC HANDLING ---
        # -------------------------------------------------------------
        # 1️⃣ Domestic Violence Department
        # -------------------------------------------------------------
        if dept == "domestic violence department":
           

            # Fuzzy match for Order Type / Status
            order_type_col = next((c for c in subdf.columns if "ordertype" in c.replace(" ", "")), None)
            order_status_col = next((c for c in subdf.columns if "orderstatus" in c.replace(" ", "")), None)

            subdf["order type"] = subdf[order_type_col].astype(str) if order_type_col else ""
            subdf["order status"] = subdf[order_status_col].astype(str) if order_status_col else ""

            if "hearing date" not in subdf.columns and "hearingdate" in subdf.columns:
                subdf["hearing date"] = subdf["hearingdate"]
            if "case number" not in subdf.columns and "casenumber" in subdf.columns:
                subdf["case number"] = subdf["casenumber"]

            PRETTY_NAMES = {
                "name": "Name",
                "case number": "Case Number",
                "address": "Address",
                "order type": "Order Type",
                "hearing date": "Hearing Date",
                "order status": "Order Status",
            }
            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())

        # -------------------------------------------------------------
        # 2️⃣ Field Services Department
        # -------------------------------------------------------------
        elif dept == "field services department":
           
            # --- Name handling ---
            if "tenant defendant or respondent name" in subdf.columns:
                subdf["name"] = subdf["tenant defendant or respondent name"]
            elif "tenant defendant or respondent" in subdf.columns:
                subdf["name"] = subdf["tenant defendant or respondent"]
            elif "respondent name" in subdf.columns:
                subdf["name"] = subdf["respondent name"]
            elif {"first name", "last name"}.issubset(subdf.columns):
                subdf["name"] = subdf["first name"].fillna("") + " " + subdf["last name"].fillna("")
            else:
                subdf["name"] = ""

            # Normalize key fields
            if "court document type" in subdf.columns:
                subdf["court document type"] = subdf["court document type"].astype(str)
            else:
                subdf["court document type"] = ""

            if "court issued date" in subdf.columns:
                subdf["hearing date"] = subdf["court issued date"].astype(str)
            elif "trial date" in subdf.columns:
                subdf["hearing date"] = subdf["trial date"].astype(str)
            else:
                subdf["hearing date"] = ""

            # --- Current Disposition (handle all FS variants safely) ---
            # Normalize possible disposition columns
            disp_col = None
                
            for c in subdf.columns:
                c_clean = c.lower().replace(" ", "")
                if "adminstrative" in c_clean or "administrative" in c_clean:
                    disp_col = c
                    break

            # Use the first found column
            if disp_col:
                subdf["current disposition"] = subdf[disp_col].astype(str)
            else:
                subdf["current disposition"] = ""

            # ✅ Fix NaN merging issue and prefer non-empty value if duplicates exist
            if "adminstrative status" in subdf.columns and "administrative status" in subdf.columns:
                subdf["current disposition"] = (
                    subdf["adminstrative status"].fillna(subdf["administrative status"])
                )

            # ✅ Final fill and clean-up
            subdf["current disposition"] = subdf["current disposition"].fillna("").astype(str)
            

            PRETTY_NAMES = {
                "name": "Name",
                "case number": "Case Number",
                "address": "Address",
                "court document type": "Court Document Type",
                "intake date": "Intake Date",
                "current disposition": "Current Disposition",
            }
            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())
        # -------------------------------------------------------------
        # 2️⃣a Field Services Department - Civil Intake
        # -------------------------------------------------------------
        elif dept.lower() == "field services department - civil intake":
            

            # --- Name ---
            if "tenant, defendant, or respondent name" in subdf.columns:
                subdf["name"] = subdf["tenant, defendant, or respondent name"]
            elif "tenant defendant or respondent name" in subdf.columns:
                subdf["name"] = subdf["tenant defendant or respondent name"]
            else:
                subdf["name"] = ""

            # --- Address ---
            if "tenant, defendant or respondent address" in subdf.columns:
                subdf["address"] = subdf["tenant, defendant or respondent address"]
            elif "tenant defendant or respondent address" in subdf.columns:
                subdf["address"] = subdf["tenant defendant or respondent address"]
            else:
                subdf["address"] = ""

            # --- Normalize key fields ---
            subdf["court document type"] = subdf.get("court document type", "")

            print("\n🧩 DEBUG: ORIGINAL DF COLUMNS for", dept)
            for col in df.columns:
                print("   →", repr(col))
            
        


            # 🪪 Debug check
            print("✅ Intake Date final sample for", dept, ":", subdf["intake date"].dropna().head(5).tolist())



            subdf = subdf.copy()

            if "administrative status" in subdf.columns and subdf["administrative status"].notna().any():
                subdf["current disposition"] = subdf["administrative status"]

            PRETTY_NAMES = {
                "name": "Name",
                "case number": "Case Number",
                "address": "Address",
                "court document type": "Court Document Type",
                "intake date": "Intake Date",
                "current disposition": "Current Disposition",
            }
            print("✅ Intake Date sample (pre-clean):", subdf.get("intake date", pd.Series(dtype=object)).dropna().head(5).tolist())

            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())

        # -------------------------------------------------------------
        # 2️⃣b Field Services Department - Warrants
        # -------------------------------------------------------------
        elif dept == "field services department - warrants":
                   

            # --- Name ---
            if "tenant defendant or respondent name" in subdf.columns:
                subdf["name"] = subdf["tenant defendant or respondent name"]
            elif {"first name", "last name"}.issubset(subdf.columns):
                subdf["name"] = subdf["first name"].fillna("") + " " + subdf["last name"].fillna("")
            else:
                subdf["name"] = ""

            # --- Address ---
            if "tenant defendant or respondent address" in subdf.columns:
                subdf["address"] = subdf["tenant defendant or respondent address"]
            else:
                subdf["address"] = ""

            # --- Court doc type ---
            subdf["court document type"] = subdf.get("court document type", "")

           

             # --- Disposition (misspelled + correct spellings) ---
            disp_col = None
            for c in subdf.columns:
                c_clean = c.lower().replace(" ", "").replace("_", "")
                if "adminstrative" in c_clean or "administrative" in c_clean:
                    disp_col = c
                    break
            if disp_col:
                subdf["current disposition"] = subdf[disp_col].astype(str)
            else:
                subdf["current disposition"] = ""

            # Merge misspelled + correct versions if both exist
            if "adminstrative status" in subdf.columns and "administrative status" in subdf.columns:
                subdf["current disposition"] = (
                    subdf["adminstrative status"]
                    .fillna(subdf["administrative status"])
                    .astype(str)
                )

            # Final clean-up: replace NaN strings + fill
            subdf["current disposition"] = (
                subdf["current disposition"]
                .replace("nan", "")
                .fillna("")
            )

            PRETTY_NAMES = {
                "name": "Name",
                "case number": "Case Number",
                "address": "Address",
                "court document type": "Court Document Type",
                "intake date": "Intake Date",
                "current disposition": "Current Disposition",
            }
            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())

        # -------------------------------------------------------------
        # 3️⃣ Default (Warrants / others)
        # -------------------------------------------------------------
        else:
            PRETTY_NAMES = {
                "name": "Name",
                "case number": "Case Number",
                "address": "Address",
                "court document type": "Court Document Type",
                "hearing date": "Hearing Date",
                "current disposition": "Current Disposition",
            }
            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())
            for col in DISPLAY_COLUMNS:
                if col not in subdf.columns:
                    subdf[col] = ""

        # --- FINALIZE CLEAN OUTPUT ---
        for col in DISPLAY_COLUMNS:
            if col not in subdf.columns:
                subdf[col] = ""
        clean = subdf[DISPLAY_COLUMNS].fillna("")
        clean.rename(columns=PRETTY_NAMES, inplace=True)
        filtered[dept.title()] = clean.to_dict(orient="records")

    return filtered



@app.route("/")
def home():
    return render_template("index.html")


# --- Main Search Route ---
@app.route("/search_all")
def search_all():
    if df.empty:
        return jsonify({"error": "No data loaded"}), 500

    params = {k: request.args.get(k, "").strip().lower() for k in
          ["name", "dob", "race", "sex", "address", "case_number", "parcel_id", "intake_date"]}

    all_filtered = {}
    grouped_data = enforce_department_columns(df)

    for dept, records in grouped_data.items():
        
        

        dept_df = pd.DataFrame(records)
        if dept_df.empty:
            continue

        # ✅ Make a lowercase copy ONLY for filtering
        search_df = dept_df.copy()
        search_df.columns = [re.sub(r"[^a-z0-9 ]", "", c.lower().strip()) for c in search_df.columns]

        # --- Apply AND filtering logic on search_df ---
        for field, value in params.items():
            # ❗ Skip text filtering for intake_date — let the datetime logic handle it
            if field == "intake_date":
                continue
            value = (value or "").strip()  # ensure no spaces or None
            if not value:
                continue


            value_clean = re.sub(r"\s+", "", value)
            possible_cols = [c for c in search_df.columns if field.replace("_", " ") in c.lower()]
            if not possible_cols:
                continue

            col = possible_cols[0]
            search_df = search_df[
                search_df[col]
                .astype(str)
                .str.lower()
                .str.replace(r"\s+", "", regex=True)
                .str.contains(value_clean, na=False)
            ]
        # --- DATE RANGE FILTER using intake_date (Flatpickr "start to end") ---
        raw = request.args.get("intake_date", "").strip()
        print("BACKEND RECEIVED DATE:", repr(raw))   # ← ADD THIS

        if raw:
            raw = raw.replace("  ", " ").strip()

            # Detect ranges safely
            if "to" in raw:
                parts = [p.strip() for p in raw.split("to")]

                # CASE 1: full valid range
                if len(parts) == 2 and parts[0] and parts[1]:
                    start_str = parts[0]
                    end_str   = parts[1]

                # CASE 2: "2025-10-20 to " (single date)
                elif len(parts) >= 1 and parts[0]:
                    start_str = end_str = parts[0]

                else:
                    start_str = end_str = None
            else:
                # CASE 3: "2025-10-20" (single click)
                start_str = end_str = raw

            if start_str and end_str:
                start = pd.to_datetime(start_str, errors="coerce").normalize()
                end   = pd.to_datetime(end_str, errors="coerce").normalize()

                if pd.notna(start) and pd.notna(end):

                    dept_lower = dept.lower()

                    if dept_lower == "domestic violence department":
                        date_col = "hearing date"
                    elif dept_lower == "field services department - civil intake":
                        date_col = "intake date"
                    elif dept_lower == "field services department - warrants":
                        date_col = "intake date"
                    else:
                        date_col = None

                    if date_col and date_col in search_df.columns:
                        cleaned = (
                            search_df[date_col]
                            .astype(str)
                            .str.replace(",", "", regex=False)
                            .str.strip()
                        )

                        parsed = pd.to_datetime(cleaned, errors="coerce").dt.normalize()

                        mask = (parsed >= start) & (parsed <= end)
                        search_df = search_df[mask.fillna(False)]
        

        

        

                    

        # ✅ Map filtered lowercase results back to the original, pretty-cased data
        if not search_df.empty:
            # sync dept_df with search_df's index AFTER filtering
            dept_filtered = dept_df.loc[search_df.index].copy()
            filtered_rows = dept_filtered
            limited = filtered_rows.head(200)
            all_filtered[dept] = {
                "count": len(limited),
                "records": limited.to_dict(orient="records")
            }

    return jsonify(all_filtered)



if __name__ == "__main__":
    app.run(debug=True)
