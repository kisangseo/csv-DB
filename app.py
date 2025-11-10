from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
from azure.storage.blob import ContainerClient
import chardet
import re
import os

app = Flask(__name__)
CORS(app)

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
        print(f"📂 Loading container: {container_name}")
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
                print(f"✅ Loaded {len(df)} rows for {df['department'].iloc[0].title()}")

    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()


# Load all data at startup
df = load_all_data()

def enforce_department_columns(df):
    import re

    filtered = {}

    for dept, subdf in df.groupby("department", dropna=False):
        dept = dept.lower().strip()
        subdf.columns = [re.sub(r"[^a-z0-9 ]", "", c.lower().strip()) for c in subdf.columns]

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
            print(f"🔧 Debug DV columns: {list(subdf.columns)}")

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
                "hearing date": "Intake Date",
                "order status": "Order Status",
            }
            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())

        # -------------------------------------------------------------
        # 2️⃣ Field Services Department
        # -------------------------------------------------------------
        elif dept == "field services department":
            print(f"🔧 Debug FS columns: {list(subdf.columns)}")
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
            disp_cols = [
                c for c in subdf.columns
                if "administrative status" in c or "adminstrative status" in c or "service disposition" in c
            ]

            # Use the first found column
            if disp_cols:
                subdf["current disposition"] = subdf[disp_cols[0]].astype(str)
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
                "hearing date": "Intake Date",
                "current disposition": "Current Disposition",
            }
            DISPLAY_COLUMNS = list(PRETTY_NAMES.keys())
        # -------------------------------------------------------------
        # 2️⃣a Field Services Department - Civil Intake
        # -------------------------------------------------------------
        elif dept == "field services department - civil intake":
            print(f"🔧 Debug Civil Intake columns: {list(subdf.columns)}")

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
            subdf["intake date"] = subdf.get("intake date", "")
            subdf["current disposition"] = subdf.get("administrative status", "")

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
        # 2️⃣b Field Services Department - Warrants
        # -------------------------------------------------------------
        elif dept == "field services department - warrants":
            print(f"🔧 Debug Warrants columns: {list(subdf.columns)}")

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

            # --- Dates ---
            if "court issued date" in subdf.columns:
                subdf["hearing date"] = subdf["court issued date"].astype(str)
            elif "trial date" in subdf.columns:
                subdf["hearing date"] = subdf["trial date"].astype(str)
            else:
                subdf["hearing date"] = ""

            # --- Disposition ---
            disp_cols = [
                c for c in subdf.columns
                if "administrative status" in c or "adminstrative status" in c or "service disposition" in c
            ]
            if disp_cols:
                subdf["current disposition"] = subdf[disp_cols[0]].astype(str)
            else:
                subdf["current disposition"] = ""

            PRETTY_NAMES = {
                "name": "Name",
                "case number": "Case Number",
                "address": "Address",
                "court document type": "Court Document Type",
                "hearing date": "Hearing Date",
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
        # 🚫 Skip Warrants Department if user searched by Intake Date
        intake_date = request.args.get("intake_date", "").strip()
        if intake_date and "warrants" in dept.lower():
            continue

        dept_df = pd.DataFrame(records)
        if dept_df.empty:
            continue

        # ✅ Make a lowercase copy ONLY for filtering
        search_df = dept_df.copy()
        search_df.columns = [re.sub(r"[^a-z0-9 ]", "", c.lower().strip()) for c in search_df.columns]

        # --- Apply AND filtering logic on search_df ---
        for field, value in params.items():
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
        print(f"🧩 Dept: {dept} | Rows before any date filtering: {len(search_df)}")
        print(search_df.head(3))
        # ✅ Handle intake_date search across departments (DV + Civil)
        intake_date = request.args.get("intake_date", "").strip()
        if intake_date:
            try:
                input_date = pd.to_datetime(intake_date).date()

                # possible column names that represent intake/order dates
                possible_date_cols = [
                    "intake date",
                    "hearing date",
                    "date order was issued",
                    "court issued date",
                    "date and time of reissue"
                ]

                # find which column exists in the current department
                found_col = next(
                    (
                        c for c in search_df.columns
                        if any(k.replace(" ", "") in c.replace(" ", "") for k in possible_date_cols)
                    ),
                    None
                )

                if found_col:
                    # Clean and normalize the date column
                    search_df["parsed_intake"] = (
                        pd.to_datetime(
                            search_df[found_col],
                            errors="coerce",
                            infer_datetime_format=True,
                            yearfirst=False
                        )
                        .apply(
                            lambda d: d + pd.offsets.DateOffset(years=100)
                            if pd.notnull(d) and d.year < 1970
                            else d
                        )
                        .dt.floor("D")
                    )

                    before = len(search_df)
                    search_df = search_df[search_df["parsed_intake"].dt.date == input_date]
                    after = len(search_df)

                    print(
                        f"📅 Using column '{found_col}' | Searching for {input_date} | "
                        f"Before filter: {before} | After filter: {after}"
                    )
                else:
                    print(
                        f"⚠️ No matching date column found in department '{dept}' | "
                        f"Columns: {list(search_df.columns)}"
                    )

            except Exception as e:
                print("⚠️ Date filter error:", e)
                pass

        # ✅ Map filtered lowercase results back to the original, pretty-cased data
        if not search_df.empty:
            filtered_rows = dept_df.loc[search_df.index]
            limited = filtered_rows.head(200)
            all_filtered[dept] = {
                "count": len(limited),
                "records": limited.to_dict(orient="records")
            }

    return jsonify(all_filtered)



if __name__ == "__main__":
    app.run(debug=True)
