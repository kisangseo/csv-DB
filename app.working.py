from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
import chardet
import re

app = Flask(__name__)
CORS(app)

# --- Azure Blob Configuration ---
AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=bcsobcf;"
    "AccountKey=jivFSuIpTOXv30ruihnQB6iE5/p8z2Z0KUihqSsjlYNHjIouD7eIB93bogR9u3t0aGkcqv94EalX+AStQg/yMQ==;"
    "EndpointSuffix=core.windows.net"
)

# --- Containers per department ---
CONTAINERS = {
    "Domestic Violence Department": "dvcsv",
    "Warrants Department": "csv",
    "Field Services Department": "fscsv"
}


def merge_address_fields(df):
    parts = []
    for col in ["address", "city", "subregion"]:
        if col in df.columns:
            parts.append(df[col].astype(str))
    if parts:
        merged = pd.Series([" ".join(p).strip() for p in zip(*parts)])
        merged = merged.apply(lambda x: re.sub(r"\s{2,}", " ", x))
        merged = merged.apply(lambda x: ", ".join(filter(None, x.split())))
        return merged
    return pd.Series([""] * len(df))


def load_all_csvs():
    blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    dept_frames = []

    for dept_name, container_name in CONTAINERS.items():
        print(f"📂 Loading container: {container_name}")
        try:
            container_client = blob_service.get_container_client(container_name)
        except Exception as e:
            print(f"❌ Could not connect to container {container_name}: {e}")
            continue

        container_dfs = []
        for blob in container_client.list_blobs():
            if not blob.name.endswith(".csv"):
                continue
            try:
                blob_client = container_client.get_blob_client(blob)
                raw_bytes = blob_client.download_blob().readall()
                enc = chardet.detect(raw_bytes).get("encoding", "utf-8")
                data = raw_bytes.decode(enc, errors="replace")
                df = pd.read_csv(StringIO(data), sep=None, engine="python")

                df["department"] = dept_name.lower()
                df.columns = [
                    re.sub(r"[^a-z0-9 ]", "", c.lower()).strip()
                    for c in df.columns
                ]

                print(f"📑 Columns in {blob.name}: {df.columns.tolist()}")
                container_dfs.append(df)
            except Exception as e:
                print(f"❌ Error reading {blob.name}: {e}")

        if container_dfs:
            dept_df = pd.concat(container_dfs, ignore_index=True)
            dept_frames.append(dept_df)
            print(f"✅ Loaded {len(dept_df)} rows for {dept_name}")

    if not dept_frames:
        print("⚠️ No CSV files loaded from any container.")
        return pd.DataFrame()

    combined = pd.concat(dept_frames, ignore_index=True)
    print("✅ Departments loaded:", combined["department"].unique())
    return combined


def enforce_department_columns(df):
    REQUIRED_COLUMNS = [
        "case number",
        "name",
        "address",
        "court document type",
        "hearing date",
        "current disposition",
    ]

    PRETTY_NAMES = {
        "case number": "Case Number",
        "name": "Name",
        "address": "Address",
        "court document type": "Court Document Type",
        "hearing date": "Hearing Date",
        "current disposition": "Current Disposition",
    }

    filtered = {}

    for dept, subdf in df.groupby("department", dropna=False):
        dept_title = dept.title()
        subdf.columns = [re.sub(r"[^a-z0-9 ]", "", c.lower().strip()) for c in subdf.columns]
        print(f"🔎 Columns detected for {dept_title}: {list(subdf.columns)}")

        # --- Name detection ---
        if dept == "domestic violence department":
            if "respondent name" in subdf.columns:
                subdf["name"] = subdf["respondent name"]
                print(f"✅ Using 'respondent name' for Domestic Violence Department")
            else:
                subdf["name"] = ""
                print(f"⚠️ No respondent name found for DV Department")
        elif "tenant defendant or respondent name" in subdf.columns:
            subdf["name"] = subdf["tenant defendant or respondent name"]
            print(f"✅ Using 'tenant defendant or respondent name' for {dept_title}")
        elif "first name" in subdf.columns and "last name" in subdf.columns:
            subdf["name"] = subdf["first name"].fillna("") + " " + subdf["last name"].fillna("")
            print(f"✅ Using 'first name' + 'last name' for {dept_title}")
        else:
            name_candidates = [c for c in subdf.columns if "name" in c]
            if name_candidates:
                subdf["name"] = subdf[name_candidates[0]]
                print(f"⚙️ Using name column '{name_candidates[0]}' for {dept_title}")
            else:
                subdf["name"] = ""
                print(f"⚠️ No name column found for {dept_title}")

        # --- Address detection ---
        if "tenant defendant or respondent address" in subdf.columns:
            subdf["address"] = subdf["tenant defendant or respondent address"]
        elif {"address", "city", "subregion"}.issubset(subdf.columns):
            subdf["address"] = (
                subdf["address"].fillna("")
                + ", "
                + subdf["city"].fillna("")
                + ", "
                + subdf["subregion"].fillna("")
            ).str.replace(r"\s+", " ", regex=True).str.strip()
        elif "address" in subdf.columns:
            subdf["address"] = subdf["address"]
        else:
            subdf["address"] = merge_address_fields(subdf)

        # --- Hearing Date / Court Issued Date handling ---
        if dept == "field services department":
            if "court issued date" in subdf.columns:
                subdf["hearing date"] = subdf["court issued date"]
                print(f"✅ Using 'court issued date' as 'hearing date' for Field Services Department")
            else:
                subdf["hearing date"] = ""
        elif "hearing date" not in subdf.columns:
            subdf["hearing date"] = ""

        # --- Current disposition / admin status ---
        if "current disposition" not in subdf.columns and "administrative status" in subdf.columns:
            subdf["current disposition"] = subdf["administrative status"]
        elif "current disposition" not in subdf.columns:
            subdf["current disposition"] = ""

        # --- Ensure all required columns exist ---
        for col in REQUIRED_COLUMNS:
            if col not in subdf.columns:
                subdf[col] = ""

        clean = subdf[REQUIRED_COLUMNS].fillna("")
        clean.rename(columns=PRETTY_NAMES, inplace=True)
        filtered[dept_title] = clean.to_dict(orient="records")

        print(f"✅ Prepared {len(clean)} rows for {dept_title}")
        print(clean.head(3))

    return filtered


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search_all", methods=["GET"])
def search_all():
    df = load_all_csvs()
    if df.empty:
        return jsonify({})

    name = request.args.get("name", "").lower().strip()
    results = pd.DataFrame()

    for col in df.columns:
        if "name" in col.lower():
            matched = df[df[col].astype(str).str.lower().str.contains(name, na=False)]
            if not matched.empty:
                print(f"🔍 Match in column: {col}")
                results = pd.concat([results, matched])

    if results.empty:
        print("⚠️ No matches found")
        return jsonify({})

    results = results.fillna("").drop_duplicates().reset_index(drop=True)
    grouped = enforce_department_columns(results)

    for dept in grouped:
        grouped[dept] = grouped[dept][:200]

    print("✅ Returning:", list(grouped.keys()))
    return jsonify(grouped)


if __name__ == "__main__":
    app.run(debug=True)
