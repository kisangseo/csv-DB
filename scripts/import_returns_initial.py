"""One-time import of Cognito Returns XLSX exports and their matching PDF ZIPs.

The source workbooks and ZIPs are intentionally supplied at runtime and are not
stored in this repository.
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from returns import payload_from_export_row, upsert_return


CONTAINER_NAME = os.getenv("CIVIL_PAPERS_CONTAINER_NAME", "civilpapers").strip() or "civilpapers"
PREFIX = os.getenv("CIVIL_RETURN_FILES_PREFIX", "return_pdfs").strip().strip("/") or "return_pdfs"


def normalized_case(value):
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def case_from_pdf_filename(filename):
    basename = Path(filename).name
    basename = re.sub(r"(?i)^Baltimore City Sheriff.s Office Return\s*-\s*", "", basename)
    basename = re.sub(r"(?i)\.pdf$", "", basename)
    basename = re.sub(r"\(\d+\)$", "", basename)
    return basename.strip()


def safe_blob_part(value, fallback):
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip()).strip("._")
    return cleaned or fallback


def load_pairs(xlsx_path, zip_path, expected_disposition):
    frame = pd.read_excel(xlsx_path, dtype=object)
    with ZipFile(zip_path) as archive:
        pdf_infos = [item for item in archive.infolist() if not item.is_dir()]
        if len(frame) != len(pdf_infos):
            raise RuntimeError(
                f"{Path(xlsx_path).name} has {len(frame)} rows but {Path(zip_path).name} has {len(pdf_infos)} PDFs."
            )
        pairs = []
        for index, (row, pdf_info) in enumerate(zip(frame.to_dict(orient="records"), pdf_infos), start=2):
            if Path(pdf_info.filename).is_absolute() or ".." in Path(pdf_info.filename).parts:
                raise RuntimeError(f"Unsafe ZIP member: {pdf_info.filename}")
            document = row.get("Document")
            pdf_case = case_from_pdf_filename(pdf_info.filename)
            if normalized_case(document) != normalized_case(pdf_case):
                raise RuntimeError(
                    f"Row {index} case {document!r} does not match PDF {pdf_info.filename!r}."
                )
            disposition_value = row.get("Service Disp")
            disposition = str(disposition_value or "").strip()
            if not disposition:
                # Cognito exports occasionally leave this cell blank even though
                # the row came from a disposition-specific export/view.
                row["Service Disp"] = expected_disposition
            elif disposition.lower() != expected_disposition.lower():
                raise RuntimeError(
                    f"Row {index} has disposition {disposition_value!r}; expected {expected_disposition!r}."
                )
            pairs.append((row, pdf_info.filename, archive.read(pdf_info)))
    return pairs


def upload_and_import(container, conn, row, pdf_filename, pdf_bytes):
    from azure.storage.blob import ContentSettings

    payload = payload_from_export_row(row)
    case_number = payload.get("case_number")
    entry_number = payload.get("cognito_entry_number")
    disposition_folder = "served" if payload.get("service_disposition") == "Served" else "non-est"
    case_folder = safe_blob_part(case_number, "unknown-case")
    entry_folder = safe_blob_part(entry_number, "unknown-entry")
    filename = safe_blob_part(Path(pdf_filename).name, "return.pdf")
    blob_name = f"{PREFIX}/{disposition_folder}/{case_folder}/cognito-{entry_folder}/{filename}"
    container.upload_blob(
        name=blob_name,
        data=io.BytesIO(pdf_bytes),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/pdf"),
        metadata={
            "source": "cognito_initial_export",
            "case_number": str(case_number or "")[:200],
            "cognito_entry_number": str(entry_number or "")[:200],
            "original_filename": Path(pdf_filename).name[:200],
        },
    )
    payload.update(
        {
            "blob_container": CONTAINER_NAME,
            "blob_name": blob_name,
            "original_filename": Path(pdf_filename).name,
            "content_type": "application/pdf",
            "source_payload_json": {key: None if pd.isna(value) else str(value) for key, value in row.items()},
        }
    )
    return upsert_return(conn, payload, actor_email="system:initial-cognito-import")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--served-xlsx", required=True)
    parser.add_argument("--served-zip", required=True)
    parser.add_argument("--non-est-xlsx", required=True)
    parser.add_argument("--non-est-zip", required=True)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    sources = [
        (args.served_xlsx, args.served_zip, "Served"),
        (args.non_est_xlsx, args.non_est_zip, "Non Est"),
    ]
    all_pairs = []
    for xlsx_path, zip_path, disposition in sources:
        all_pairs.extend(load_pairs(xlsx_path, zip_path, disposition))

    if args.validate_only:
        print({"status": "validated", "records": len(all_pairs), "pdfs": len(all_pairs)})
        return

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not configured.")

    from azure.storage.blob import ContainerClient
    from db_connect import get_conn

    container = ContainerClient.from_connection_string(connection_string, CONTAINER_NAME)
    conn = get_conn()
    inserted = 0
    updated = 0
    try:
        for row, pdf_filename, pdf_bytes in all_pairs:
            _, was_inserted = upload_and_import(container, conn, row, pdf_filename, pdf_bytes)
            if was_inserted:
                inserted += 1
            else:
                updated += 1
    finally:
        conn.close()

    print(
        {
            "status": "ok",
            "records": len(all_pairs),
            "inserted": inserted,
            "updated": updated,
            "pdfs_uploaded": len(all_pairs),
            "container": CONTAINER_NAME,
            "prefix": PREFIX,
        }
    )


if __name__ == "__main__":
    main()
