"""One-time import of Cognito Returns XLSX exports and their matching PDF ZIPs.

The source workbooks and ZIPs are intentionally supplied at runtime and are not
stored in this repository.
"""

from __future__ import annotations

import argparse
from collections import defaultdict, deque
import io
import os
import re
import sys
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from returns import is_hard_copy_return, payload_from_export_row, upsert_return


CONTAINER_NAME = os.getenv("CIVIL_PAPERS_CONTAINER_NAME", "civilpapers").strip() or "civilpapers"
PREFIX = os.getenv("CIVIL_RETURN_FILES_PREFIX", "return_pdfs").strip().strip("/") or "return_pdfs"
STANDARD_SOURCE_FILES = {
    "Served": (
        "BaltimoreCitySheriffsOfficeReturn2 served.xlsx",
        "BaltimoreCitySheriffsOfficeReturn Served.zip",
    ),
    "Non Est": (
        "BaltimoreCitySheriffsOfficeReturn2 non est.xlsx",
        "BaltimoreCitySheriffsOfficeReturn2 non est.zip",
    ),
}
HARD_COPY_SOURCE_FILES = (
    "BaltimoreCitySheriffsOfficeReturn2 hard copy.xlsx",
    "BaltimoreCitySheriffsOfficeReturn2 hard copy.zip",
)


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


def workbook_disposition(xlsx_path):
    frame = pd.read_excel(xlsx_path, dtype=object)
    if "Service Disp" not in frame.columns:
        return None
    values = {
        str(value).strip().lower()
        for value in frame["Service Disp"].dropna()
        if str(value).strip()
    }
    if values == {"served"}:
        return "Served"
    if values == {"non est"}:
        return "Non Est"
    return None


def pair_matches(xlsx_path, zip_path):
    frame = pd.read_excel(xlsx_path, dtype=object)
    with ZipFile(zip_path) as archive:
        pdf_infos = [item for item in archive.infolist() if not item.is_dir()]
    if "Document" not in frame.columns:
        return False
    row_cases = {normalized_case(document) for document in frame["Document"]}
    pdf_cases = {normalized_case(case_from_pdf_filename(item.filename)) for item in pdf_infos}
    row_cases.discard("")
    pdf_cases.discard("")
    return bool(row_cases & pdf_cases)


def discover_sources(source_dir):
    source_dir = Path(source_dir)
    standard_sources = [
        (source_dir / xlsx_name, source_dir / zip_name, disposition)
        for disposition, (xlsx_name, zip_name) in STANDARD_SOURCE_FILES.items()
    ]
    hard_copy_source = (
        source_dir / HARD_COPY_SOURCE_FILES[0],
        source_dir / HARD_COPY_SOURCE_FILES[1],
        None,
    )
    if all(xlsx_path.is_file() and zip_path.is_file() for xlsx_path, zip_path, _ in standard_sources):
        if hard_copy_source[0].is_file() and hard_copy_source[1].is_file():
            standard_sources.append(hard_copy_source)
        return standard_sources

    xlsx_files = sorted(source_dir.glob("*.xlsx"), key=lambda path: path.stat().st_mtime, reverse=True)
    zip_files = sorted(source_dir.glob("*.zip"), key=lambda path: path.stat().st_mtime, reverse=True)
    sources = []
    for expected_disposition in ("Served", "Non Est"):
        workbooks = [path for path in xlsx_files if workbook_disposition(path) == expected_disposition]
        matches = [
            (xlsx_path, zip_path, expected_disposition)
            for xlsx_path in workbooks
            for zip_path in zip_files
            if pair_matches(xlsx_path, zip_path)
        ]
        if not matches:
            raise RuntimeError(
                f"Could not find a matching {expected_disposition} XLSX and ZIP in {source_dir}."
            )
        sources.append(matches[0])
    if hard_copy_source[0].is_file() and hard_copy_source[1].is_file():
        sources.append(hard_copy_source)
    return sources


def load_pairs(xlsx_path, zip_path, expected_disposition):
    frame = pd.read_excel(xlsx_path, dtype=object)
    with ZipFile(zip_path) as archive:
        pdf_infos = [item for item in archive.infolist() if not item.is_dir()]
        pdfs_by_case = defaultdict(deque)
        for pdf_info in pdf_infos:
            if Path(pdf_info.filename).is_absolute() or ".." in Path(pdf_info.filename).parts:
                raise RuntimeError(f"Unsafe ZIP member: {pdf_info.filename}")
            pdfs_by_case[normalized_case(case_from_pdf_filename(pdf_info.filename))].append(pdf_info)

        pairs = []
        skipped_rows = 0
        for index, row in enumerate(frame.to_dict(orient="records"), start=2):
            document = row.get("Document")
            case_key = normalized_case(document)
            if not case_key or not pdfs_by_case[case_key]:
                skipped_rows += 1
                continue
            pdf_info = pdfs_by_case[case_key].popleft()
            disposition_value = row.get("Service Disp")
            disposition = str(disposition_value or "").strip()
            if not disposition and expected_disposition:
                # Cognito exports occasionally leave this cell blank even though
                # the row came from a disposition-specific export/view.
                row["Service Disp"] = expected_disposition
            elif expected_disposition and disposition.lower() != expected_disposition.lower():
                raise RuntimeError(
                    f"Row {index} has disposition {disposition_value!r}; expected {expected_disposition!r}."
                )
            pairs.append((row, pdf_info.filename, archive.read(pdf_info)))
        skipped_pdfs = sum(len(items) for items in pdfs_by_case.values())
        print(
            {
                "source": Path(xlsx_path).name,
                "matched": len(pairs),
                "rows_without_pdf": skipped_rows,
                "pdfs_without_row": skipped_pdfs,
            }
        )
    return pairs


def upload_and_import(container, conn, row, pdf_filename, pdf_bytes):
    from azure.storage.blob import ContentSettings

    payload = payload_from_export_row(row)
    case_number = payload.get("case_number")
    entry_number = payload.get("cognito_entry_number")
    disposition_folder = "served" if payload.get("service_disposition") == "Served" else "non-est"
    if is_hard_copy_return(payload):
        disposition_folder = f"hard-copy/{disposition_folder}"
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
    parser.add_argument("--source-dir")
    parser.add_argument("--served-xlsx")
    parser.add_argument("--served-zip")
    parser.add_argument("--non-est-xlsx")
    parser.add_argument("--non-est-zip")
    parser.add_argument("--hard-copy-xlsx")
    parser.add_argument("--hard-copy-zip")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--replace-all",
        action="store_true",
        help="Delete all current Returns and their activity before importing this source set.",
    )
    args = parser.parse_args()

    if args.source_dir:
        sources = discover_sources(args.source_dir)
    elif args.hard_copy_xlsx or args.hard_copy_zip:
        if not (args.hard_copy_xlsx and args.hard_copy_zip):
            parser.error("Provide both --hard-copy-xlsx and --hard-copy-zip.")
        standard_values = (args.served_xlsx, args.served_zip, args.non_est_xlsx, args.non_est_zip)
        if any(standard_values) and not all(standard_values):
            parser.error("Provide all four --served/--non-est XLSX and ZIP arguments, or none of them.")
        sources = []
        if all(standard_values):
            sources.extend(
                [
                    (args.served_xlsx, args.served_zip, "Served"),
                    (args.non_est_xlsx, args.non_est_zip, "Non Est"),
                ]
            )
        sources.append((args.hard_copy_xlsx, args.hard_copy_zip, None))
    elif all((args.served_xlsx, args.served_zip, args.non_est_xlsx, args.non_est_zip)):
        sources = [
            (args.served_xlsx, args.served_zip, "Served"),
            (args.non_est_xlsx, args.non_est_zip, "Non Est"),
        ]
    else:
        parser.error(
            "Use --source-dir, provide both --hard-copy arguments, or provide all four "
            "explicit --served/--non-est XLSX and ZIP arguments."
        )
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
    deleted = 0
    try:
        if args.replace_all:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM search.Returns")
            deleted = int(cur.fetchone()[0])
            cur.execute("DELETE FROM search.mdec_return_activity_log")
            cur.execute("DELETE FROM search.Returns")
            conn.commit()
            print({"status": "cleared", "returns_deleted": deleted})
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
            "deleted": deleted,
            "pdfs_uploaded": len(all_pairs),
            "container": CONTAINER_NAME,
            "prefix": PREFIX,
        }
    )


if __name__ == "__main__":
    main()
