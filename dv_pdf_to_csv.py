"""
Simple one-off DV PDF -> CSV extractor.

For now this is intentionally hard-coded to one PDF file path so you can
quickly validate extraction without wiring the web app.
"""

from __future__ import annotations

import csv
import os
import re
import time
from pathlib import Path
import requests


# ---------------------------------------------------------------------------
# Hard-coded input file. Replace this with your PDF filename/path.
# Example: Path("sample_dv.pdf")
# ---------------------------------------------------------------------------
INPUT_PDF = Path("sample_dv.pdf")


def extract_text_with_doc_intelligence(pdf_path: Path) -> list[str]:
    endpoint = (os.getenv("DOC_INTELLIGENCE_ENDPOINT") or "").strip().rstrip("/")
    key = (os.getenv("DOC_INTELLIGENCE_KEY") or "").strip()
    if not endpoint or not key:
        raise RuntimeError("Missing DOC_INTELLIGENCE_ENDPOINT or DOC_INTELLIGENCE_KEY env vars.")

    analyze_url = (
        f"{endpoint}/formrecognizer/documentModels/prebuilt-read:analyze"
        f"?api-version=2023-07-31"
    )
    with pdf_path.open("rb") as f:
        pdf_bytes = f.read()

    start = requests.post(
        analyze_url,
        headers={
            "Ocp-Apim-Subscription-Key": key,
            "Content-Type": "application/pdf",
        },
        data=pdf_bytes,
        timeout=30,
    )
    if start.status_code != 202:
        raise RuntimeError(
            f"Document Intelligence analyze start failed ({start.status_code}): {start.text[:300]}"
        )

    operation_url = start.headers.get("operation-location")
    if not operation_url:
        raise RuntimeError("Document Intelligence response missing operation-location header.")

    status = "notStarted"
    payload = {}
    for _ in range(30):
        poll = requests.get(
            operation_url,
            headers={"Ocp-Apim-Subscription-Key": key},
            timeout=30,
        )
        if poll.status_code != 200:
            raise RuntimeError(
                f"Document Intelligence polling failed ({poll.status_code}): {poll.text[:300]}"
            )
        payload = poll.json()
        status = (payload.get("status") or "").lower()
        if status in {"succeeded", "failed"}:
            break
        time.sleep(1)

    if status != "succeeded":
        raise RuntimeError(f"Document Intelligence analysis did not succeed (status={status}).")

    analyze_result = payload.get("analyzeResult") or {}
    pages = analyze_result.get("pages") or []
    page_texts = []
    for page in pages:
        lines = page.get("lines") or []
        page_texts.append("\n".join([(line.get("content") or "") for line in lines]).strip())

    if not page_texts:
        content = (analyze_result.get("content") or "").strip()
        if content:
            page_texts = [content]

    return page_texts


def extract_dv_fields(pdf_path: Path) -> dict[str, str]:
    pages = extract_text_with_doc_intelligence(pdf_path)
    full_text = "\n".join(pages)
    if not full_text.strip():
        raise RuntimeError("No extractable text found from Document Intelligence.")
    page_1 = pages[0] if pages else ""
    page_5 = pages[4] if len(pages) >= 5 else full_text

    case_match = re.search(r"Case No\.\s*([A-Z0-9-]+)", full_text, flags=re.IGNORECASE)
    case_number = case_match.group(1).strip().upper() if case_match else ""

    respondent_match = re.search(r"RESPONDENT\s+([A-Z][A-Z\s.'-]+)", page_1)
    respondent_name = respondent_match.group(1).split("\n")[0].strip() if respondent_match else ""

    order_type = ""
    for pattern in (
        r"(TEMPORARY PROTECTIVE ORDER)",
        r"(INTERIM PROTECTIVE ORDER)",
        r"(FINAL PROTECTIVE ORDER)",
    ):
        m = re.search(pattern, page_1, flags=re.IGNORECASE)
        if m:
            order_type = m.group(1).upper().strip()
            break

    if not order_type:
        m = re.search(r"CERTIFICATION OF\s+([A-Z ]+ORDER)", page_5, flags=re.IGNORECASE)
        if m:
            order_type = re.sub(r"\s+", " ", m.group(1)).upper().strip()

    issue_match = re.search(r"Date:\s*(\d{2}/\d{2}/\d{4})", page_5, flags=re.IGNORECASE)
    issue_date = issue_match.group(1) if issue_match else ""

    return {
        "case_number": case_number,
        "respondent_name": respondent_name,
        "issue_date": issue_date,
        "type": order_type,
    }


def main() -> None:
    if not INPUT_PDF.exists():
        raise FileNotFoundError(
            f"Hard-coded PDF not found: {INPUT_PDF}\n"
            "Set INPUT_PDF in dv_pdf_to_csv.py to your local PDF path."
        )

    row = extract_dv_fields(INPUT_PDF)

    output_csv = INPUT_PDF.with_suffix(".csv")
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["case_number", "respondent_name", "issue_date", "type"],
        )
        writer.writeheader()
        writer.writerow(row)

    print(f"Wrote: {output_csv}")
    print(row)


if __name__ == "__main__":
    main()
