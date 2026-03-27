"""
Simple one-off DV PDF -> CSV extractor.

For now this is intentionally hard-coded to one PDF file path so you can
quickly validate extraction without wiring the web app.
"""

from __future__ import annotations

import csv
import io
import re
import importlib.util
from pathlib import Path


# ---------------------------------------------------------------------------
# Hard-coded input file. Replace this with your PDF filename/path.
# Example: Path("sample_dv.pdf")
# ---------------------------------------------------------------------------
INPUT_PDF = Path("sample_dv.pdf")


def get_pdf_reader_class():
    if importlib.util.find_spec("pypdf") is not None:
        return __import__("pypdf").PdfReader, "pypdf"
    if importlib.util.find_spec("PyPDF2") is not None:
        return __import__("PyPDF2").PdfReader, "PyPDF2"
    raise RuntimeError("Missing dependency: install `pypdf` or `PyPDF2` first.")


def ocr_text_from_page_images(page) -> str:
    if importlib.util.find_spec("pytesseract") is None or importlib.util.find_spec("PIL") is None:
        return ""

    pytesseract = __import__("pytesseract")
    pil_image_module = __import__("PIL.Image", fromlist=["open"])
    chunks = []

    page_images = getattr(page, "images", None) or []
    for embedded_image in page_images:
        img_bytes = getattr(embedded_image, "data", None)
        if not img_bytes:
            continue
        try:
            img = pil_image_module.open(io.BytesIO(img_bytes))
            chunks.append(pytesseract.image_to_string(img))
        except Exception:
            continue

    return "\n".join([c for c in chunks if str(c).strip()])


def extract_dv_fields(pdf_path: Path) -> dict[str, str]:
    PdfReader, reader_name = get_pdf_reader_class()
    reader = PdfReader(str(pdf_path))
    pages = []
    for page in reader.pages:
        extracted = page.extract_text() or ""
        if extracted.strip():
            pages.append(extracted)
            continue
        ocr_text = ocr_text_from_page_images(page) if reader_name == "pypdf" else ""
        pages.append(ocr_text)

    full_text = "\n".join(pages)
    if not full_text.strip():
        raise RuntimeError(
            "No extractable text found. For scanned PDFs install Pillow + pytesseract and Tesseract OCR."
        )
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
