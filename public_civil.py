import re


PUBLIC_CIVIL_FIELDS = (
    "intake_date",
    "case_number",
    "court_document_type",
    "court_issued_date",
    "administrative_status",
    "served_on",
)


def normalize_public_case_number(value):
    """Return an uppercase, punctuation-free case number for exact matching."""
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def public_civil_record(row):
    """Allowlist the only fields that may be rendered by the public page."""
    return {field: row.get(field) for field in PUBLIC_CIVIL_FIELDS}
