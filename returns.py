"""Returns queue persistence, Cognito parsing, search, and audit helpers."""

from __future__ import annotations

import json
import re
import threading
from datetime import date, datetime
from html.parser import HTMLParser


RETURN_STATUS_VALUES = ("Signed", "Uploaded", "Hard Copy Returned", "Hold", "Pending")
MANUAL_RETURN_STATUSES = {"Uploaded", "Hard Copy Returned", "Hold", "Pending"}
_schema_lock = threading.Lock()
_schema_ready = False


RETURN_FIELDS = (
    "cognito_entry_number",
    "case_number",
    "submitted_at",
    "document_type",
    "date_issued",
    "type_of_rfs",
    "type_of_child_support",
    "child_support_show_cause_type",
    "petitioner_name",
    "respondent_name",
    "service_address",
    "service_unit",
    "attempt_date",
    "service_disposition",
    "method_of_service",
    "prior_attempt_date",
    "prior_attempt_location",
    "adult_served_name",
    "relationship_to_respondent",
    "reason_for_non_est",
    "attempt_notes",
    "parent_document_id",
    "unit_id",
    "return_deputy",
    "return_rank",
    "return_email",
    "method_to_confirm_id_age",
    "signature_value",
    "signature_status",
    "date_signed",
    "member_reporting",
    "return_sequence",
    "date_received",
    "intake_date",
    "court_issue_date",
    "court",
    "bcso_status",
    "reason_for_hold",
    "mdec_status",
    "blob_container",
    "blob_name",
    "original_filename",
    "content_type",
    "source_email_message_id",
    "source_email_attachment_id",
    "source_email_subject",
    "source_email_received_at",
    "source_payload_json",
)


LABEL_TO_FIELD = {
    "#": "cognito_entry_number",
    "entry": "cognito_entry_number",
    "entry number": "cognito_entry_number",
    "baltimore city sheriffs office retu id": "cognito_entry_number",
    "document": "case_number",
    "case number": "case_number",
    "date submitted": "submitted_at",
    "entry date submitted": "submitted_at",
    "type": "document_type",
    "date issued": "date_issued",
    "type of rfs": "type_of_rfs",
    "type of child support": "type_of_child_support",
    "child support show cause type": "child_support_show_cause_type",
    "petitioner": "petitioner_name",
    "resp name": "respondent_name",
    "respondent": "respondent_name",
    "address": "service_address",
    "unit": "service_unit",
    "date attempted": "attempt_date",
    "service disp": "service_disposition",
    "service disposition": "service_disposition",
    "method of service": "method_of_service",
    "prior attempt date": "prior_attempt_date",
    "location of prior attempt": "prior_attempt_location",
    "name of adult": "adult_served_name",
    "relationship to respondent": "relationship_to_respondent",
    "reason for non est": "reason_for_non_est",
    "notes from attempt": "attempt_notes",
    "parent document": "parent_document_id",
    "unit id": "unit_id",
    "return deputy": "return_deputy",
    "return rank": "return_rank",
    "return email": "return_email",
    "method to confirm id and age": "method_to_confirm_id_age",
    "signature": "signature_value",
    "date signed": "date_signed",
    "member reporting": "member_reporting",
    "return sequence": "return_sequence",
    "date received": "date_received",
    "intake date": "intake_date",
    "court issue date": "court_issue_date",
    "court": "court",
    "status": "source_status",
}


DATE_FIELDS = {
    "date_issued",
    "attempt_date",
    "prior_attempt_date",
    "date_signed",
    "date_received",
    "intake_date",
    "court_issue_date",
}
DATETIME_FIELDS = {"submitted_at", "source_email_received_at"}


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, (datetime, date)):
        return value
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def normalize_label(value):
    text = str(value or "").strip().rstrip(":")
    # Newer Cognito XLSX exports use compact field names such as RespName,
    # DateAttempted, ServiceDisp, and Entry_DateSubmitted.
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", text)
    text = re.sub(r"[_\-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def normalize_service_disposition(value):
    text = clean_value(value)
    if not text:
        return None
    compact = re.sub(r"[^a-z]", "", text.lower())
    if compact in {"nonest", "nonexistent", "notserved", "unserved"}:
        return "Non Est"
    if "served" in text.lower() and "not served" not in text.lower():
        return "Served"
    return text


def is_hard_copy_return_type(value):
    """Return True for Cognito Types that require a physical return."""
    text = clean_value(value)
    if not text:
        return False
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    first_token = normalized.split(" ", 1)[0].upper() if normalized else ""
    return first_token in {"CS", "JV", "SP"} or any(
        phrase in normalized for phrase in ("child support", "juvenile", "subpoena")
    )


def signature_is_captured(value):
    text = str(value or "").strip().lower()
    return text in {"captured", "signed", "yes", "true"} or bool(text and text not in {"no", "false", "none", "blank"})


def derived_signature_status(payload):
    # Returns reach this pipeline only after Cognito has generated the signed PDF.
    return "Signed"


def normalize_return_payload(payload):
    normalized = {field: clean_value(payload.get(field)) for field in RETURN_FIELDS}
    normalized["service_disposition"] = normalize_service_disposition(normalized.get("service_disposition"))
    normalized["signature_status"] = derived_signature_status(normalized)
    if not normalized.get("bcso_status") or normalized.get("bcso_status") == "Needs Signature":
        normalized["bcso_status"] = "Signed"
    return normalized


class _EntryDetailsParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self._row = None
        self._cell = None

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in {"td", "th"} and self._cell is not None:
            value = clean_value("".join(self._cell)) or ""
            self._row.append(value)
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


def parse_cognito_entry_details(body_content):
    parser = _EntryDetailsParser()
    parser.feed(str(body_content or ""))
    payload = {}
    for row in parser.rows:
        cells = [clean_value(cell) for cell in row if clean_value(cell)]
        if len(cells) < 2:
            continue
        field = LABEL_TO_FIELD.get(normalize_label(cells[0]))
        if field:
            payload[field] = cells[1]
    return payload


def payload_from_export_row(row):
    payload = {}
    for label, value in dict(row).items():
        field = LABEL_TO_FIELD.get(normalize_label(label))
        if field:
            payload[field] = value
    return normalize_return_payload(payload)


def ensure_returns_tables(conn):
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        _ensure_returns_tables(conn)
        _schema_ready = True


def _ensure_returns_tables(conn):
    cur = conn.cursor()
    # App Service can start multiple workers at once. Serialize schema upgrades
    # across processes so two workers cannot drop/add the same constraint.
    cur.execute(
        """
        DECLARE @lock_result INT;
        EXEC @lock_result = sys.sp_getapplock
            @Resource = 'bcso_returns_schema_upgrade_v4',
            @LockMode = 'Exclusive',
            @LockOwner = 'Session',
            @LockTimeout = 30000;
        IF @lock_result < 0
            THROW 51000, 'Could not acquire Returns schema upgrade lock.', 1;
        """
    )
    cur.execute(
        """
        IF OBJECT_ID('search.Returns', 'U') IS NULL AND OBJECT_ID('search.mdec_returns', 'U') IS NOT NULL
            EXEC sp_rename 'search.mdec_returns', 'Returns'

        IF OBJECT_ID('search.Returns', 'U') IS NULL
        BEGIN
            CREATE TABLE search.Returns (
                mdec_return_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                cognito_entry_number NVARCHAR(100) NULL,
                case_number NVARCHAR(150) NOT NULL,
                submitted_at DATETIME2 NULL,
                document_type NVARCHAR(500) NULL,
                date_issued DATE NULL,
                type_of_rfs NVARCHAR(500) NULL,
                type_of_child_support NVARCHAR(500) NULL,
                child_support_show_cause_type NVARCHAR(500) NULL,
                petitioner_name NVARCHAR(500) NULL,
                respondent_name NVARCHAR(500) NULL,
                service_address NVARCHAR(1000) NULL,
                service_unit NVARCHAR(255) NULL,
                attempt_date DATE NULL,
                service_disposition NVARCHAR(50) NULL,
                method_of_service NVARCHAR(500) NULL,
                prior_attempt_date DATE NULL,
                prior_attempt_location NVARCHAR(1000) NULL,
                adult_served_name NVARCHAR(500) NULL,
                relationship_to_respondent NVARCHAR(500) NULL,
                reason_for_non_est NVARCHAR(1000) NULL,
                attempt_notes NVARCHAR(MAX) NULL,
                parent_document_id NVARCHAR(500) NULL,
                unit_id NVARCHAR(100) NULL,
                return_deputy NVARCHAR(500) NULL,
                return_rank NVARCHAR(255) NULL,
                return_email NVARCHAR(320) NULL,
                method_to_confirm_id_age NVARCHAR(1000) NULL,
                signature_value NVARCHAR(100) NULL,
                signature_status NVARCHAR(50) NOT NULL DEFAULT ('Signed'),
                date_signed DATE NULL,
                member_reporting NVARCHAR(500) NULL,
                return_sequence NVARCHAR(100) NULL,
                date_received DATE NULL,
                intake_date DATE NULL,
                court_issue_date DATE NULL,
                court NVARCHAR(255) NULL,
                bcso_status NVARCHAR(50) NOT NULL DEFAULT ('Signed'),
                reason_for_hold NVARCHAR(1000) NULL,
                mdec_status NVARCHAR(50) NULL,
                blob_container NVARCHAR(255) NULL,
                blob_name NVARCHAR(1000) NULL,
                original_filename NVARCHAR(500) NULL,
                content_type NVARCHAR(255) NULL,
                source_email_message_id NVARCHAR(1000) NULL,
                source_email_attachment_id NVARCHAR(1000) NULL,
                source_email_subject NVARCHAR(1000) NULL,
                source_email_received_at DATETIME2 NULL,
                source_payload_json NVARCHAR(MAX) NULL,
                ingestion_status NVARCHAR(50) NOT NULL DEFAULT ('processed'),
                ingestion_error NVARCHAR(MAX) NULL,
                is_active BIT NOT NULL DEFAULT (1),
                created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
        END
        """
    )

    required_columns = {
        "cognito_entry_number": "NVARCHAR(100) NULL",
        "case_number": "NVARCHAR(150) NULL",
        "submitted_at": "DATETIME2 NULL",
        "document_type": "NVARCHAR(500) NULL",
        "date_issued": "DATE NULL",
        "type_of_rfs": "NVARCHAR(500) NULL",
        "type_of_child_support": "NVARCHAR(500) NULL",
        "child_support_show_cause_type": "NVARCHAR(500) NULL",
        "petitioner_name": "NVARCHAR(500) NULL",
        "respondent_name": "NVARCHAR(500) NULL",
        "service_disposition": "NVARCHAR(50) NULL",
        "service_address": "NVARCHAR(1000) NULL",
        "service_unit": "NVARCHAR(255) NULL",
        "attempt_date": "DATE NULL",
        "method_of_service": "NVARCHAR(500) NULL",
        "prior_attempt_date": "DATE NULL",
        "prior_attempt_location": "NVARCHAR(1000) NULL",
        "adult_served_name": "NVARCHAR(500) NULL",
        "relationship_to_respondent": "NVARCHAR(500) NULL",
        "reason_for_non_est": "NVARCHAR(1000) NULL",
        "attempt_notes": "NVARCHAR(MAX) NULL",
        "parent_document_id": "NVARCHAR(500) NULL",
        "unit_id": "NVARCHAR(100) NULL",
        "return_deputy": "NVARCHAR(500) NULL",
        "return_rank": "NVARCHAR(255) NULL",
        "return_email": "NVARCHAR(320) NULL",
        "method_to_confirm_id_age": "NVARCHAR(1000) NULL",
        "signature_value": "NVARCHAR(100) NULL",
        "signature_status": "NVARCHAR(50) NULL",
        "date_signed": "DATE NULL",
        "member_reporting": "NVARCHAR(500) NULL",
        "return_sequence": "NVARCHAR(100) NULL",
        "date_received": "DATE NULL",
        "intake_date": "DATE NULL",
        "court_issue_date": "DATE NULL",
        "court": "NVARCHAR(255) NULL",
        "bcso_status": "NVARCHAR(50) NULL",
        "reason_for_hold": "NVARCHAR(1000) NULL",
        "mdec_status": "NVARCHAR(50) NULL",
        "blob_container": "NVARCHAR(255) NULL",
        "blob_name": "NVARCHAR(1000) NULL",
        "original_filename": "NVARCHAR(500) NULL",
        "content_type": "NVARCHAR(255) NULL",
        "source_email_message_id": "NVARCHAR(1000) NULL",
        "source_email_attachment_id": "NVARCHAR(1000) NULL",
        "source_email_subject": "NVARCHAR(1000) NULL",
        "source_email_received_at": "DATETIME2 NULL",
        "source_payload_json": "NVARCHAR(MAX) NULL",
        "ingestion_status": "NVARCHAR(50) NULL",
        "ingestion_error": "NVARCHAR(MAX) NULL",
        "is_active": "BIT NULL",
        "created_at": "DATETIME2 NULL",
        "updated_at": "DATETIME2 NULL",
    }
    for column, definition in required_columns.items():
        cur.execute(
            f"IF COL_LENGTH('search.Returns', '{column}') IS NULL "
            f"ALTER TABLE search.Returns ADD [{column}] {definition}"
        )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID('search.Returns')
          AND name = 'CK_mdec_returns_bcso_status_v4'
        """
    )
    if int(cur.fetchone()[0]) == 0:
        cur.execute(
            """
            SELECT name
            FROM sys.check_constraints
            WHERE parent_object_id = OBJECT_ID('search.Returns')
              AND definition LIKE '%bcso_status%'
            """
        )
        for row in cur.fetchall():
            constraint_name = str(row[0]).replace("]", "]]" )
            cur.execute(f"ALTER TABLE search.Returns DROP CONSTRAINT [{constraint_name}]")
        cur.execute(
            "UPDATE search.Returns SET bcso_status = 'Uploaded' WHERE bcso_status = 'Uploaded to MDEC'"
        )
        cur.execute(
            "UPDATE search.Returns SET signature_status = 'Signed' WHERE signature_status IS NULL OR signature_status = 'Needs Signature'"
        )
        cur.execute(
            "UPDATE search.Returns SET bcso_status = 'Signed' WHERE bcso_status IS NULL OR bcso_status = 'Needs Signature'"
        )
        cur.execute(
            """
            ALTER TABLE search.Returns WITH NOCHECK
            ADD CONSTRAINT CK_mdec_returns_bcso_status_v4
            CHECK (bcso_status IS NULL OR bcso_status IN ('Signed', 'Uploaded', 'Hard Copy Returned', 'Hold', 'Pending'))
            """
        )

    cur.execute(
        """
        IF OBJECT_ID('search.mdec_return_activity_log', 'U') IS NULL
        BEGIN
            CREATE TABLE search.mdec_return_activity_log (
                activity_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                mdec_return_id INT NOT NULL,
                activity_type NVARCHAR(100) NOT NULL,
                old_value NVARCHAR(1000) NULL,
                new_value NVARCHAR(1000) NULL,
                activity_summary NVARCHAR(2000) NOT NULL,
                actor_email NVARCHAR(320) NULL,
                created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
            )
        END
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_mdec_return_activity_log_return_created'
              AND object_id = OBJECT_ID('search.mdec_return_activity_log')
        )
        CREATE INDEX IX_mdec_return_activity_log_return_created
            ON search.mdec_return_activity_log(mdec_return_id, created_at DESC)
        """
    )
    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_mdec_returns_search'
              AND object_id = OBJECT_ID('search.Returns')
        )
        CREATE INDEX IX_mdec_returns_search
            ON search.Returns(case_number, respondent_name, attempt_date)
        """
    )
    conn.commit()
    cur.execute(
        "EXEC sys.sp_releaseapplock @Resource = 'bcso_returns_schema_upgrade_v4', @LockOwner = 'Session'"
    )


def log_return_activity(cur, return_id, activity_type, summary, actor_email, old_value=None, new_value=None):
    cur.execute(
        """
        INSERT INTO search.mdec_return_activity_log (
            mdec_return_id, activity_type, old_value, new_value, activity_summary, actor_email
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        int(return_id), activity_type, clean_value(old_value), clean_value(new_value), summary, actor_email,
    )


def _find_existing_return(cur, payload):
    entry_number = clean_value(payload.get("cognito_entry_number"))
    if entry_number:
        cur.execute(
            "SELECT TOP 1 mdec_return_id, bcso_status FROM search.Returns WHERE cognito_entry_number = ? ORDER BY mdec_return_id DESC",
            entry_number,
        )
        row = cur.fetchone()
        if row:
            return int(row[0]), clean_value(row[1])

    message_id = clean_value(payload.get("source_email_message_id"))
    attachment_id = clean_value(payload.get("source_email_attachment_id"))
    if message_id and attachment_id:
        cur.execute(
            """
            SELECT TOP 1 mdec_return_id, bcso_status
            FROM search.Returns
            WHERE source_email_message_id = ? AND source_email_attachment_id = ?
            ORDER BY mdec_return_id DESC
            """,
            message_id, attachment_id,
        )
        row = cur.fetchone()
        if row:
            return int(row[0]), clean_value(row[1])

    # Initial imports performed before compact Cognito headers were supported
    # did not retain the Cognito entry number. The original PDF filename and
    # case number remain stable, so use them to repair those rows in place.
    original_filename = clean_value(payload.get("original_filename"))
    case_number = clean_value(payload.get("case_number"))
    if original_filename and case_number:
        cur.execute(
            """
            SELECT TOP 1 mdec_return_id, bcso_status
            FROM search.Returns
            WHERE LOWER(LTRIM(RTRIM(COALESCE(original_filename, '')))) =
                  LOWER(LTRIM(RTRIM(?)))
              AND UPPER(REPLACE(REPLACE(COALESCE(case_number, ''), '-', ''), ' ', '')) =
                  UPPER(REPLACE(REPLACE(?, '-', ''), ' ', ''))
            ORDER BY updated_at DESC, mdec_return_id DESC
            """,
            original_filename, case_number,
        )
        row = cur.fetchone()
        if row:
            return int(row[0]), clean_value(row[1])

    respondent = clean_value(payload.get("respondent_name"))
    attempt_date = clean_value(payload.get("attempt_date"))
    if case_number:
        cur.execute(
            """
            SELECT TOP 1 mdec_return_id, bcso_status
            FROM search.Returns
            WHERE UPPER(REPLACE(REPLACE(COALESCE(case_number, ''), '-', ''), ' ', '')) =
                  UPPER(REPLACE(REPLACE(?, '-', ''), ' ', ''))
              AND LOWER(LTRIM(RTRIM(COALESCE(respondent_name, '')))) = LOWER(LTRIM(RTRIM(COALESCE(?, ''))))
              AND (CAST(attempt_date AS date) = CAST(? AS date) OR (attempt_date IS NULL AND ? IS NULL))
            ORDER BY updated_at DESC, mdec_return_id DESC
            """,
            case_number, respondent or "", attempt_date, attempt_date,
        )
        row = cur.fetchone()
        if row:
            return int(row[0]), clean_value(row[1])
    return None, None


def upsert_return(conn, payload, actor_email="system:cognito-email"):
    ensure_returns_tables(conn)
    normalized = normalize_return_payload(payload)
    if not normalized.get("case_number"):
        raise ValueError("Return is missing a case number (Cognito Document field).")

    cur = conn.cursor()
    return_id, existing_status = _find_existing_return(cur, normalized)
    incoming_status = normalized.get("bcso_status") or normalized.get("signature_status")
    if existing_status in MANUAL_RETURN_STATUSES:
        if is_hard_copy_return_type(normalized.get("document_type")) and existing_status == "Uploaded":
            normalized["bcso_status"] = "Hard Copy Returned"
        else:
            normalized["bcso_status"] = existing_status
    elif existing_status == "Signed" and incoming_status == "Needs Signature":
        normalized["bcso_status"] = existing_status

    column_values = {field: normalized.get(field) for field in RETURN_FIELDS}
    column_values["source_payload_json"] = json.dumps(
        payload.get("source_payload_json") or payload,
        default=str,
        ensure_ascii=False,
    )

    if return_id:
        assignments = []
        params = []
        for field, value in column_values.items():
            if value is None:
                continue
            sql_value = value
            if field in DATE_FIELDS:
                assignments.append(f"[{field}] = TRY_CONVERT(date, ?)")
            elif field in DATETIME_FIELDS:
                assignments.append(f"[{field}] = TRY_CONVERT(datetime2, ?)")
            else:
                assignments.append(f"[{field}] = ?")
            params.append(sql_value)
        assignments.extend(["ingestion_status = 'processed'", "ingestion_error = NULL", "updated_at = SYSUTCDATETIME()"])
        params.append(return_id)
        cur.execute(
            f"UPDATE search.Returns SET {', '.join(assignments)} WHERE mdec_return_id = ?",
            *params,
        )
        new_status = normalized.get("bcso_status")
        if new_status and existing_status != new_status:
            log_return_activity(
                cur,
                return_id,
                "status_changed",
                f"Status changed from {existing_status or 'empty'} to {new_status}.",
                actor_email,
                existing_status,
                new_status,
            )
        if normalized.get("blob_name"):
            log_return_activity(cur, return_id, "pdf_updated", "Return PDF received and stored.", actor_email)
        conn.commit()
        return return_id, False

    fields = []
    placeholders = []
    values = []
    for field, value in column_values.items():
        if value is None:
            continue
        fields.append(f"[{field}]")
        if field in DATE_FIELDS:
            placeholders.append("TRY_CONVERT(date, ?)")
        elif field in DATETIME_FIELDS:
            placeholders.append("TRY_CONVERT(datetime2, ?)")
        else:
            placeholders.append("?")
        values.append(value)
    cur.execute(
        f"""
        INSERT INTO search.Returns ({', '.join(fields)})
        OUTPUT INSERTED.mdec_return_id
        VALUES ({', '.join(placeholders)})
        """,
        *values,
    )
    return_id = int(cur.fetchone()[0])
    log_return_activity(
        cur,
        return_id,
        "created",
        f"Return created with status {normalized.get('bcso_status')}.",
        actor_email,
        None,
        normalized.get("bcso_status"),
    )
    if normalized.get("blob_name"):
        log_return_activity(cur, return_id, "pdf_stored", "Return PDF received and stored.", actor_email)
    conn.commit()
    return return_id, True


def _format_rows(cursor):
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def search_returns(conn, filters, exclude_uploaded=False):
    ensure_returns_tables(conn)
    clauses = ["is_active = 1"]
    params = []
    if exclude_uploaded:
        clauses.append("COALESCE(bcso_status, '') <> 'Uploaded'")
    query = clean_value(filters.get("query"))
    if query:
        clauses.append(
            "LOWER(CONCAT(COALESCE(case_number, ''), ' ', COALESCE(respondent_name, ''), ' ', "
            "COALESCE(petitioner_name, ''), ' ', COALESCE(member_reporting, ''))) LIKE ?"
        )
        params.append(f"%{query.lower()}%")
    case_number = clean_value(filters.get("case_number"))
    if case_number:
        clauses.append("LOWER(COALESCE(case_number, '')) LIKE ?")
        params.append(f"%{case_number.lower()}%")
    date_start = clean_value(filters.get("date_start"))
    date_end = clean_value(filters.get("date_end"))
    if date_start and date_end:
        clauses.append(
            "CAST(COALESCE(attempt_date, date_signed, submitted_at, intake_date, court_issue_date) AS date) BETWEEN CAST(? AS date) AND CAST(? AS date)"
        )
        params.extend([date_start, date_end])
    elif clean_value(filters.get("last_x_days")):
        clauses.append(
            "CAST(COALESCE(attempt_date, date_signed, submitted_at, intake_date, court_issue_date) AS date) >= DATEADD(day, -?, CAST(GETDATE() AS date))"
        )
        params.append(int(filters["last_x_days"]))

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            mdec_return_id,
            cognito_entry_number,
            case_number,
            FORMAT(submitted_at, 'yyyy-MM-ddTHH:mm:ss') AS submitted_at,
            document_type,
            FORMAT(date_issued, 'yyyy-MM-dd') AS date_issued,
            type_of_rfs,
            type_of_child_support,
            child_support_show_cause_type,
            petitioner_name,
            respondent_name,
            service_address,
            service_unit,
            FORMAT(attempt_date, 'yyyy-MM-dd') AS attempt_date,
            service_disposition,
            method_of_service,
            FORMAT(prior_attempt_date, 'yyyy-MM-dd') AS prior_attempt_date,
            prior_attempt_location,
            adult_served_name,
            relationship_to_respondent,
            reason_for_non_est,
            attempt_notes,
            parent_document_id,
            unit_id,
            return_deputy,
            return_rank,
            return_email,
            method_to_confirm_id_age,
            signature_value,
            signature_status,
            FORMAT(date_signed, 'yyyy-MM-dd') AS date_signed,
            member_reporting,
            return_sequence,
            FORMAT(date_received, 'yyyy-MM-dd') AS date_received,
            FORMAT(intake_date, 'yyyy-MM-dd') AS intake_date,
            FORMAT(court_issue_date, 'yyyy-MM-dd') AS court_issue_date,
            court,
            bcso_status,
            reason_for_hold,
            mdec_status,
            CASE WHEN blob_name IS NULL OR LTRIM(RTRIM(blob_name)) = '' THEN 0 ELSE 1 END AS has_pdf,
            FORMAT(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time', 'yyyy-MM-dd h:mm tt') AS updated_at
        FROM search.Returns
        WHERE {' AND '.join(clauses)}
        ORDER BY COALESCE(attempt_date, date_signed, CAST(submitted_at AS date), intake_date, court_issue_date) DESC,
                 mdec_return_id DESC
        """,
        *params,
    )
    return _format_rows(cur)


def get_return(conn, return_id):
    cur = conn.cursor()
    cur.execute("SELECT * FROM search.Returns WHERE mdec_return_id = ? AND is_active = 1", int(return_id))
    rows = _format_rows(cur)
    return rows[0] if rows else None


def fetch_return_activity(conn, return_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            activity_id,
            activity_type,
            old_value,
            new_value,
            activity_summary,
            actor_email,
            FORMAT(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time', 'yyyy-MM-dd h:mm tt') AS created_at
        FROM search.mdec_return_activity_log
        WHERE mdec_return_id = ?
          AND COALESCE(actor_email, '') NOT LIKE 'system:%'
        ORDER BY created_at DESC, activity_id DESC
        """,
        int(return_id),
    )
    return _format_rows(cur)


def update_return_status(conn, return_id, status, actor_email, reason_for_hold=None):
    status = clean_value(status)
    if status not in RETURN_STATUS_VALUES:
        raise ValueError("Status must be Signed, Uploaded, Hard Copy Returned, Hold, or Pending.")
    cur = conn.cursor()
    cur.execute(
        "SELECT bcso_status, document_type FROM search.Returns WHERE mdec_return_id = ? AND is_active = 1",
        int(return_id),
    )
    row = cur.fetchone()
    if not row:
        return False
    old_status = clean_value(row[0])
    hard_copy_required = is_hard_copy_return_type(row[1])
    if hard_copy_required and status == "Uploaded":
        raise ValueError("This return requires a hard copy. Use Hard Copy Returned instead of Uploaded.")
    if not hard_copy_required and status == "Hard Copy Returned":
        raise ValueError("Hard Copy Returned is only available for Child Support, Juvenile, and Subpoena returns.")
    if old_status == status and (status != "Hold" or not clean_value(reason_for_hold)):
        return True
    cur.execute(
        """
        UPDATE search.Returns
        SET bcso_status = ?,
            reason_for_hold = CASE WHEN ? = 'Hold' THEN ? ELSE NULL END,
            updated_at = SYSUTCDATETIME()
        WHERE mdec_return_id = ?
        """,
        status, status, clean_value(reason_for_hold), int(return_id),
    )
    summary = f"Status changed from {old_status or 'empty'} to {status}."
    if status == "Hold" and clean_value(reason_for_hold):
        summary += f" Reason: {clean_value(reason_for_hold)}"
    log_return_activity(cur, return_id, "status_changed", summary, actor_email, old_status, status)
    conn.commit()
    return True
