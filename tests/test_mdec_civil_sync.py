import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from mdec_civil_sync import (
    MDEC_MATCH_WINDOW_DAYS,
    MDEC_BATCH_SIZE,
    MDEC_DOWNLOAD_TIMEOUT_SECONDS,
    MDEC_RUN_TIME_BUDGET_SECONDS,
    MDEC_RETRY_MINUTES,
    _extract_document_links,
    _is_probable_document_link,
    _unwrap_secure_web_url,
    download_combined_pdf,
    download_pdf,
    fetch_mdec_documents,
    find_best_civil_record,
    normalize_case_number,
    parse_submission_datetime,
    source_filename,
    sync_mdec_civil_documents,
)


class FakeResponse:
    def __init__(self, url, content, content_type, error=None, status_code=200, payload=None):
        self.url = url
        self.content = content
        self.headers = {"content-type": content_type}
        self.text = content.decode("utf-8", errors="ignore")
        self.error = error
        self.status_code = status_code
        self.payload = payload

    def raise_for_status(self):
        if self.error:
            raise self.error
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses, post_responses=None):
        self.responses = responses
        self.post_responses = post_responses or {}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses[url]

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.post_responses[url]


class FakeCursor:
    def __init__(self, row=None, rows=None):
        self.row = row
        self.rows = rows or []
        self.sql = ""
        self.params = ()

    def execute(self, sql, *params):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self):
        self.cursor_value = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class MdecCivilSyncTests(unittest.TestCase):
    def test_case_number_normalization(self):
        self.assertEqual(normalize_case_number(" D-01-CV-26-029285 "), "D01CV26029285")

    def test_combined_pdf_uses_civil_papers_filename(self):
        self.assertEqual(
            source_filename({"case_number": "C-24-CV-25-010055"}),
            "Civil Papers C-24-CV-25-010055.pdf",
        )

    def test_submission_datetime_formats(self):
        self.assertEqual(parse_submission_datetime("8/19/2026 1:49 PM"), datetime(2026, 8, 19, 13, 49))
        self.assertEqual(parse_submission_datetime("2026-08-19T13:49:00Z"), datetime(2026, 8, 19, 13, 49))
        self.assertEqual(parse_submission_datetime("2026-03-16"), datetime(2026, 3, 16))
        self.assertEqual(parse_submission_datetime("3/16/26"), datetime(2026, 3, 16))
        self.assertEqual(parse_submission_datetime("3/16/2026"), datetime(2026, 3, 16))
        self.assertEqual(parse_submission_datetime("3/16/2026 11:20 AM EST"), datetime(2026, 3, 16, 11, 20))
        self.assertEqual(parse_submission_datetime("3/16/26 11:20:45 AM EDT"), datetime(2026, 3, 16, 11, 20, 45))

    def test_match_uses_priority_before_date_distance(self):
        cur = FakeCursor((456,))
        self.assertEqual(find_best_civil_record(cur, "D-01-CV-26-029285", datetime(2026, 8, 19)), 456)
        self.assertIn("THEN 0", cur.sql)
        self.assertIn("THEN 1", cur.sql)
        self.assertIn("THEN 2", cur.sql)
        self.assertIn("THEN 3", cur.sql)
        order_by = cur.sql[cur.sql.index("ORDER BY"):]
        self.assertLess(order_by.index("THEN 0"), order_by.index("ABS(DATEDIFF"))
        self.assertEqual(cur.params[2], MDEC_MATCH_WINDOW_DAYS)
        self.assertEqual(cur.params[4], MDEC_MATCH_WINDOW_DAYS)

    def test_match_checks_issue_or_intake_within_ten_days(self):
        cur = FakeCursor(None)
        self.assertIsNone(find_best_civil_record(cur, "C-24-CV-26-000001", datetime(2026, 8, 20)))
        self.assertIn("TRY_CONVERT(date, NULLIF", cur.sql)
        self.assertIn("CONVERT(nvarchar(50), issue_date)", cur.sql)
        self.assertIn("CONVERT(nvarchar(50), court_issued_date)", cur.sql)
        self.assertNotIn("CAST(COALESCE(issue_date, court_issued_date) AS date)", cur.sql)
        self.assertIn("OR ABS(DATEDIFF", cur.sql)

    def test_extracts_relative_servedocument_link(self):
        links = _extract_document_links(
            "https://efilemd.tylertech.cloud/view/123",
            '<a href="/ServeDocument.ashx?id=456">Return</a>',
        )
        self.assertEqual(links, ["https://efilemd.tylertech.cloud/ServeDocument.ashx?id=456"])

    def test_document_filter_rejects_namespace_and_media_assets(self):
        self.assertFalse(_is_probable_document_link("https://www.w3.org/1999/xhtml"))
        self.assertFalse(_is_probable_document_link("https://www.w3.org/cms-uploads/animation.mp4"))
        self.assertTrue(_is_probable_document_link("https://efilemd.tylertech.cloud/ServeDocument.ashx?id=456"))
        links = _extract_document_links(
            "https://efilemd.tylertech.cloud/view/123",
            """
            <html xmlns="https://www.w3.org/1999/xhtml">
              <a href="https://www.w3.org/cms-uploads/animation.mp4">Animation</a>
              <a href="/ServeDocument.ashx?id=456">Download document</a>
            </html>
            """,
        )
        self.assertEqual(links, ["https://efilemd.tylertech.cloud/ServeDocument.ashx?id=456"])

    def test_unwraps_cisco_secure_web_target(self):
        wrapped = "https://secure-web.cisco.com/1/abc?url=https%3A%2F%2Fefilemd.tylertech.cloud%2Fdoc%2F7"
        self.assertEqual(_unwrap_secure_web_url(wrapped), "https://efilemd.tylertech.cloud/doc/7")

    def test_download_pdf_follows_html_link_and_sets_referer(self):
        landing = "https://efilemd.tylertech.cloud/view/123"
        pdf_url = "https://efilemd.tylertech.cloud/ServeDocument.ashx?id=456"
        session = FakeSession({
            landing: FakeResponse(landing, b'<html><a href="/ServeDocument.ashx?id=456">PDF</a></html>', "text/html"),
            pdf_url: FakeResponse(pdf_url, b"%PDF-1.7 test", "application/pdf"),
        })
        self.assertEqual(download_pdf(landing, session=session), b"%PDF-1.7 test")
        self.assertEqual(session.calls[1][1]["headers"], {"Referer": landing})

    def test_download_pdf_skips_failed_child_and_continues_to_pdf(self):
        landing = "https://efilemd.tylertech.cloud/view/123"
        failed = "https://efilemd.tylertech.cloud/download?id=bad"
        pdf_url = "https://efilemd.tylertech.cloud/ServeDocument.ashx?id=456"
        session = FakeSession({
            landing: FakeResponse(
                landing,
                f'<html><a href="{failed}">Download</a><a href="{pdf_url}">PDF</a></html>'.encode(),
                "text/html",
            ),
            failed: FakeResponse(failed, b"rate limited", "text/plain", RuntimeError("429")),
            pdf_url: FakeResponse(pdf_url, b"%PDF-1.7 test", "application/pdf"),
        })
        self.assertEqual(download_pdf(landing, session=session), b"%PDF-1.7 test")

    def test_downloads_one_mdec_combined_pdf_job(self):
        start_url = "https://mdec.example/download-case/C-24-CV-25-003100/combined/start"
        file_url = "https://mdec.example/download-case/jobs/job-123/file"
        session = FakeSession(
            {file_url: FakeResponse(file_url, b"%PDF-1.7 combined", "application/pdf")},
            {start_url: FakeResponse(start_url, b"", "application/json", payload={"ok": True, "job_id": "job-123"})},
        )
        self.assertEqual(download_combined_pdf(start_url, session=session), b"%PDF-1.7 combined")
        self.assertEqual([call[0] for call in session.calls], [start_url, file_url])

    def test_candidate_query_skips_terminal_matches_but_keeps_new_and_retryable_documents(self):
        self.assertEqual(MDEC_RETRY_MINUTES, 10)
        self.assertEqual(MDEC_BATCH_SIZE, 10)
        self.assertLessEqual(MDEC_DOWNLOAD_TIMEOUT_SECONDS, 15)
        self.assertLessEqual(MDEC_RUN_TIME_BUDGET_SECONDS, 150)
        conn = FakeConnection()
        conn.cursor_value = FakeCursor(rows=[])
        self.assertEqual(fetch_mdec_documents(conn), [])
        sql = conn.cursor_value.sql
        self.assertIn("sync.source_document_id IS NULL AND pdf.id IS NULL", sql)
        self.assertIn("SELECT TOP (10)", sql)
        self.assertIn("sync.sync_status IN ('unmatched', 'failed')", sql)
        self.assertIn("CONCAT('combined:', cd.normalized_case_number)", sql)
        self.assertIn("JSON_VALUE(pdf.source_json, '$.source_version')", sql)
        self.assertIn("JSON_VALUE(pdf.source_json, '$.combined_format_version')", sql)
        self.assertIn("ROW_NUMBER() OVER", sql)
        self.assertIn("cd.source_version DESC", sql)
        self.assertIn("THEN 0", sql)
        self.assertIn("THEN 1", sql)
        self.assertIn("> 1", sql)
        self.assertIn("next_retry_at", sql)
        self.assertIn("DATEADD(day, -60, CAST(SYSUTCDATETIME() AS date))", sql)
        self.assertIn("TRY_CONVERT(date, parsed.submission_date_text, 23)", sql)
        self.assertIn("TRY_CONVERT(date, parsed.submission_date_text, 101)", sql)
        self.assertIn("TRY_CONVERT(date, parsed.submission_date_text, 1)", sql)
        self.assertIn("ORDER BY cd.parsed_submission_date DESC", sql)

    def test_combined_format_version_forces_one_time_pdf_refresh(self):
        source = (Path(__file__).resolve().parents[1] / "mdec_civil_sync.py").read_text()
        self.assertIn("MDEC_COMBINED_FORMAT_VERSION = 2", source)
        self.assertIn("existing_format_version < MDEC_COMBINED_FORMAT_VERSION", source)
        self.assertIn('"combined_format_version": MDEC_COMBINED_FORMAT_VERSION', source)

    def test_fetches_one_combined_candidate_per_case(self):
        conn = FakeConnection()
        conn.cursor_value = FakeCursor(rows=[(
            91, "C-24-CV-25-003100", "2026-08-27", "document.pdf",
            "lead.pdf", "Sheriff Service", "C24CV25003100",
        )])
        documents = fetch_mdec_documents(conn)
        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["source_document_id"], "combined:C24CV25003100")
        self.assertEqual(documents[0]["source_version"], 91)
        self.assertTrue(documents[0]["download_url"].endswith("/download-case/C-24-CV-25-003100/combined/start"))

    def test_sync_reads_mdec_documents_from_shared_target_database(self):
        conn = FakeConnection()
        document = {
            "source_document_id": "7",
            "case_number": "D-01-CV-26-029285",
            "submission_at": datetime(2026, 8, 19, 13, 49),
        }
        with (
            patch("mdec_civil_sync.fetch_mdec_documents", return_value=[document]) as fetch,
            patch("mdec_civil_sync.find_best_civil_record", return_value=456),
            patch("mdec_civil_sync.get_civil_record_priority", return_value=0),
            patch("mdec_civil_sync.upsert_mdec_document", return_value=(1, True)),
            patch("mdec_civil_sync.record_sync_status") as record_status,
        ):
            result = sync_mdec_civil_documents(conn, object())

        fetch.assert_called_once_with(conn)
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(conn.commits, 1)
        self.assertTrue(record_status.call_args.kwargs["terminal"])

    def test_nonterminal_match_is_recorded_for_future_priority_rechecks(self):
        conn = FakeConnection()
        document = {
            "source_document_id": "8",
            "case_number": "D-01-CV-26-029286",
            "submission_at": datetime(2026, 8, 20),
        }
        with (
            patch("mdec_civil_sync.fetch_mdec_documents", return_value=[document]),
            patch("mdec_civil_sync.find_best_civil_record", return_value=789),
            patch("mdec_civil_sync.get_civil_record_priority", return_value=2),
            patch("mdec_civil_sync.upsert_mdec_document", return_value=(2, False)),
            patch("mdec_civil_sync.record_sync_status") as record_status,
        ):
            result = sync_mdec_civil_documents(conn, object())

        self.assertEqual(result["updated"], 1)
        self.assertFalse(record_status.call_args.kwargs["terminal"])


if __name__ == "__main__":
    unittest.main()
