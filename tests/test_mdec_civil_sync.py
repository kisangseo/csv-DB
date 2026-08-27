import unittest
from datetime import datetime
from unittest.mock import patch

from mdec_civil_sync import (
    MDEC_MATCH_WINDOW_DAYS,
    _extract_document_links,
    _unwrap_secure_web_url,
    download_pdf,
    find_best_civil_record,
    normalize_case_number,
    parse_submission_datetime,
    sync_mdec_civil_documents,
)


class FakeResponse:
    def __init__(self, url, content, content_type):
        self.url = url
        self.content = content
        self.headers = {"content-type": content_type}
        self.text = content.decode("utf-8", errors="ignore")

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses[url]


class FakeCursor:
    def __init__(self, row=None):
        self.row = row
        self.sql = ""
        self.params = ()

    def execute(self, sql, *params):
        self.sql = sql
        self.params = params

    def fetchone(self):
        return self.row


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

    def test_submission_datetime_formats(self):
        self.assertEqual(parse_submission_datetime("8/19/2026 1:49 PM"), datetime(2026, 8, 19, 13, 49))
        self.assertEqual(parse_submission_datetime("2026-08-19T13:49:00Z"), datetime(2026, 8, 19, 13, 49))

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
        self.assertIn("COALESCE(issue_date, court_issued_date)", cur.sql)
        self.assertIn("OR ABS(DATEDIFF", cur.sql)

    def test_extracts_relative_servedocument_link(self):
        links = _extract_document_links(
            "https://efilemd.tylertech.cloud/view/123",
            '<a href="/ServeDocument.ashx?id=456">Return</a>',
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
            patch("mdec_civil_sync.upsert_mdec_document", return_value=(1, True)),
        ):
            result = sync_mdec_civil_documents(conn, object())

        fetch.assert_called_once_with(conn)
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(conn.commits, 1)


if __name__ == "__main__":
    unittest.main()
