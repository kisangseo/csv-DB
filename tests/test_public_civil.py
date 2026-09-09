import unittest
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

sys.modules.setdefault("pyodbc", MagicMock())
import app as application
from public_civil import PUBLIC_CIVIL_FIELDS, normalize_public_case_number, public_civil_record


ROOT = Path(__file__).resolve().parents[1]


class FakeCursor:
    __slots__ = ("rows", "sql", "params")

    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = ()

    def execute(self, sql, *params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


class PublicCivilTests(unittest.TestCase):
    def setUp(self):
        application.app.config.update(TESTING=True)
        self.client = application.app.test_client()

    def test_case_number_normalization_is_case_and_format_independent(self):
        self.assertEqual(
            normalize_public_case_number(" c-24-fm-26 / 001478 "),
            "C24FM26001478",
        )

    def test_allowlist_removes_sensitive_and_action_fields(self):
        source = {
            "intake_date": "2026-09-01",
            "case_number": "C-24-CV-26-1",
            "court_document_type": "WOS",
            "court_issued_date": "2026-09-01",
            "administrative_status": "Received",
            "served_on": "09-03-2026 02:47 PM",
            "served_by": "Deputy Example",
            "respondent": "PRIVATE NAME",
            "address": "PRIVATE ADDRESS",
            "download_url": "PRIVATE URL",
        }
        result = public_civil_record(source)
        self.assertEqual(tuple(result), PUBLIC_CIVIL_FIELDS)
        self.assertNotIn("PRIVATE", str(result))

    def test_blank_partial_and_oversized_searches_do_not_query_database(self):
        with patch.object(application, "get_conn") as get_conn:
            blank = self.client.get("/public/civil-papers")
            partial = self.client.get("/public/civil-papers?case_number=C-24")
            oversized = self.client.get(
                "/public/civil-papers?case_number=" + ("A" * 100)
            )
        self.assertEqual(blank.status_code, 200)
        self.assertEqual(partial.status_code, 200)
        self.assertEqual(oversized.status_code, 200)
        self.assertIn(b"complete case number", partial.data)
        self.assertIn(b"complete case number", oversized.data)
        get_conn.assert_not_called()

    def test_public_search_uses_exact_normalized_case_and_six_column_view(self):
        row = (
            "2026-09-01", "C-24-CV-26-001234", "WOS", "2026-08-30",
            "Received", "09-03-2026 02:47 PM",
        )
        connection = FakeConnection([row])
        with patch.object(application, "get_conn", return_value=connection):
            response = self.client.get(
                "/public/civil-papers?case_number=c%2024-cv-26-001234"
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"C-24-CV-26-001234", response.data)
        self.assertIn(b"09-03-2026 02:47 PM", response.data)
        self.assertNotIn(b"Deputy", response.data)
        self.assertEqual(connection.cursor_instance.params, ("C24CV26001234",))
        self.assertIn("SELECT TOP (50)", connection.cursor_instance.sql)
        self.assertIn("AS served_on", connection.cursor_instance.sql)
        self.assertNotIn("respondent", connection.cursor_instance.sql.lower())
        self.assertNotIn("address", connection.cursor_instance.sql.lower())
        self.assertTrue(connection.closed)

    def test_public_response_has_security_headers(self):
        response = self.client.get("/public/civil-papers")
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.headers["X-Frame-Options"], "DENY")
        self.assertEqual(response.headers["X-Robots-Tag"], "noindex, nofollow")
        self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])

    def test_internal_home_and_search_api_require_login(self):
        home = self.client.get("/")
        search = self.client.get("/search_all")
        self.assertEqual(home.status_code, 302)
        self.assertEqual(home.headers["Location"], "/login")
        self.assertEqual(search.status_code, 401)

    def test_template_contains_no_sensitive_columns_or_actions(self):
        template = (ROOT / "templates" / "public_civil_papers.html").read_text()
        for prohibited in (
            "Respondent", "Petitioner", "Address", "Download", "Edit", "Served By"
        ):
            self.assertNotIn(prohibited, template)
        self.assertIn("Served On", template)

    def test_public_brand_asset_is_present_and_referenced(self):
        template = (ROOT / "templates" / "public_civil_papers.html").read_text()
        asset = ROOT / "static" / "images" / "bcso-shield.webp"
        self.assertTrue(asset.is_file())
        self.assertGreater(asset.stat().st_size, 0)
        self.assertIn("images/bcso-shield.webp", template)


if __name__ == "__main__":
    unittest.main()
