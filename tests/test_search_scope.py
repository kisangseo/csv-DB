import unittest
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

sys.modules.setdefault("pyodbc", MagicMock())
import app as application
from search_sql import _build_filters_sql


class SearchScopeTests(unittest.TestCase):
    def setUp(self):
        application.app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.client = application.app.test_client()
        with self.client.session_transaction() as session:
            session["user_id"] = "tester@example.gov"

    def test_search_sections_default_to_all_and_support_clear_all(self):
        default_filters = application.parse_search_filters({})
        cleared_filters = application.parse_search_filters({"search_sections": "__none__"})
        selected_filters = application.parse_search_filters(
            {"search_sections": "civil_papers,returns,not-a-real-section"}
        )

        self.assertEqual(default_filters["search_sections"], set(application.SEARCH_SECTION_KEYS))
        self.assertEqual(cleared_filters["search_sections"], set())
        self.assertEqual(selected_filters["search_sections"], {"civil_papers", "returns"})

    def test_department_scope_is_parameterized(self):
        where_sql, params = _build_filters_sql(
            "",
            departments=["Civil Papers", "DOC Jail Population"],
        )

        self.assertIn("COALESCE(department, '')", where_sql)
        self.assertIn("IN (?, ?)", where_sql)
        self.assertEqual(params, ["civil papers", "doc jail population"])

    @patch.object(application, "ENABLE_APT_BACKFILL_ON_SEARCH", False)
    @patch.object(application, "read_dv_pdf_records")
    @patch.object(application, "search_returns")
    @patch.object(application, "search_daily_logs")
    @patch.object(application, "search_by_name")
    @patch.object(application, "enrich_civil_return_pdf_history")
    @patch.object(application, "get_conn")
    def test_route_skips_unselected_data_sources(
        self,
        get_conn,
        enrich_history,
        search_by_name,
        search_daily_logs,
        search_returns,
        read_dv_pdf_records,
    ):
        get_conn.return_value = MagicMock()
        search_by_name.return_value = []
        enrich_history.return_value = []

        response = self.client.get(
            "/search_all?search_sections=civil_papers&intake_date=2026-08-01%20to%202026-09-23"
        )

        self.assertEqual(response.status_code, 200)
        search_by_name.assert_called_once()
        self.assertEqual(search_by_name.call_args.kwargs["departments"], ["Civil Papers"])
        search_daily_logs.assert_not_called()
        search_returns.assert_not_called()
        read_dv_pdf_records.assert_not_called()
        self.assertEqual(response.get_json(), {"Civil Papers": {"count": 0, "records": []}})

    def test_template_has_all_search_section_controls(self):
        template = (Path(__file__).resolve().parents[1] / "templates" / "index.html").read_text()
        self.assertIn('id="searchSectionsMenu"', template)
        for key in application.SEARCH_SECTION_KEYS:
            self.assertIn(f'value="{key}"', template)

    def test_civil_pdf_history_batches_large_record_sets(self):
        cursor = MagicMock()
        cursor.description = [(name,) for name in (
            "id", "record_id", "case_number", "intake_date", "email_subject",
            "email_from", "email_received_at", "original_filename", "blob_name",
            "parse_status", "created_at",
        )]
        cursor.fetchall.return_value = []
        connection = MagicMock()
        connection.cursor.return_value = cursor

        with patch.object(application, "get_conn", return_value=connection), patch.object(
            application, "ensure_civil_return_pdfs_table"
        ):
            result = application.fetch_civil_return_pdf_history_for_records(range(1, 2102))

        self.assertEqual(result, {})
        self.assertEqual(cursor.execute.call_count, 3)
        self.assertTrue(all(len(call.args) <= 1001 for call in cursor.execute.call_args_list))
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
