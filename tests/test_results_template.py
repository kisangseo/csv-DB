import unittest
from pathlib import Path


TEMPLATE = Path(__file__).resolve().parents[1] / "templates" / "index.html"


class ResultsTemplateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = TEMPLATE.read_text()

    def test_initial_sections_show_zero_and_are_collapsed(self):
        self.assertIn('title.textContent = `▶ ${label} — 0 records`;', self.template)

    def test_search_results_start_collapsed_and_keep_count_in_heading(self):
        self.assertIn('content.className = "department-content collapsed";', self.template)
        self.assertIn('title.textContent = `▶ ${baseTitle} — ${recordLabel}`;', self.template)
        self.assertIn(
            'title.textContent = `${collapsed ? "▶" : "▼"} ${baseTitle} — ${recordLabel}`;',
            self.template,
        )

    def test_zero_result_departments_are_not_filtered_out(self):
        self.assertNotIn("if (hasActiveSearchFilters && count === 0)", self.template)

    def test_read_only_daily_logs_keeps_notes_deputy_name_and_radio_id_columns(self):
        self.assertIn(
            "const dataHeaders = showActions ? headers.slice(0, -1) : headers;",
            self.template,
        )
        self.assertIn(
            'headers = ["Event Number","Received At","Event Status","Activity Type","Address","Notes","Additional Report","Deputy Name","Radio ID"]',
            self.template,
        )
        self.assertIn(',"Notes": "notes_or_narrative"', self.template)
        self.assertIn(',"Deputy Name": "name"', self.template)
        self.assertIn(',"Radio ID": "radio_id"', self.template)


if __name__ == "__main__":
    unittest.main()
