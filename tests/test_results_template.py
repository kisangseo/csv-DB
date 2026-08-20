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
            'headers = ["Event Number","Arrival Time","Event Status","Activity Type","Address","Notes","Additional Report","Deputy Name","Radio ID"]',
            self.template,
        )
        self.assertIn(',"Arrival Time": "arrival_time"', self.template)
        self.assertIn(',"Notes": "notes_or_narrative"', self.template)
        self.assertIn(',"Deputy Name": "name"', self.template)
        self.assertIn(',"Radio ID": "radio_id"', self.template)

    def test_returns_uses_existing_search_and_has_details_activity_and_status(self):
        self.assertIn('"Returns",', self.template)
        self.assertIn('headers = ["Case Number","Respondent","Petitioner","Service Disposition","Status","Date Attempted / Served","Deputy Reporting"]', self.template)
        self.assertIn('title.textContent = "Activity Log";', self.template)
        self.assertIn('["Signed", "Hard Copy Returned", "Hold", "Pending"]', self.template)
        self.assertIn('badge.textContent = "HARD COPY REQUIRED";', self.template)
        self.assertIn('toggleReturnDetailRow(tr, row, headers.length);', self.template)
        self.assertNotIn('placeholder="Search returns"', self.template)

    def test_returns_queue_loads_collapsed_and_hides_uploaded_only_by_default(self):
        self.assertIn('department: "Returns",', self.template)
        self.assertIn('returnsQueue: true,', self.template)
        self.assertIn('preserveExistingSections: true,', self.template)
        self.assertIn('params.set("returns_queue", "1");', self.template)
        self.assertIn('summaryRow.bcso_status === "Uploaded"', self.template)
        self.assertIn('document.createTextNode(" Show uploaded")', self.template)
        self.assertIn('params.set("include_uploaded", "1");', self.template)
        self.assertIn('uploadedCheckbox.checked = Boolean(options.includeUploaded);', self.template)

    def test_every_returns_header_sorts_ascending_then_descending(self):
        self.assertIn('wireReturnsHeaderSort(table, th, headerIndex, h);', self.template)
        self.assertIn('function sortReturnsTable(table, columnIndex, headerLabel, direction)', self.template)
        self.assertIn('currentDirection === RETURNS_SORT_DIRECTIONS.asc', self.template)
        self.assertIn('numeric: true', self.template)
        self.assertIn('row.classList.contains("return-detail-row")', self.template)


if __name__ == "__main__":
    unittest.main()
