import unittest
from pathlib import Path

from returns import (
    allowed_return_statuses,
    derived_signature_status,
    is_hard_copy_return,
    normalize_service_disposition,
    parse_cognito_entry_details,
    payload_from_export_row,
)


class ReturnsParsingTests(unittest.TestCase):
    def test_parses_cognito_entry_details_table(self):
        html = """
        <table>
          <tr><td>DOCUMENT</td><td>D-00-CV-00-000001</td></tr>
          <tr><td>RESP NAME</td><td>TEST RESPONDENT</td></tr>
          <tr><td>SERVICE DISP</td><td>Served</td></tr>
          <tr><td>SIGNATURE</td><td>Captured</td></tr>
          <tr><td>DATE SIGNED</td><td>8/12/2026</td></tr>
        </table>
        """
        payload = parse_cognito_entry_details(html)
        self.assertEqual(payload["case_number"], "D-00-CV-00-000001")
        self.assertEqual(payload["respondent_name"], "TEST RESPONDENT")
        self.assertEqual(payload["service_disposition"], "Served")
        self.assertEqual(payload["signature_value"], "Captured")

    def test_maps_export_document_to_case_number_and_signature(self):
        payload = payload_from_export_row(
            {
                "#": 8075,
                "Document": "00-C-00-000001",
                "Service Disp": "Non Est",
                "Signature": "Captured",
            }
        )
        self.assertEqual(payload["cognito_entry_number"], "8075")
        self.assertEqual(payload["case_number"], "00-C-00-000001")
        self.assertEqual(payload["service_disposition"], "Non Est")
        self.assertEqual(payload["signature_status"], "Signed")
        self.assertEqual(payload["bcso_status"], "Signed")

    def test_maps_new_compact_cognito_export_headers(self):
        payload = payload_from_export_row(
            {
                "BaltimoreCitySheriffsOfficeRetu_Id": 8123,
                "Entry_DateSubmitted": "2026-08-20 08:30:00",
                "Document": "D-01-CV-26-029285",
                "RespName": "DATROWN BANKS",
                "Petitioner": "LETECIA ROLLINS ESQUIRE",
                "DateAttempted": "2026-08-19",
                "ServiceDisp": "Served",
                "MemberReporting": "Sergeant Christopher Tillery",
                "ReturnSequence": "0399",
            }
        )
        self.assertEqual(payload["cognito_entry_number"], "8123")
        self.assertEqual(payload["respondent_name"], "DATROWN BANKS")
        self.assertEqual(payload["attempt_date"], "2026-08-19")
        self.assertEqual(payload["service_disposition"], "Served")
        self.assertEqual(payload["member_reporting"], "Sergeant Christopher Tillery")
        self.assertEqual(payload["return_sequence"], "0399")

    def test_every_ingested_return_is_signed(self):
        self.assertEqual(derived_signature_status({"signature_value": None}), "Signed")

    def test_date_signed_is_not_required_for_signed_workflow(self):
        self.assertEqual(
            derived_signature_status({"signature_value": None, "date_signed": "2026-08-19"}),
            "Signed",
        )

    def test_normalizes_non_est_variants(self):
        self.assertEqual(normalize_service_disposition("NON-EST"), "Non Est")

    def test_exact_hard_copy_filter_groups(self):
        signed = {"signature_value": "Captured"}
        positives = (
            {**signed, "document_type": "JV - Juvenile", "case_number": "C-24-XX"},
            {**signed, "type_of_child_support": "Show Cause", "case_number": "C-24-CV-1"},
            {**signed, "case_number": "C-24-CR-1"},
            {**signed, "case_number": "24-P-1"},
            {**signed, "case_number": "24-D-1"},
            {**signed, "case_number": "c-24-jv-1"},
        )
        for payload in positives:
            with self.subTest(payload=payload):
                self.assertTrue(is_hard_copy_return(payload))

    def test_hard_copy_filter_requires_signature(self):
        self.assertFalse(is_hard_copy_return({"case_number": "24-P-1"}))

    def test_hard_copy_filter_rejects_nonmatching_returns(self):
        self.assertFalse(is_hard_copy_return({
            "signature_value": "Captured",
            "document_type": "RFS - Request for Service",
            "case_number": "D-01-CV-26-1",
        }))
        self.assertFalse(is_hard_copy_return({
            "signature_value": "Captured",
            "document_type": "JV - Juvenile",
            "case_number": "D-01-CV-26-1",
        }))

    def test_status_choices_follow_hard_copy_filter(self):
        hard_copy = {"signature_value": "Captured", "case_number": "24-P-1"}
        normal = {"signature_value": "Captured", "case_number": "D-01-CV-26-1"}
        self.assertIn("Hard Copy Returned", allowed_return_statuses(hard_copy))
        self.assertNotIn("Uploaded", allowed_return_statuses(hard_copy))
        self.assertIn("Uploaded", allowed_return_statuses(normal))
        self.assertNotIn("Hard Copy Returned", allowed_return_statuses(normal))

    def test_system_activity_is_hidden_and_importer_supports_fresh_replace(self):
        root = Path(__file__).resolve().parents[1]
        returns_source = (root / "returns.py").read_text()
        importer_source = (root / "scripts" / "import_returns_initial.py").read_text()
        self.assertIn("COALESCE(actor_email, '') NOT LIKE 'system:%'", returns_source)
        self.assertIn('"--replace-all"', importer_source)
        self.assertIn('DELETE FROM search.mdec_return_activity_log', importer_source)
        self.assertIn('DELETE FROM search.Returns', importer_source)

    def test_initial_import_skips_unmatched_rows_and_pdfs(self):
        importer_source = (
            Path(__file__).resolve().parents[1] / "scripts" / "import_returns_initial.py"
        ).read_text()
        self.assertIn("pdfs_by_case = defaultdict(deque)", importer_source)
        self.assertIn("rows_without_pdf", importer_source)
        self.assertIn("pdfs_without_row", importer_source)

    def test_existing_backfill_can_be_repaired_by_pdf_filename(self):
        returns_source = (Path(__file__).resolve().parents[1] / "returns.py").read_text()
        self.assertIn("original_filename and case_number", returns_source)
        self.assertIn("COALESCE(original_filename, '')", returns_source)


if __name__ == "__main__":
    unittest.main()
