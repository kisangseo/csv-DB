import unittest

from returns import (
    derived_signature_status,
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

    def test_every_ingested_return_is_signed(self):
        self.assertEqual(derived_signature_status({"signature_value": None}), "Signed")

    def test_date_signed_is_not_required_for_signed_workflow(self):
        self.assertEqual(
            derived_signature_status({"signature_value": None, "date_signed": "2026-08-19"}),
            "Signed",
        )

    def test_normalizes_non_est_variants(self):
        self.assertEqual(normalize_service_disposition("NON-EST"), "Non Est")


if __name__ == "__main__":
    unittest.main()
