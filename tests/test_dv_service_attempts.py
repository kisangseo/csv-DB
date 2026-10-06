import unittest
import sys
from unittest.mock import MagicMock, patch

sys.modules.setdefault("pyodbc", MagicMock())
import app as application


class DvServiceAttemptTests(unittest.TestCase):
    def test_normalizes_case_number_for_matching(self):
        self.assertEqual(application.normalize_dv_case_number("D-08-CV-26-002560"), "D08CV26002560")
        self.assertEqual(application.normalize_dv_case_number(" d08 cv 26002560 "), "D08CV26002560")

    def test_parses_every_table_field_from_cognito_email(self):
        body = """
        <table>
          <tr><td>CASE NUMBER</td><td>D08CV26002560</td></tr>
          <tr><td>ATTEMPT DISPOSITION</td><td>Attempted</td></tr>
          <tr><td>WHY SHOULD IT NOT BE ATTEMPTED AGAIN</td><td>Respondent&nbsp;Lives Elsewhere</td></tr>
          <tr><td>NOTES FROM ATTEMPT</td><td><strong>No answer</strong> at residence</td></tr>
        </table>
        """
        fields = application.parse_service_attempt_email_fields(body)
        self.assertEqual(fields["CASE NUMBER"], "D08CV26002560")
        self.assertEqual(fields["ATTEMPT DISPOSITION"], "Attempted")
        self.assertEqual(fields["WHY SHOULD IT NOT BE ATTEMPTED AGAIN"], "Respondent Lives Elsewhere")
        self.assertEqual(fields["NOTES FROM ATTEMPT"], "No answer at residence")

    def test_field_lookup_is_case_and_punctuation_insensitive(self):
        fields = {"Will There Be An Additional Report?": "No"}
        self.assertEqual(
            application._service_attempt_field(fields, "WILL THERE BE AN ADDITIONAL REPORT"),
            "No",
        )

    def test_plain_text_email_fields_are_preserved(self):
        fields = application.parse_service_attempt_email_fields(
            "CASE NUMBER: D08CV26002560\nNOTES FROM ATTEMPT: No answer"
        )
        self.assertEqual(fields["CASE NUMBER"], "D08CV26002560")
        self.assertEqual(fields["NOTES FROM ATTEMPT"], "No answer")

    def test_dedupe_key_is_stable_and_attachment_specific(self):
        first = application.service_attempt_dedupe_key("message-1", "attachment-1")
        self.assertEqual(first, application.service_attempt_dedupe_key("message-1", "attachment-1"))
        self.assertNotEqual(first, application.service_attempt_dedupe_key("message-1", "attachment-2"))
        self.assertEqual(len(first), 64)

    def test_attempt_download_requires_login(self):
        client = application.app.test_client()
        response = client.get("/dv-pdf/attempts/1/download")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/login"))

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    def test_attempt_ingest_endpoint_requires_key(self):
        client = application.app.test_client()
        response = client.get("/ingest-dv-service-attempts")
        self.assertEqual(response.status_code, 401)

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    @patch.object(application, "ingest_dv_service_attempt_emails_for_run")
    def test_attempt_ingest_endpoint_runs_synchronously(self, ingest_mock):
        ingest_mock.return_value = {
            "status": "ok",
            "candidates": 2,
            "ingested": 2,
            "moved_to_processed": 2,
            "failed": 0,
        }
        client = application.app.test_client()
        response = client.post(
            "/ingest-dv-service-attempts",
            headers={"X-Ingest-Key": "test-key"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["ingested"], 2)
        ingest_mock.assert_called_once_with()

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    @patch.object(application, "ingest_dv_service_attempt_emails_for_run")
    def test_attempt_ingest_endpoint_reports_failure(self, ingest_mock):
        ingest_mock.return_value = {"status": "failed", "error": "Graph failed", "ingested": 0}
        client = application.app.test_client()
        response = client.get(
            "/ingest-dv-service-attempts",
            headers={"X-Ingest-Key": "test-key"},
        )
        self.assertEqual(response.status_code, 500)


if __name__ == "__main__":
    unittest.main()
