import ast
import unittest
from pathlib import Path

from returns import (
    allowed_return_statuses,
    derived_signature_status,
    is_hard_copy_return,
    normalize_service_disposition,
    normalize_return_payload,
    parse_cognito_entry_details,
    payload_from_export_row,
)


class ReturnsParsingTests(unittest.TestCase):
    def test_return_download_filename_uses_case_number(self):
        app_path = Path(__file__).resolve().parents[1] / "app.py"
        module = ast.parse(app_path.read_text())
        function = next(
            node for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "safe_return_download_filename"
        )
        namespace = {"re": __import__("re")}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(app_path), "exec"), namespace)
        self.assertEqual(
            namespace["safe_return_download_filename"]("D-01-CV-26-029848"),
            "Baltimore City Sheriff's Office Return - D-01-CV-26-029848.pdf",
        )

    def test_email_petitioner_is_not_overwritten_by_concatenated_pdf_fields(self):
        app_path = Path(__file__).resolve().parents[1] / "app.py"
        module = ast.parse(app_path.read_text())
        function = next(
            node for node in module.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "build_return_email_payload"
        )
        namespace = {
            "parse_cognito_entry_details": parse_cognito_entry_details,
            "normalize_return_payload": normalize_return_payload,
            "parse_graph_datetime": lambda value: value,
            "CIVIL_PAPERS_CONTAINER_NAME": "civilpapers",
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(app_path), "exec"), namespace)
        html = """
        <table>
          <tr><td>DOCUMENT</td><td>C-02-CR-26-000424</td></tr>
          <tr><td>PETITIONER</td><td>STATE OF MARYLAND</td></tr>
          <tr><td>RESP NAME</td><td>NAKEI HAWKINS</td></tr>
        </table>
        """
        parsed_pdf = {
            "case_number": "C-02-CR-26-000424",
            "petitioner_name": (
                "STATE OF MARYLAND Person Served NAKEI HAWKINS "
                "Address 1037 Reverdy Rd, Baltimore, MD"
            ),
            "respondent_name": "NAKEI HAWKINS",
        }
        payload = namespace["build_return_email_payload"](
            {"body": {"content": html}}, {}, parsed_pdf, "return.pdf", "records@example.com"
        )
        self.assertEqual(payload["petitioner_name"], "STATE OF MARYLAND")
        self.assertEqual(payload["respondent_name"], "NAKEI HAWKINS")

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

    def test_returns_schema_allows_hard_copy_returned(self):
        returns_source = (Path(__file__).resolve().parents[1] / "returns.py").read_text()
        self.assertIn("CK_Returns_bcso_status", returns_source)
        self.assertIn("'Hard Copy Returned'", returns_source)
        self.assertIn("DROP CONSTRAINT", returns_source)

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
        self.assertIn('"--hard-copy-xlsx"', importer_source)
        self.assertIn('"--hard-copy-zip"', importer_source)
        self.assertIn("sources.append((args.hard_copy_xlsx, args.hard_copy_zip, None))", importer_source)
        self.assertIn('disposition_folder = f"hard-copy/{disposition_folder}"', importer_source)

    def test_existing_backfill_can_be_repaired_by_pdf_filename(self):
        returns_source = (Path(__file__).resolve().parents[1] / "returns.py").read_text()
        self.assertIn("original_filename and case_number", returns_source)
        self.assertIn("COALESCE(original_filename, '')", returns_source)

    def test_returns_default_to_latest_real_user_activity(self):
        returns_source = (Path(__file__).resolve().parents[1] / "returns.py").read_text()
        self.assertIn("OUTER APPLY (", returns_source)
        self.assertIn("activity.mdec_return_id = returns_record.mdec_return_id", returns_source)
        self.assertIn("COALESCE(activity.actor_email, '') NOT LIKE 'system:%'", returns_source)
        self.assertIn(
            "ORDER BY COALESCE(last_user_action.last_action_at, returns_record.created_at, submitted_at) DESC",
            returns_source,
        )

    def test_return_activity_is_displayed_in_eastern_time(self):
        returns_source = (Path(__file__).resolve().parents[1] / "returns.py").read_text()
        self.assertIn(
            "created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time'",
            returns_source,
        )

    def test_civil_download_suppresses_legacy_mdec_duplicate(self):
        app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text()
        self.assertIn("return_pdf.source_document_id NOT LIKE 'combined:%'", app_source)
        self.assertIn("combined_pdf.source_document_id LIKE 'combined:%'", app_source)
        self.assertIn("download_names=download_names", app_source)

    def test_processing_lock_is_atomic_and_fifteen_minutes(self):
        source = (Path(__file__).resolve().parents[1] / "returns.py").read_text()
        self.assertIn("class ReturnProcessingConflict", source)
        self.assertIn("WITH (UPDLOCK, ROWLOCK)", source)
        self.assertIn("DATEADD(MINUTE, 15, SYSUTCDATETIME())", source)
        self.assertIn('"processing_taken_over"', source)


    def test_any_authenticated_user_can_update_return_status(self):
        app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text()
        route_start = app_source.index("def patch_return_status(return_id):")
        route_end = app_source.index(
            '@app.route("/returns/<int:return_id>/download"', route_start
        )
        route_source = app_source[route_start:route_end]
        self.assertIn('if "user_id" not in session:', route_source)
        self.assertNotIn("can_edit_records()", route_source)
        self.assertNotIn("permission to update return status", route_source)
        self.assertIn('return jsonify({"error": f"Unable to update status: {exc}"}), 500', route_source)


if __name__ == "__main__":
    unittest.main()
