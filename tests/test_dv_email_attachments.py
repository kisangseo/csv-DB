import base64
import io
import sys
import unittest
import zipfile
from unittest.mock import MagicMock, patch

sys.modules.setdefault("pyodbc", MagicMock())
import app as application


class DvEmailAttachmentTests(unittest.TestCase):
    def test_processed_backfill_batch_is_stable_and_bounded(self):
        messages = [
            {"id": "c", "receivedDateTime": "2026-03-03T00:00:00Z"},
            {"id": "a", "receivedDateTime": "2026-03-01T00:00:00Z"},
            {"id": "b", "receivedDateTime": "2026-03-02T00:00:00Z"},
        ]
        batch = application.select_processed_dv_email_batch(
            messages, limit=1, offset=1
        )
        self.assertEqual([message["id"] for message in batch], ["b"])

    def test_build_email_record_accepts_uppercase_field_labels(self):
        record, _ = application.build_dv_email_record({
            "subject": "DV Order",
            "entry_details": {
                "CASE NUMBER": "D-08-CV-26-002560",
                "RESPONDENT NAME": "Test Person",
                "ORDER TYPE": "Final Protective Order",
            },
            "blob_name": "dv_pdf/email_orders/order.pdf",
            "pdf_download": "/dv-pdf/file/dv_pdf/email_orders/order.pdf",
        })
        self.assertEqual(record["case_number"], "D-08-CV-26-002560")
        self.assertEqual(record["respondent_name"], "Test Person")
        self.assertEqual(record["order_type"], "Final Protective Order")
        self.assertEqual(record["blob_name"], "dv_pdf/email_orders/order.pdf")

    def test_selects_largest_non_inline_pdf(self):
        selected = application.select_dv_order_pdf_attachment([
            {"id": "inline", "name": "signature.pdf", "size": 9000, "isInline": True},
            {"id": "small", "name": "cover.pdf", "size": 1000, "isInline": False},
            {"id": "order", "name": "order.pdf", "size": 5000, "isInline": False},
            {"id": "image", "name": "logo.png", "size": 8000, "isInline": False},
        ])
        self.assertEqual(selected["id"], "order")

    def test_graph_attachment_content_is_downloaded_when_not_expanded(self):
        response = MagicMock()
        response.json.return_value = {"contentBytes": base64.b64encode(b"%PDF-test").decode("ascii")}
        response.raise_for_status.return_value = None
        with patch.object(application.requests, "get", return_value=response) as get_mock:
            content = application.download_graph_pdf_attachment(
                {"Authorization": "Bearer test"},
                "mailbox@example.gov",
                "message-id",
                {"id": "attachment-id", "name": "order.pdf"},
            )
        self.assertEqual(content, b"%PDF-test")
        self.assertIn("/attachments/attachment-id", get_mock.call_args.args[0])

    def test_dv_order_email_without_pdf_is_allowed(self):
        response = MagicMock()
        response.json.return_value = {
            "value": [{"id": "image", "name": "logo.png", "size": 100}]
        }
        response.raise_for_status.return_value = None
        with patch.object(application.requests, "get", return_value=response):
            attachment, content = application.get_graph_dv_order_pdf(
                {"Authorization": "Bearer test"}, "mailbox@example.gov", "message-id"
            )
        self.assertIsNone(attachment)
        self.assertIsNone(content)

    @patch.object(application, "get_dv_files_container")
    def test_email_pdf_blob_name_is_deterministic(self, container_mock):
        blob = MagicMock()
        blob.exists.return_value = False
        container_mock.return_value.get_blob_client.return_value = blob

        first = application.upload_dv_order_email_pdf(
            b"%PDF-test", "D-08-CV-26-002560", "message-1", "DV Order.pdf"
        )
        second = application.upload_dv_order_email_pdf(
            b"%PDF-test", "D-08-CV-26-002560", "message-1", "DV Order.pdf"
        )

        self.assertEqual(first, second)
        self.assertTrue(first.startswith("dv_pdf/email_orders/D-08-CV-26-002560/"))
        self.assertTrue(first.endswith("_DV_Order.pdf"))

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    @patch.object(application, "ingest_dv_email_payloads_for_run")
    def test_backfill_endpoint_explicitly_enables_processed_folder(self, ingest_mock):
        ingest_mock.return_value = {"status": "ok", "backfilled": 3}
        response = application.app.test_client().post(
            "/ingest-dv-orders?backfill_processed=1",
            headers={"X-Ingest-Key": "test-key"},
        )
        self.assertEqual(response.status_code, 200)
        ingest_mock.assert_called_once_with(
            include_processed=True,
            processed_limit=None,
            processed_offset=0,
            processed_only=True,
        )

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    @patch.object(application, "ingest_dv_email_payloads_for_run")
    def test_backfill_endpoint_passes_batch_parameters(self, ingest_mock):
        ingest_mock.return_value = {"status": "ok", "processed_batch_count": 5}
        response = application.app.test_client().post(
            "/ingest-dv-orders?backfill_processed=1&backfill_limit=5&backfill_offset=10",
            headers={"X-Ingest-Key": "test-key"},
        )
        self.assertEqual(response.status_code, 200)
        ingest_mock.assert_called_once_with(
            include_processed=True,
            processed_limit=5,
            processed_offset=10,
            processed_only=True,
        )

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    def test_backfill_endpoint_rejects_oversized_batch(self):
        response = application.app.test_client().post(
            "/ingest-dv-orders?backfill_processed=1&backfill_limit=100",
            headers={"X-Ingest-Key": "test-key"},
        )
        self.assertEqual(response.status_code, 400)

    @patch.dict(application.os.environ, {"RETURNS_INGEST_KEY": "test-key"})
    def test_backfill_endpoint_requires_ingest_key(self):
        response = application.app.test_client().post(
            "/ingest-dv-orders?backfill_processed=1"
        )
        self.assertEqual(response.status_code, 401)

    @patch.object(application, "reconcile_dv_service_attempts")
    @patch.object(application, "ensure_dv_service_attempts_table")
    @patch.object(application, "get_dv_files_container")
    @patch.object(application, "get_conn")
    def test_record_download_zips_original_and_attempt_pdf(
        self, get_conn_mock, container_mock, _ensure_mock, _reconcile_mock
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("D-08-CV-26-002560", "dv_pdf/email_orders/order.pdf")
        cursor.fetchall.return_value = [
            ("dv_pdf/service_attempts/attempt.pdf", "Service Attempt.pdf")
        ]
        get_conn_mock.return_value.cursor.return_value = cursor

        container = container_mock.return_value
        container.list_blobs.return_value = []

        def blob_client(name):
            blob = MagicMock()
            blob.download_blob.return_value.readall.return_value = name.encode("utf-8")
            props = MagicMock()
            props.metadata = {
                "original_filename": "DV Order.pdf" if "email_orders" in name else "Service Attempt.pdf"
            }
            props.content_settings.content_type = "application/pdf"
            blob.get_blob_properties.return_value = props
            return blob

        container.get_blob_client.side_effect = blob_client
        client = application.app.test_client()
        with client.session_transaction() as session:
            session["user_id"] = 1
        response = client.get(
            "/dv-pdf/files/download?case_number=D-08-CV-26-002560&record_id=42"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/zip")
        self.assertIn("D-08-CV-26-002560.zip", response.headers["Content-Disposition"])
        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            self.assertEqual(
                set(archive.namelist()), {"DV Order.pdf", "Service Attempt.pdf"}
            )


if __name__ == "__main__":
    unittest.main()
