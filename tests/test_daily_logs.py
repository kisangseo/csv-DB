import unittest

from daily_logs import search_daily_logs


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.description = [
            ("event_number",),
            ("arrival_time",),
            ("event_status",),
            ("activity_type",),
            ("address",),
            ("city",),
            ("state",),
            ("postal_code",),
            ("notes_or_narrative",),
            ("additional_report",),
            ("name",),
            ("radio_id",),
        ]
        self.sql = None
        self.params = None

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)

    def cursor(self):
        return self.cursor_instance


class SearchDailyLogsTests(unittest.TestCase):
    def base_filters(self):
        return {
            "query": "",
            "case_number": None,
            "date_start": None,
            "date_end": None,
            "last_x_days": None,
        }

    def test_maps_esri_event_columns_to_dicts(self):
        row = (
            "E-123",
            "2025-12-12 11:09:22",
            "Open",
            "Patrol",
            "100 Main St",
            "Baltimore",
            "MD",
            "21201",
            "Assisted resident with a lockout.",
            "Yes",
            "Example Name",
            9820,
        )
        connection = FakeConnection([row])

        records = search_daily_logs(connection, self.base_filters())

        self.assertEqual(records[0]["event_number"], "E-123")
        self.assertEqual(records[0]["arrival_time"], "2025-12-12 11:09:22")
        self.assertEqual(records[0]["postal_code"], "21201")
        self.assertEqual(records[0]["notes_or_narrative"], "Assisted resident with a lockout.")
        self.assertEqual(records[0]["name"], "Example Name")
        self.assertEqual(records[0]["radio_id"], 9820)
        self.assertIn("e.[name]", connection.cursor_instance.sql)
        self.assertNotIn("JSON_VALUE", connection.cursor_instance.sql)
        self.assertIn("e.notes_or_narrative", connection.cursor_instance.sql)
        self.assertIn("e.[name] AS name", connection.cursor_instance.sql)
        self.assertIn("e.radio_id", connection.cursor_instance.sql)
        self.assertIn("NULLIF(LTRIM(RTRIM(e.event_number)), '')", connection.cursor_instance.sql)
        self.assertIn("THEN e.generated_event_number", connection.cursor_instance.sql)
        self.assertIn("AS event_number", connection.cursor_instance.sql)
        self.assertIn("FROM dbo.esri_events AS e", connection.cursor_instance.sql)
        self.assertIn("TRY_CONVERT(BIGINT, e.arrival_time) / 1000", connection.cursor_instance.sql)
        self.assertIn("TRY_CONVERT(INT,", connection.cursor_instance.sql)
        self.assertIn("AT TIME ZONE 'UTC'", connection.cursor_instance.sql)
        self.assertIn("AT TIME ZONE 'Eastern Standard Time'", connection.cursor_instance.sql)
        self.assertIn("AS arrival_time", connection.cursor_instance.sql)
        self.assertIn("ORDER BY", connection.cursor_instance.sql)
        self.assertNotIn("AS received_at", connection.cursor_instance.sql)

    def test_applies_name_event_number_and_date_range_filters(self):
        filters = self.base_filters()
        filters.update(
            {
                "query": "Smith",
                "case_number": "EVENT-9",
                "date_start": "2026-06-01",
                "date_end": "2026-06-10",
            }
        )
        connection = FakeConnection([])

        search_daily_logs(connection, filters)

        self.assertIn("LOWER(COALESCE(e.[name], '')) LIKE ?", connection.cursor_instance.sql)
        self.assertIn("LOWER(COALESCE(COALESCE(", connection.cursor_instance.sql)
        self.assertIn("NULLIF(LTRIM(RTRIM(e.event_number)), '')", connection.cursor_instance.sql)
        self.assertIn("THEN e.generated_event_number", connection.cursor_instance.sql)
        self.assertIn("CAST(CAST(", connection.cursor_instance.sql)
        self.assertIn("AS date) BETWEEN CAST(? AS date) AND CAST(? AS date)", connection.cursor_instance.sql)
        self.assertEqual(
            connection.cursor_instance.params,
            ["%smith%", "%event-9%", "2026-06-01", "2026-06-10"],
        )

    def test_applies_last_x_days_when_no_date_range_is_selected(self):
        filters = self.base_filters()
        filters["last_x_days"] = "7"
        connection = FakeConnection([])

        search_daily_logs(connection, filters)

        self.assertIn(">= DATEADD(day, -?", connection.cursor_instance.sql)
        self.assertIn("Eastern Standard Time", connection.cursor_instance.sql)
        self.assertEqual(connection.cursor_instance.params, [7])


if __name__ == "__main__":
    unittest.main()
