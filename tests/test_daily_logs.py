import unittest

from daily_logs import search_daily_logs


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.description = [
            ("event_number",),
            ("received_at",),
            ("event_status",),
            ("activity_type",),
            ("address",),
            ("city",),
            ("state",),
            ("postal_code",),
            ("additional_report",),
            ("event_name",),
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
            "2026-06-10T08:30:00",
            "Open",
            "Patrol",
            "100 Main St",
            "Baltimore",
            "MD",
            "21201",
            "Yes",
            "Example Name",
        )
        connection = FakeConnection([row])

        records = search_daily_logs(connection, self.base_filters())

        self.assertEqual(records[0]["event_number"], "E-123")
        self.assertEqual(records[0]["received_at"], "2026-06-10T08:30:00")
        self.assertEqual(records[0]["postal_code"], "21201")
        self.assertEqual(records[0]["event_name"], "Example Name")
        self.assertIn("e.[name]", connection.cursor_instance.sql)
        self.assertIn("JSON_VALUE(e.raw_payload, '$.Name')", connection.cursor_instance.sql)
        self.assertIn("AS event_name", connection.cursor_instance.sql)
        self.assertIn("FROM dbo.esri_events AS e", connection.cursor_instance.sql)
        self.assertIn("ORDER BY e.received_at DESC, e.id DESC", connection.cursor_instance.sql)

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

        self.assertIn("LOWER(COALESCE(", connection.cursor_instance.sql)
        self.assertIn("e.[name]", connection.cursor_instance.sql)
        self.assertIn("LOWER(COALESCE(e.event_number, '')) LIKE ?", connection.cursor_instance.sql)
        self.assertIn("CAST(e.received_at AS date) BETWEEN", connection.cursor_instance.sql)
        self.assertEqual(
            connection.cursor_instance.params,
            ["%smith%", "%event-9%", "2026-06-01", "2026-06-10"],
        )

    def test_applies_last_x_days_when_no_date_range_is_selected(self):
        filters = self.base_filters()
        filters["last_x_days"] = "7"
        connection = FakeConnection([])

        search_daily_logs(connection, filters)

        self.assertIn("e.received_at >= DATEADD(day, -?", connection.cursor_instance.sql)
        self.assertEqual(connection.cursor_instance.params, [7])


if __name__ == "__main__":
    unittest.main()
