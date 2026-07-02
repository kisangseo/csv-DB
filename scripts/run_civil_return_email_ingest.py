"""Run the Baltimore City Sheriff's Office return-PDF email ingest once.

This is intended for manual App Service/console runs when you want to process
return emails without running the full hourly ingest pipeline.
"""

from app import ingest_civil_return_email_payloads_for_run


if __name__ == "__main__":
    print(ingest_civil_return_email_payloads_for_run())
