"""One-time Returns backfill from the mailbox processed folder."""

from app import ingest_civil_return_email_payloads_for_run


if __name__ == "__main__":
    print(ingest_civil_return_email_payloads_for_run(source_folder="processed", move_to_processed=False))
