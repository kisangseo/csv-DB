"""One-time Returns backfill from the mailbox processed folder."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import ingest_civil_return_email_payloads_for_run


if __name__ == "__main__":
    print(ingest_civil_return_email_payloads_for_run(source_folder="processed", move_to_processed=False))
