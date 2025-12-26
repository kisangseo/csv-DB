from ingest import ingest_warrants_csv, ingest_all_odyssey_civil_blobs

if __name__ == "__main__":
    print("=== STARTING WARRANTS INGEST ===")
    ingest_warrants_csv()

    print("=== STARTING ODYSSEY CIVIL INGEST ===")
    ingest_all_odyssey_civil_blobs()

    print("=== INGEST COMPLETE ===")