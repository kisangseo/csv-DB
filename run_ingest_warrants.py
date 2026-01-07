from ingest import ingest_warrants_csv, ingest_all_odyssey_civil_blobs,  ingest_wor_csv, ingest_bcso_active_warrants_csv

if __name__ == "__main__":
    print("=== STARTING WARRANTS INGEST ===")
    ingest_warrants_csv()

    print("=== STARTING ODYSSEY CIVIL INGEST ===")
    ingest_all_odyssey_civil_blobs()

    print("=== STARTING WOR INGEST ===")
    ingest_wor_csv()

    print("=== STARTING ACTIVE WARRANT INGEST ===")
    ingest_bcso_active_warrants_csv()


    print("=== INGEST COMPLETE ===")