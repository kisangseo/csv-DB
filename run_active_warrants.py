from ingest import ingest_bcso_active_warrants_csv

if __name__ == "__main__":
    print("=== Starting BCSO Active Warrants ingest ===")
    ingest_bcso_active_warrants_csv()
    print("=== Done ===")