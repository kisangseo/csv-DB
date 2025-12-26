from ingest import ingest_warrants_csv, ingest_all_odyssey_civil_blobs

if __name__ == "__main__":
    ingest_warrants_csv()
    ingest_all_odyssey_civil_blobs()