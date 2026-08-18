# Returns setup

## Required application settings

Configure these values in Azure App Service and restart the application:

- `AZURE_STORAGE_CONNECTION_STRING`
- the existing SQL settings used by `db_connect.py`
- the existing Microsoft Graph settings: `MS_GRAPH_TENANT_ID`, `MS_GRAPH_CLIENT_ID`, and `MS_GRAPH_CLIENT_SECRET`
- `RETURNS_INGEST_KEY`: a new random secret used only by the scheduled Returns scan

Add the same `RETURNS_INGEST_KEY` value as a GitHub Actions repository secret. The
`run-returns-ingest` workflow calls the protected endpoint every 10 minutes.

## Initial import

Copy the two XLSX exports and two ZIP files to a temporary folder in the deployed
App Service environment, then run:

```bash
python scripts/import_returns_initial.py \
  --served-xlsx "/tmp/BaltimoreCitySheriffsOfficeReturn2 (1).xlsx" \
  --served-zip "/tmp/BaltimoreCitySheriffsOfficeReturn Served.zip" \
  --non-est-xlsx "/tmp/BaltimoreCitySheriffsOfficeReturn2.xlsx" \
  --non-est-zip "/tmp/BaltimoreCitySheriffsOfficeReturn2 non est.zip"
```

The importer validates every row/PDF pair before making changes, uploads files to
`civilpapers/return_pdfs/served/...` or `civilpapers/return_pdfs/non-est/...`, and
upserts the corresponding `search.Returns` record. It is safe to run again: the
same deterministic Blob names are overwritten and Cognito entry numbers are
updated rather than duplicated.

Do not commit the XLSX or ZIP source files to this public repository.

## Optional processed-folder backfill

To scan historical matching messages already in the mailbox's `processed` folder
without moving them again:

```bash
python scripts/backfill_returns_from_processed.py
```
