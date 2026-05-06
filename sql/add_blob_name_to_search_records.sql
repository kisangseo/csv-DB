-- Optional compatibility migration:
-- Adds blob_name to search.records for environments where WOR upload code
-- still writes directly to search.records.blob_name.
-- Safe to run multiple times.

IF COL_LENGTH('search.records', 'blob_name') IS NULL
BEGIN
    ALTER TABLE search.records
    ADD blob_name NVARCHAR(512) NULL;
END
GO

