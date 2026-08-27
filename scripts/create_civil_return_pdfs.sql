IF SCHEMA_ID('search') IS NULL
    EXEC('CREATE SCHEMA search');
GO

IF OBJECT_ID('search.civil_return_pdfs', 'U') IS NULL
BEGIN
    CREATE TABLE search.civil_return_pdfs (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        record_id INT NULL,
        case_number NVARCHAR(100) NOT NULL,
        intake_date DATE NULL,
        mailbox NVARCHAR(320) NULL,
        message_id NVARCHAR(1000) NOT NULL,
        conversation_id NVARCHAR(1000) NULL,
        email_subject NVARCHAR(1000) NULL,
        email_from NVARCHAR(500) NULL,
        email_received_at DATETIME2 NULL,
        attachment_id NVARCHAR(1000) NOT NULL,
        original_filename NVARCHAR(500) NULL,
        blob_name NVARCHAR(1000) NOT NULL,
        content_type NVARCHAR(255) NULL,
        pdf_case_number NVARCHAR(100) NULL,
        pdf_intake_date DATE NULL,
        pdf_respondent_name NVARCHAR(500) NULL,
        pdf_service_disposition NVARCHAR(255) NULL,
        pdf_deputy NVARCHAR(500) NULL,
        parse_status NVARCHAR(50) NOT NULL CONSTRAINT DF_civil_return_pdfs_parse_status DEFAULT ('matched'),
        parse_error NVARCHAR(MAX) NULL,
        source_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL CONSTRAINT DF_civil_return_pdfs_created_at DEFAULT SYSUTCDATETIME()
    );
END;
GO


IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_civil_return_pdfs_record_created'
      AND object_id = OBJECT_ID('search.civil_return_pdfs')
)
BEGIN
    CREATE INDEX IX_civil_return_pdfs_record_created
        ON search.civil_return_pdfs(record_id, created_at DESC);
END;
GO

IF OBJECT_ID('search.mdec_civil_sync_status', 'U') IS NULL
BEGIN
    CREATE TABLE search.mdec_civil_sync_status (
        source_document_id NVARCHAR(200) NOT NULL PRIMARY KEY,
        case_number NVARCHAR(100) NULL,
        sync_status NVARCHAR(30) NOT NULL,
        matched_record_id INT NULL,
        matched_pdf_id INT NULL,
        attempt_count INT NOT NULL CONSTRAINT DF_mdec_civil_sync_attempt_count DEFAULT (0),
        last_attempt_at DATETIME2 NULL,
        next_retry_at DATETIME2 NULL,
        last_error NVARCHAR(2000) NULL,
        completed_at DATETIME2 NULL,
        updated_at DATETIME2 NOT NULL CONSTRAINT DF_mdec_civil_sync_updated_at DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_mdec_civil_sync_retry'
      AND object_id = OBJECT_ID('search.mdec_civil_sync_status')
)
BEGIN
    CREATE INDEX IX_mdec_civil_sync_retry
        ON search.mdec_civil_sync_status(sync_status, next_retry_at)
        INCLUDE (source_document_id, matched_record_id);
END;
GO

IF COL_LENGTH('search.civil_return_pdfs', 'source_system') IS NULL
    ALTER TABLE search.civil_return_pdfs ADD source_system NVARCHAR(50) NULL;
GO

IF COL_LENGTH('search.civil_return_pdfs', 'source_document_id') IS NULL
    ALTER TABLE search.civil_return_pdfs ADD source_document_id NVARCHAR(200) NULL;
GO

IF COL_LENGTH('search.civil_return_pdfs', 'source_submission_at') IS NULL
    ALTER TABLE search.civil_return_pdfs ADD source_submission_at DATETIME2 NULL;
GO

IF COL_LENGTH('search.civil_return_pdfs', 'source_download_url') IS NULL
    ALTER TABLE search.civil_return_pdfs ADD source_download_url NVARCHAR(2000) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UX_civil_return_pdfs_source_document'
      AND object_id = OBJECT_ID('search.civil_return_pdfs')
)
BEGIN
    CREATE UNIQUE INDEX UX_civil_return_pdfs_source_document
        ON search.civil_return_pdfs(source_system, source_document_id)
        WHERE source_system IS NOT NULL AND source_document_id IS NOT NULL;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_civil_return_pdfs_case_intake'
      AND object_id = OBJECT_ID('search.civil_return_pdfs')
)
BEGIN
    CREATE INDEX IX_civil_return_pdfs_case_intake
        ON search.civil_return_pdfs(case_number, intake_date);
END;
GO

IF OBJECT_ID('search.civil_return_pdf_downloads', 'U') IS NULL
BEGIN
    CREATE TABLE search.civil_return_pdf_downloads (
        id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        return_pdf_id INT NOT NULL,
        record_id INT NULL,
        downloaded_by_email NVARCHAR(320) NULL,
        download_route NVARCHAR(100) NULL,
        downloaded_at DATETIME2 NOT NULL CONSTRAINT DF_civil_return_pdf_downloads_downloaded_at DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_civil_return_pdf_downloads_return_pdf'
      AND object_id = OBJECT_ID('search.civil_return_pdf_downloads')
)
BEGIN
    CREATE INDEX IX_civil_return_pdf_downloads_return_pdf
        ON search.civil_return_pdf_downloads(return_pdf_id, downloaded_at DESC);
END;
GO
