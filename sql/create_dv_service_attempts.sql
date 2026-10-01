IF OBJECT_ID('search.dv_service_attempts', 'U') IS NULL
BEGIN
    CREATE TABLE search.dv_service_attempts (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        dv_pdf_record_id INT NULL,
        normalized_case_number NVARCHAR(128) NOT NULL,
        case_number NVARCHAR(128) NOT NULL,
        order_type NVARCHAR(255) NULL,
        respondent_name NVARCHAR(500) NULL,
        reporting_member NVARCHAR(500) NULL,
        reporting_member_email NVARCHAR(320) NULL,
        address_attempted NVARCHAR(1000) NULL,
        attempt_disposition NVARCHAR(255) NULL,
        arrival_at DATETIME2 NULL,
        clear_at DATETIME2 NULL,
        details_json NVARCHAR(MAX) NOT NULL,
        mailbox NVARCHAR(320) NULL,
        dedupe_key CHAR(64) NOT NULL,
        message_id NVARCHAR(1000) NOT NULL,
        attachment_id NVARCHAR(1000) NOT NULL,
        original_filename NVARCHAR(500) NULL,
        blob_name NVARCHAR(1000) NOT NULL,
        email_received_at DATETIME2 NULL,
        created_at DATETIME2 NOT NULL
            CONSTRAINT DF_dv_service_attempts_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_dv_service_attempts_dedupe_key UNIQUE (dedupe_key)
    );

    CREATE INDEX IX_dv_service_attempts_case
        ON search.dv_service_attempts(normalized_case_number);
    CREATE INDEX IX_dv_service_attempts_record
        ON search.dv_service_attempts(dv_pdf_record_id);
END;
