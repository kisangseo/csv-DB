/* Idempotent schema patch for Survey123 /esri-webhook1 fields */
IF COL_LENGTH('search.records', 're_issue') IS NULL
    ALTER TABLE search.records ADD re_issue NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'request_for_service_type') IS NULL
    ALTER TABLE search.records ADD request_for_service_type NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'court_issued_date') IS NULL
    ALTER TABLE search.records ADD court_issued_date DATETIME NULL;
IF COL_LENGTH('search.records', 'trial_date') IS NULL
    ALTER TABLE search.records ADD trial_date DATETIME NULL;
IF COL_LENGTH('search.records', 'service_days') IS NULL
    ALTER TABLE search.records ADD service_days INT NULL;
IF COL_LENGTH('search.records', 'expiration_date') IS NULL
    ALTER TABLE search.records ADD expiration_date DATETIME NULL;
IF COL_LENGTH('search.records', 'check_or_money_order_number') IS NULL
    ALTER TABLE search.records ADD check_or_money_order_number NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'payment_amount') IS NULL
    ALTER TABLE search.records ADD payment_amount DECIMAL(18,2) NULL;
IF COL_LENGTH('search.records', 'tenant_defendant_or_respondent') IS NULL
    ALTER TABLE search.records ADD tenant_defendant_or_respondent NVARCHAR(500) NULL;
IF COL_LENGTH('search.records', 'tenant_defendant_or_respondent_address') IS NULL
    ALTER TABLE search.records ADD tenant_defendant_or_respondent_address NVARCHAR(500) NULL;
IF COL_LENGTH('search.records', 'apartment_unit_or_secondary_address') IS NULL
    ALTER TABLE search.records ADD apartment_unit_or_secondary_address NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'area_number') IS NULL
    ALTER TABLE search.records ADD area_number NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'post_number') IS NULL
    ALTER TABLE search.records ADD post_number NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'petitioner_or_plaintiff_name') IS NULL
    ALTER TABLE search.records ADD petitioner_or_plaintiff_name NVARCHAR(500) NULL;
IF COL_LENGTH('search.records', 'petitioner_address') IS NULL
    ALTER TABLE search.records ADD petitioner_address NVARCHAR(500) NULL;
IF COL_LENGTH('search.records', 'administrative_status') IS NULL
    ALTER TABLE search.records ADD administrative_status NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'service_method') IS NULL
    ALTER TABLE search.records ADD service_method NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'scheduled_date') IS NULL
    ALTER TABLE search.records ADD scheduled_date DATETIME NULL;
IF COL_LENGTH('search.records', 'unable_to_serve_reason') IS NULL
    ALTER TABLE search.records ADD unable_to_serve_reason NVARCHAR(500) NULL;
IF COL_LENGTH('search.records', 'relationship') IS NULL
    ALTER TABLE search.records ADD relationship NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'age') IS NULL
    ALTER TABLE search.records ADD age NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'race') IS NULL
    ALTER TABLE search.records ADD race NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'sex') IS NULL
    ALTER TABLE search.records ADD sex NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'height') IS NULL
    ALTER TABLE search.records ADD height NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'weight') IS NULL
    ALTER TABLE search.records ADD weight NVARCHAR(100) NULL;
IF COL_LENGTH('search.records', 'served_by') IS NULL
    ALTER TABLE search.records ADD served_by NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'attempt_1') IS NULL
    ALTER TABLE search.records ADD attempt_1 DATETIME NULL;
IF COL_LENGTH('search.records', 'attempt_2') IS NULL
    ALTER TABLE search.records ADD attempt_2 DATETIME NULL;
IF COL_LENGTH('search.records', 'attempt_3') IS NULL
    ALTER TABLE search.records ADD attempt_3 DATETIME NULL;
IF COL_LENGTH('search.records', 'notes') IS NULL
    ALTER TABLE search.records ADD notes NVARCHAR(MAX) NULL;
IF COL_LENGTH('search.records', 'parcel_pin') IS NULL
    ALTER TABLE search.records ADD parcel_pin NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'serving_or_attempting_deputy') IS NULL
    ALTER TABLE search.records ADD serving_or_attempting_deputy NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'assigned_deputy') IS NULL
    ALTER TABLE search.records ADD assigned_deputy NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'due_date') IS NULL
    ALTER TABLE search.records ADD due_date DATETIME NULL;
IF COL_LENGTH('search.records', 'date_time_served') IS NULL
    ALTER TABLE search.records ADD date_time_served DATETIME NULL;
IF COL_LENGTH('search.records', 'globalid') IS NULL
    ALTER TABLE search.records ADD globalid NVARCHAR(255) NULL;
IF COL_LENGTH('search.records', 'objectid') IS NULL
    ALTER TABLE search.records ADD objectid NVARCHAR(255) NULL;
