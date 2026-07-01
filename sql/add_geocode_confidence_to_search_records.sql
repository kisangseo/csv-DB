IF COL_LENGTH('search.records', 'geocode_confidence') IS NULL
BEGIN
    ALTER TABLE search.records ADD geocode_confidence FLOAT NULL;
END
