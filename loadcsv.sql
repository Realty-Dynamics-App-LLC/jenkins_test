-- psql -U postgres -d postgres -h 20.84.118.206 -p 5432 -f loadcsv.sql
-- enter your postgres password


CREATE OR REPLACE FUNCTION load_csv_data() RETURNS void AS $$
BEGIN

    DROP TABLE IF EXISTS parcelidtopin;
    DROP TABLE IF EXISTS latest_sales_data;
    -- Create tables for CSV data if they don't exist
    EXECUTE '
    CREATE TABLE IF NOT EXISTS parcelidtopin (
        parcel_id TEXT,
        pin TEXT
    )';

    EXECUTE '
    CREATE TABLE IF NOT EXISTS latest_sales_data (
        "ParcelID" TEXT,
        Sale_Date TEXT,
        Sale_Price TEXT,
        "PIN" TEXT,
        Alt_Key TEXT
    )';

    -- Load CSV data into the tables
    EXECUTE '
    COPY parcelidtopin FROM ''/home/azureuser/internship_2024/data/parcel_id_to_pin_conversion_table.csv'' DELIMITER '','' CSV HEADER';

    EXECUTE '
    COPY latest_sales_data FROM ''/home/azureuser/internship_2024/data/latest_sales_data.csv'' DELIMITER '','' CSV HEADER';
END;
$$ LANGUAGE plpgsql;

SELECT load_csv_data();