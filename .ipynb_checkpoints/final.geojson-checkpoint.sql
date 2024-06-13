-- psql -U postgres -d postgres -h 20.84.118.206 -p 5432 -f final.geojson.sql
-- enter your postgres password


CREATE OR REPLACE FUNCTION merge_data() RETURNS void AS $$
BEGIN
    -- Step 1: Create the intermediate result with a full join between fl_geojson and parcelidtopin
    CREATE TABLE IF NOT EXISTS fl_geojson_parcel_pin AS
    SELECT
        f.*,
        p.parcel_id,
        p.pin
    FROM
        fl_geojson f
    LEFT JOIN
        parcelidtopin p ON f.parcelno = p.parcel_id;

     -- Step 2: Create the intermediate result with a full join between temp_merged1 and latest_sales_data
    CREATE TABLE IF NOT EXISTS final_ AS
    SELECT
        tm.*,
        l.*
    FROM
        fl_geojson_parcel_pin tm
    LEFT JOIN
        latest_sales_data l ON  tm.pin = l."PIN";

    -- -- Step 3: Create the merged_fl_geojson table if it doesn't exist
    -- CREATE TABLE IF NOT EXISTS merged_fl_geojson AS
    -- SELECT * FROM temp_merged2 WHERE 1 = 0;  -- Create with the same structure but no data

    -- -- Insert data from temp_merged2
    -- INSERT INTO merged_fl_geojson
    -- SELECT 
    --     tm.*
    -- FROM temp_merged2 tm;

    UPDATE final_
    SET
        sale_price = l.sale_price,
        sale_date = l.sale_date,
        alt_key = l.alt_key,
        "ParcelID" = l."ParcelID",
        "PIN" = l."PIN"
    FROM latest_sales_data l 
    WHERE final_.parcelno = l."ParcelID" AND final_."PIN" IS NULL;
    
    DELETE FROM final_ WHERE sale_price IS NULL;

    ALTER TABLE final_
    DROP COLUMN pin;


END;
$$ LANGUAGE plpgsql;

-- Execute the function

SELECT merge_data();
CREATE INDEX idx_geom ON final_ using SPGIST(wkb_geometry);
--ogr2ogr -f "GeoJSON" merged_fl.geojson PG:"host=20.84.118.206 port=5432 dbname=postgres user=postgres password=1234" "merged_fl_geojson"


