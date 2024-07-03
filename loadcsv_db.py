from sqlalchemy import create_engine, text
import psycopg2
import csv

# Replace placeholders with your actual credentials and file paths
connection_string = "postgresql+psycopg2://postgres:1234@20.84.118.206:5432/postgres"
csv_path_parcelid = "/app/parcel_id_to_pin_conversion_table.csv"
csv_path_sales = "/app/latest_sales_data.csv"

# Using SQLAlchemy
def load_data_with_sqlalchemy():
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        # Drop tables if they exist
        connection.execute(text("DROP TABLE IF EXISTS parcelidtopin"))
        connection.execute(text("DROP TABLE IF EXISTS latest_sales_data"))

        # Create tables
        create_parcelidtopin = text("""
        CREATE TABLE IF NOT EXISTS parcelidtopin (
            parcel_id TEXT,
            pin TEXT
        );
        """)
        connection.execute(create_parcelidtopin)

        create_sales_data = text("""
        CREATE TABLE IF NOT EXISTS latest_sales_data (
            "ParcelID" TEXT,
            Sale_Date TEXT,
            Sale_Price TEXT,
            "PIN" TEXT,
            Alt_Key TEXT
        );
        """)
        connection.execute(create_sales_data)

        # Load CSV data
        load_parcelid = text(f"""
        COPY parcelidtopin FROM '{csv_path_parcelid}' DELIMITER ',' CSV HEADER;
        """)
        connection.execute(load_parcelid)

        load_sales = text(f"""
        COPY latest_sales_data FROM '{csv_path_sales}' DELIMITER ',' CSV HEADER;
        """)
        connection.execute(load_sales)

    print("CSV data loaded successfully with SQLAlchemy!")

# Using psycopg2
def load_data_with_psycopg2():
    connection_string = "dbname='postgres' user='postgres' password='1234' host='localhost' port='5432'"
    csv_path_parcelid = "/app/parcel_id_to_pin_conversion_table.csv"
    csv_path_sales = "/app/latest_sales_data.csv"

    try:
        # Connect to the database
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

        # Drop tables if they exist
        cursor.execute("DROP TABLE IF EXISTS parcelidtopin")
        cursor.execute("DROP TABLE IF EXISTS latest_sales_data")

        # Create tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS parcelidtopin (
            parcel_id TEXT,
            pin TEXT
        );
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS latest_sales_data (
            "ParcelID" TEXT,
            Sale_Date TEXT,
            Sale_Price TEXT,
            "PIN" TEXT,
            Alt_Key TEXT
        );
        """)

        # Load CSV data
        with open(csv_path_parcelid, 'r') as f:
            next(f)  # Skip the header row
            cursor.copy_expert("COPY parcelidtopin FROM STDIN WITH CSV HEADER DELIMITER ','", f)

        with open(csv_path_sales, 'r') as f:
            next(f)  # Skip the header row
            cursor.copy_expert("COPY latest_sales_data FROM STDIN WITH CSV HEADER DELIMITER ','", f)

        # Commit the changes
        connection.commit()

        print("CSV data loaded successfully with psycopg2!")
    except Exception as e:
        print(f"An error occurred during data processing: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

# Uncomment one of the following lines to choose which method to use:
# load_data_with_sqlalchemy()
# load_data_with_psycopg2()
