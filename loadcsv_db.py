#!/usr/bin/env python
# coding: utf-8


import psycopg2
import csv

# Replace placeholders with your actual credentials and file paths
connection_string = "dbname='postgres' user='postgres' password='1234' host='localhost' port='5432'"
csv_path_parcelid = "/home/jason/Desktop/internship/parcel_id_to_pin_conversion_table.csv"
csv_path_sales = "/home/jason/Desktop/internship/latest_sales_data.csv"

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

    print("CSV data loaded successfully!")
except Exception as e:
    print(f"An error occurred during data processing: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()


# In[ ]:





# In[ ]:




