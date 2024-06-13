#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlalchemy
import geoalchemy2

print(sqlalchemy.__version__) # 1.4.51
print(geoalchemy2.__version__) # 0.9.2
print("python=3.8")


# In[2]:


from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, Float, select, update, delete, text
import sqlalchemy
from geoalchemy2 import Geometry

print(sqlalchemy.__version__)
# Define the connection string components
username = "postgres"
password = "1234"
host = "172.208.27.131"
port = "5432"
database = "postgres"

# Construct the connection string using an f-string
connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)


# In[ ]:


# Function to check if an index exists
def index_exists(table_name, index_name):
    index_exists_query = f"""
    SELECT 1 
    FROM pg_indexes 
    WHERE tablename = '{table_name}' 
    AND indexname = '{index_name}';
    """
    result = connection.execute(index_exists_query).fetchone()
    return result is not None


# In[ ]:


with engine.connect() as connection:
    try:
        # Check and create index idx_latest_sales_data on latest_sales_data("PIN")
        if not index_exists('latest_sales_data', 'idx_latest_sales_data'):
            connection.execute('CREATE INDEX idx_latest_sales_data ON latest_sales_data("PIN");')
            print("CREATED idx_latest_sales_data index")
        else:
            print("Index idx_latest_sales_data already exists, skipping creation")
        
        # Check and create index idx_pin_table on parcelidtopin(pin)
        if not index_exists('parcelidtopin', 'idx_pin_table'):
            connection.execute('CREATE INDEX idx_pin_table ON parcelidtopin(pin);')
            print("CREATED idx_pin_table index")
        else:
            print("Index idx_pin_table already exists, skipping creation")
    
    except Exception as e:
        print(f"An error occurred during data processing: {e}")
        


# In[3]:


with engine.connect() as connection:
    try:
        # Create temporary tables
        connection.execute("""
            CREATE TABLE IF NOT EXISTS fl_geojson_parcel_pin AS
            SELECT f.*, p.parcel_id, p.pin
            FROM fl_geojson f
            LEFT JOIN parcelidtopin p ON f.parcelno = p.parcel_id
        """)
        
        print("CREATED fl_geojson_parcel_pin table")
        
        # Check and create index idx_fl_geojson_pin on fl_geojson_parcel_pin(pin)
        if not index_exists('fl_geojson_parcel_pin', 'idx_fl_geojson_pin'):
            connection.execute('CREATE INDEX idx_fl_geojson_pin ON fl_geojson_parcel_pin(pin);')
            print("CREATED idx_fl_geojson_pin index")
        else:
            print("Index idx_fl_geojson_pin already exists, skipping creation")


    except Exception as e:
        print(f"An error occurred during data processing: {e}")



# In[5]:


with engine.connect() as connection:
    try:
        # Check if the table final_sales exists before creating it
        table_exists_query = """
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_name = 'final_sales';
        """
        result = connection.execute(table_exists_query).fetchone()
        
        if not result:
            connection.execute("""
                CREATE TABLE final_sales AS
                SELECT f.*, l.sale_price, l.sale_date, l.alt_key, l."ParcelID", l."PIN"
                FROM fl_geojson_parcel_pin f
                LEFT JOIN latest_sales_data l ON f.pin = l."PIN"
            """)
            print("CREATED final_sales table")
        else:
            print("Table final_sales already exists, skipping creation")

        # Check if the index idx_geom_final_sales exists before creating it
        index_exists_query = """
        SELECT 1 
        FROM pg_indexes 
        WHERE tablename = 'final_sales' 
        AND indexname = 'idx_geom_final_sales';
        """
        result = connection.execute(index_exists_query).fetchone()
        
        if not result:
            connection.execute('CREATE INDEX idx_geom_final_sales ON final_sales USING SPGIST(wkb_geometry);')
            print("CREATED idx_geom_final_sales index")
        else:
            print("Index idx_geom_final_sales already exists, skipping creation")
            
    except Exception as e:
        print(f"An error occurred during data processing: {e}")


# In[6]:


with engine.connect() as connection:
    try:
        # Update final_ table with matching data from latest_sales_data
        connection.execute("""
        UPDATE final_sales
        SET
            sale_price = l.sale_price,
            sale_date = l.sale_date,
            alt_key = l.alt_key,
            "ParcelID" = l."ParcelID",
            "PIN" = l."PIN"
        FROM latest_sales_data l 
        WHERE final_sales.parcel_id = l."ParcelID" AND final_sales."PIN" IS NULL;
        """)
        
        # Delete rows from final_ with missing sale_price and capture the number of rows deleted
        delete_result = connection.execute("DELETE FROM final_sales WHERE sale_price IS NULL;")
        rows_deleted = delete_result.rowcount
        print(f"Deleted {rows_deleted} rows from final_sales table where sale_price is NULL.")

        # Drop the pin column from final_ table
        connection.execute(text("ALTER TABLE final_sales DROP COLUMN pin"))
        print("Dropped pin column from final_sales table.")

        # Create a spatial index on the geometry column (assuming SPGIST extension is available)
        if engine.dialect.name == 'postgresql':
            connection.execute(text("CREATE INDEX idx_final_geom ON final_sales USING SPGIST (wkb_geometry)"))
            print("CREATED idx_final_geom index on wkb_geometry column.")
    except Exception as e:
        print(f"An error occurred during data processing: {e}")

print("Data merged and index created successfully!")


# In[ ]:





# In[ ]:




