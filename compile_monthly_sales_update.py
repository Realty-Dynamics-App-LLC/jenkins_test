#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import psycopg2
import json


# Use the following block of code when checking schema for a batch of geojson files in a directory.

# In[2]:


# We'll first check the schema of each file

import os
import fiona
from shapely.geometry import shape
import geopandas as gpd
from dask import delayed, compute
import dask.dataframe as dd

directory_path = 'data/sales_gis/monthly_updates'

# Function to read first n lines of a GeoJSON file into a GeoDataFrame
def read_n_features(filepath, n):
    features = []
    with fiona.open(filepath) as src:
        for i, feature in enumerate(src):
            if i >= n:
                break
            features.append(feature)
    
    # Convert to GeoDataFrame
    return gpd.GeoDataFrame.from_features(features)

geojson_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.geojson')]

# Load GeoJSON files
gdfs = [read_n_features(f, 5) for f in geojson_files]


# In[2]:


dbname = "florida_database"
user = "postgres"
password = "team_password"
host = "40.114.30.220"
port = "5432"
county_name = "Hillsborough"
sql_query = f"""
SELECT pin, parcelno, ST_AsGeoJSON(wkb_geometry) AS geom
FROM florida_latest_xl_table WHERE county_name='{county_name}';
"""

def fetch_spatial_data(dbname, user, password, host, port, sql_query):
    try:
        # Connect to your database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        # Create a cursor object
        cur = conn.cursor()
        
        # Execute the query
        cur.execute(sql_query)
        
        # Fetch the results
        rows = cur.fetchall()
        
        # Process the results
        spatial_data = []
        for row in rows:
            # Convert the GeoJSON string into a Python dictionary
            geom_json = json.loads(row[2])
            spatial_data.append({
                "PIN": row[0],
                "Parcel Number": row[1],
                "geom_json": geom_json
            })
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        
        return spatial_data

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
parcel_geometry =  fetch_spatial_data(dbname, user, password, host, port, sql_query)

gdf = gpd.GeoDataFrame(parcel_geometry)

gdf["geometry"] = gdf.loc[:,"geom_json"].apply(lambda x: shape(x))

gdf.set_geometry("geometry")
gdf.crs = gdf.loc[0,"geom_json"]["crs"]["properties"]["name"]


# For one-off updates, we'll use a more custom approach as shown below.

# ### Hillsborough

# In[2]:


gdf = gpd.read_file('data/GIS/2023/hillsborough_2023pin.zip')


# In[3]:


# Function to transform the PIN based on county rules
def transform_pin(parcel_id, county, county_code):
    if county in ['Bradford', 'Desoto', 'Jackson'] or county_code in [14, 24, 42]:
        return parcel_id.lstrip('R')
    elif county == 'Duval' or county_code == 26:
        return parcel_id.rstrip('R')
    elif county in ['Brevard', 'Escambia'] or county_code in [15, 27]:
        return parcel_id  # No transformation
    elif county == 'Indian River' or county_code == 41:
        return parcel_id.replace('.', '/')
    elif county == 'Monroe' or county_code == 54:
        return parcel_id[8:]
    elif county == 'Orange' or county_code == 58:
        parts = [parcel_id[:2], parcel_id[2:4], parcel_id[4:6], parcel_id[6:10], parcel_id[10:12], parcel_id[12:]]
        return '-'.join(parts)
    elif county == 'Hillsborough' or county_code == 39:
        try:
            transformed_string = (parcel_id[-1] + "-" +
                                  parcel_id[4:6] + "-" +
                                  parcel_id[2:4] + "-" +
                                  parcel_id[0:2] + "-" +
                                  parcel_id[6:9] + "-" +
                                  parcel_id[9:15] + "-" +
                                  parcel_id[15:20] + "." +
                                  parcel_id[20])
            return transformed_string
        except:
            pass
    elif county == 'Glades' or county_code == 32:
        return f"{parcel_id[:3]}-{parcel_id[3:5]}-{parcel_id[5:7]}-{parcel_id[7:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Gulf' or county_code == 33:
        return f"{parcel_id[:5]}-{parcel_id[5:]}"
    elif county == 'Hardee' or county_code == 35:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:15]}-{parcel_id[15:]}"
    elif county == 'Jackson' or county_code == 42:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:13]}-{parcel_id[13:]}"
    elif county == 'Madison' or county_code == 50:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:13]}-{parcel_id[13:]}"
    elif county == 'Monroe' or county_code == 54:
        return f"{parcel_id[:8]}-{parcel_id[8:]}"
    elif county == 'Pinellas' or county_code == 62:
        return f"{parcel_id[6:8]}-{parcel_id[3:5]}-{parcel_id[:2]}-{parcel_id[9:14]}-{parcel_id[15:18]}-{parcel_id[19:]}"
    elif county == 'Walton' or county_code == 76:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:11]}-{parcel_id[11:14]}-{parcel_id[14:]}"
    elif county == 'Washington' or county_code == 77:
        return f"{parcel_id[:8]}-{parcel_id[8:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Calhoun' or county_code == 17:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Dixie' or county_code == 25:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Hamilton' or county_code == 34:
        return f"{parcel_id[:4]}-{parcel_id[4:7]}"    
    else:
        return parcel_id  # Default case if county not listed

# Apply the transformation
gdf['PIN'] = gdf.apply(lambda row: transform_pin(row['PARCELNO'], 'Hillsborough', 39), axis=1)


# In[4]:


cutoff_date = pd.to_datetime('2023-08-01')


# In[5]:


hillsborough_latest = gpd.read_file('data/sales_gis/monthly_updates/hillsborough/allsales_02_23_2024.zip')

hillsborough_latest.columns


# In[6]:


hillsborough_latest.rename(columns = {'S_DATE':'SALE DATE', 'S_AMT': 'SALE AMOUNT'}, inplace = True)

hillsborough_latest['SALE DATE'] = pd.to_datetime(hillsborough_latest['SALE DATE'])

hillsborough_latest = hillsborough_latest[hillsborough_latest['SALE DATE'] >= cutoff_date][['PIN', 'FOLIO', 'SALE DATE', 'SALE AMOUNT', 'GRANTOR', 'GRANTEE']]

hillsborough_latest.reset_index(drop = True, inplace = True)


# In[7]:


hillsborough_latest = hillsborough_latest.merge(gdf, on = 'PIN', how = 'left')


# In[8]:


hillsborough_latest = gpd.GeoDataFrame(hillsborough_latest, geometry = 'geometry')
hillsborough_latest.crs = gdf.crs


# In[9]:


hillsborough_latest.to_file('data/sales_gis/monthly_updates/latest_sales/hillsborough_latest_sales.geojson')


# In[2]:


latest_owners = gpd.read_file('data/sales_gis/monthly_updates/hillsborough/parcel_02_23_2024.zip', include_fields = ['S_DATE', 'PIN', 'OWNER', 'ADDR_1', 'ADDR_2', 'CITY', 'STATE', 'ZIP'])
latest_owners.dropna(subset = 'PIN', inplace = True)
latest_owners.dropna(subset = 'S_DATE', inplace = True)
latest_owners.sort_values(by = 'S_DATE', ascending = False, inplace = True)
latest_owners.drop(columns=['S_DATE', 'geometry'], inplace = True)
latest_owners.drop_duplicates(subset='PIN', keep='first')
latest_owners.reset_index(drop = True, inplace = True)
latest_owners.info(show_counts = True)


# In[3]:


def update_owner_names(dbname, user, password, host, port, dataframe):
    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.autocommit = True  # Ensure changes are immediately committed
        
        # Create a cursor object
        cur = conn.cursor()

        # SQL statement to update owner names based on PIN
        update_sql = """
        UPDATE florida_parcels_latest
        SET own_name = %s,
            own_addr1 = %s,
            own_addr2 = %s,
            own_city = %s,
            own_state = %s,
            own_zipcd = %s
        WHERE pin = %s;
        """
        
        counter = 0
        # Iterate through the DataFrame rows
        for index, row in dataframe.iterrows():
            try:
                if counter%1000==0:
                    print(counter)
                pin = row['PIN']
                own_name = row['OWNER']
                own_addr1 = row['ADDR_1']
                own_addr2 = row['ADDR_2']
                own_city = row['CITY']
                own_state = row['STATE']
                own_zipcd = row['ZIP'][:5]
            
                # Execute the update query with the current PIN and OWNER
                cur.execute(update_sql, (own_name, own_addr1, own_addr2, own_city, own_state, own_zipcd, pin))
                counter+=1
            except:
                pass
        
        # Close the cursor and the connection
        cur.close()
        conn.close()
        
        print("Owner names updated successfully.")
        
    except Exception as e:
        print(f"An error occurred: {e}")


# In[4]:


dbname = "florida_database"
user = "postgres"
password = "team_password"
host = "40.114.30.220"
port = "5432"

update_owner_names(dbname, user, password, host, port, latest_owners[368_000:])


# ### Polk

# In[2]:


gdf = gpd.read_file('data/GIS/2023/polk_2023pin.zip')
polk_latest = pd.read_csv('data/sales_gis/monthly_updates/polk/ftp_sales.txt')


# In[3]:


# Function to transform the PIN based on county rules
def transform_pin(parcel_id, county, county_code):
    if county in ['Bradford', 'Desoto', 'Jackson'] or county_code in [14, 24, 42]:
        return parcel_id.lstrip('R')
    elif county == 'Duval' or county_code == 26:
        return parcel_id.rstrip('R')
    elif county in ['Brevard', 'Escambia'] or county_code in [15, 27]:
        return parcel_id  # No transformation
    elif county == 'Indian River' or county_code == 41:
        return parcel_id.replace('.', '/')
    elif county == 'Monroe' or county_code == 54:
        return parcel_id[8:]
    elif county == 'Orange' or county_code == 58:
        parts = [parcel_id[:2], parcel_id[2:4], parcel_id[4:6], parcel_id[6:10], parcel_id[10:12], parcel_id[12:]]
        return '-'.join(parts)
    elif county == 'Hillsborough' or county_code == 39:
        try:
            transformed_string = (parcel_id[-1] + "-" +
                                  parcel_id[4:6] + "-" +
                                  parcel_id[2:4] + "-" +
                                  parcel_id[0:2] + "-" +
                                  parcel_id[6:9] + "-" +
                                  parcel_id[9:15] + "-" +
                                  parcel_id[15:20] + "." +
                                  parcel_id[20])
            return transformed_string
        except:
            pass
    elif county == 'Glades' or county_code == 32:
        return f"{parcel_id[:3]}-{parcel_id[3:5]}-{parcel_id[5:7]}-{parcel_id[7:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Gulf' or county_code == 33:
        return f"{parcel_id[:5]}-{parcel_id[5:]}"
    elif county == 'Hardee' or county_code == 35:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:15]}-{parcel_id[15:]}"
    elif county == 'Jackson' or county_code == 42:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:13]}-{parcel_id[13:]}"
    elif county == 'Madison' or county_code == 50:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:13]}-{parcel_id[13:]}"
    elif county == 'Monroe' or county_code == 54:
        return f"{parcel_id[:8]}-{parcel_id[8:]}"
    elif county == 'Pinellas' or county_code == 62:
        return f"{parcel_id[6:8]}-{parcel_id[3:5]}-{parcel_id[:2]}-{parcel_id[9:14]}-{parcel_id[15:18]}-{parcel_id[19:]}"
    elif county == 'Walton' or county_code == 76:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:11]}-{parcel_id[11:14]}-{parcel_id[14:]}"
    elif county == 'Washington' or county_code == 77:
        return f"{parcel_id[:8]}-{parcel_id[8:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Calhoun' or county_code == 17:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Dixie' or county_code == 25:
        return f"{parcel_id[:2]}-{parcel_id[2:4]}-{parcel_id[4:6]}-{parcel_id[6:10]}-{parcel_id[10:14]}-{parcel_id[14:]}"
    elif county == 'Hamilton' or county_code == 34:
        return f"{parcel_id[:4]}-{parcel_id[4:7]}"    
    else:
        return str(parcel_id)  # Default case if county not listed


# In[4]:


# Apply the transformation
gdf['PIN'] = gdf.apply(lambda row: transform_pin(row['PARCELNO'], 'Polk', 63), axis=1)

polk_latest['PIN'] = polk_latest.apply(lambda row: transform_pin(row['PARCEL_ID'], 'Polk', 63), axis=1)


# In[5]:


polk_latest.columns


# In[6]:


cutoff_date = pd.to_datetime('2023-08-01')


# In[7]:


polk_latest.rename(columns = {'SALEDT':'SALE DATE', 'PRICE': 'SALE AMOUNT'}, inplace = True)

polk_latest['SALE DATE'] = pd.to_datetime(polk_latest['SALE DATE'])

polk_latest = polk_latest[polk_latest['SALE DATE'] >= cutoff_date][['PIN', 'SALE DATE', 'SALE AMOUNT', 'GRANTOR', 'GRANTEE']]

polk_latest.reset_index(drop = True, inplace = True)


# In[11]:


polk_latest = polk_latest.merge(gdf, on = 'PIN', how = 'left')

polk_latest = gpd.GeoDataFrame(polk_latest, geometry = 'geometry')
polk_latest.crs = gdf.crs

polk_latest.to_file('data/sales_gis/monthly_updates/latest_sales/polk_latest_sales.geojson')


# In[14]:


latest_owners = pd.read_csv('data/sales_gis/monthly_updates/polk/ftp_parcel.txt', include_fields = ['S_DATE', 'PIN', 'OWNER', 'ADDR_1', 'ADDR_2', 'CITY', 'STATE', 'ZIP'])
latest_owners.dropna(subset = 'PIN', inplace = True)
latest_owners.dropna(subset = 'S_DATE', inplace = True)
latest_owners.sort_values(by = 'S_DATE', ascending = False, inplace = True)
latest_owners.drop(columns=['S_DATE', 'geometry'], inplace = True)
latest_owners.drop_duplicates(subset='PIN', keep='first')
latest_owners.reset_index(drop = True, inplace = True)
latest_owners.info(show_counts = True)


# In[ ]:




