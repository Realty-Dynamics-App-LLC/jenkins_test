#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
import shutil
import fiona


# In[2]:


def load_and_concatenate_shp_files(directory):
    gdfs = []  # list to store GeoDataFrames
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                with ZipFile(zip_path, 'r') as zip_ref:
                    extracted_files = zip_ref.namelist()
                    zip_ref.extractall(root)
                    for extracted_file in extracted_files:
                        if extracted_file.endswith(".shp"):
                            try:
                                with fiona.open(os.path.join(root, extracted_file)) as src:
                                    features = [feat for feat in src if feat['geometry'] is not None and len(feat['geometry']['coordinates'][0]) >= 4]
                                    gdf = gpd.GeoDataFrame.from_features(features, crs=src.crs)
                                    gdf = gdf.to_crs("EPSG:4326")  # Transform to a common CRS
                                    gdfs.append(gdf)
                            except Exception as e:
                                print(f"Error loading file {extracted_file}: {e}")
                            finally:
                                # Clean up all extracted files
                                for extracted_file in extracted_files:
                                    if not extracted_file.endswith(".zip"):
                                        try:
                                            os.remove(os.path.join(root, extracted_file))
                                        except Exception as e:
                                            print(f"Error cleaning up file {extracted_file}: {e}")
    return pd.concat(gdfs, ignore_index=True)

# Use the function
gdf = load_and_concatenate_shp_files("data/GIS/2023")

# Save to a new .geojson file
gdf.to_file("data/GIS/florida_2023.geojson", driver='GeoJSON')


# In[ ]:




