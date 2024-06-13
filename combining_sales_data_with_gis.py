#!/usr/bin/env python
# coding: utf-8

# In[1]:


import geopandas as gpd
import pandas as pd


# In[3]:


conversion_table = pd.read_csv('/datadrive/parcel_id_to_pin_conversion_table.csv')


# In[4]:


conversion_table.info(show_counts=True)


# In[ ]:


gis_data = gpd.read_file('/datadrive/florida_2023.geojson')


# In[ ]:


gis_data.info(show_counts=True)


# In[ ]:


gis_data = gis_data.merge(conversion_table, left_on='PARCELNO', right_on='parcel_id')


# In[ ]:


gis_data.info(show_counts=True)
gis_data.to_file('/datadrive/florida_2023_with_pin.geojson', driver='GeoJSON') 


# In[2]:


gis_data = gpd.read_file('/datadrive/florida_2023_with_pin.geojson')
latest_sales_data = pd.read_csv('/datadrive/latest_sales_data.csv')


# In[5]:


latest_sales_data.info(show_counts=True)


# In[6]:


# First, merge on 'PIN' and 'pin'
merged_data = gis_data.merge(latest_sales_data, left_on='pin', right_on='PIN', how='left')

merged_data.info(show_counts=True)


# In[7]:


# Then, for the rows that didn't find a match, merge on 'ParcelID' and 'parcel_id'
no_pin_match = merged_data[merged_data['PIN'].isna()]
match_on_parcel_id = no_pin_match.merge(latest_sales_data, left_on='parcel_id', right_on='ParcelID', how='left')

match_on_parcel_id.info(show_counts=True)


# In[8]:


# Combine the two dataframes
final_merged_data = pd.concat([merged_data[~merged_data['PIN'].isna()], match_on_parcel_id])
final_merged_data.info(show_counts=True)


# In[9]:


final_merged_data.drop(columns=['ParcelID', 'ParcelID_x', 'Sale Date_x', 'Sale Price_x', 'PIN_x', 'PIN_y'], inplace=True)
final_merged_data['Sale Price'] = final_merged_data['Sale Price'].fillna(final_merged_data['Sale Price_y'])
final_merged_data['Sale Date'] = final_merged_data['Sale Date'].fillna(final_merged_data['Sale Date_y'])
final_merged_data = final_merged_data.drop_duplicates(subset=['parcel_id', 'pin', 'Sale Date', 'Sale Price'], keep='first')
final_merged_data.dropna(subset=['Sale Price'], inplace=True)
final_merged_data = final_merged_data[['parcel_id', 'pin', 'Sale Date', 'Sale Price', 'geometry']]
final_merged_data.reset_index(drop=True, inplace=True)
final_merged_data.to_file('/datadrive/final_sales_gis_file.geojson', driver='GeoJSON')


# In[10]:


final_merged_data.info(show_counts=True)


# In[ ]:




