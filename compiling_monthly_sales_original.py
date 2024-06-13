#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import openpyxl
import xlrd
import pandas as pd
import json
from dbfread import DBF


# In[2]:


root = 'data/latest_sales_data'

# Initialize an empty dictionary to hold dataframes
dataframes = {}

# Define a function to load a file into a dataframe
def load_file_to_df(path, filename):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        return pd.read_excel(os.path.join(path, filename))
    elif filename.endswith('.csv'):
        return pd.read_csv(os.path.join(path, filename), on_bad_lines='skip')
    elif filename.endswith('.json') or filename.endswith('.geojson'):
        with open(os.path.join(path, filename)) as f:
            data = json.load(f)
        return pd.json_normalize(data)
    elif filename.endswith('.dbf'):
        dbf = DBF(os.path.join(path, filename))
        return pd.DataFrame(iter(dbf))
    else:
        return None

# Walk through each directory
for root, dirs, files in os.walk(root):
    for file in files:
        # Ignore metadata files
        if 'metadata' not in file.lower():
            print(f'Loading {file} from {root}')
            df = load_file_to_df(root, file)
            if df is not None:
                # Use the directory name as the key
                key = os.path.basename(root)
                # If key already exists, append the new dataframe
                if key in dataframes:
                    dataframes[key] = pd.concat([dataframes[key], df])
                else:
                    dataframes[key] = df


# In[3]:


for key in dataframes.keys():
    print(key)
    dataframes[key].info(show_counts=True)


# In[4]:


root = 'data/latest_sales_data'

# Initialize an empty dictionary to hold metadata dataframes
metadata_dfs = {}

# Define a function to load a file into a dataframe
def load_metadata_file_to_df(path, filename):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        return pd.read_excel(os.path.join(path, filename))
    elif filename.endswith('.csv'):
        return pd.read_csv(os.path.join(path, filename))
    elif filename.endswith('.json') or filename.endswith('.geojson'):
        with open(os.path.join(path, filename)) as f:
            data = json.load(f)
        return pd.json_normalize(data)
    elif filename.endswith('.dbf'):
        dbf = DBF(os.path.join(path, filename))
        return pd.DataFrame(iter(dbf))
    else:
        return None

# Walk through each directory
for root, dirs, files in os.walk('.'):
    for file in files:
        # Only load metadata files
        if 'metadata' in file.lower():
            df = load_metadata_file_to_df(root, file)
            if df is not None:
                # Use the directory name as the key
                key = os.path.basename(root)
                # If key already exists, append the new dataframe
                if key in metadata_dfs:
                    metadata_dfs[key] = pd.concat([metadata_dfs[key], df])
                else:
                    metadata_dfs[key] = df


# In[5]:


for key in metadata_dfs.keys():
    print(key)
    metadata_dfs[key].info(show_counts=True)


# In[6]:


# Iterate over each key in the dataframes dictionary
for key in dataframes.keys():
    # Get the current dataframe and its corresponding metadata dataframe
    df = dataframes[key]
    metadata_df = metadata_dfs[key]

    # Create a dictionary mapping original column names to effective column names
    # Use strip to remove leading and trailing spaces
    rename_dict = pd.Series(metadata_df['Effective Column Name'].str.strip().values, index=metadata_df['Original Column Name'].str.strip()).to_dict()

    # Select only the columns that are in the rename_dict keys and rename them
    try:
        dataframes[key] = df[rename_dict.keys()].rename(columns=rename_dict)
    except KeyError as e:
        print(f"Error in {key} metadata: {str(e)}. Please check the 'Original Column Name' in the metadata file.")


# In[7]:


# Concatenate all dataframes in the dictionary
combined_df = pd.concat(dataframes.values(), ignore_index=True)


# In[8]:


combined_df.info(show_counts=True)


# In[9]:


combined_df.to_csv('data/compiled_latest_sales_data.csv', index=False)


# In[ ]:




