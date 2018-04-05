#!/usr/bin/env python3 
from sqlalchemy import create_engine

import os
dbname = os.getenv("PGDATABASE")
host = os.getenv("PGHOST", "localhost")
engine = create_engine("postgresql://" + host + "/" + dbname)

# Get government directory XML
import requests
user_agent_url = 'https://www.directory.gov.au/sites/default/files/export.xml'
xml_data = requests.get(user_agent_url).content

# Parse XML into data frame
import xml.etree.ElementTree as ET
import pandas as pd

class XML2DataFrame:

    def __init__(self, xml_data):
        self.root = ET.XML(xml_data)

    def parse_root(self, root):
        return [self.parse_element(child) for child in iter(root)]

    def parse_element(self, element, parsed=None):
        if parsed is None:
            parsed = dict()
        for key in element.keys():
            parsed[key] = element.attrib.get(key)
        if element.text:
            parsed[element.tag] = element.text
        for child in list(element):
            self.parse_element(child, parsed)
        return parsed

    def process_data(self):
        structure_data = self.parse_root(self.root)
        return pd.DataFrame(structure_data)

xml2df = XML2DataFrame(xml_data)

df = xml2df.process_data()

# Rearrange columns
import numpy as np

# Arrange the columns so that those with the least missing values appear on the left.
indices = np.argsort(df.isnull().sum())
df = df.iloc[:, indices]

# Arrange the first few columns so that we have, in order, 
# unique_record_id, content_id, updated, and type as
# the first four columns. 
df = df.iloc[:, [1, 3, 0, 2] + list(range(4, len(df.columns)))]

# Fix date columns

# Let's start to put the columns which have dates into datetime format
# Get rid of the "Determined by the appointer" entries in end_date by 
# setting these entries to None
df.loc[df['end_date'] == "Determined by the appointer", "end_date"] = None

# For the other date columns, start_date, end_date and creation_date, 
# let's convert to datetime format in the utc timezone, 
# assuming the datetimes are currently in Sydney time.
from pytz import timezone, utc

def local_to_utc(x):
    x.replace(tzinfo = timezone('Australia/Sydney')).astimezone(utc)
    return(x)

df['updated'] = pd.to_datetime(df['updated']).apply(local_to_utc)
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])
df['creation_date'] = pd.to_datetime(df['creation_date'])                 

# Now consider all the columns that should be in integer format
def is_integer(input):
    try:
        num = int(input)
    except ValueError:
        return False
    except TypeError:
        return False
    return True

def col_is_integer(col):
    return sum(not is_integer(x) for x in col.value_counts().index) == 0

int_col_names = [colname for colname in df.columns 
                     if(col_is_integer(df[colname]))]

df[int_col_names] = df[int_col_names].apply(pd.to_numeric)

# Write data to database

# This is rather slow!
df.to_sql('directory', engine, schema="aus_gov_board",
                     if_exists="replace", index=False)                    

# Do some database-related clean-up
engine.execute("""
    SET search_path TO aus_gov_board;
    ALTER TABLE directory OWNER TO aus_gov_board;
    GRANT SELECT ON directory TO aus_gov_board_access;
""")
    
# Convert the float columns to integer
for col in int_col_names:
    engine.execute("""
        ALTER TABLE aus_gov_board.directory
        ALTER COLUMN %s TYPE integer;
    """ % col)

engine.dispose()