#!/usr/bin/env python3 

def is_integer(input):
    try:
        num = int(input)
    except ValueError:
        return False
    except TypeError:
        return False
    return True

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
xml_dataframe = xml2df.process_data()


                     

# Arrange the columns so that those with the least missing values appear on the left.

indices = np.argsort(xml_dataframe.isnull().sum())
xml_dataframe = xml_dataframe.iloc[:, indices]

# Arrange the first few columns so that we have, in order, unique_record_id, content_id, updated, and type as
# the first four columns. 

indices = [1, 3, 0, 2] + [i for i in range(4, 81)]
xml_dataframe2 = xml_dataframe2.iloc[:, indices]
xml_dataframe2.head()
                     
                     
# Let's start to put the columns which have dates into datetime format
# Get rid of the "Determined by the appointer" entries in end_date by setting these entries to None
 
xml_dataframe.loc[np.logical_and(xml_dataframe['end_date'] == "Determined by the appointer", xml_dataframe['end_date'].notnull()), 'end_date'] = None 



# For the other date columns, start_date, end_date and creation_date, let's convert to datetime format in the utc timezone.
import datetime as dt
from dateutil.tz import *
import pytz
 
local = tzlocal()

xml_dataframe['updated'] = pd.to_datetime(xml_dataframe['updated']).apply(lambda x: x.replace(tzinfo = local).astimezone(pytz.utc))
xml_dataframe['start_date'] = pd.to_datetime(xml_dataframe['start_date'])
xml_dataframe['end_date'] = pd.to_datetime(xml_dataframe['end_date'])
xml_dataframe['creation_date'] = pd.to_datetime(xml_dataframe['creation_date'])
                     
                     
                     
# Now consider all the columns that should be in integer format

int_col_names = []

for colname in xml_dataframe.columns:
    if(sum([not is_integer(x) for x in xml_dataframe[colname].value_counts().index]) == 0):
        int_col_names.append(colname)

xml_dataframe[int_col_names] = xml_dataframe[int_col_names].apply(pd.to_numeric)
                     
                     
                     
xml_dataframe.to_sql('directory', engine, schema="aus_gov_board",
                     if_exists="replace", index=False)                    
                     
                     
                     
# Do some database-related clean-up
engine.execute(
    """
    SET search_path TO aus_gov_board;
    
    ALTER TABLE directory OWNER TO aus_gov_board;

    GRANT SELECT ON directory TO aus_gov_board_access;
    """)
    
# Convert the float columns to integer

engine.execute(
    """
    ALTER TABLE directory
    ALTER COLUMN importance TYPE integer
    ALTER COLUMN role_belongs_to TYPE integer
    ALTER COLUMN contact TYPE integer
    ALTER COLUMN parent_organisation TYPE integer
    ALTER COLUMN parent_directory_structure TYPE integer
    ALTER COLUMN portfolio TYPE integer
    ALTER COLUMN max_members TYPE integer
    ALTER COLUMN current_budget_total_expenditure TYPE integer
    ALTER COLUMN asl TYPE integer
    ALTER COLUMN current_budget_total_appropriations TYPE integer
    ALTER COLUMN parent_non_portfolio TYPE integer
    ALTER COLUMN parent_board_non_board TYPE integer;
    """)

