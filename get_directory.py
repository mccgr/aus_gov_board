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
xml_dataframe = xml2df.process_data()

xml_dataframe.to_sql('directory', engine, schema="aus_gov_board",
                     if_exists="replace", index=False)
                     
# Do some database-related clean-up
engine.execute(
    """
    SET search_path TO aus_gov_board;
    
    ALTER TABLE directory OWNER TO aus_gov_board;

    GRANT SELECT ON directory TO aus_gov_board_access;
    """)
